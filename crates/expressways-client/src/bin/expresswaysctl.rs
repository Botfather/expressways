use std::fs;
use std::path::PathBuf;

use anyhow::{Context, bail};
use chrono::{Duration, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use expressways_audit::{load_events, verify_file};
use expressways_auth::CapabilityIssuer;
use expressways_client::{Client, Endpoint};
use expressways_protocol::{
    Action, CapabilityClaims, CapabilityScope, Classification, ControlCommand, ControlRequest,
    RetentionClass, TopicSpec,
};
use uuid::Uuid;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, value_enum, default_value_t = TransportKind::Tcp)]
    transport: TransportKind,
    #[arg(long, default_value = "127.0.0.1:7766")]
    address: String,
    #[arg(long, default_value = "./tmp/expressways.sock")]
    socket: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TransportKind {
    Tcp,
    Unix,
}

#[derive(Debug, Args)]
struct TokenArgs {
    #[arg(long, conflicts_with = "token_file")]
    token: Option<String>,
    #[arg(long)]
    token_file: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum Command {
    GenerateKeypair {
        #[arg(long, default_value = "dev")]
        key_id: String,
        #[arg(long)]
        private_key: PathBuf,
        #[arg(long)]
        public_key: PathBuf,
    },
    IssueToken {
        #[arg(long, default_value = "dev")]
        key_id: String,
        #[arg(long)]
        private_key: PathBuf,
        #[arg(long)]
        principal: String,
        #[arg(long, default_value = "expressways")]
        audience: String,
        #[arg(long, default_value_t = 3600)]
        expires_in_seconds: i64,
        #[arg(long = "scope", value_parser = parse_scope)]
        scopes: Vec<CapabilityScope>,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    AuthState {
        #[command(flatten)]
        token: TokenArgs,
    },
    Metrics {
        #[command(flatten)]
        token: TokenArgs,
    },
    RevokeToken {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        token_id: Uuid,
    },
    RevokePrincipal {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        principal: String,
    },
    RevokeKey {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        key_id: String,
    },
    Health {
        #[command(flatten)]
        token: TokenArgs,
    },
    CreateTopic {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        topic: String,
        #[arg(long, default_value = "operational")]
        retention_class: RetentionClass,
        #[arg(long, default_value = "internal")]
        classification: Classification,
    },
    Publish {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        topic: String,
        #[arg(long)]
        payload: String,
        #[arg(long)]
        classification: Option<Classification>,
    },
    Consume {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        topic: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    VerifyAudit {
        #[arg(long, default_value = "./var/audit/audit.jsonl")]
        path: PathBuf,
    },
    ExportAudit {
        #[arg(long, default_value = "./var/audit/audit.jsonl")]
        path: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::GenerateKeypair {
            key_id,
            private_key,
            public_key,
        } => {
            let issuer = CapabilityIssuer::generate(key_id);
            issuer.write_private_key(&private_key)?;
            issuer.write_public_key(&public_key)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "private_key": private_key,
                    "public_key": public_key
                }))?
            );
            Ok(())
        }
        Command::IssueToken {
            key_id,
            private_key,
            principal,
            audience,
            expires_in_seconds,
            scopes,
            output,
        } => {
            if scopes.is_empty() {
                bail!("at least one --scope must be provided");
            }

            let issuer = CapabilityIssuer::from_private_key_file(key_id, private_key)?;
            let token_id = Uuid::now_v7();
            let expires_at = Utc::now() + Duration::seconds(expires_in_seconds);
            let claims = CapabilityClaims {
                token_id,
                principal: principal.clone(),
                audience: audience.clone(),
                issued_at: Utc::now(),
                expires_at,
                scopes,
            };
            let token = issuer.issue(claims)?;
            if let Some(path) = output {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&path, &token)
                    .with_context(|| format!("failed to write {}", path.display()))?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "token_file": path,
                        "token_id": token_id,
                        "principal": principal,
                        "audience": audience,
                        "expires_at": expires_at,
                    }))?
                );
            } else {
                println!("{token}");
            }
            Ok(())
        }
        Command::VerifyAudit { path } => {
            let report = verify_file(&path)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Command::ExportAudit { path, output } => {
            let verification = verify_file(&path)?;
            let events = load_events(&path)?;
            if let Some(parent) = output.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(
                &output,
                serde_json::to_vec_pretty(&serde_json::json!({
                    "verification": verification,
                    "events": events,
                }))?,
            )
            .with_context(|| format!("failed to write {}", output.display()))?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "output": output,
                    "event_count": events.len(),
                }))?
            );
            Ok(())
        }
        command => {
            let endpoint = endpoint_from_cli(cli.transport, cli.address, cli.socket)?;
            let mut client = Client::connect(endpoint).await?;
            let request = request_from_command(command)?;
            let response = client.send(request).await?;
            println!("{}", serde_json::to_string_pretty(&response)?);
            Ok(())
        }
    }
}

fn endpoint_from_cli(
    transport: TransportKind,
    address: String,
    socket: PathBuf,
) -> anyhow::Result<Endpoint> {
    match transport {
        TransportKind::Tcp => Ok(Endpoint::Tcp(address)),
        TransportKind::Unix => {
            #[cfg(unix)]
            {
                Ok(Endpoint::Unix(socket))
            }
            #[cfg(not(unix))]
            {
                let _ = socket;
                bail!("unix transport is not supported on this platform")
            }
        }
    }
}

fn request_from_command(command: Command) -> anyhow::Result<ControlRequest> {
    match command {
        Command::AuthState { token } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::GetAuthState,
        }),
        Command::Metrics { token } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::GetMetrics,
        }),
        Command::RevokeToken { token, token_id } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::RevokeToken { token_id },
        }),
        Command::RevokePrincipal { token, principal } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::RevokePrincipal { principal },
        }),
        Command::RevokeKey { token, key_id } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::RevokeKey { key_id },
        }),
        Command::Health { token } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::Health,
        }),
        Command::CreateTopic {
            token,
            topic,
            retention_class,
            classification,
        } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::CreateTopic {
                topic: TopicSpec {
                    name: topic,
                    retention_class,
                    default_classification: classification,
                },
            },
        }),
        Command::Publish {
            token,
            topic,
            payload,
            classification,
        } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::Publish {
                topic,
                classification,
                payload,
            },
        }),
        Command::Consume {
            token,
            topic,
            offset,
            limit,
        } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::Consume {
                topic,
                offset,
                limit,
            },
        }),
        Command::GenerateKeypair { .. }
        | Command::IssueToken { .. }
        | Command::VerifyAudit { .. }
        | Command::ExportAudit { .. } => {
            bail!("this command does not produce a control request")
        }
    }
}

fn resolve_token(args: TokenArgs) -> anyhow::Result<String> {
    if let Some(token) = args.token {
        return Ok(token);
    }

    if let Some(path) = args.token_file {
        let token = fs::read_to_string(&path)
            .with_context(|| format!("failed to read token file {}", path.display()))?;
        return Ok(token.trim().to_owned());
    }

    bail!("a capability token is required via --token or --token-file")
}

fn parse_scope(value: &str) -> Result<CapabilityScope, String> {
    let (resource, actions_raw) = value
        .rsplit_once(':')
        .ok_or_else(|| "scope must look like resource:action[,action]".to_owned())?;
    let actions = actions_raw
        .split(',')
        .map(|item| item.parse::<Action>())
        .collect::<Result<Vec<_>, _>>()?;

    if actions.is_empty() {
        return Err("scope must contain at least one action".to_owned());
    }

    Ok(CapabilityScope {
        resource: resource.to_owned(),
        actions,
    })
}
