use std::fs;
use std::path::PathBuf;

use anyhow::{Context, bail};
use chrono::{Duration, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use expressways_audit::{load_events, verify_file};
use expressways_auth::CapabilityIssuer;
use expressways_client::{Client, Endpoint};
use expressways_protocol::{
    Action, AgentEndpoint, AgentQuery, AgentRegistration, AgentSchemaRef, CapabilityClaims,
    CapabilityScope, Classification, ControlCommand, ControlRequest, ControlResponse,
    RetentionClass, StreamFrame, TopicSpec,
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

#[derive(Debug, Args, Clone)]
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
    RegisterAgent {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        agent_id: String,
        #[arg(long)]
        display_name: String,
        #[arg(long)]
        version: String,
        #[arg(long, default_value = "")]
        summary: String,
        #[arg(long = "skill")]
        skills: Vec<String>,
        #[arg(long = "subscribe")]
        subscriptions: Vec<String>,
        #[arg(long = "publish-topic")]
        publications: Vec<String>,
        #[arg(long = "schema", value_parser = parse_schema)]
        schemas: Vec<AgentSchemaRef>,
        #[arg(long, default_value = "control_tcp")]
        endpoint_transport: String,
        #[arg(long)]
        endpoint_address: String,
        #[arg(long, default_value = "internal")]
        classification: Classification,
        #[arg(long, default_value = "operational")]
        retention_class: RetentionClass,
        #[arg(long)]
        ttl_seconds: Option<u64>,
    },
    HeartbeatAgent {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        agent_id: String,
    },
    ListAgents {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        skill: Option<String>,
        #[arg(long)]
        topic: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long, default_value_t = false)]
        include_stale: bool,
    },
    WatchAgents {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        skill: Option<String>,
        #[arg(long)]
        topic: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long, default_value_t = false)]
        include_stale: bool,
        #[arg(long)]
        cursor: Option<u64>,
        #[arg(long, default_value_t = 100)]
        max_events: usize,
        #[arg(long, default_value_t = 30000)]
        wait_timeout_ms: u64,
        #[arg(long, default_value_t = false)]
        follow: bool,
    },
    WatchAgentsStream {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        skill: Option<String>,
        #[arg(long)]
        topic: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long, default_value_t = false)]
        include_stale: bool,
        #[arg(long)]
        cursor: Option<u64>,
        #[arg(long, default_value_t = 100)]
        max_events: usize,
        #[arg(long, default_value_t = 30000)]
        wait_timeout_ms: u64,
        #[arg(long, default_value_t = true)]
        resume: bool,
        #[arg(long, default_value_t = 250)]
        retry_delay_ms: u64,
    },
    CleanupStaleAgents {
        #[command(flatten)]
        token: TokenArgs,
    },
    RemoveAgent {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        agent_id: String,
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
        Command::WatchAgents {
            token,
            skill,
            topic,
            principal,
            include_stale,
            cursor,
            max_events,
            wait_timeout_ms,
            follow,
        } => {
            let endpoint = endpoint_from_cli(cli.transport, cli.address, cli.socket)?;
            let mut client = Client::connect(endpoint).await?;
            let capability_token = resolve_token(token)?;
            let query = AgentQuery {
                skill,
                topic,
                principal,
                include_stale,
            };
            let mut next_cursor = cursor;

            loop {
                let response = client
                    .send(ControlRequest {
                        capability_token: capability_token.clone(),
                        command: ControlCommand::WatchAgents {
                            query: query.clone(),
                            cursor: next_cursor,
                            max_events,
                            wait_timeout_ms,
                        },
                    })
                    .await?;
                println!("{}", serde_json::to_string_pretty(&response)?);

                match response {
                    ControlResponse::RegistryEvents { cursor, .. } => {
                        if !follow {
                            break;
                        }
                        next_cursor = Some(cursor);
                    }
                    ControlResponse::Error { .. } => break,
                    other => {
                        bail!("unexpected response for watch-agents: {other:?}");
                    }
                }
            }

            Ok(())
        }
        Command::WatchAgentsStream {
            token,
            skill,
            topic,
            principal,
            include_stale,
            cursor,
            max_events,
            wait_timeout_ms,
            resume,
            retry_delay_ms,
        } => {
            let endpoint = endpoint_from_cli(cli.transport, cli.address, cli.socket)?;
            let capability_token = resolve_token(token)?;
            let query = AgentQuery {
                skill,
                topic,
                principal,
                include_stale,
            };

            let mut next_cursor = cursor;
            loop {
                let client = Client::connect(endpoint.clone()).await?;
                let mut stream = client.into_stream();

                let opened = stream
                    .open(ControlRequest {
                        capability_token: capability_token.clone(),
                        command: ControlCommand::OpenAgentWatchStream {
                            query: query.clone(),
                            cursor: next_cursor,
                            max_events,
                            wait_timeout_ms,
                        },
                    })
                    .await?;
                println!("{}", serde_json::to_string_pretty(&opened)?);

                match opened {
                    StreamFrame::AgentWatchOpened { cursor } => {
                        next_cursor = Some(cursor);
                    }
                    StreamFrame::StreamError { .. } | StreamFrame::StreamClosed { .. } => {
                        return Ok(());
                    }
                    other => {
                        bail!("unexpected opening frame for watch-agents-stream: {other:?}");
                    }
                }

                let mut should_resume = false;
                let mut saw_terminal_frame = false;
                while let Some(frame) = stream.next_frame().await? {
                    println!("{}", serde_json::to_string_pretty(&frame)?);
                    match frame {
                        StreamFrame::RegistryEvents { cursor, .. }
                        | StreamFrame::KeepAlive { cursor } => {
                            next_cursor = Some(cursor);
                        }
                        StreamFrame::StreamClosed { cursor, .. } => {
                            next_cursor = Some(cursor);
                            should_resume = resume;
                            saw_terminal_frame = true;
                            break;
                        }
                        StreamFrame::StreamError { code, .. } => {
                            if resume && code == "connection_closed" {
                                should_resume = true;
                                saw_terminal_frame = true;
                                break;
                            }
                            return Ok(());
                        }
                        StreamFrame::AgentWatchOpened { .. } => {
                            bail!("unexpected additional opening frame from watch stream");
                        }
                    }
                }

                if !saw_terminal_frame && resume {
                    should_resume = true;
                }

                if !should_resume {
                    return Ok(());
                }

                tokio::time::sleep(std::time::Duration::from_millis(retry_delay_ms)).await;
            }
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
        Command::RegisterAgent {
            token,
            agent_id,
            display_name,
            version,
            summary,
            skills,
            subscriptions,
            publications,
            schemas,
            endpoint_transport,
            endpoint_address,
            classification,
            retention_class,
            ttl_seconds,
        } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::RegisterAgent {
                registration: AgentRegistration {
                    agent_id,
                    display_name,
                    version,
                    summary,
                    skills,
                    subscriptions,
                    publications,
                    schemas,
                    endpoint: AgentEndpoint {
                        transport: endpoint_transport,
                        address: endpoint_address,
                    },
                    classification,
                    retention_class,
                    ttl_seconds,
                },
            },
        }),
        Command::HeartbeatAgent { token, agent_id } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::HeartbeatAgent { agent_id },
        }),
        Command::ListAgents {
            token,
            skill,
            topic,
            principal,
            include_stale,
        } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::ListAgents {
                query: AgentQuery {
                    skill,
                    topic,
                    principal,
                    include_stale,
                },
            },
        }),
        Command::WatchAgents { .. } => {
            bail!("watch-agents is handled directly by the CLI runtime")
        }
        Command::WatchAgentsStream { .. } => {
            bail!("watch-agents-stream is handled directly by the CLI runtime")
        }
        Command::CleanupStaleAgents { token } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::CleanupStaleAgents,
        }),
        Command::RemoveAgent { token, agent_id } => Ok(ControlRequest {
            capability_token: resolve_token(token)?,
            command: ControlCommand::RemoveAgent { agent_id },
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

fn parse_schema(value: &str) -> Result<AgentSchemaRef, String> {
    let (name, version) = value
        .split_once(':')
        .ok_or_else(|| "schema must look like name:version".to_owned())?;
    if name.trim().is_empty() || version.trim().is_empty() {
        return Err("schema name and version must both be present".to_owned());
    }

    Ok(AgentSchemaRef {
        name: name.trim().to_owned(),
        version: version.trim().to_owned(),
    })
}
