use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, bail};
use chrono::Utc;
use clap::{Args, Parser, Subcommand, ValueEnum};
use expressways_client::{Client, Endpoint};
use expressways_orchestrator::{
    AssignmentRequirements, OrchestratorState, apply_event, apply_snapshot, load_state,
    query_from_requirements, record_assignment, save_state, select_agent,
};
use expressways_protocol::{
    Classification, ControlCommand, ControlRequest, ControlResponse, RetentionClass, StreamFrame,
    TopicSpec,
};
use tokio::time::sleep;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, value_enum, default_value_t = TransportKind::Tcp)]
    transport: TransportKind,
    #[arg(long, default_value = "127.0.0.1:7766")]
    address: String,
    #[arg(long, default_value = "./tmp/expressways.sock")]
    socket: PathBuf,
    #[arg(long, default_value = "info")]
    log_level: String,
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
    Supervise {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = "./var/orchestrator/state.json")]
        state_path: PathBuf,
        #[arg(long, default_value_t = 250)]
        retry_delay_ms: u64,
    },
    Assign {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = "./var/orchestrator/state.json")]
        state_path: PathBuf,
        #[arg(long)]
        skill: Option<String>,
        #[arg(long)]
        topic: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long, default_value = "orchestrator.assignments")]
        decision_topic: String,
        #[arg(long, default_value_t = true)]
        bootstrap: bool,
    },
    ShowState {
        #[arg(long, default_value = "./var/orchestrator/state.json")]
        state_path: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log_level)?;
    let endpoint = endpoint_from_cli(cli.transport, cli.address, cli.socket)?;

    match cli.command {
        Command::Supervise {
            token,
            state_path,
            retry_delay_ms,
        } => {
            let capability_token = resolve_token(token)?;
            supervise(endpoint, capability_token, state_path, retry_delay_ms).await
        }
        Command::Assign {
            token,
            state_path,
            skill,
            topic,
            principal,
            decision_topic,
            bootstrap,
        } => {
            let capability_token = resolve_token(token)?;
            let requirements = AssignmentRequirements {
                skill,
                topic,
                principal,
            };
            assign(
                endpoint,
                capability_token,
                state_path,
                requirements,
                decision_topic,
                bootstrap,
            )
            .await
        }
        Command::ShowState { state_path } => {
            let state = load_state(&state_path)?;
            println!("{}", serde_json::to_string_pretty(&state)?);
            Ok(())
        }
    }
}

fn init_tracing(log_level: &str) -> anyhow::Result<()> {
    let filter = EnvFilter::try_new(log_level)
        .or_else(|_| EnvFilter::try_new(format!("expressways_orchestrator={log_level}")))
        .context("invalid log level")?;

    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(false)
        .with_span_list(false)
        .init();

    Ok(())
}

async fn supervise(
    endpoint: Endpoint,
    capability_token: String,
    state_path: PathBuf,
    retry_delay_ms: u64,
) -> anyhow::Result<()> {
    let mut state = bootstrap_state(&endpoint, &capability_token).await?;
    save_state(&state_path, &state)?;
    info!(
        state_path = %state_path.display(),
        cursor = state.source_cursor,
        agent_count = state.agents.len(),
        "orchestrator bootstrap snapshot saved"
    );

    loop {
        let client = match Client::connect(endpoint.clone()).await {
            Ok(client) => client,
            Err(error) => {
                warn!(error = %error, "failed to connect orchestrator watch stream");
                sleep(Duration::from_millis(retry_delay_ms)).await;
                continue;
            }
        };
        let mut stream = client.into_stream();
        let query = query_from_requirements(
            &AssignmentRequirements {
                skill: None,
                topic: None,
                principal: None,
            },
            true,
        );
        let opened = stream
            .open(ControlRequest {
                capability_token: capability_token.clone(),
                command: ControlCommand::OpenAgentWatchStream {
                    query,
                    cursor: Some(state.source_cursor),
                    max_events: 100,
                    wait_timeout_ms: 30_000,
                },
            })
            .await;
        let opened = match opened {
            Ok(frame) => frame,
            Err(error) => {
                warn!(error = %error, "failed to open orchestrator watch stream");
                sleep(Duration::from_millis(retry_delay_ms)).await;
                continue;
            }
        };

        match opened {
            StreamFrame::AgentWatchOpened { cursor } => {
                state.source_cursor = cursor;
                info!(cursor, "orchestrator watch stream opened");
            }
            StreamFrame::StreamError { code, message } => {
                warn!(code, message, "orchestrator stream open rejected");
                if code == "watch_cursor_expired" {
                    state = bootstrap_state(&endpoint, &capability_token).await?;
                    save_state(&state_path, &state)?;
                }
                sleep(Duration::from_millis(retry_delay_ms)).await;
                continue;
            }
            other => {
                bail!("unexpected opening frame for orchestrator supervisor: {other:?}");
            }
        }

        let mut should_reconnect = false;
        while let Some(frame) = stream.next_frame().await? {
            match frame {
                StreamFrame::RegistryEvents { events, cursor } => {
                    for event in events {
                        apply_event(&mut state, event);
                    }
                    state.source_cursor = cursor;
                    save_state(&state_path, &state)?;
                    info!(
                        state_path = %state_path.display(),
                        cursor,
                        agent_count = state.agents.len(),
                        "orchestrator state updated from registry events"
                    );
                }
                StreamFrame::KeepAlive { cursor } => {
                    state.source_cursor = cursor;
                }
                StreamFrame::StreamClosed { cursor, reason } => {
                    state.source_cursor = cursor;
                    info!(
                        cursor,
                        reason, "orchestrator watch stream closed; reconnecting"
                    );
                    should_reconnect = true;
                    break;
                }
                StreamFrame::StreamError { code, message } => {
                    warn!(code, message, "orchestrator stream error");
                    if code == "watch_cursor_expired" {
                        state = bootstrap_state(&endpoint, &capability_token).await?;
                        save_state(&state_path, &state)?;
                    }
                    should_reconnect = true;
                    break;
                }
                StreamFrame::AgentWatchOpened { .. } => {}
            }
        }

        if !should_reconnect {
            warn!("orchestrator watch stream ended unexpectedly; reconnecting");
        }
        sleep(Duration::from_millis(retry_delay_ms)).await;
    }
}

async fn assign(
    endpoint: Endpoint,
    capability_token: String,
    state_path: PathBuf,
    requirements: AssignmentRequirements,
    decision_topic: String,
    bootstrap: bool,
) -> anyhow::Result<()> {
    let state = match load_state(&state_path) {
        Ok(state) => state,
        Err(error) if bootstrap => {
            warn!(error = %error, "orchestrator state missing or invalid; bootstrapping from broker");
            let state = bootstrap_state(&endpoint, &capability_token).await?;
            save_state(&state_path, &state)?;
            state
        }
        Err(error) => return Err(error),
    };

    let selected = select_agent(&state, &requirements, Utc::now())
        .cloned()
        .context("no eligible agent matched the assignment requirements")?;
    let mut next_state = state.clone();
    let decision = record_assignment(&mut next_state, requirements, &selected, Utc::now());
    let payload = serde_json::to_string(&decision)?;

    let mut client = Client::connect(endpoint).await?;
    ensure_decision_topic(&mut client, &capability_token, &decision_topic).await?;
    let response = client
        .send(ControlRequest {
            capability_token,
            command: ControlCommand::Publish {
                topic: decision_topic.clone(),
                classification: Some(Classification::Internal),
                payload,
            },
        })
        .await?;

    match response {
        ControlResponse::PublishAccepted {
            message_id,
            offset,
            classification,
        } => {
            save_state(&state_path, &next_state)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "decision": decision,
                    "decision_topic": decision_topic,
                    "message_id": message_id,
                    "offset": offset,
                    "classification": classification,
                    "state_path": state_path,
                }))?
            );
            Ok(())
        }
        ControlResponse::Error { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected assignment publish response: {other:?}"),
    }
}

async fn bootstrap_state(
    endpoint: &Endpoint,
    capability_token: &str,
) -> anyhow::Result<OrchestratorState> {
    let mut client = Client::connect(endpoint.clone()).await?;
    let response = client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::ListAgents {
                query: query_from_requirements(
                    &AssignmentRequirements {
                        skill: None,
                        topic: None,
                        principal: None,
                    },
                    true,
                ),
            },
        })
        .await?;

    match response {
        ControlResponse::Agents { agents, cursor } => {
            let mut state = OrchestratorState::default();
            apply_snapshot(&mut state, agents, cursor);
            Ok(state)
        }
        ControlResponse::Error { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected bootstrap response: {other:?}"),
    }
}

async fn ensure_decision_topic(
    client: &mut Client,
    capability_token: &str,
    topic: &str,
) -> anyhow::Result<()> {
    let response = client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::CreateTopic {
                topic: TopicSpec {
                    name: topic.to_owned(),
                    retention_class: RetentionClass::Operational,
                    default_classification: Classification::Internal,
                },
            },
        })
        .await?;

    match response {
        ControlResponse::TopicCreated { .. } | ControlResponse::Error { .. } => Ok(()),
        other => bail!("unexpected create-topic response: {other:?}"),
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

fn resolve_token(args: TokenArgs) -> anyhow::Result<String> {
    if let Some(token) = args.token {
        return Ok(token);
    }
    if let Some(path) = args.token_file {
        let token = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read token file {}", path.display()))?;
        return Ok(token.trim().to_owned());
    }

    bail!("a capability token is required via --token or --token-file")
}
