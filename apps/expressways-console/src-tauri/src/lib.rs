use expressways_client::{Client, Endpoint};
use expressways_protocol::{
    AdopterStatusView, AgentCard, AgentQuery, AuthStateView, BrokerMetricsView, ControlCommand,
    ControlRequest, ControlResponse, RegistryEvent, StoredMessage, StreamFrame,
};
use serde::{Deserialize, Serialize};
use tauri::{Emitter, State};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConsoleSettings {
    transport: String,
    address: String,
    socket_path: String,
    token: String,
}

#[derive(Debug, Clone, Serialize)]
struct HealthView {
    node_name: String,
    status: String,
}

#[derive(Debug, Clone, Serialize)]
struct MonitorSnapshot {
    health: HealthView,
    metrics: BrokerMetricsView,
    adopters: Vec<AdopterStatusView>,
    auth: AuthStateView,
    agents: Vec<AgentCard>,
    cursor: u64,
}

#[derive(Debug, Clone, Serialize)]
struct TopicConsumeResult {
    topic: String,
    messages: Vec<StoredMessage>,
    next_offset: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum RegistryStreamEventKind {
    Opened,
    Events,
    Keepalive,
    Closed,
    Error,
}

#[derive(Debug, Clone, Serialize)]
struct RegistryStreamEventPayload {
    kind: RegistryStreamEventKind,
    cursor: Option<u64>,
    events: Vec<RegistryEvent>,
    message: Option<String>,
}

#[derive(Default)]
struct RegistryStreamState {
    task: Mutex<Option<JoinHandle<()>>>,
}

#[tauri::command]
async fn monitor_snapshot(settings: ConsoleSettings) -> Result<MonitorSnapshot, String> {
    if settings.token.trim().is_empty() {
        return Err("capability token is required".to_owned());
    }

    let endpoint = build_endpoint(&settings)?;
    let mut client = Client::connect(endpoint)
        .await
        .map_err(|error| format!("failed to connect: {error}"))?;

    let health = match send_command(&mut client, &settings.token, ControlCommand::Health).await? {
        ControlResponse::Health { node_name, status } => HealthView { node_name, status },
        response => {
            return Err(format!(
                "unexpected response for health command: {}",
                response_name(&response)
            ));
        }
    };

    let metrics =
        match send_command(&mut client, &settings.token, ControlCommand::GetMetrics).await? {
            ControlResponse::Metrics { metrics } => metrics,
            response => {
                return Err(format!(
                    "unexpected response for metrics command: {}",
                    response_name(&response)
                ));
            }
        };

    let adopters =
        match send_command(&mut client, &settings.token, ControlCommand::GetAdopters).await? {
            ControlResponse::Adopters { adopters } => adopters,
            response => {
                return Err(format!(
                    "unexpected response for adopters command: {}",
                    response_name(&response)
                ));
            }
        };

    let auth =
        match send_command(&mut client, &settings.token, ControlCommand::GetAuthState).await? {
            ControlResponse::AuthState { state } => state,
            response => {
                return Err(format!(
                    "unexpected response for auth command: {}",
                    response_name(&response)
                ));
            }
        };

    let (agents, cursor) = match send_command(
        &mut client,
        &settings.token,
        ControlCommand::ListAgents {
            query: AgentQuery {
                include_stale: true,
                ..AgentQuery::default()
            },
        },
    )
    .await?
    {
        ControlResponse::Agents { agents, cursor } => (agents, cursor),
        response => {
            return Err(format!(
                "unexpected response for list_agents command: {}",
                response_name(&response)
            ));
        }
    };

    Ok(MonitorSnapshot {
        health,
        metrics,
        adopters,
        auth,
        agents,
        cursor,
    })
}

#[tauri::command]
async fn monitor_consume_topic(
    settings: ConsoleSettings,
    topic: String,
    offset: u64,
    limit: usize,
) -> Result<TopicConsumeResult, String> {
    if settings.token.trim().is_empty() {
        return Err("capability token is required".to_owned());
    }

    if topic.trim().is_empty() {
        return Err("topic is required".to_owned());
    }

    let endpoint = build_endpoint(&settings)?;
    let mut client = Client::connect(endpoint)
        .await
        .map_err(|error| format!("failed to connect: {error}"))?;

    match send_command(
        &mut client,
        &settings.token,
        ControlCommand::Consume {
            topic: topic.clone(),
            offset,
            limit: limit.max(1),
        },
    )
    .await?
    {
        ControlResponse::Messages {
            topic,
            messages,
            next_offset,
        } => Ok(TopicConsumeResult {
            topic,
            messages,
            next_offset,
        }),
        response => Err(format!(
            "unexpected response for consume command: {}",
            response_name(&response)
        )),
    }
}

#[tauri::command]
async fn monitor_start_registry_stream(
    app: tauri::AppHandle,
    state: State<'_, RegistryStreamState>,
    settings: ConsoleSettings,
    cursor: Option<u64>,
) -> Result<(), String> {
    if settings.token.trim().is_empty() {
        return Err("capability token is required".to_owned());
    }

    let mut task_slot = state.task.lock().await;
    if let Some(task) = task_slot.take() {
        task.abort();
    }

    let task = tokio::spawn(async move {
        let endpoint = match build_endpoint(&settings) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                let _ = emit_registry_stream(
                    &app,
                    RegistryStreamEventPayload {
                        kind: RegistryStreamEventKind::Error,
                        cursor: None,
                        events: Vec::new(),
                        message: Some(error),
                    },
                );
                return;
            }
        };

        let client = match Client::connect(endpoint).await {
            Ok(client) => client,
            Err(error) => {
                let _ = emit_registry_stream(
                    &app,
                    RegistryStreamEventPayload {
                        kind: RegistryStreamEventKind::Error,
                        cursor: None,
                        events: Vec::new(),
                        message: Some(format!("failed to connect: {error}")),
                    },
                );
                return;
            }
        };

        let mut stream = client.into_stream();
        let open = stream
            .open(ControlRequest {
                capability_token: settings.token.clone(),
                command: ControlCommand::OpenAgentWatchStream {
                    query: AgentQuery {
                        include_stale: true,
                        ..AgentQuery::default()
                    },
                    cursor,
                    max_events: 100,
                    wait_timeout_ms: 30_000,
                },
            })
            .await;

        let open_frame = match open {
            Ok(frame) => frame,
            Err(error) => {
                let _ = emit_registry_stream(
                    &app,
                    RegistryStreamEventPayload {
                        kind: RegistryStreamEventKind::Error,
                        cursor: None,
                        events: Vec::new(),
                        message: Some(format!("failed to open stream: {error}")),
                    },
                );
                return;
            }
        };

        if !handle_stream_frame(&app, open_frame) {
            return;
        }

        loop {
            match stream.next_frame().await {
                Ok(Some(frame)) => {
                    if !handle_stream_frame(&app, frame) {
                        break;
                    }
                }
                Ok(None) => {
                    let _ = emit_registry_stream(
                        &app,
                        RegistryStreamEventPayload {
                            kind: RegistryStreamEventKind::Closed,
                            cursor: None,
                            events: Vec::new(),
                            message: Some("stream ended by server".to_owned()),
                        },
                    );
                    break;
                }
                Err(error) => {
                    let _ = emit_registry_stream(
                        &app,
                        RegistryStreamEventPayload {
                            kind: RegistryStreamEventKind::Error,
                            cursor: None,
                            events: Vec::new(),
                            message: Some(format!("stream read failed: {error}")),
                        },
                    );
                    break;
                }
            }
        }
    });

    *task_slot = Some(task);
    Ok(())
}

#[tauri::command]
async fn monitor_stop_registry_stream(state: State<'_, RegistryStreamState>) -> Result<(), String> {
    let mut task_slot = state.task.lock().await;
    if let Some(task) = task_slot.take() {
        task.abort();
    }
    Ok(())
}

fn build_endpoint(settings: &ConsoleSettings) -> Result<Endpoint, String> {
    match settings.transport.as_str() {
        "tcp" => Ok(Endpoint::Tcp(settings.address.clone())),
        "unix" => {
            #[cfg(unix)]
            {
                Ok(Endpoint::Unix(settings.socket_path.clone().into()))
            }
            #[cfg(not(unix))]
            {
                Err("unix transport is not supported on this platform".to_owned())
            }
        }
        other => Err(format!("unsupported transport: {other}")),
    }
}

fn handle_stream_frame(app: &tauri::AppHandle, frame: StreamFrame) -> bool {
    match frame {
        StreamFrame::AgentWatchOpened { cursor } => {
            let _ = emit_registry_stream(
                app,
                RegistryStreamEventPayload {
                    kind: RegistryStreamEventKind::Opened,
                    cursor: Some(cursor),
                    events: Vec::new(),
                    message: None,
                },
            );
            true
        }
        StreamFrame::RegistryEvents { events, cursor } => {
            let _ = emit_registry_stream(
                app,
                RegistryStreamEventPayload {
                    kind: RegistryStreamEventKind::Events,
                    cursor: Some(cursor),
                    events,
                    message: None,
                },
            );
            true
        }
        StreamFrame::KeepAlive { cursor } => {
            let _ = emit_registry_stream(
                app,
                RegistryStreamEventPayload {
                    kind: RegistryStreamEventKind::Keepalive,
                    cursor: Some(cursor),
                    events: Vec::new(),
                    message: None,
                },
            );
            true
        }
        StreamFrame::StreamError { code, message } => {
            let _ = emit_registry_stream(
                app,
                RegistryStreamEventPayload {
                    kind: RegistryStreamEventKind::Error,
                    cursor: None,
                    events: Vec::new(),
                    message: Some(format!("{code}: {message}")),
                },
            );
            false
        }
        StreamFrame::StreamClosed { cursor, reason } => {
            let _ = emit_registry_stream(
                app,
                RegistryStreamEventPayload {
                    kind: RegistryStreamEventKind::Closed,
                    cursor: Some(cursor),
                    events: Vec::new(),
                    message: Some(reason),
                },
            );
            false
        }
    }
}

fn emit_registry_stream(
    app: &tauri::AppHandle,
    payload: RegistryStreamEventPayload,
) -> Result<(), String> {
    app.emit("registry-stream-event", payload)
        .map_err(|error| format!("emit failed: {error}"))
}

async fn send_command(
    client: &mut Client,
    token: &str,
    command: ControlCommand,
) -> Result<ControlResponse, String> {
    let response = client
        .send(ControlRequest {
            capability_token: token.to_owned(),
            command,
        })
        .await
        .map_err(|error| error.to_string())?;

    if let ControlResponse::Error { code, message } = &response {
        return Err(format!("{code}: {message}"));
    }

    Ok(response)
}

fn response_name(response: &ControlResponse) -> &'static str {
    match response {
        ControlResponse::Health { .. } => "health",
        ControlResponse::Metrics { .. } => "metrics",
        ControlResponse::Adopters { .. } => "adopters",
        ControlResponse::AuthState { .. } => "auth_state",
        ControlResponse::AgentRegistered { .. } => "agent_registered",
        ControlResponse::AgentHeartbeat { .. } => "agent_heartbeat",
        ControlResponse::Agents { .. } => "agents",
        ControlResponse::RegistryEvents { .. } => "registry_events",
        ControlResponse::AgentsCleanedUp { .. } => "agents_cleaned_up",
        ControlResponse::AgentRemoved { .. } => "agent_removed",
        ControlResponse::TopicCreated { .. } => "topic_created",
        ControlResponse::RevocationUpdated { .. } => "revocation_updated",
        ControlResponse::ArtifactStored { .. } => "artifact_stored",
        ControlResponse::ArtifactMetadata { .. } => "artifact_metadata",
        ControlResponse::Artifact { .. } => "artifact",
        ControlResponse::PublishAccepted { .. } => "publish_accepted",
        ControlResponse::Messages { .. } => "messages",
        ControlResponse::Error { .. } => "error",
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(RegistryStreamState::default())
        .invoke_handler(tauri::generate_handler![
            monitor_snapshot,
            monitor_consume_topic,
            monitor_start_registry_stream,
            monitor_stop_registry_stream
        ])
        .setup(|app| {
            if cfg!(debug_assertions) {
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                )?;
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
