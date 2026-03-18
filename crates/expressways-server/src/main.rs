mod config;
mod metrics;
mod quota;
mod registry;
mod registry_events;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use expressways_audit::{AuditDecision, AuditOutcome, AuditSink, DraftAuditEvent};
use expressways_auth::{AuthSnapshot, CapabilityVerifier, RevocationList, VerifiedCapability};
use expressways_policy::PolicyEngine;
use expressways_protocol::{
    Action, AgentQuery, AgentRegistration, AuthIssuerView, AuthPrincipalView, AuthRevocationView,
    AuthStateView, BROKER_RESOURCE, ControlCommand, ControlRequest, ControlResponse,
    REGISTRY_RESOURCE, RegistryEventKind, StreamFrame, TopicSpec, registry_entry_resource,
    topic_resource,
};
use expressways_storage::{DiskPressurePolicy, RetentionPolicy, Storage, StorageConfig};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::{AppConfig, RegistryBackend, ServerConfig, TransportKind};
use crate::metrics::MetricsCollector;
use crate::quota::QuotaManager;
use crate::registry::AgentRegistry;
use crate::registry_events::{RegistryEventError, RegistryEventHub};

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, default_value = "configs/expressways.example.toml")]
    config: std::path::PathBuf,
}

#[derive(Debug)]
struct BrokerState {
    node_name: String,
    verifier: CapabilityVerifier,
    policy: PolicyEngine,
    quotas: QuotaManager,
    metrics: MetricsCollector,
    storage: Mutex<Storage>,
    registry: Mutex<AgentRegistry>,
    registry_events: RegistryEventHub,
    stream_limits: StreamLimits,
    audit: Mutex<AuditSink>,
}

#[derive(Debug, Clone, Copy)]
struct StreamLimits {
    send_timeout: Duration,
    idle_keepalive_limit: u64,
}

#[derive(Debug, Clone)]
struct RequestIdentity {
    principal: String,
    principal_kind: String,
    quota_profile: String,
    token_id: String,
}

enum ListenerHandle {
    Tcp {
        listener: TcpListener,
        bind: String,
    },
    #[cfg(unix)]
    Unix {
        listener: UnixListener,
        socket_path: std::path::PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = AppConfig::load(&cli.config)?;
    init_tracing(&config.server.log_level)?;

    let state = Arc::new(
        build_state(&config)
            .with_context(|| format!("failed to build broker from {}", cli.config.display()))?,
    );
    let listener = bind_listener(&config.server)?;

    match &listener {
        ListenerHandle::Tcp { bind, .. } => info!(
            node = %config.server.node_name,
            transport = "tcp",
            listen_addr = %bind,
            data_dir = %config.server.data_dir.display(),
            audit_path = %config.audit.path.display(),
            "expressways server listening"
        ),
        #[cfg(unix)]
        ListenerHandle::Unix { socket_path, .. } => info!(
            node = %config.server.node_name,
            transport = "unix",
            socket = %socket_path.display(),
            data_dir = %config.server.data_dir.display(),
            audit_path = %config.audit.path.display(),
            "expressways server listening"
        ),
    }

    record_internal_event(
        &state,
        "system:server",
        Action::Admin,
        BROKER_RESOURCE,
        AuditDecision::Allow,
        AuditOutcome::Succeeded,
        Some("startup complete".to_owned()),
    )
    .await?;

    run_listener(listener, Arc::clone(&state)).await?;

    record_internal_event(
        &state,
        "system:server",
        Action::Admin,
        BROKER_RESOURCE,
        AuditDecision::Allow,
        AuditOutcome::Succeeded,
        Some("shutdown complete".to_owned()),
    )
    .await?;

    Ok(())
}

fn init_tracing(log_level: &str) -> anyhow::Result<()> {
    let filter = EnvFilter::try_new(log_level)
        .or_else(|_| EnvFilter::try_new(format!("expressways_server={log_level}")))
        .context("invalid log level")?;

    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(false)
        .with_span_list(false)
        .init();

    Ok(())
}

fn build_state(config: &AppConfig) -> anyhow::Result<BrokerState> {
    let storage = Storage::new(StorageConfig {
        data_dir: config.server.data_dir.clone(),
        segment_max_bytes: config.storage.segment_max_bytes,
        default_retention_class: config.storage.retention_class.clone(),
        default_classification: config.storage.default_classification.clone(),
        retention_policy: RetentionPolicy {
            ephemeral_max_bytes: config.storage.ephemeral_retention_bytes,
            operational_max_bytes: config.storage.operational_retention_bytes,
            regulated_max_bytes: config.storage.regulated_retention_bytes,
        },
        disk_pressure: DiskPressurePolicy {
            max_total_bytes: config.storage.max_total_bytes,
            reclaim_target_bytes: config.storage.reclaim_target_bytes,
        },
    })?;
    let audit = AuditSink::new(&config.audit.path)?;
    let verifier = CapabilityVerifier::from_config(&config.auth)?;
    let quotas = QuotaManager::new(config.quotas.clone())?;
    let registry = build_registry(config);
    let registry_events = RegistryEventHub::new(config.registry.event_history_limit);
    quotas.validate_principals(
        config
            .auth
            .principals
            .iter()
            .map(|principal| principal.quota_profile.as_str()),
    )?;

    Ok(BrokerState {
        node_name: config.server.node_name.clone(),
        verifier,
        policy: PolicyEngine::new(config.policy.clone()),
        quotas,
        metrics: MetricsCollector::new(),
        storage: Mutex::new(storage),
        registry: Mutex::new(registry),
        registry_events,
        stream_limits: StreamLimits {
            send_timeout: Duration::from_millis(config.registry.stream_send_timeout_ms.max(1)),
            idle_keepalive_limit: config.registry.stream_idle_keepalive_limit.max(1),
        },
        audit: Mutex::new(audit),
    })
}

fn build_registry(config: &AppConfig) -> AgentRegistry {
    let path = config
        .registry
        .path
        .clone()
        .unwrap_or_else(|| config.server.data_dir.join("registry").join("agents.json"));

    match config.registry.backend {
        RegistryBackend::File => AgentRegistry::file(path, config.registry.default_ttl_seconds),
    }
}

fn bind_listener(config: &ServerConfig) -> anyhow::Result<ListenerHandle> {
    match config.transport {
        TransportKind::Tcp => {
            let bind = config
                .listen_addr
                .clone()
                .unwrap_or_else(|| "127.0.0.1:7766".to_owned());
            let listener = std::net::TcpListener::bind(&bind)
                .with_context(|| format!("failed to bind TCP listener at {bind}"))?;
            listener
                .set_nonblocking(true)
                .context("failed to set TCP listener nonblocking")?;
            Ok(ListenerHandle::Tcp {
                listener: TcpListener::from_std(listener)
                    .context("failed to create tokio TCP listener")?,
                bind,
            })
        }
        TransportKind::Unix => {
            #[cfg(unix)]
            {
                let socket_path = config
                    .socket_path
                    .clone()
                    .unwrap_or_else(|| std::path::PathBuf::from("./tmp/expressways.sock"));
                prepare_socket(&socket_path)?;
                let listener = UnixListener::bind(&socket_path).with_context(|| {
                    format!("failed to bind Unix socket at {}", socket_path.display())
                })?;
                Ok(ListenerHandle::Unix {
                    listener,
                    socket_path,
                })
            }
            #[cfg(not(unix))]
            {
                anyhow::bail!("unix transport is not supported on this platform")
            }
        }
    }
}

#[cfg(unix)]
fn prepare_socket(path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    if path.exists() {
        std::fs::remove_file(path)
            .with_context(|| format!("failed to remove existing socket {}", path.display()))?;
    }

    Ok(())
}

async fn run_listener(listener: ListenerHandle, state: Arc<BrokerState>) -> anyhow::Result<()> {
    match listener {
        ListenerHandle::Tcp { listener, .. } => run_tcp_listener(listener, state).await,
        #[cfg(unix)]
        ListenerHandle::Unix {
            listener,
            socket_path,
        } => {
            let result = run_unix_listener(listener, Arc::clone(&state)).await;
            if socket_path.exists() {
                let _ = std::fs::remove_file(&socket_path);
            }
            result
        }
    }
}

async fn run_tcp_listener(listener: TcpListener, state: Arc<BrokerState>) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (stream, remote) = accept.context("failed to accept TCP connection")?;
                stream.set_nodelay(true).context("failed to set TCP_NODELAY")?;
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, state).await {
                        error!(error = %error, remote = %remote, "client handler exited with error");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to listen for shutdown signal")?;
                info!("shutdown requested");
                break;
            }
        }
    }

    Ok(())
}

#[cfg(unix)]
async fn run_unix_listener(listener: UnixListener, state: Arc<BrokerState>) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (stream, _) = accept.context("failed to accept Unix connection")?;
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, state).await {
                        error!(error = %error, "client handler exited with error");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to listen for shutdown signal")?;
                info!("shutdown requested");
                break;
            }
        }
    }

    Ok(())
}

async fn handle_connection<T>(stream: T, state: Arc<BrokerState>) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut framed = Framed::new(stream, LinesCodec::new());

    while let Some(frame) = framed.next().await {
        let line = match frame {
            Ok(line) => line,
            Err(error) => {
                warn!(error = %error, "failed to read request frame");
                continue;
            }
        };

        let request = match serde_json::from_str::<ControlRequest>(&line) {
            Ok(request) => request,
            Err(error) => {
                warn!(error = %error, "failed to decode request");
                let serialized = serde_json::to_string(&ControlResponse::error(
                    "invalid_request",
                    error.to_string(),
                ))?;
                framed.send(serialized).await?;
                continue;
            }
        };

        if matches!(request.command, ControlCommand::OpenAgentWatchStream { .. }) {
            serve_streaming_request(&mut framed, &state, request).await?;
            break;
        }

        let response = process_request(&state, request).await;
        let serialized = serde_json::to_string(&response)?;
        framed.send(serialized).await?;
    }

    Ok(())
}

async fn serve_streaming_request<T>(
    framed: &mut Framed<T, LinesCodec>,
    state: &Arc<BrokerState>,
    request: ControlRequest,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let (capability_token, query, cursor, max_events, wait_timeout_ms) = match request.command {
        ControlCommand::OpenAgentWatchStream {
            query,
            cursor,
            max_events,
            wait_timeout_ms,
        } => (
            request.capability_token,
            query,
            cursor,
            max_events,
            wait_timeout_ms,
        ),
        _ => {
            send_stream_frame(
                framed,
                &StreamFrame::StreamError {
                    code: "invalid_request".to_owned(),
                    message: "streaming transport only supports open_agent_watch_stream".to_owned(),
                },
                state.stream_limits.send_timeout,
            )
            .await?;
            return Ok(());
        }
    };

    let resource = REGISTRY_RESOURCE.to_owned();
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Admin,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => {
            send_stream_frame(
                framed,
                &stream_error_from_response(response),
                state.stream_limits.send_timeout,
            )
            .await?;
            return Ok(());
        }
    };

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some(format!(
            "open registry watch stream from cursor {:?}",
            cursor
        )),
    )
    .await
    {
        send_stream_frame(
            framed,
            &stream_error_from_response(response),
            state.stream_limits.send_timeout,
        )
        .await?;
        return Ok(());
    }

    let mut next_cursor = match state.registry_events.resolve_cursor(cursor).await {
        Ok(cursor) => cursor,
        Err(error) => {
            let code = match error {
                RegistryEventError::CursorExpired { .. } => "watch_cursor_expired",
            };
            let message = error.to_string();
            let _ = finalize_failure(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(message.clone()),
            )
            .await;
            send_stream_frame(
                framed,
                &StreamFrame::StreamError {
                    code: code.to_owned(),
                    message,
                },
                state.stream_limits.send_timeout,
            )
            .await?;
            return Ok(());
        }
    };

    if let Err(error) = send_stream_frame(
        framed,
        &StreamFrame::AgentWatchOpened {
            cursor: next_cursor,
        },
        state.stream_limits.send_timeout,
    )
    .await
    {
        let _ = finalize_failure(
            state,
            &identity,
            Action::Admin,
            &resource,
            Some(format!("failed to open registry watch stream: {error}")),
        )
        .await;
        return Ok(());
    }

    state.metrics.record_stream_opened();

    if let Err(error) = finalize_success(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some(format!(
            "opened registry watch stream at cursor {next_cursor}"
        )),
    )
    .await
    {
        send_stream_frame(
            framed,
            &StreamFrame::StreamError {
                code: "audit_failure".to_owned(),
                message: error.to_string(),
            },
            state.stream_limits.send_timeout,
        )
        .await?;
        return Ok(());
    }

    info!(
        principal = %identity.principal,
        principal_kind = %identity.principal_kind,
        quota_profile = %identity.quota_profile,
        token_id = %identity.token_id,
        resource = %resource,
        cursor = next_cursor,
        max_events,
        wait_timeout_ms,
        "registry watch stream opened"
    );

    let stream_started = Instant::now();
    let mut idle_keepalives = 0u64;
    loop {
        let watch_started = Instant::now();
        match state
            .registry_events
            .watch(&query, Some(next_cursor), max_events, wait_timeout_ms)
            .await
        {
            Ok(batch) => {
                let timed_out = batch.timed_out;
                let delivered_events = batch.events.len();
                let frame = if timed_out {
                    StreamFrame::KeepAlive {
                        cursor: batch.cursor,
                    }
                } else {
                    StreamFrame::RegistryEvents {
                        events: batch.events,
                        cursor: batch.cursor,
                    }
                };
                next_cursor = batch.cursor;

                match send_stream_frame(framed, &frame, state.stream_limits.send_timeout).await {
                    Ok(()) => {
                        if timed_out {
                            idle_keepalives += 1;
                            state.metrics.record_stream_keepalive();
                            if idle_keepalives >= state.stream_limits.idle_keepalive_limit {
                                state.metrics.record_stream_idle_timeout();
                                let reason =
                                    format!("idle_timeout_after_{}_keepalives", idle_keepalives);
                                let _ = send_stream_frame(
                                    framed,
                                    &StreamFrame::StreamClosed {
                                        cursor: next_cursor,
                                        reason: reason.clone(),
                                    },
                                    state.stream_limits.send_timeout,
                                )
                                .await;
                                info!(
                                    principal = %identity.principal,
                                    principal_kind = %identity.principal_kind,
                                    quota_profile = %identity.quota_profile,
                                    token_id = %identity.token_id,
                                    resource = %resource,
                                    cursor = next_cursor,
                                    opened_for_ms = stream_started.elapsed().as_millis(),
                                    keepalives = idle_keepalives,
                                    "registry watch stream closed after idle timeout"
                                );
                                break;
                            }
                        } else {
                            idle_keepalives = 0;
                            state
                                .metrics
                                .record_stream_delivery(delivered_events, watch_started.elapsed());
                        }
                    }
                    Err(StreamSendError::TimedOut) => {
                        state
                            .metrics
                            .record_stream_delivery_failure(watch_started.elapsed());
                        state.metrics.record_stream_slow_consumer_drop();
                        warn!(
                            principal = %identity.principal,
                            principal_kind = %identity.principal_kind,
                            quota_profile = %identity.quota_profile,
                            token_id = %identity.token_id,
                            resource = %resource,
                            cursor = next_cursor,
                            send_timeout_ms = state.stream_limits.send_timeout.as_millis(),
                            "registry watch stream dropped due to slow consumer"
                        );
                        break;
                    }
                    Err(StreamSendError::Failed(error)) => {
                        state
                            .metrics
                            .record_stream_delivery_failure(watch_started.elapsed());
                        info!(
                            principal = %identity.principal,
                            principal_kind = %identity.principal_kind,
                            quota_profile = %identity.quota_profile,
                            token_id = %identity.token_id,
                            resource = %resource,
                            cursor = next_cursor,
                            error = %error,
                            "registry watch stream closed by client"
                        );
                        break;
                    }
                }
            }
            Err(error) => {
                let code = match error {
                    RegistryEventError::CursorExpired { .. } => "watch_cursor_expired",
                };
                let message = error.to_string();
                warn!(
                    principal = %identity.principal,
                    principal_kind = %identity.principal_kind,
                    quota_profile = %identity.quota_profile,
                    token_id = %identity.token_id,
                    resource = %resource,
                    cursor = next_cursor,
                    error = %message,
                    "registry watch stream failed"
                );
                let _ = send_stream_frame(
                    framed,
                    &StreamFrame::StreamError {
                        code: code.to_owned(),
                        message,
                    },
                    state.stream_limits.send_timeout,
                )
                .await;
                break;
            }
        }
    }

    state.metrics.record_stream_closed();

    Ok(())
}

async fn send_stream_frame<T>(
    framed: &mut Framed<T, LinesCodec>,
    frame: &StreamFrame,
    send_timeout: Duration,
) -> Result<(), StreamSendError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let serialized = serde_json::to_string(frame).map_err(StreamSendError::from)?;
    tokio::time::timeout(send_timeout, framed.send(serialized))
        .await
        .map_err(|_| StreamSendError::TimedOut)?
        .map_err(StreamSendError::from)?;
    Ok(())
}

#[derive(Debug)]
enum StreamSendError {
    TimedOut,
    Failed(anyhow::Error),
}

impl std::fmt::Display for StreamSendError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimedOut => formatter.write_str("stream send timed out"),
            Self::Failed(error) => std::fmt::Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for StreamSendError {}

impl From<anyhow::Error> for StreamSendError {
    fn from(error: anyhow::Error) -> Self {
        Self::Failed(error)
    }
}

impl From<serde_json::Error> for StreamSendError {
    fn from(error: serde_json::Error) -> Self {
        Self::Failed(error.into())
    }
}

impl From<tokio_util::codec::LinesCodecError> for StreamSendError {
    fn from(error: tokio_util::codec::LinesCodecError) -> Self {
        Self::Failed(error.into())
    }
}

fn stream_error_from_response(response: ControlResponse) -> StreamFrame {
    match response {
        ControlResponse::Error { code, message } => StreamFrame::StreamError { code, message },
        other => StreamFrame::StreamError {
            code: "unexpected_response".to_owned(),
            message: format!("expected error response, got {other:?}"),
        },
    }
}

async fn process_request(state: &BrokerState, request: ControlRequest) -> ControlResponse {
    let action = match &request.command {
        ControlCommand::Health => Action::Health,
        ControlCommand::Publish { .. } => Action::Publish,
        ControlCommand::Consume { .. } => Action::Consume,
        ControlCommand::GetAuthState
        | ControlCommand::GetMetrics
        | ControlCommand::RegisterAgent { .. }
        | ControlCommand::HeartbeatAgent { .. }
        | ControlCommand::ListAgents { .. }
        | ControlCommand::WatchAgents { .. }
        | ControlCommand::OpenAgentWatchStream { .. }
        | ControlCommand::CleanupStaleAgents
        | ControlCommand::RemoveAgent { .. }
        | ControlCommand::CreateTopic { .. }
        | ControlCommand::RevokeToken { .. }
        | ControlCommand::RevokePrincipal { .. }
        | ControlCommand::RevokeKey { .. } => Action::Admin,
    };
    state.metrics.record_request(&action);

    match request.command {
        ControlCommand::Health => handle_health(state, request.capability_token).await,
        ControlCommand::GetMetrics => handle_get_metrics(state, request.capability_token).await,
        ControlCommand::GetAuthState => {
            handle_get_auth_state(state, request.capability_token).await
        }
        ControlCommand::RegisterAgent { registration } => {
            handle_register_agent(state, request.capability_token, registration).await
        }
        ControlCommand::HeartbeatAgent { agent_id } => {
            handle_heartbeat_agent(state, request.capability_token, agent_id).await
        }
        ControlCommand::ListAgents { query } => {
            handle_list_agents(state, request.capability_token, query).await
        }
        ControlCommand::WatchAgents {
            query,
            cursor,
            max_events,
            wait_timeout_ms,
        } => {
            handle_watch_agents(
                state,
                request.capability_token,
                query,
                cursor,
                max_events,
                wait_timeout_ms,
            )
            .await
        }
        ControlCommand::OpenAgentWatchStream { .. } => ControlResponse::error(
            "invalid_request",
            "streaming watch must use the streaming transport",
        ),
        ControlCommand::CleanupStaleAgents => {
            handle_cleanup_stale_agents(state, request.capability_token).await
        }
        ControlCommand::RemoveAgent { agent_id } => {
            handle_remove_agent(state, request.capability_token, agent_id).await
        }
        ControlCommand::CreateTopic { topic } => {
            handle_create_topic(state, request.capability_token, topic).await
        }
        ControlCommand::RevokeToken { token_id } => {
            handle_revoke_token(state, request.capability_token, token_id).await
        }
        ControlCommand::RevokePrincipal { principal } => {
            handle_revoke_principal(state, request.capability_token, principal).await
        }
        ControlCommand::RevokeKey { key_id } => {
            handle_revoke_key(state, request.capability_token, key_id).await
        }
        ControlCommand::Publish {
            topic,
            classification,
            payload,
        } => {
            handle_publish(
                state,
                request.capability_token,
                topic,
                classification,
                payload,
            )
            .await
        }
        ControlCommand::Consume {
            topic,
            offset,
            limit,
        } => handle_consume(state, request.capability_token, topic, offset, limit).await,
    }
}

async fn handle_health(state: &BrokerState, capability_token: String) -> ControlResponse {
    let resource = BROKER_RESOURCE.to_owned();
    let identity =
        match authenticate_and_authorize(state, &capability_token, Action::Health, &resource).await
        {
            Ok(identity) => identity,
            Err(response) => return response,
        };

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Health,
        &resource,
        Some("health check".to_owned()),
    )
    .await
    {
        return response;
    }

    if let Err(error) = finalize_success(
        state,
        &identity,
        Action::Health,
        &resource,
        Some("health check succeeded".to_owned()),
    )
    .await
    {
        state.metrics.record_audit_failure();
        return ControlResponse::error("audit_failure", error.to_string());
    }

    ControlResponse::Health {
        node_name: state.node_name.clone(),
        status: "ok".to_owned(),
    }
}

async fn handle_get_metrics(state: &BrokerState, capability_token: String) -> ControlResponse {
    let resource = BROKER_RESOURCE.to_owned();
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Admin,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => return response,
    };

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some("fetch metrics".to_owned()),
    )
    .await
    {
        return response;
    }

    let storage_stats = {
        let storage = state.storage.lock().await;
        match storage.stats() {
            Ok(stats) => stats,
            Err(error) => {
                state.metrics.record_storage_failure();
                let message = error.to_string();
                let _ = finalize_failure(
                    state,
                    &identity,
                    Action::Admin,
                    &resource,
                    Some(message.clone()),
                )
                .await;
                return ControlResponse::error("metrics_error", message);
            }
        }
    };
    let audit_summary = state.audit.lock().await.summary();
    let metrics = state.metrics.snapshot(storage_stats, audit_summary);

    if let Err(error) = finalize_success(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some("fetched metrics".to_owned()),
    )
    .await
    {
        return ControlResponse::error("audit_failure", error.to_string());
    }

    ControlResponse::Metrics { metrics }
}

async fn handle_get_auth_state(state: &BrokerState, capability_token: String) -> ControlResponse {
    let resource = BROKER_RESOURCE.to_owned();
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Admin,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => return response,
    };

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some("fetch auth state".to_owned()),
    )
    .await
    {
        return response;
    }

    let snapshot = match state.verifier.snapshot() {
        Ok(snapshot) => snapshot,
        Err(error) => {
            let message = error.to_string();
            let _ = finalize_failure(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(message.clone()),
            )
            .await;
            return ControlResponse::error("auth_state_error", message);
        }
    };

    if let Err(error) = finalize_success(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some("fetched auth state".to_owned()),
    )
    .await
    {
        state.metrics.record_audit_failure();
        return ControlResponse::error("audit_failure", error.to_string());
    }

    ControlResponse::AuthState {
        state: auth_state_view(snapshot),
    }
}

async fn handle_register_agent(
    state: &BrokerState,
    capability_token: String,
    registration: AgentRegistration,
) -> ControlResponse {
    let resource = registry_entry_resource(&registration.agent_id);
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Admin,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => return response,
    };

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some(format!("register agent {}", registration.agent_id)),
    )
    .await
    {
        return response;
    }

    let registered = {
        let registry = state.registry.lock().await;
        registry.register(&identity.principal, registration)
    };

    match registered {
        Ok(card) => {
            if let Err(error) = finalize_success(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(format!("registered agent {}", card.agent_id)),
            )
            .await
            {
                state.metrics.record_audit_failure();
                return ControlResponse::error("audit_failure", error.to_string());
            }
            let _ = state
                .registry_events
                .record(RegistryEventKind::Registered, card.clone())
                .await;

            info!(
                principal = %identity.principal,
                principal_kind = %identity.principal_kind,
                quota_profile = %identity.quota_profile,
                token_id = %identity.token_id,
                resource = %resource,
                agent_id = %card.agent_id,
                endpoint_transport = %card.endpoint.transport,
                endpoint_address = %card.endpoint.address,
                "agent registered"
            );
            ControlResponse::AgentRegistered { card }
        }
        Err(error) => {
            let message = error.to_string();
            let _ = finalize_failure(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(message.clone()),
            )
            .await;
            ControlResponse::error("registry_error", message)
        }
    }
}

async fn handle_list_agents(
    state: &BrokerState,
    capability_token: String,
    query: AgentQuery,
) -> ControlResponse {
    let resource = REGISTRY_RESOURCE.to_owned();
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Admin,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => return response,
    };

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some("list discovery registry".to_owned()),
    )
    .await
    {
        return response;
    }

    let cursor = state.registry_events.current_cursor().await;
    let listed = {
        let registry = state.registry.lock().await;
        registry.list(&query)
    };

    match listed {
        Ok(agents) => {
            if let Err(error) = finalize_success(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(format!("listed {} discovery agents", agents.len())),
            )
            .await
            {
                state.metrics.record_audit_failure();
                return ControlResponse::error("audit_failure", error.to_string());
            }

            info!(
                principal = %identity.principal,
                principal_kind = %identity.principal_kind,
                quota_profile = %identity.quota_profile,
                token_id = %identity.token_id,
                resource = %resource,
                result_count = agents.len(),
                cursor,
                "discovery registry queried"
            );
            ControlResponse::Agents { agents, cursor }
        }
        Err(error) => {
            let message = error.to_string();
            let _ = finalize_failure(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(message.clone()),
            )
            .await;
            ControlResponse::error("registry_error", message)
        }
    }
}

async fn handle_watch_agents(
    state: &BrokerState,
    capability_token: String,
    query: AgentQuery,
    cursor: Option<u64>,
    max_events: usize,
    wait_timeout_ms: u64,
) -> ControlResponse {
    let resource = REGISTRY_RESOURCE.to_owned();
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Admin,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => return response,
    };

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some(format!("watch discovery registry from cursor {:?}", cursor)),
    )
    .await
    {
        return response;
    }

    match state
        .registry_events
        .watch(&query, cursor, max_events, wait_timeout_ms)
        .await
    {
        Ok(batch) => {
            if let Err(error) = finalize_success(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(format!(
                    "returned {} registry events; timed_out={}",
                    batch.events.len(),
                    batch.timed_out
                )),
            )
            .await
            {
                state.metrics.record_audit_failure();
                return ControlResponse::error("audit_failure", error.to_string());
            }

            info!(
                principal = %identity.principal,
                principal_kind = %identity.principal_kind,
                quota_profile = %identity.quota_profile,
                token_id = %identity.token_id,
                resource = %resource,
                event_count = batch.events.len(),
                cursor = batch.cursor,
                timed_out = batch.timed_out,
                "registry watch completed"
            );
            ControlResponse::RegistryEvents {
                events: batch.events,
                cursor: batch.cursor,
                timed_out: batch.timed_out,
            }
        }
        Err(error) => {
            let code = match error {
                RegistryEventError::CursorExpired { .. } => "watch_cursor_expired",
            };
            let message = error.to_string();
            let _ = finalize_failure(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(message.clone()),
            )
            .await;
            ControlResponse::error(code, message)
        }
    }
}

async fn handle_heartbeat_agent(
    state: &BrokerState,
    capability_token: String,
    agent_id: String,
) -> ControlResponse {
    let resource = registry_entry_resource(&agent_id);
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Admin,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => return response,
    };

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some(format!("heartbeat agent {agent_id}")),
    )
    .await
    {
        return response;
    }

    let heartbeat = {
        let registry = state.registry.lock().await;
        registry.heartbeat(&identity.principal, &identity.principal_kind, &agent_id)
    };

    match heartbeat {
        Ok(card) => {
            if let Err(error) = finalize_success(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(format!("heartbeat recorded for agent {agent_id}")),
            )
            .await
            {
                state.metrics.record_audit_failure();
                return ControlResponse::error("audit_failure", error.to_string());
            }
            let _ = state
                .registry_events
                .record(RegistryEventKind::Heartbeated, card.clone())
                .await;

            info!(
                principal = %identity.principal,
                principal_kind = %identity.principal_kind,
                quota_profile = %identity.quota_profile,
                token_id = %identity.token_id,
                resource = %resource,
                agent_id = %card.agent_id,
                expires_at = %card.expires_at,
                "agent heartbeat recorded"
            );
            ControlResponse::AgentHeartbeat { card }
        }
        Err(error) => {
            let message = error.to_string();
            let _ = finalize_failure(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(message.clone()),
            )
            .await;
            ControlResponse::error("registry_error", message)
        }
    }
}

async fn handle_cleanup_stale_agents(
    state: &BrokerState,
    capability_token: String,
) -> ControlResponse {
    let resource = REGISTRY_RESOURCE.to_owned();
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Admin,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => return response,
    };

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some("cleanup stale discovery agents".to_owned()),
    )
    .await
    {
        return response;
    }

    let cleanup = {
        let registry = state.registry.lock().await;
        registry.cleanup_stale()
    };

    match cleanup {
        Ok(removed_cards) => {
            let removed_agent_ids = removed_cards
                .iter()
                .map(|card| card.agent_id.clone())
                .collect::<Vec<_>>();
            if let Err(error) = finalize_success(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(format!(
                    "cleaned up {} stale discovery agents",
                    removed_agent_ids.len()
                )),
            )
            .await
            {
                state.metrics.record_audit_failure();
                return ControlResponse::error("audit_failure", error.to_string());
            }
            for card in removed_cards {
                let _ = state
                    .registry_events
                    .record(RegistryEventKind::CleanedUp, card)
                    .await;
            }

            info!(
                principal = %identity.principal,
                principal_kind = %identity.principal_kind,
                quota_profile = %identity.quota_profile,
                token_id = %identity.token_id,
                resource = %resource,
                removed_count = removed_agent_ids.len(),
                "stale discovery agents cleaned up"
            );
            ControlResponse::AgentsCleanedUp { removed_agent_ids }
        }
        Err(error) => {
            let message = error.to_string();
            let _ = finalize_failure(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(message.clone()),
            )
            .await;
            ControlResponse::error("registry_error", message)
        }
    }
}

async fn handle_remove_agent(
    state: &BrokerState,
    capability_token: String,
    agent_id: String,
) -> ControlResponse {
    let resource = registry_entry_resource(&agent_id);
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Admin,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => return response,
    };

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some(format!("remove agent {agent_id}")),
    )
    .await
    {
        return response;
    }

    let removed = {
        let registry = state.registry.lock().await;
        registry.remove(&identity.principal, &identity.principal_kind, &agent_id)
    };

    match removed {
        Ok(card) => {
            if let Err(error) = finalize_success(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(format!("removed agent {agent_id}")),
            )
            .await
            {
                state.metrics.record_audit_failure();
                return ControlResponse::error("audit_failure", error.to_string());
            }
            let _ = state
                .registry_events
                .record(RegistryEventKind::Removed, card)
                .await;

            info!(
                principal = %identity.principal,
                principal_kind = %identity.principal_kind,
                quota_profile = %identity.quota_profile,
                token_id = %identity.token_id,
                resource = %resource,
                agent_id = %agent_id,
                "agent removed from registry"
            );
            ControlResponse::AgentRemoved { agent_id }
        }
        Err(error) => {
            let message = error.to_string();
            let _ = finalize_failure(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(message.clone()),
            )
            .await;
            ControlResponse::error("registry_error", message)
        }
    }
}

async fn handle_create_topic(
    state: &BrokerState,
    capability_token: String,
    topic: TopicSpec,
) -> ControlResponse {
    let resource = topic_resource(&topic.name);
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Admin,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => return response,
    };

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some(format!("create topic {}", topic.name)),
    )
    .await
    {
        return response;
    }

    let created = {
        let storage = state.storage.lock().await;
        storage.ensure_topic(topic)
    };

    match created {
        Ok(topic) => {
            if let Err(error) = finalize_success(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(format!("created topic {}", topic.name)),
            )
            .await
            {
                state.metrics.record_audit_failure();
                return ControlResponse::error("audit_failure", error.to_string());
            }

            info!(
                principal = %identity.principal,
                principal_kind = %identity.principal_kind,
                quota_profile = %identity.quota_profile,
                token_id = %identity.token_id,
                resource = %resource,
                "topic created"
            );
            ControlResponse::TopicCreated { topic }
        }
        Err(error) => {
            state.metrics.record_storage_failure();
            let message = error.to_string();
            let _ = finalize_failure(
                state,
                &identity,
                Action::Admin,
                &resource,
                Some(message.clone()),
            )
            .await;
            ControlResponse::error("storage_error", message)
        }
    }
}

async fn handle_revoke_token(
    state: &BrokerState,
    capability_token: String,
    token_id: uuid::Uuid,
) -> ControlResponse {
    let resource = BROKER_RESOURCE.to_owned();
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Admin,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => return response,
    };

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some(format!("revoke token {token_id}")),
    )
    .await
    {
        return response;
    }

    match state.verifier.revoke_token(token_id) {
        Ok(revocations) => {
            revocation_success_response(
                state,
                &identity,
                &resource,
                revocations,
                format!("revoked token {token_id}"),
            )
            .await
        }
        Err(error) => {
            revocation_failure_response(
                state,
                &identity,
                &resource,
                format!("failed to revoke token {token_id}: {error}"),
            )
            .await
        }
    }
}

async fn handle_revoke_principal(
    state: &BrokerState,
    capability_token: String,
    principal: String,
) -> ControlResponse {
    let resource = BROKER_RESOURCE.to_owned();
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Admin,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => return response,
    };

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some(format!("revoke principal {principal}")),
    )
    .await
    {
        return response;
    }

    match state.verifier.revoke_principal(&principal) {
        Ok(revocations) => {
            revocation_success_response(
                state,
                &identity,
                &resource,
                revocations,
                format!("revoked principal {principal}"),
            )
            .await
        }
        Err(error) => {
            revocation_failure_response(
                state,
                &identity,
                &resource,
                format!("failed to revoke principal {principal}: {error}"),
            )
            .await
        }
    }
}

async fn handle_revoke_key(
    state: &BrokerState,
    capability_token: String,
    key_id: String,
) -> ControlResponse {
    let resource = BROKER_RESOURCE.to_owned();
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Admin,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => return response,
    };

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some(format!("revoke issuer key {key_id}")),
    )
    .await
    {
        return response;
    }

    match state.verifier.revoke_key(&key_id) {
        Ok(revocations) => {
            revocation_success_response(
                state,
                &identity,
                &resource,
                revocations,
                format!("revoked issuer key {key_id}"),
            )
            .await
        }
        Err(error) => {
            revocation_failure_response(
                state,
                &identity,
                &resource,
                format!("failed to revoke issuer key {key_id}: {error}"),
            )
            .await
        }
    }
}

async fn handle_publish(
    state: &BrokerState,
    capability_token: String,
    topic: String,
    classification: Option<expressways_protocol::Classification>,
    payload: String,
) -> ControlResponse {
    let started_at = Instant::now();
    let resource = topic_resource(&topic);
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Publish,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => return response,
    };

    if let Err(response) = enforce_publish_quota(state, &identity, &resource, payload.len()).await {
        return response;
    }

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Publish,
        &resource,
        Some(format!("publish to {topic}")),
    )
    .await
    {
        return response;
    }

    let appended = {
        let storage = state.storage.lock().await;
        storage.append(&topic, &identity.principal, classification, payload)
    };

    match appended {
        Ok(message) => {
            if let Err(error) = finalize_success(
                state,
                &identity,
                Action::Publish,
                &resource,
                Some(format!("published offset {}", message.offset)),
            )
            .await
            {
                state.metrics.record_audit_failure();
                state
                    .metrics
                    .record_publish_result(false, started_at.elapsed());
                return ControlResponse::error("audit_failure", error.to_string());
            }
            state
                .metrics
                .record_publish_result(true, started_at.elapsed());

            info!(
                principal = %identity.principal,
                principal_kind = %identity.principal_kind,
                quota_profile = %identity.quota_profile,
                token_id = %identity.token_id,
                resource = %resource,
                offset = message.offset,
                "message published"
            );
            ControlResponse::PublishAccepted {
                message_id: message.message_id,
                offset: message.offset,
                classification: message.classification,
            }
        }
        Err(error) => {
            state.metrics.record_storage_failure();
            state
                .metrics
                .record_publish_result(false, started_at.elapsed());
            let message = error.to_string();
            let _ = finalize_failure(
                state,
                &identity,
                Action::Publish,
                &resource,
                Some(message.clone()),
            )
            .await;
            ControlResponse::error("storage_error", message)
        }
    }
}

async fn handle_consume(
    state: &BrokerState,
    capability_token: String,
    topic: String,
    offset: u64,
    limit: usize,
) -> ControlResponse {
    let started_at = Instant::now();
    let resource = topic_resource(&topic);
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Consume,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => return response,
    };

    if let Err(response) = enforce_consume_quota(state, &identity, &resource, limit).await {
        return response;
    }

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Consume,
        &resource,
        Some(format!("consume from {topic} starting at {offset}")),
    )
    .await
    {
        return response;
    }

    let result = {
        let storage = state.storage.lock().await;
        let messages = storage.read_from(&topic, offset, limit);
        let next_offset = storage.next_offset(&topic);
        (messages, next_offset)
    };

    match result {
        (Ok(messages), Ok(next_offset)) => {
            if let Err(error) = finalize_success(
                state,
                &identity,
                Action::Consume,
                &resource,
                Some(format!("returned {} messages", messages.len())),
            )
            .await
            {
                state.metrics.record_audit_failure();
                state
                    .metrics
                    .record_consume_result(false, started_at.elapsed());
                return ControlResponse::error("audit_failure", error.to_string());
            }
            state
                .metrics
                .record_consume_result(true, started_at.elapsed());

            info!(
                principal = %identity.principal,
                principal_kind = %identity.principal_kind,
                quota_profile = %identity.quota_profile,
                token_id = %identity.token_id,
                resource = %resource,
                count = messages.len(),
                "messages consumed"
            );
            ControlResponse::Messages {
                topic,
                messages,
                next_offset,
            }
        }
        (Err(error), _) | (_, Err(error)) => {
            state.metrics.record_storage_failure();
            state
                .metrics
                .record_consume_result(false, started_at.elapsed());
            let message = error.to_string();
            let _ = finalize_failure(
                state,
                &identity,
                Action::Consume,
                &resource,
                Some(message.clone()),
            )
            .await;
            ControlResponse::error("storage_error", message)
        }
    }
}

async fn revocation_success_response(
    state: &BrokerState,
    identity: &RequestIdentity,
    resource: &str,
    revocations: RevocationList,
    detail: String,
) -> ControlResponse {
    if let Err(error) =
        finalize_success(state, identity, Action::Admin, resource, Some(detail)).await
    {
        state.metrics.record_audit_failure();
        return ControlResponse::error("audit_failure", error.to_string());
    }

    ControlResponse::RevocationUpdated {
        revocations: revocation_view(revocations),
    }
}

async fn revocation_failure_response(
    state: &BrokerState,
    identity: &RequestIdentity,
    resource: &str,
    detail: String,
) -> ControlResponse {
    let _ = finalize_failure(
        state,
        identity,
        Action::Admin,
        resource,
        Some(detail.clone()),
    )
    .await;
    ControlResponse::error("revocation_error", detail)
}

async fn enforce_publish_quota(
    state: &BrokerState,
    identity: &RequestIdentity,
    resource: &str,
    payload_bytes: usize,
) -> Result<(), ControlResponse> {
    match state
        .quotas
        .enforce_publish(&identity.principal, &identity.quota_profile, payload_bytes)
        .await
    {
        Ok(()) => Ok(()),
        Err(error) => {
            state.metrics.record_quota_denial();
            warn!(
                principal = %identity.principal,
                principal_kind = %identity.principal_kind,
                quota_profile = %identity.quota_profile,
                token_id = %identity.token_id,
                resource = %resource,
                error = %error,
                "publish denied by quota policy"
            );
            let _ = record_internal_event(
                state,
                &identity.principal,
                Action::Publish,
                resource,
                AuditDecision::Deny,
                AuditOutcome::Rejected,
                with_identity_detail(Some(error.to_string()), identity),
            )
            .await;
            Err(ControlResponse::error("quota_exceeded", error.to_string()))
        }
    }
}

async fn enforce_consume_quota(
    state: &BrokerState,
    identity: &RequestIdentity,
    resource: &str,
    limit: usize,
) -> Result<(), ControlResponse> {
    match state
        .quotas
        .enforce_consume(&identity.principal, &identity.quota_profile, limit)
        .await
    {
        Ok(()) => Ok(()),
        Err(error) => {
            state.metrics.record_quota_denial();
            warn!(
                principal = %identity.principal,
                principal_kind = %identity.principal_kind,
                quota_profile = %identity.quota_profile,
                token_id = %identity.token_id,
                resource = %resource,
                error = %error,
                "consume denied by quota policy"
            );
            let _ = record_internal_event(
                state,
                &identity.principal,
                Action::Consume,
                resource,
                AuditDecision::Deny,
                AuditOutcome::Rejected,
                with_identity_detail(Some(error.to_string()), identity),
            )
            .await;
            Err(ControlResponse::error("quota_exceeded", error.to_string()))
        }
    }
}

async fn authenticate_and_authorize(
    state: &BrokerState,
    capability_token: &str,
    action: Action,
    resource: &str,
) -> Result<RequestIdentity, ControlResponse> {
    let verified = match state.verifier.verify(capability_token) {
        Ok(verified) => verified,
        Err(error) => {
            state.metrics.record_auth_failure();
            warn!(
                action = %action,
                resource = %resource,
                error = %error,
                "capability verification failed"
            );
            let _ = record_internal_event(
                state,
                "auth:unknown",
                action.clone(),
                resource,
                AuditDecision::Deny,
                AuditOutcome::Rejected,
                Some(error.to_string()),
            )
            .await;
            return Err(ControlResponse::error(
                "invalid_capability",
                error.to_string(),
            ));
        }
    };

    if let Err(error) = verified.authorize(resource, &action) {
        state.metrics.record_policy_denial();
        warn!(
            principal = %verified.principal(),
            principal_kind = %verified.principal_kind().as_str(),
            quota_profile = %verified.quota_profile(),
            token_id = %verified.token_id(),
            action = %action,
            resource = %resource,
            error = %error,
            "capability scope denied request"
        );
        let _ = record_internal_event(
            state,
            verified.principal(),
            action.clone(),
            resource,
            AuditDecision::Deny,
            AuditOutcome::Rejected,
            Some(format!(
                "token {}: {}; quota_profile={}; principal_kind={}",
                verified.token_id(),
                error,
                verified.quota_profile(),
                verified.principal_kind().as_str()
            )),
        )
        .await;
        return Err(ControlResponse::error("access_denied", error.to_string()));
    }

    authorize_by_policy(state, &verified, resource, &action).await?;

    Ok(RequestIdentity {
        principal: verified.principal().to_owned(),
        principal_kind: verified.principal_kind().as_str().to_owned(),
        quota_profile: verified.quota_profile().to_owned(),
        token_id: verified.token_id(),
    })
}

async fn authorize_by_policy(
    state: &BrokerState,
    verified: &VerifiedCapability,
    resource: &str,
    action: &Action,
) -> Result<(), ControlResponse> {
    match state
        .policy
        .authorize(verified.principal(), resource, action)
    {
        Ok(evaluation) => {
            info!(
                principal = %verified.principal(),
                principal_kind = %verified.principal_kind().as_str(),
                quota_profile = %verified.quota_profile(),
                token_id = %verified.token_id(),
                action = %action,
                resource = %resource,
                rule = ?evaluation.matched_rule,
                "request authorized"
            );
            Ok(())
        }
        Err(error) => {
            state.metrics.record_policy_denial();
            warn!(
                principal = %verified.principal(),
                principal_kind = %verified.principal_kind().as_str(),
                quota_profile = %verified.quota_profile(),
                token_id = %verified.token_id(),
                action = %action,
                resource = %resource,
                error = %error,
                "request denied by policy"
            );
            let _ = record_internal_event(
                state,
                verified.principal(),
                action.clone(),
                resource,
                AuditDecision::Deny,
                AuditOutcome::Rejected,
                Some(format!(
                    "token {}: {}; quota_profile={}; principal_kind={}",
                    verified.token_id(),
                    error,
                    verified.quota_profile(),
                    verified.principal_kind().as_str()
                )),
            )
            .await;
            Err(ControlResponse::error("access_denied", error.to_string()))
        }
    }
}

async fn record_attempt(
    state: &BrokerState,
    identity: &RequestIdentity,
    action: Action,
    resource: &str,
    detail: Option<String>,
) -> Result<(), ControlResponse> {
    record_internal_event(
        state,
        &identity.principal,
        action,
        resource,
        AuditDecision::Allow,
        AuditOutcome::Attempted,
        with_identity_detail(detail, identity),
    )
    .await
    .map(|_| ())
    .map_err(|error| {
        state.metrics.record_audit_failure();
        ControlResponse::error("audit_failure", error.to_string())
    })
}

async fn finalize_success(
    state: &BrokerState,
    identity: &RequestIdentity,
    action: Action,
    resource: &str,
    detail: Option<String>,
) -> anyhow::Result<()> {
    record_internal_event(
        state,
        &identity.principal,
        action,
        resource,
        AuditDecision::Allow,
        AuditOutcome::Succeeded,
        with_identity_detail(detail, identity),
    )
    .await
    .map(|_| ())
}

async fn finalize_failure(
    state: &BrokerState,
    identity: &RequestIdentity,
    action: Action,
    resource: &str,
    detail: Option<String>,
) -> anyhow::Result<()> {
    record_internal_event(
        state,
        &identity.principal,
        action,
        resource,
        AuditDecision::Allow,
        AuditOutcome::Failed,
        with_identity_detail(detail, identity),
    )
    .await
    .map(|_| ())
}

fn with_identity_detail(detail: Option<String>, identity: &RequestIdentity) -> Option<String> {
    let suffix = format!(
        "token_id={}; quota_profile={}; principal_kind={}",
        identity.token_id, identity.quota_profile, identity.principal_kind
    );

    match detail {
        Some(detail) => Some(format!("{detail}; {suffix}")),
        None => Some(suffix),
    }
}

fn auth_state_view(snapshot: AuthSnapshot) -> AuthStateView {
    AuthStateView {
        audience: snapshot.audience,
        issuers: snapshot
            .issuers
            .into_iter()
            .map(|issuer| AuthIssuerView {
                key_id: issuer.key_id,
                status: issuer.status.as_str().to_owned(),
            })
            .collect(),
        principals: snapshot
            .principals
            .into_iter()
            .map(|principal| AuthPrincipalView {
                id: principal.id,
                kind: principal.kind.as_str().to_owned(),
                display_name: principal.display_name,
                status: principal.status.as_str().to_owned(),
                allowed_key_ids: principal.allowed_key_ids,
                quota_profile: principal.quota_profile,
            })
            .collect(),
        revocations: revocation_view(snapshot.revocations),
    }
}

fn revocation_view(mut revocations: RevocationList) -> AuthRevocationView {
    revocations.revoked_tokens.sort();
    revocations.revoked_principals.sort();
    revocations.revoked_key_ids.sort();

    AuthRevocationView {
        revoked_tokens: revocations.revoked_tokens,
        revoked_principals: revocations.revoked_principals,
        revoked_key_ids: revocations.revoked_key_ids,
    }
}

async fn record_internal_event(
    state: &BrokerState,
    principal: &str,
    action: Action,
    resource: &str,
    decision: AuditDecision,
    outcome: AuditOutcome,
    detail: Option<String>,
) -> anyhow::Result<()> {
    let mut audit = state.audit.lock().await;
    let event = audit.append(DraftAuditEvent {
        principal: principal.to_owned(),
        action,
        resource: resource.to_owned(),
        decision,
        outcome,
        detail,
    })?;

    info!(
        audit_path = %audit.path().display(),
        audit_hash = %event.hash,
        principal = %event.principal,
        action = %event.action.as_str(),
        resource = %event.resource,
        "audit event recorded"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use chrono::{Duration, Utc};
    use expressways_auth::{
        AuthConfig, CapabilityIssuer, IssuerStatus, PrincipalKind, PrincipalRecord,
        PrincipalStatus, TrustedIssuerConfig,
    };
    use expressways_policy::{DefaultDecision, PolicyConfig, Rule};
    use expressways_protocol::{
        AgentEndpoint, AgentQuery, AgentRegistration, CapabilityClaims, CapabilityScope,
        Classification, ControlCommand, ControlResponse, RegistryEventKind, RetentionClass,
        StreamFrame,
    };
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::duplex;
    use tokio_util::codec::{Framed, LinesCodec};
    use uuid::Uuid;

    use super::*;
    use crate::config::{AuditSection, RegistrySection, StorageSection};
    use crate::quota::{BackpressureMode, QuotaConfig, QuotaProfile};

    fn test_root() -> PathBuf {
        std::env::temp_dir().join(format!("expressways-server-{}", Uuid::now_v7()))
    }

    fn write_issuer(root: &Path, key_id: &str) -> (CapabilityIssuer, PathBuf) {
        let issuer = CapabilityIssuer::generate(key_id);
        let public_key_path = root.join("auth").join(format!("{key_id}.public"));
        issuer
            .write_public_key(&public_key_path)
            .expect("write public key");
        (issuer, public_key_path)
    }

    fn principal(id: &str, kind: PrincipalKind, quota_profile: &str) -> PrincipalRecord {
        PrincipalRecord {
            id: id.to_owned(),
            kind,
            display_name: id.to_owned(),
            status: PrincipalStatus::Active,
            allowed_key_ids: vec!["dev".to_owned()],
            quota_profile: quota_profile.to_owned(),
        }
    }

    fn quota_profile(name: &str, publish_payload_max_bytes: usize) -> QuotaProfile {
        QuotaProfile {
            name: name.to_owned(),
            publish_payload_max_bytes,
            publish_requests_per_window: 10,
            publish_window_seconds: 1,
            consume_max_limit: 20,
            consume_requests_per_window: 10,
            consume_window_seconds: 1,
            backpressure_mode: BackpressureMode::Reject,
            backpressure_delay_ms: 0,
        }
    }

    fn app_config(root: &Path, public_key_path: PathBuf) -> AppConfig {
        AppConfig {
            server: ServerConfig {
                node_name: "test-node".to_owned(),
                transport: TransportKind::Tcp,
                listen_addr: Some("127.0.0.1:7766".to_owned()),
                socket_path: None,
                data_dir: root.join("data"),
                log_level: "info".to_owned(),
            },
            storage: StorageSection {
                segment_max_bytes: 4096,
                retention_class: RetentionClass::Operational,
                default_classification: Classification::Internal,
                ephemeral_retention_bytes: 4096,
                operational_retention_bytes: 8192,
                regulated_retention_bytes: 16384,
                max_total_bytes: 65536,
                reclaim_target_bytes: 49152,
            },
            audit: AuditSection {
                path: root.join("audit").join("audit.jsonl"),
            },
            auth: AuthConfig {
                audience: "expressways".to_owned(),
                revocation_path: root.join("auth").join("revocations.json"),
                issuers: vec![TrustedIssuerConfig {
                    key_id: "dev".to_owned(),
                    public_key_path,
                    status: IssuerStatus::Active,
                }],
                principals: vec![
                    principal("local:developer", PrincipalKind::Developer, "operator"),
                    principal("local:agent-alpha", PrincipalKind::Agent, "agent"),
                ],
            },
            quotas: QuotaConfig {
                profiles: vec![quota_profile("operator", 1024), quota_profile("agent", 4)],
            },
            registry: RegistrySection {
                backend: crate::config::RegistryBackend::File,
                path: Some(root.join("registry").join("agents.json")),
                default_ttl_seconds: 300,
                event_history_limit: 64,
                stream_send_timeout_ms: 1_000,
                stream_idle_keepalive_limit: 12,
            },
            policy: PolicyConfig {
                default_decision: DefaultDecision::Deny,
                rules: vec![
                    Rule {
                        principal: "local:developer".to_owned(),
                        resource: "system:broker".to_owned(),
                        actions: vec![Action::Admin, Action::Health],
                    },
                    Rule {
                        principal: "local:developer".to_owned(),
                        resource: "topic:*".to_owned(),
                        actions: vec![Action::Admin, Action::Publish, Action::Consume],
                    },
                    Rule {
                        principal: "local:developer".to_owned(),
                        resource: "registry:agents*".to_owned(),
                        actions: vec![Action::Admin],
                    },
                    Rule {
                        principal: "local:agent-alpha".to_owned(),
                        resource: "topic:*".to_owned(),
                        actions: vec![Action::Publish, Action::Consume],
                    },
                    Rule {
                        principal: "local:agent-alpha".to_owned(),
                        resource: "registry:agents*".to_owned(),
                        actions: vec![Action::Admin],
                    },
                ],
            },
        }
    }

    fn issue_token(
        issuer: &CapabilityIssuer,
        principal: &str,
        scopes: Vec<CapabilityScope>,
    ) -> (Uuid, String) {
        let token_id = Uuid::now_v7();
        let token = issuer
            .issue(CapabilityClaims {
                token_id,
                principal: principal.to_owned(),
                audience: "expressways".to_owned(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + Duration::minutes(10),
                scopes,
            })
            .expect("issue token");
        (token_id, token)
    }

    #[tokio::test]
    async fn revoked_token_requests_are_denied_and_audited() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let config = app_config(&root, public_key_path);
        let state = build_state(&config).expect("build broker state");
        let (token_id, token) = issue_token(
            &issuer,
            "local:developer",
            vec![CapabilityScope {
                resource: BROKER_RESOURCE.to_owned(),
                actions: vec![Action::Health],
            }],
        );

        state.verifier.revoke_token(token_id).expect("revoke token");

        let response = process_request(
            &state,
            ControlRequest {
                capability_token: token,
                command: ControlCommand::Health,
            },
        )
        .await;

        match response {
            ControlResponse::Error { code, message } => {
                assert_eq!(code, "invalid_capability");
                assert!(message.contains("revoked"));
            }
            other => panic!("expected error response, got {other:?}"),
        }

        let audit = fs::read_to_string(&config.audit.path).expect("read audit");
        assert!(audit.contains("\"decision\":\"deny\""));
        assert!(audit.contains(&token_id.to_string()));
    }

    #[tokio::test]
    async fn quota_denials_are_rejected_and_audited() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let config = app_config(&root, public_key_path);
        let state = build_state(&config).expect("build broker state");
        let (_, token) = issue_token(
            &issuer,
            "local:agent-alpha",
            vec![CapabilityScope {
                resource: "topic:*".to_owned(),
                actions: vec![Action::Publish],
            }],
        );

        let response = process_request(
            &state,
            ControlRequest {
                capability_token: token,
                command: ControlCommand::Publish {
                    topic: "tasks".to_owned(),
                    classification: None,
                    payload: "payload-too-large".to_owned(),
                },
            },
        )
        .await;

        match response {
            ControlResponse::Error { code, message } => {
                assert_eq!(code, "quota_exceeded");
                assert!(message.contains("publish"));
            }
            other => panic!("expected error response, got {other:?}"),
        }

        let audit = fs::read_to_string(&config.audit.path).expect("read audit");
        assert!(audit.contains("\"decision\":\"deny\""));
        assert!(audit.contains("quota_profile=agent"));
    }

    #[tokio::test]
    async fn auth_state_and_revocation_admin_commands_work() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let config = app_config(&root, public_key_path);
        let state = build_state(&config).expect("build broker state");
        let (token_id, token) = issue_token(
            &issuer,
            "local:developer",
            vec![
                CapabilityScope {
                    resource: BROKER_RESOURCE.to_owned(),
                    actions: vec![Action::Admin, Action::Health],
                },
                CapabilityScope {
                    resource: "topic:*".to_owned(),
                    actions: vec![Action::Publish, Action::Consume, Action::Admin],
                },
            ],
        );

        let auth_state = process_request(
            &state,
            ControlRequest {
                capability_token: token.clone(),
                command: ControlCommand::GetAuthState,
            },
        )
        .await;

        match auth_state {
            ControlResponse::AuthState { state } => {
                assert_eq!(state.audience, "expressways");
                assert_eq!(state.issuers.len(), 1);
                assert_eq!(state.principals.len(), 2);
            }
            other => panic!("expected auth state response, got {other:?}"),
        }

        let revoke = process_request(
            &state,
            ControlRequest {
                capability_token: token,
                command: ControlCommand::RevokeToken { token_id },
            },
        )
        .await;

        match revoke {
            ControlResponse::RevocationUpdated { revocations } => {
                assert!(revocations.revoked_tokens.contains(&token_id));
            }
            other => panic!("expected revocation update, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn discovery_registry_commands_are_persisted_and_filtered() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let config = app_config(&root, public_key_path);
        let state = build_state(&config).expect("build broker state");
        let (_, agent_token) = issue_token(
            &issuer,
            "local:agent-alpha",
            vec![CapabilityScope {
                resource: "registry:agents*".to_owned(),
                actions: vec![Action::Admin],
            }],
        );
        let (_, developer_token) = issue_token(
            &issuer,
            "local:developer",
            vec![CapabilityScope {
                resource: "registry:agents*".to_owned(),
                actions: vec![Action::Admin],
            }],
        );

        let register = process_request(
            &state,
            ControlRequest {
                capability_token: agent_token.clone(),
                command: ControlCommand::RegisterAgent {
                    registration: AgentRegistration {
                        agent_id: "summarizer".to_owned(),
                        display_name: "Summarizer".to_owned(),
                        version: "1.2.3".to_owned(),
                        summary: "Summarizes large documents".to_owned(),
                        skills: vec!["pdf".to_owned(), "summarize".to_owned()],
                        subscriptions: vec!["topic:tasks".to_owned()],
                        publications: vec!["topic:results".to_owned()],
                        schemas: Vec::new(),
                        endpoint: AgentEndpoint {
                            transport: "control_tcp".to_owned(),
                            address: "127.0.0.1:8811".to_owned(),
                        },
                        classification: Classification::Internal,
                        retention_class: RetentionClass::Operational,
                        ttl_seconds: None,
                    },
                },
            },
        )
        .await;

        match register {
            ControlResponse::AgentRegistered { card } => {
                assert_eq!(card.agent_id, "summarizer");
                assert_eq!(card.principal, "local:agent-alpha");
            }
            other => panic!("expected agent registered response, got {other:?}"),
        }

        let listed = process_request(
            &state,
            ControlRequest {
                capability_token: developer_token.clone(),
                command: ControlCommand::ListAgents {
                    query: AgentQuery {
                        skill: Some("pdf".to_owned()),
                        topic: None,
                        principal: None,
                        include_stale: false,
                    },
                },
            },
        )
        .await;

        match listed {
            ControlResponse::Agents { agents, cursor } => {
                assert_eq!(agents.len(), 1);
                assert_eq!(agents[0].agent_id, "summarizer");
                assert!(cursor >= 1);
            }
            other => panic!("expected agents response, got {other:?}"),
        }

        let remove = process_request(
            &state,
            ControlRequest {
                capability_token: developer_token,
                command: ControlCommand::RemoveAgent {
                    agent_id: "summarizer".to_owned(),
                },
            },
        )
        .await;

        match remove {
            ControlResponse::AgentRemoved { agent_id } => {
                assert_eq!(agent_id, "summarizer");
            }
            other => panic!("expected agent removed response, got {other:?}"),
        }

        let persisted = fs::read_to_string(root.join("registry").join("agents.json"))
            .expect("read persisted registry");
        assert!(persisted.contains("\"agents\": []"));
    }

    #[tokio::test]
    async fn discovery_registry_prevents_cross_principal_takeover() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let config = app_config(&root, public_key_path);
        let state = build_state(&config).expect("build broker state");
        let (_, developer_token) = issue_token(
            &issuer,
            "local:developer",
            vec![CapabilityScope {
                resource: "registry:agents*".to_owned(),
                actions: vec![Action::Admin],
            }],
        );
        let (_, agent_token) = issue_token(
            &issuer,
            "local:agent-alpha",
            vec![CapabilityScope {
                resource: "registry:agents*".to_owned(),
                actions: vec![Action::Admin],
            }],
        );

        let _ = process_request(
            &state,
            ControlRequest {
                capability_token: agent_token,
                command: ControlCommand::RegisterAgent {
                    registration: AgentRegistration {
                        agent_id: "summarizer".to_owned(),
                        display_name: "Summarizer".to_owned(),
                        version: "1.0.0".to_owned(),
                        summary: "Summarizes large documents".to_owned(),
                        skills: vec!["summarize".to_owned()],
                        subscriptions: vec!["topic:tasks".to_owned()],
                        publications: vec!["topic:results".to_owned()],
                        schemas: Vec::new(),
                        endpoint: AgentEndpoint {
                            transport: "control_tcp".to_owned(),
                            address: "127.0.0.1:8811".to_owned(),
                        },
                        classification: Classification::Internal,
                        retention_class: RetentionClass::Operational,
                        ttl_seconds: None,
                    },
                },
            },
        )
        .await;

        let response = process_request(
            &state,
            ControlRequest {
                capability_token: developer_token,
                command: ControlCommand::RegisterAgent {
                    registration: AgentRegistration {
                        agent_id: "summarizer".to_owned(),
                        display_name: "Override".to_owned(),
                        version: "2.0.0".to_owned(),
                        summary: "Tries to take over the registration".to_owned(),
                        skills: vec!["rewrite".to_owned()],
                        subscriptions: vec!["topic:tasks".to_owned()],
                        publications: vec!["topic:results".to_owned()],
                        schemas: Vec::new(),
                        endpoint: AgentEndpoint {
                            transport: "control_tcp".to_owned(),
                            address: "127.0.0.1:9911".to_owned(),
                        },
                        classification: Classification::Internal,
                        retention_class: RetentionClass::Operational,
                        ttl_seconds: None,
                    },
                },
            },
        )
        .await;

        match response {
            ControlResponse::Error { code, message } => {
                assert_eq!(code, "registry_error");
                assert!(message.contains("owned by"));
            }
            other => panic!("expected registry error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn discovery_registry_heartbeat_and_cleanup_manage_stale_entries() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let config = app_config(&root, public_key_path);
        let state = build_state(&config).expect("build broker state");
        let (_, developer_token) = issue_token(
            &issuer,
            "local:developer",
            vec![CapabilityScope {
                resource: "registry:agents*".to_owned(),
                actions: vec![Action::Admin],
            }],
        );

        let register = process_request(
            &state,
            ControlRequest {
                capability_token: developer_token.clone(),
                command: ControlCommand::RegisterAgent {
                    registration: AgentRegistration {
                        agent_id: "ephemeral-agent".to_owned(),
                        display_name: "Ephemeral Agent".to_owned(),
                        version: "0.1.0".to_owned(),
                        summary: "Short-lived test agent".to_owned(),
                        skills: vec!["cleanup".to_owned()],
                        subscriptions: vec!["topic:tasks".to_owned()],
                        publications: vec!["topic:results".to_owned()],
                        schemas: Vec::new(),
                        endpoint: AgentEndpoint {
                            transport: "control_tcp".to_owned(),
                            address: "127.0.0.1:8822".to_owned(),
                        },
                        classification: Classification::Internal,
                        retention_class: RetentionClass::Ephemeral,
                        ttl_seconds: Some(1),
                    },
                },
            },
        )
        .await;

        match register {
            ControlResponse::AgentRegistered { .. } => {}
            other => panic!("expected agent registered response, got {other:?}"),
        };

        let registry_path = root.join("registry").join("agents.json");
        set_registry_expiry(
            &registry_path,
            "ephemeral-agent",
            Utc::now() - Duration::seconds(5),
        );

        let hidden = process_request(
            &state,
            ControlRequest {
                capability_token: developer_token.clone(),
                command: ControlCommand::ListAgents {
                    query: AgentQuery {
                        skill: None,
                        topic: None,
                        principal: None,
                        include_stale: false,
                    },
                },
            },
        )
        .await;
        match hidden {
            ControlResponse::Agents { agents, .. } => assert!(agents.is_empty()),
            other => panic!("expected agent registered response, got {other:?}"),
        }

        let heartbeat = process_request(
            &state,
            ControlRequest {
                capability_token: developer_token.clone(),
                command: ControlCommand::HeartbeatAgent {
                    agent_id: "ephemeral-agent".to_owned(),
                },
            },
        )
        .await;
        match heartbeat {
            ControlResponse::AgentHeartbeat { card } => {
                assert!(card.expires_at > Utc::now());
            }
            other => panic!("expected heartbeat response, got {other:?}"),
        }

        let now_visible = process_request(
            &state,
            ControlRequest {
                capability_token: developer_token.clone(),
                command: ControlCommand::ListAgents {
                    query: AgentQuery::default(),
                },
            },
        )
        .await;
        match now_visible {
            ControlResponse::Agents { agents, .. } => assert_eq!(agents.len(), 1),
            other => panic!("expected agents response, got {other:?}"),
        }

        set_registry_expiry(
            &registry_path,
            "ephemeral-agent",
            Utc::now() - Duration::seconds(5),
        );

        let cleanup = process_request(
            &state,
            ControlRequest {
                capability_token: developer_token.clone(),
                command: ControlCommand::CleanupStaleAgents,
            },
        )
        .await;
        match cleanup {
            ControlResponse::AgentsCleanedUp { removed_agent_ids } => {
                assert_eq!(removed_agent_ids, vec!["ephemeral-agent".to_owned()]);
            }
            other => panic!("expected cleanup response, got {other:?}"),
        }

        let remaining = process_request(
            &state,
            ControlRequest {
                capability_token: developer_token,
                command: ControlCommand::ListAgents {
                    query: AgentQuery {
                        include_stale: true,
                        ..AgentQuery::default()
                    },
                },
            },
        )
        .await;
        match remaining {
            ControlResponse::Agents { agents, .. } => assert!(agents.is_empty()),
            other => panic!("expected agents response, got {other:?}"),
        }
    }

    fn set_registry_expiry(path: &Path, agent_id: &str, expires_at: chrono::DateTime<Utc>) {
        let mut document: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(path).expect("read registry document"))
                .expect("parse registry document");
        let agents = document["agents"]
            .as_array_mut()
            .expect("registry agents array");
        let agent = agents
            .iter_mut()
            .find(|entry| entry["agent_id"] == agent_id)
            .expect("registry agent entry");
        agent["expires_at"] = serde_json::Value::String(expires_at.to_rfc3339());
        fs::write(
            path,
            serde_json::to_vec_pretty(&document).expect("serialize registry document"),
        )
        .expect("write registry document");
    }

    #[tokio::test]
    async fn watch_agents_returns_registry_event_batches() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let config = app_config(&root, public_key_path);
        let state = Arc::new(build_state(&config).expect("build broker state"));
        let (_, developer_token) = issue_token(
            &issuer,
            "local:developer",
            vec![CapabilityScope {
                resource: "registry:agents*".to_owned(),
                actions: vec![Action::Admin],
            }],
        );

        let listed = process_request(
            state.as_ref(),
            ControlRequest {
                capability_token: developer_token.clone(),
                command: ControlCommand::ListAgents {
                    query: AgentQuery::default(),
                },
            },
        )
        .await;
        let cursor = match listed {
            ControlResponse::Agents { cursor, .. } => cursor,
            other => panic!("expected agents response, got {other:?}"),
        };

        let watch_state = Arc::clone(&state);
        let watch_token = developer_token.clone();
        let watch = tokio::spawn(async move {
            process_request(
                watch_state.as_ref(),
                ControlRequest {
                    capability_token: watch_token,
                    command: ControlCommand::WatchAgents {
                        query: AgentQuery::default(),
                        cursor: Some(cursor),
                        max_events: 10,
                        wait_timeout_ms: 2_000,
                    },
                },
            )
            .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let register = process_request(
            state.as_ref(),
            ControlRequest {
                capability_token: developer_token.clone(),
                command: ControlCommand::RegisterAgent {
                    registration: AgentRegistration {
                        agent_id: "watch-agent".to_owned(),
                        display_name: "Watch Agent".to_owned(),
                        version: "1.0.0".to_owned(),
                        summary: "Emits a watch event".to_owned(),
                        skills: vec!["watch".to_owned()],
                        subscriptions: vec!["topic:tasks".to_owned()],
                        publications: vec!["topic:results".to_owned()],
                        schemas: Vec::new(),
                        endpoint: AgentEndpoint {
                            transport: "control_tcp".to_owned(),
                            address: "127.0.0.1:8855".to_owned(),
                        },
                        classification: Classification::Internal,
                        retention_class: RetentionClass::Operational,
                        ttl_seconds: None,
                    },
                },
            },
        )
        .await;
        assert!(matches!(register, ControlResponse::AgentRegistered { .. }));

        let watched = watch.await.expect("watch join");
        match watched {
            ControlResponse::RegistryEvents {
                events,
                cursor,
                timed_out,
            } => {
                assert!(!timed_out);
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].kind, RegistryEventKind::Registered);
                assert_eq!(events[0].card.agent_id, "watch-agent");
                assert!(cursor >= events[0].sequence);
            }
            other => panic!("expected registry events response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn watch_agents_times_out_cleanly_without_changes() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let config = app_config(&root, public_key_path);
        let state = build_state(&config).expect("build broker state");
        let (_, developer_token) = issue_token(
            &issuer,
            "local:developer",
            vec![CapabilityScope {
                resource: "registry:agents*".to_owned(),
                actions: vec![Action::Admin],
            }],
        );

        let watched = process_request(
            &state,
            ControlRequest {
                capability_token: developer_token,
                command: ControlCommand::WatchAgents {
                    query: AgentQuery::default(),
                    cursor: None,
                    max_events: 10,
                    wait_timeout_ms: 1,
                },
            },
        )
        .await;

        match watched {
            ControlResponse::RegistryEvents {
                events, timed_out, ..
            } => {
                assert!(events.is_empty());
                assert!(timed_out);
            }
            other => panic!("expected registry events response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn open_agent_watch_stream_emits_open_and_event_frames() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let config = app_config(&root, public_key_path);
        let state = Arc::new(build_state(&config).expect("build broker state"));
        let (_, developer_token) = issue_token(
            &issuer,
            "local:developer",
            vec![CapabilityScope {
                resource: "registry:agents*".to_owned(),
                actions: vec![Action::Admin],
            }],
        );

        let (client_stream, server_stream) = duplex(8 * 1024);
        let server_state = Arc::clone(&state);
        let server =
            tokio::spawn(async move { handle_connection(server_stream, server_state).await });
        let mut client = Framed::new(client_stream, LinesCodec::new());

        client
            .send(
                serde_json::to_string(&ControlRequest {
                    capability_token: developer_token.clone(),
                    command: ControlCommand::OpenAgentWatchStream {
                        query: AgentQuery::default(),
                        cursor: None,
                        max_events: 10,
                        wait_timeout_ms: 2_000,
                    },
                })
                .expect("serialize stream-open request"),
            )
            .await
            .expect("send stream-open request");

        let opened = client
            .next()
            .await
            .expect("opening frame")
            .expect("opening frame io");
        let opened: StreamFrame = serde_json::from_str(&opened).expect("parse opening frame");
        match opened {
            StreamFrame::AgentWatchOpened { cursor } => assert_eq!(cursor, 0),
            other => panic!("expected watch-opened frame, got {other:?}"),
        }

        let register = process_request(
            state.as_ref(),
            ControlRequest {
                capability_token: developer_token,
                command: ControlCommand::RegisterAgent {
                    registration: AgentRegistration {
                        agent_id: "stream-watch-agent".to_owned(),
                        display_name: "Stream Watch Agent".to_owned(),
                        version: "1.0.0".to_owned(),
                        summary: "Emits streamed watch events".to_owned(),
                        skills: vec!["watch".to_owned()],
                        subscriptions: vec!["topic:tasks".to_owned()],
                        publications: vec!["topic:results".to_owned()],
                        schemas: Vec::new(),
                        endpoint: AgentEndpoint {
                            transport: "control_tcp".to_owned(),
                            address: "127.0.0.1:8856".to_owned(),
                        },
                        classification: Classification::Internal,
                        retention_class: RetentionClass::Operational,
                        ttl_seconds: None,
                    },
                },
            },
        )
        .await;
        assert!(matches!(register, ControlResponse::AgentRegistered { .. }));

        let event_frame = client
            .next()
            .await
            .expect("event frame")
            .expect("event frame io");
        let event_frame: StreamFrame =
            serde_json::from_str(&event_frame).expect("parse event frame");
        match event_frame {
            StreamFrame::RegistryEvents { events, cursor } => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].kind, RegistryEventKind::Registered);
                assert_eq!(events[0].card.agent_id, "stream-watch-agent");
                assert!(cursor >= events[0].sequence);
            }
            other => panic!("expected registry-events frame, got {other:?}"),
        }

        drop(client);
        server
            .await
            .expect("server task join")
            .expect("server task");
    }

    #[tokio::test]
    async fn open_agent_watch_stream_emits_keepalive_when_idle() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let config = app_config(&root, public_key_path);
        let state = Arc::new(build_state(&config).expect("build broker state"));
        let (_, developer_token) = issue_token(
            &issuer,
            "local:developer",
            vec![CapabilityScope {
                resource: "registry:agents*".to_owned(),
                actions: vec![Action::Admin],
            }],
        );

        let (client_stream, server_stream) = duplex(8 * 1024);
        let server_state = Arc::clone(&state);
        let server =
            tokio::spawn(async move { handle_connection(server_stream, server_state).await });
        let mut client = Framed::new(client_stream, LinesCodec::new());

        client
            .send(
                serde_json::to_string(&ControlRequest {
                    capability_token: developer_token,
                    command: ControlCommand::OpenAgentWatchStream {
                        query: AgentQuery::default(),
                        cursor: None,
                        max_events: 10,
                        wait_timeout_ms: 1,
                    },
                })
                .expect("serialize stream-open request"),
            )
            .await
            .expect("send stream-open request");

        let opened = client
            .next()
            .await
            .expect("opening frame")
            .expect("opening frame io");
        let opened: StreamFrame = serde_json::from_str(&opened).expect("parse opening frame");
        assert!(matches!(opened, StreamFrame::AgentWatchOpened { .. }));

        let keepalive = client
            .next()
            .await
            .expect("keepalive frame")
            .expect("keepalive frame io");
        let keepalive: StreamFrame =
            serde_json::from_str(&keepalive).expect("parse keepalive frame");
        assert!(matches!(keepalive, StreamFrame::KeepAlive { .. }));

        drop(client);
        server
            .await
            .expect("server task join")
            .expect("server task");
    }

    #[tokio::test]
    async fn open_agent_watch_stream_closes_after_idle_limit_and_resumes_from_cursor() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let mut config = app_config(&root, public_key_path);
        config.registry.stream_idle_keepalive_limit = 2;
        let state = Arc::new(build_state(&config).expect("build broker state"));
        let (_, developer_token) = issue_token(
            &issuer,
            "local:developer",
            vec![CapabilityScope {
                resource: "registry:agents*".to_owned(),
                actions: vec![Action::Admin],
            }],
        );

        let (client_stream, server_stream) = duplex(8 * 1024);
        let server_state = Arc::clone(&state);
        let server =
            tokio::spawn(async move { handle_connection(server_stream, server_state).await });
        let mut client = Framed::new(client_stream, LinesCodec::new());

        client
            .send(
                serde_json::to_string(&ControlRequest {
                    capability_token: developer_token.clone(),
                    command: ControlCommand::OpenAgentWatchStream {
                        query: AgentQuery::default(),
                        cursor: None,
                        max_events: 10,
                        wait_timeout_ms: 1,
                    },
                })
                .expect("serialize stream-open request"),
            )
            .await
            .expect("send stream-open request");

        let opened = client
            .next()
            .await
            .expect("opening frame")
            .expect("opening frame io");
        let opened: StreamFrame = serde_json::from_str(&opened).expect("parse opening frame");
        assert!(matches!(
            opened,
            StreamFrame::AgentWatchOpened { cursor: 0 }
        ));

        let keepalive_one = client
            .next()
            .await
            .expect("first keepalive")
            .expect("first keepalive io");
        let keepalive_one: StreamFrame =
            serde_json::from_str(&keepalive_one).expect("parse first keepalive");
        assert!(matches!(
            keepalive_one,
            StreamFrame::KeepAlive { cursor: 0 }
        ));

        let keepalive_two = client
            .next()
            .await
            .expect("second keepalive")
            .expect("second keepalive io");
        let keepalive_two: StreamFrame =
            serde_json::from_str(&keepalive_two).expect("parse second keepalive");
        assert!(matches!(
            keepalive_two,
            StreamFrame::KeepAlive { cursor: 0 }
        ));

        let closed = client
            .next()
            .await
            .expect("closed frame")
            .expect("closed frame io");
        let closed: StreamFrame = serde_json::from_str(&closed).expect("parse closed frame");
        let resume_cursor = match closed {
            StreamFrame::StreamClosed { cursor, reason } => {
                assert!(reason.contains("idle_timeout_after_2_keepalives"));
                cursor
            }
            other => panic!("expected stream-closed frame, got {other:?}"),
        };

        drop(client);
        server
            .await
            .expect("server task join")
            .expect("server task");

        let (resume_client_stream, resume_server_stream) = duplex(8 * 1024);
        let resume_state = Arc::clone(&state);
        let resume_server =
            tokio::spawn(
                async move { handle_connection(resume_server_stream, resume_state).await },
            );
        let mut resume_client = Framed::new(resume_client_stream, LinesCodec::new());

        resume_client
            .send(
                serde_json::to_string(&ControlRequest {
                    capability_token: developer_token.clone(),
                    command: ControlCommand::OpenAgentWatchStream {
                        query: AgentQuery::default(),
                        cursor: Some(resume_cursor),
                        max_events: 10,
                        wait_timeout_ms: 2_000,
                    },
                })
                .expect("serialize resumed stream-open request"),
            )
            .await
            .expect("send resumed stream-open request");

        let resumed_open = resume_client
            .next()
            .await
            .expect("resumed opening frame")
            .expect("resumed opening frame io");
        let resumed_open: StreamFrame =
            serde_json::from_str(&resumed_open).expect("parse resumed opening frame");
        assert!(matches!(
            resumed_open,
            StreamFrame::AgentWatchOpened { cursor } if cursor == resume_cursor
        ));

        let register = process_request(
            state.as_ref(),
            ControlRequest {
                capability_token: developer_token,
                command: ControlCommand::RegisterAgent {
                    registration: AgentRegistration {
                        agent_id: "resumed-watch-agent".to_owned(),
                        display_name: "Resumed Watch Agent".to_owned(),
                        version: "1.0.0".to_owned(),
                        summary: "Arrives after reconnect".to_owned(),
                        skills: vec!["watch".to_owned()],
                        subscriptions: vec!["topic:tasks".to_owned()],
                        publications: vec!["topic:results".to_owned()],
                        schemas: Vec::new(),
                        endpoint: AgentEndpoint {
                            transport: "control_tcp".to_owned(),
                            address: "127.0.0.1:8857".to_owned(),
                        },
                        classification: Classification::Internal,
                        retention_class: RetentionClass::Operational,
                        ttl_seconds: None,
                    },
                },
            },
        )
        .await;
        assert!(matches!(register, ControlResponse::AgentRegistered { .. }));

        let resumed_event = resume_client
            .next()
            .await
            .expect("resumed event frame")
            .expect("resumed event frame io");
        let resumed_event: StreamFrame =
            serde_json::from_str(&resumed_event).expect("parse resumed event frame");
        match resumed_event {
            StreamFrame::RegistryEvents { events, cursor } => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].card.agent_id, "resumed-watch-agent");
                assert!(cursor >= events[0].sequence);
            }
            other => panic!("expected resumed registry-events frame, got {other:?}"),
        }

        drop(resume_client);
        resume_server
            .await
            .expect("resumed server task join")
            .expect("resumed server task");
    }

    #[tokio::test]
    async fn open_agent_watch_stream_drops_slow_consumers_and_records_metrics() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let mut config = app_config(&root, public_key_path);
        config.registry.stream_send_timeout_ms = 10;
        config.registry.stream_idle_keepalive_limit = 100;
        let state = Arc::new(build_state(&config).expect("build broker state"));
        let (_, developer_token) = issue_token(
            &issuer,
            "local:developer",
            vec![CapabilityScope {
                resource: "registry:agents*".to_owned(),
                actions: vec![Action::Admin],
            }],
        );

        let (client_stream, server_stream) = duplex(128);
        let server_state = Arc::clone(&state);
        let server =
            tokio::spawn(async move { handle_connection(server_stream, server_state).await });
        let mut client = Framed::new(client_stream, LinesCodec::new());

        client
            .send(
                serde_json::to_string(&ControlRequest {
                    capability_token: developer_token.clone(),
                    command: ControlCommand::OpenAgentWatchStream {
                        query: AgentQuery::default(),
                        cursor: None,
                        max_events: 10,
                        wait_timeout_ms: 2_000,
                    },
                })
                .expect("serialize stream-open request"),
            )
            .await
            .expect("send stream-open request");

        let opened = client
            .next()
            .await
            .expect("opening frame")
            .expect("opening frame io");
        let opened: StreamFrame = serde_json::from_str(&opened).expect("parse opening frame");
        assert!(matches!(opened, StreamFrame::AgentWatchOpened { .. }));

        let register = process_request(
            state.as_ref(),
            ControlRequest {
                capability_token: developer_token,
                command: ControlCommand::RegisterAgent {
                    registration: AgentRegistration {
                        agent_id: "slow-consumer-agent".to_owned(),
                        display_name: "Slow Consumer Agent".to_owned(),
                        version: "1.0.0".to_owned(),
                        summary: "x".repeat(8 * 1024),
                        skills: vec!["watch".to_owned()],
                        subscriptions: vec!["topic:tasks".to_owned()],
                        publications: vec!["topic:results".to_owned()],
                        schemas: Vec::new(),
                        endpoint: AgentEndpoint {
                            transport: "control_tcp".to_owned(),
                            address: "127.0.0.1:8858".to_owned(),
                        },
                        classification: Classification::Internal,
                        retention_class: RetentionClass::Operational,
                        ttl_seconds: None,
                    },
                },
            },
        )
        .await;
        assert!(matches!(register, ControlResponse::AgentRegistered { .. }));

        server
            .await
            .expect("server task join")
            .expect("server task");

        let storage_stats = {
            let storage = state.storage.lock().await;
            storage.stats().expect("storage stats")
        };
        let audit_summary = state.audit.lock().await.summary();
        let metrics = state.metrics.snapshot(storage_stats, audit_summary);
        assert_eq!(metrics.streams.slow_consumer_drops, 1);
        assert_eq!(metrics.streams.delivery_failures, 1);
        assert_eq!(metrics.streams.closed_streams, 1);
        assert_eq!(metrics.streams.open_streams, 0);
    }

    #[tokio::test]
    async fn metrics_command_reports_runtime_counters() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let config = app_config(&root, public_key_path);
        let state = build_state(&config).expect("build broker state");
        let (_, token) = issue_token(
            &issuer,
            "local:developer",
            vec![
                CapabilityScope {
                    resource: BROKER_RESOURCE.to_owned(),
                    actions: vec![Action::Admin, Action::Health],
                },
                CapabilityScope {
                    resource: "topic:*".to_owned(),
                    actions: vec![Action::Publish, Action::Consume, Action::Admin],
                },
            ],
        );

        let _ = process_request(
            &state,
            ControlRequest {
                capability_token: token.clone(),
                command: ControlCommand::Health,
            },
        )
        .await;
        let _ = process_request(
            &state,
            ControlRequest {
                capability_token: token.clone(),
                command: ControlCommand::CreateTopic {
                    topic: TopicSpec {
                        name: "tasks".to_owned(),
                        retention_class: RetentionClass::Operational,
                        default_classification: Classification::Internal,
                    },
                },
            },
        )
        .await;
        let _ = process_request(
            &state,
            ControlRequest {
                capability_token: token.clone(),
                command: ControlCommand::Publish {
                    topic: "tasks".to_owned(),
                    classification: None,
                    payload: "hello".to_owned(),
                },
            },
        )
        .await;

        let metrics_response = process_request(
            &state,
            ControlRequest {
                capability_token: token,
                command: ControlCommand::GetMetrics,
            },
        )
        .await;

        match metrics_response {
            ControlResponse::Metrics { metrics } => {
                assert!(metrics.total_requests >= 4);
                assert!(metrics.publish.requests >= 1);
                assert!(metrics.publish.successes >= 1);
                assert!(metrics.storage.topic_count >= 1);
                assert!(metrics.audit.event_count >= 1);
            }
            other => panic!("expected metrics response, got {other:?}"),
        }
    }
}
