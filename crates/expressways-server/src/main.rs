mod adopters;
mod artifacts;
mod config;
mod metrics;
mod quota;
mod registry;
mod registry_events;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use expressways_audit::{
    AuditDecision, AuditError, AuditLogSummary, AuditOutcome, AuditSink, DraftAuditEvent,
};
use expressways_auth::{AuthSnapshot, CapabilityVerifier, RevocationList, VerifiedCapability};
use expressways_policy::PolicyEngine;
use expressways_protocol::{
    ARTIFACT_COLLECTION_RESOURCE, Action, AgentQuery, AgentRegistration, AuthIssuerView,
    AuthPrincipalView, AuthRevocationView, AuthStateView, BROKER_RESOURCE, ControlCommand,
    ControlRequest, ControlResponse, ControlWireEnvelope, REGISTRY_RESOURCE, RegistryEventKind,
    RetentionClass, StreamFrame, TopicSpec, artifact_resource, registry_entry_resource,
    topic_resource,
};
use expressways_storage::{DiskPressurePolicy, RetentionPolicy, Storage, StorageConfig};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::adopters::AdopterManager;
use crate::artifacts::{ArtifactError, ArtifactStore, PutArtifactRequest};
use crate::config::{AppConfig, RegistryBackend, ResilienceSection, ServerConfig, TransportKind};
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
    resilience: ResilienceRuntime,
    service_mode: ServiceModeTracker,
    metrics: MetricsCollector,
    storage: Mutex<Option<Storage>>,
    artifacts: Mutex<Option<ArtifactStore>>,
    registry: Mutex<AgentRegistry>,
    registry_events: RegistryEventHub,
    adopters: AdopterManager,
    stream_limits: StreamLimits,
    audit: Mutex<ManagedAuditSink>,
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

#[derive(Debug, Clone)]
struct ResilienceRuntime {
    allow_degraded_runtime: bool,
    listener_retry_delay: Duration,
}

#[derive(Debug, Default)]
struct ServiceModeState {
    degraded_components: BTreeMap<String, String>,
}

#[derive(Debug, Default)]
pub(crate) struct ServiceModeTracker {
    inner: StdMutex<ServiceModeState>,
}

impl ServiceModeTracker {
    fn mark_degraded(&self, component: impl Into<String>, detail: impl Into<String>) {
        self.inner
            .lock()
            .expect("service mode lock")
            .degraded_components
            .insert(component.into(), detail.into());
    }

    fn clear_component(&self, component: &str) {
        self.inner
            .lock()
            .expect("service mode lock")
            .degraded_components
            .remove(component);
    }

    fn status(&self) -> String {
        if self
            .inner
            .lock()
            .expect("service mode lock")
            .degraded_components
            .is_empty()
        {
            "ok".to_owned()
        } else {
            "degraded".to_owned()
        }
    }

    fn degraded_components(&self) -> Vec<String> {
        self.inner
            .lock()
            .expect("service mode lock")
            .degraded_components
            .iter()
            .map(|(component, detail)| format!("{component}: {detail}"))
            .collect()
    }
}

#[derive(Debug)]
struct ManagedAuditSink {
    path: std::path::PathBuf,
    sink: Option<AuditSink>,
    retry_attempts: u32,
    retry_backoff: Duration,
}

impl ManagedAuditSink {
    fn new(path: std::path::PathBuf, resilience: &ResilienceSection) -> Result<Self, AuditError> {
        Ok(Self {
            sink: Some(AuditSink::new(&path)?),
            path,
            retry_attempts: resilience.audit_retry_attempts.max(1),
            retry_backoff: Duration::from_millis(resilience.audit_retry_backoff_ms.max(1)),
        })
    }

    fn disabled(path: std::path::PathBuf, resilience: &ResilienceSection) -> Self {
        Self {
            path,
            sink: None,
            retry_attempts: resilience.audit_retry_attempts.max(1),
            retry_backoff: Duration::from_millis(resilience.audit_retry_backoff_ms.max(1)),
        }
    }

    async fn append(
        &mut self,
        draft: DraftAuditEvent,
    ) -> Result<expressways_audit::AuditEvent, AuditError> {
        let mut last_error = None;

        for attempt in 0..self.retry_attempts {
            match self.try_append(draft.clone()) {
                Ok(event) => return Ok(event),
                Err(error) => {
                    last_error = Some(error);
                    self.sink = None;
                    if attempt + 1 < self.retry_attempts {
                        tokio::time::sleep(self.retry_backoff).await;
                    }
                }
            }
        }

        Err(last_error.expect("audit append should fail with an error"))
    }

    fn try_append(
        &mut self,
        draft: DraftAuditEvent,
    ) -> Result<expressways_audit::AuditEvent, AuditError> {
        if self.sink.is_none() {
            self.sink = Some(AuditSink::new(&self.path)?);
        }

        self.sink
            .as_mut()
            .expect("audit sink should be initialized")
            .append(draft)
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn summary(&self) -> AuditLogSummary {
        self.sink
            .as_ref()
            .map(AuditSink::summary)
            .unwrap_or(AuditLogSummary {
                event_count: 0,
                last_hash: None,
            })
    }
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
    state.adopters.probe_now(&state.service_mode);
    spawn_background_adopter_probes(Arc::clone(&state));
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
    let service_mode = ServiceModeTracker::default();
    let resilience = ResilienceRuntime {
        allow_degraded_runtime: config.resilience.allow_degraded_runtime,
        listener_retry_delay: Duration::from_millis(
            config.resilience.listener_retry_delay_ms.max(1),
        ),
    };

    let storage = match Storage::new(StorageConfig {
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
    }) {
        Ok(storage) => Some(storage),
        Err(error) if config.resilience.allow_degraded_startup => {
            service_mode.mark_degraded("storage", error.to_string());
            warn!(error = %error, "storage initialization failed; starting in degraded mode");
            None
        }
        Err(error) => return Err(error.into()),
    };
    let audit = match ManagedAuditSink::new(config.audit.path.clone(), &config.resilience) {
        Ok(audit) => audit,
        Err(error) if config.resilience.allow_degraded_startup => {
            service_mode.mark_degraded("audit", error.to_string());
            warn!(error = %error, "audit initialization failed; starting in degraded mode");
            ManagedAuditSink::disabled(config.audit.path.clone(), &config.resilience)
        }
        Err(error) => return Err(error.into()),
    };
    let artifacts = match ArtifactStore::new(config.server.data_dir.join("artifacts")) {
        Ok(artifacts) => Some(artifacts),
        Err(error) if config.resilience.allow_degraded_startup => {
            service_mode.mark_degraded("artifacts", error.to_string());
            warn!(error = %error, "artifact store initialization failed; starting in degraded mode");
            None
        }
        Err(error) => return Err(error.into()),
    };
    let verifier = CapabilityVerifier::from_config(&config.auth)?;
    let quotas = QuotaManager::new(config.quotas.clone())?;
    let registry = build_registry(config);
    let registry_events = RegistryEventHub::new(config.registry.event_history_limit);
    let adopters = AdopterManager::build(config)?;
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
        resilience,
        service_mode,
        metrics: MetricsCollector::new(),
        storage: Mutex::new(storage),
        artifacts: Mutex::new(artifacts),
        registry: Mutex::new(registry),
        registry_events,
        adopters,
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

fn spawn_background_adopter_probes(state: Arc<BrokerState>) {
    if !state.adopters.has_enabled() {
        return;
    }

    tokio::spawn(async move {
        let interval = state.adopters.probe_interval();
        loop {
            tokio::time::sleep(interval).await;
            state.adopters.probe_now(&state.service_mode);
        }
    });
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
                let (stream, remote) = match accept {
                    Ok(parts) => parts,
                    Err(error) => {
                        warn!(error = %error, retry_delay_ms = state.resilience.listener_retry_delay.as_millis(), "failed to accept TCP connection; retrying");
                        tokio::time::sleep(state.resilience.listener_retry_delay).await;
                        continue;
                    }
                };
                if let Err(error) = stream.set_nodelay(true) {
                    warn!(error = %error, remote = %remote, "failed to set TCP_NODELAY");
                }
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
                let (stream, _) = match accept {
                    Ok(parts) => parts,
                    Err(error) => {
                        warn!(error = %error, retry_delay_ms = state.resilience.listener_retry_delay.as_millis(), "failed to accept Unix connection; retrying");
                        tokio::time::sleep(state.resilience.listener_retry_delay).await;
                        continue;
                    }
                };
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
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

    while let Some(frame) = framed.next().await {
        let packet = match frame {
            Ok(packet) => packet,
            Err(error) => {
                warn!(error = %error, "failed to read request frame");
                continue;
            }
        };

        let (request, attachment) = match decode_request_packet(&packet) {
            Ok(request) => request,
            Err(error) => {
                warn!(error = %error, "failed to decode request");
                send_response_packet(
                    &mut framed,
                    ControlResponse::error("invalid_request", error),
                    None,
                )
                .await?;
                continue;
            }
        };

        if matches!(request.command, ControlCommand::OpenAgentWatchStream { .. }) {
            serve_streaming_request(&mut framed, &state, request).await?;
            break;
        }

        let (response, attachment) =
            process_request_with_attachment(&state, request, attachment).await;
        send_response_packet(&mut framed, response, attachment).await?;
    }

    Ok(())
}

async fn serve_streaming_request<T>(
    framed: &mut Framed<T, LengthDelimitedCodec>,
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

fn decode_request_packet(packet: &[u8]) -> Result<(ControlRequest, Option<Vec<u8>>), String> {
    let (envelope, attachment) = ControlWireEnvelope::decode_packet(packet)?;
    match envelope {
        ControlWireEnvelope::Request { request, .. } => {
            Ok((request, (!attachment.is_empty()).then_some(attachment)))
        }
        other => Err(format!("expected request packet, got {other:?}")),
    }
}

async fn send_response_packet<T>(
    framed: &mut Framed<T, LengthDelimitedCodec>,
    response: ControlResponse,
    attachment: Option<Vec<u8>>,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let packet = ControlWireEnvelope::Response {
        response,
        attachment_length: 0,
    }
    .encode_with_attachment(attachment.as_deref())?;
    framed.send(packet.into()).await?;
    Ok(())
}

async fn send_stream_frame<T>(
    framed: &mut Framed<T, LengthDelimitedCodec>,
    frame: &StreamFrame,
    send_timeout: Duration,
) -> Result<(), StreamSendError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let serialized = ControlWireEnvelope::Stream {
        frame: frame.clone(),
    }
    .encode_with_attachment(None)
    .map_err(StreamSendError::from)?;
    tokio::time::timeout(send_timeout, framed.send(serialized.into()))
        .await
        .map_err(|_| StreamSendError::TimedOut)?
        .map_err(|error| StreamSendError::Failed(error.into()))?;
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

impl From<tokio_util::codec::LengthDelimitedCodecError> for StreamSendError {
    fn from(error: tokio_util::codec::LengthDelimitedCodecError) -> Self {
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
    process_request_with_attachment(state, request, None)
        .await
        .0
}

async fn process_request_with_attachment(
    state: &BrokerState,
    request: ControlRequest,
    attachment: Option<Vec<u8>>,
) -> (ControlResponse, Option<Vec<u8>>) {
    let action = match &request.command {
        ControlCommand::Health => Action::Health,
        ControlCommand::Publish { .. } | ControlCommand::PutArtifact { .. } => Action::Publish,
        ControlCommand::Consume { .. }
        | ControlCommand::GetArtifact { .. }
        | ControlCommand::StatArtifact { .. } => Action::Consume,
        ControlCommand::GetAuthState
        | ControlCommand::GetAdopters
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

    let response = match request.command {
        ControlCommand::Health => (handle_health(state, request.capability_token).await, None),
        ControlCommand::GetMetrics => (
            handle_get_metrics(state, request.capability_token).await,
            None,
        ),
        ControlCommand::GetAdopters => (
            handle_get_adopters(state, request.capability_token).await,
            None,
        ),
        ControlCommand::GetAuthState => (
            handle_get_auth_state(state, request.capability_token).await,
            None,
        ),
        ControlCommand::RegisterAgent { registration } => (
            handle_register_agent(state, request.capability_token, registration).await,
            None,
        ),
        ControlCommand::HeartbeatAgent { agent_id } => (
            handle_heartbeat_agent(state, request.capability_token, agent_id).await,
            None,
        ),
        ControlCommand::ListAgents { query } => (
            handle_list_agents(state, request.capability_token, query).await,
            None,
        ),
        ControlCommand::WatchAgents {
            query,
            cursor,
            max_events,
            wait_timeout_ms,
        } => (
            handle_watch_agents(
                state,
                request.capability_token,
                query,
                cursor,
                max_events,
                wait_timeout_ms,
            )
            .await,
            None,
        ),
        ControlCommand::OpenAgentWatchStream { .. } => (
            ControlResponse::error(
                "invalid_request",
                "streaming watch must use the streaming transport",
            ),
            None,
        ),
        ControlCommand::CleanupStaleAgents => (
            handle_cleanup_stale_agents(state, request.capability_token).await,
            None,
        ),
        ControlCommand::RemoveAgent { agent_id } => (
            handle_remove_agent(state, request.capability_token, agent_id).await,
            None,
        ),
        ControlCommand::CreateTopic { topic } => (
            handle_create_topic(state, request.capability_token, topic).await,
            None,
        ),
        ControlCommand::RevokeToken { token_id } => (
            handle_revoke_token(state, request.capability_token, token_id).await,
            None,
        ),
        ControlCommand::RevokePrincipal { principal } => (
            handle_revoke_principal(state, request.capability_token, principal).await,
            None,
        ),
        ControlCommand::RevokeKey { key_id } => (
            handle_revoke_key(state, request.capability_token, key_id).await,
            None,
        ),
        ControlCommand::PutArtifact {
            artifact_id,
            content_type,
            byte_length,
            sha256,
            classification,
            retention_class,
        } => (
            handle_put_artifact(
                state,
                request.capability_token,
                artifact_id,
                content_type,
                byte_length,
                sha256,
                classification,
                retention_class,
                attachment,
            )
            .await,
            None,
        ),
        ControlCommand::GetArtifact { artifact_id } => {
            handle_get_artifact(state, request.capability_token, artifact_id).await
        }
        ControlCommand::StatArtifact { artifact_id } => (
            handle_stat_artifact(state, request.capability_token, artifact_id).await,
            None,
        ),
        ControlCommand::Publish {
            topic,
            classification,
            payload,
        } => (
            handle_publish(
                state,
                request.capability_token,
                topic,
                classification,
                payload,
            )
            .await,
            None,
        ),
        ControlCommand::Consume {
            topic,
            offset,
            limit,
        } => (
            handle_consume(state, request.capability_token, topic, offset, limit).await,
            None,
        ),
    };

    response
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
        status: state.service_mode.status(),
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

    let storage_stats = match with_storage(state, |storage| storage.stats()).await {
        Ok(stats) => stats,
        Err(message) if is_degraded_service_response(&message) => unavailable_storage_stats(),
        Err(message) => {
            state.metrics.record_storage_failure();
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
    };
    let audit_summary = state.audit.lock().await.summary();
    let metrics = state.metrics.snapshot(
        storage_stats,
        audit_summary,
        state.service_mode.status(),
        state.service_mode.degraded_components(),
        state.adopters.snapshot(),
    );

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

async fn handle_get_adopters(state: &BrokerState, capability_token: String) -> ControlResponse {
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
        Some("fetch adopter status".to_owned()),
    )
    .await
    {
        return response;
    }

    let adopters = state.adopters.snapshot();

    if let Err(error) = finalize_success(
        state,
        &identity,
        Action::Admin,
        &resource,
        Some(format!("fetched {} adopters", adopters.len())),
    )
    .await
    {
        return ControlResponse::error("audit_failure", error.to_string());
    }

    ControlResponse::Adopters { adopters }
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

    let created = with_storage(state, move |storage| storage.ensure_topic(topic)).await;

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
            storage_error_response(message)
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

    let principal = identity.principal.clone();
    let appended = with_storage(state, move |storage| {
        storage.append(&topic, &principal, classification, payload)
    })
    .await;

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
            storage_error_response(message)
        }
    }
}

async fn handle_put_artifact(
    state: &BrokerState,
    capability_token: String,
    artifact_id: Option<String>,
    content_type: String,
    byte_length: u64,
    sha256: Option<String>,
    classification: Option<expressways_protocol::Classification>,
    retention_class: Option<RetentionClass>,
    attachment: Option<Vec<u8>>,
) -> ControlResponse {
    let started_at = Instant::now();
    let resource = ARTIFACT_COLLECTION_RESOURCE.to_owned();
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

    let bytes = match attachment {
        Some(bytes) => bytes,
        None => {
            let message = "put-artifact requires a binary attachment".to_owned();
            let _ = finalize_failure(
                state,
                &identity,
                Action::Publish,
                &resource,
                Some(message.clone()),
            )
            .await;
            return ControlResponse::error("invalid_request", message);
        }
    };
    if bytes.len() as u64 != byte_length {
        let message = format!(
            "artifact attachment length mismatch: declared {byte_length} bytes, got {}",
            bytes.len()
        );
        let _ = finalize_failure(
            state,
            &identity,
            Action::Publish,
            &resource,
            Some(message.clone()),
        )
        .await;
        return ControlResponse::error("invalid_request", message);
    }

    if let Err(response) = enforce_publish_quota(state, &identity, &resource, bytes.len()).await {
        return response;
    }

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Publish,
        &resource,
        Some("store broker-managed artifact".to_owned()),
    )
    .await
    {
        return response;
    }

    let artifacts = match acquire_artifact_store(state).await {
        Ok(artifacts) => artifacts,
        Err(message) => {
            let _ = finalize_failure(
                state,
                &identity,
                Action::Publish,
                &resource,
                Some(message.clone()),
            )
            .await;
            return ControlResponse::error("service_degraded", message);
        }
    };

    match artifacts.put(PutArtifactRequest {
        artifact_id,
        content_type,
        data: bytes,
        sha256,
        classification: classification.unwrap_or_default(),
        retention_class: retention_class.unwrap_or_default(),
        principal: identity.principal.clone(),
    }) {
        Ok(artifact) => {
            state.service_mode.clear_component("artifacts");
            if let Err(error) = finalize_success(
                state,
                &identity,
                Action::Publish,
                &resource,
                Some(format!(
                    "stored artifact {} ({} bytes)",
                    artifact.artifact_id, artifact.byte_length
                )),
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
            ControlResponse::ArtifactStored { artifact }
        }
        Err(error) => {
            state
                .metrics
                .record_publish_result(false, started_at.elapsed());
            let message = error.to_string();
            handle_artifact_store_error(state, &error);
            let _ =
                finalize_failure(state, &identity, Action::Publish, &resource, Some(message)).await;
            artifact_error_response(error)
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

    let topic_for_read = topic.clone();
    let result = with_storage(state, move |storage| {
        let messages = storage.read_from(&topic_for_read, offset, limit)?;
        let next_offset = storage.next_offset(&topic_for_read)?;
        Ok((messages, next_offset))
    })
    .await;

    match result {
        Ok((messages, next_offset)) => {
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
        Err(error) => {
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
            storage_error_response(message)
        }
    }
}

async fn handle_get_artifact(
    state: &BrokerState,
    capability_token: String,
    artifact_id: String,
) -> (ControlResponse, Option<Vec<u8>>) {
    let started_at = Instant::now();
    let resource = artifact_resource(&artifact_id);
    let identity = match authenticate_and_authorize(
        state,
        &capability_token,
        Action::Consume,
        &resource,
    )
    .await
    {
        Ok(identity) => identity,
        Err(response) => return (response, None),
    };

    if let Err(response) = enforce_consume_quota(state, &identity, &resource, 1).await {
        return (response, None);
    }

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Consume,
        &resource,
        Some(format!("fetch artifact {artifact_id}")),
    )
    .await
    {
        return (response, None);
    }

    let artifacts = match acquire_artifact_store(state).await {
        Ok(artifacts) => artifacts,
        Err(message) => {
            let _ = finalize_failure(
                state,
                &identity,
                Action::Consume,
                &resource,
                Some(message.clone()),
            )
            .await;
            return (ControlResponse::error("service_degraded", message), None);
        }
    };

    match artifacts.get(&artifact_id) {
        Ok((artifact, bytes)) => {
            state.service_mode.clear_component("artifacts");
            if let Err(error) = finalize_success(
                state,
                &identity,
                Action::Consume,
                &resource,
                Some(format!(
                    "returned artifact {} ({} bytes)",
                    artifact.artifact_id, artifact.byte_length
                )),
            )
            .await
            {
                state.metrics.record_audit_failure();
                state
                    .metrics
                    .record_consume_result(false, started_at.elapsed());
                return (
                    ControlResponse::error("audit_failure", error.to_string()),
                    None,
                );
            }
            state
                .metrics
                .record_consume_result(true, started_at.elapsed());
            (ControlResponse::Artifact { artifact }, Some(bytes))
        }
        Err(error) => {
            state
                .metrics
                .record_consume_result(false, started_at.elapsed());
            let message = error.to_string();
            handle_artifact_store_error(state, &error);
            let _ =
                finalize_failure(state, &identity, Action::Consume, &resource, Some(message)).await;
            (artifact_error_response(error), None)
        }
    }
}

async fn handle_stat_artifact(
    state: &BrokerState,
    capability_token: String,
    artifact_id: String,
) -> ControlResponse {
    let started_at = Instant::now();
    let resource = artifact_resource(&artifact_id);
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

    if let Err(response) = enforce_consume_quota(state, &identity, &resource, 1).await {
        return response;
    }

    if let Err(response) = record_attempt(
        state,
        &identity,
        Action::Consume,
        &resource,
        Some(format!("inspect artifact {artifact_id}")),
    )
    .await
    {
        return response;
    }

    let artifacts = match acquire_artifact_store(state).await {
        Ok(artifacts) => artifacts,
        Err(message) => {
            let _ = finalize_failure(
                state,
                &identity,
                Action::Consume,
                &resource,
                Some(message.clone()),
            )
            .await;
            return ControlResponse::error("service_degraded", message);
        }
    };

    match artifacts.stat(&artifact_id) {
        Ok(artifact) => {
            state.service_mode.clear_component("artifacts");
            if let Err(error) = finalize_success(
                state,
                &identity,
                Action::Consume,
                &resource,
                Some(format!(
                    "returned artifact metadata {}",
                    artifact.artifact_id
                )),
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
            ControlResponse::ArtifactMetadata { artifact }
        }
        Err(error) => {
            state
                .metrics
                .record_consume_result(false, started_at.elapsed());
            let message = error.to_string();
            handle_artifact_store_error(state, &error);
            let _ =
                finalize_failure(state, &identity, Action::Consume, &resource, Some(message)).await;
            artifact_error_response(error)
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

fn unavailable_storage_stats() -> expressways_storage::StorageStats {
    expressways_storage::StorageStats {
        topic_count: 0,
        segment_count: 0,
        total_bytes: 0,
        maintenance: expressways_storage::MaintenanceStats::default(),
    }
}

async fn with_storage<T>(
    state: &BrokerState,
    op: impl FnOnce(&Storage) -> Result<T, expressways_storage::StorageError>,
) -> Result<T, String> {
    let storage = state.storage.lock().await;
    let storage = match storage.as_ref() {
        Some(storage) => storage,
        None => {
            return Err(
                "storage subsystem unavailable; broker is running in degraded mode".to_owned(),
            );
        }
    };

    match op(storage) {
        Ok(result) => {
            state.service_mode.clear_component("storage");
            Ok(result)
        }
        Err(error) => {
            let message = error.to_string();
            state.service_mode.mark_degraded("storage", message.clone());
            Err(message)
        }
    }
}

async fn acquire_artifact_store(state: &BrokerState) -> Result<ArtifactStore, String> {
    let artifacts = state.artifacts.lock().await;
    match artifacts.as_ref() {
        Some(artifacts) => Ok(artifacts.clone()),
        None => {
            Err("artifact subsystem unavailable; broker is running in degraded mode".to_owned())
        }
    }
}

fn is_degraded_service_response(message: &str) -> bool {
    message.contains("degraded mode")
}

fn storage_error_response(message: String) -> ControlResponse {
    if is_degraded_service_response(&message) {
        ControlResponse::error("service_degraded", message)
    } else {
        ControlResponse::error("storage_error", message)
    }
}

fn handle_artifact_store_error(state: &BrokerState, error: &ArtifactError) {
    if matches!(
        error,
        ArtifactError::Io(_) | ArtifactError::Serialization(_)
    ) {
        state.metrics.record_storage_failure();
        state
            .service_mode
            .mark_degraded("artifacts", error.to_string());
    }
}

fn artifact_error_response(error: ArtifactError) -> ControlResponse {
    match error {
        ArtifactError::InvalidArtifactId(_) | ArtifactError::EmptyContentType => {
            ControlResponse::error("invalid_request", error.to_string())
        }
        ArtifactError::AlreadyExists(_) => {
            ControlResponse::error("artifact_exists", error.to_string())
        }
        ArtifactError::Missing(_) => {
            ControlResponse::error("artifact_not_found", error.to_string())
        }
        ArtifactError::Sha256Mismatch { .. } => {
            ControlResponse::error("integrity_error", error.to_string())
        }
        ArtifactError::Io(_) | ArtifactError::Serialization(_) => {
            ControlResponse::error("artifact_error", error.to_string())
        }
    }
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
    .or_else(|error| {
        handle_audit_failure(state, error)
            .map_err(|error| ControlResponse::error("audit_failure", error.to_string()))
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
    .or_else(|error| handle_audit_failure(state, error))
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
    .or_else(|error| handle_audit_failure(state, error))
}

fn handle_audit_failure(state: &BrokerState, error: anyhow::Error) -> anyhow::Result<()> {
    state.metrics.record_audit_failure();
    if state.resilience.allow_degraded_runtime {
        let detail = error.to_string();
        state.service_mode.mark_degraded("audit", detail.clone());
        warn!(error = %detail, "audit subsystem degraded; continuing to serve requests");
        Ok(())
    } else {
        Err(error)
    }
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
    let event = audit
        .append(DraftAuditEvent {
            principal: principal.to_owned(),
            action,
            resource: resource.to_owned(),
            decision,
            outcome,
            detail,
        })
        .await?;
    state.service_mode.clear_component("audit");

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
    use std::sync::Arc;
    use std::time::Duration as StdDuration;

    use chrono::{Duration, Utc};
    use expressways_auth::{
        AuthConfig, CapabilityIssuer, IssuerStatus, PrincipalKind, PrincipalRecord,
        PrincipalStatus, TrustedIssuerConfig,
    };
    use expressways_client::{
        AgentWorker, BoxedClientIo, CustomEndpoint, Endpoint, WorkerRunOutcome,
    };
    use expressways_policy::{DefaultDecision, PolicyConfig, Rule};
    use expressways_protocol::{
        Action, AgentEndpoint, AgentQuery, AgentRegistration, CapabilityClaims, CapabilityScope,
        Classification, ControlCommand, ControlRequest, ControlResponse, ControlWireEnvelope,
        RegistryEventKind, RetentionClass, StoredMessage, StreamFrame, TASK_EVENTS_TOPIC,
        TASKS_TOPIC, TaskEvent, TaskPayload, TaskRequirements, TaskRetryPolicy, TaskStatus,
        TaskWorkItem, TopicSpec,
    };
    use futures_util::{SinkExt, StreamExt};
    use serde_json::json;
    use tokio::io::duplex;
    use tokio::time::{Instant, sleep};
    use tokio_util::codec::{Framed, LengthDelimitedCodec};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::*;
    use crate::config::{
        AdoptersSection, AuditSection, RegistrySection, ResilienceSection, StorageSection,
    };
    use crate::quota::{BackpressureMode, QuotaConfig, QuotaProfile};

    #[allow(dead_code)]
    mod orchestrator_bin {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../expressways-orchestrator/src/main.rs"
        ));
    }

    #[allow(dead_code)]
    mod sample_agent_bin {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../expressways-client/src/bin/expressways-agent-example.rs"
        ));
    }

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
            resilience: ResilienceSection::default(),
            adopters: AdoptersSection::default(),
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
                        resource: "artifact:*".to_owned(),
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
                        resource: "artifact:*".to_owned(),
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

    fn broker_endpoint(state: Arc<BrokerState>) -> Endpoint {
        Endpoint::Custom(CustomEndpoint::new("test-broker", move || {
            let state = Arc::clone(&state);
            async move {
                let (client_stream, server_stream) = duplex(64 * 1024);
                tokio::spawn(async move {
                    let _ = handle_connection(server_stream, state).await;
                });
                Ok(Box::new(client_stream) as BoxedClientIo)
            }
        }))
    }

    async fn create_topic(state: &BrokerState, token: &str, topic: &str) {
        let response = process_request(
            state,
            ControlRequest {
                capability_token: token.to_owned(),
                command: ControlCommand::CreateTopic {
                    topic: TopicSpec {
                        name: topic.to_owned(),
                        retention_class: RetentionClass::Operational,
                        default_classification: Classification::Internal,
                    },
                },
            },
        )
        .await;

        match response {
            ControlResponse::TopicCreated { topic: created } => assert_eq!(created.name, topic),
            other => panic!("expected topic created response, got {other:?}"),
        }
    }

    async fn publish_task(state: &BrokerState, token: &str, task: &TaskWorkItem) {
        let response = process_request(
            state,
            ControlRequest {
                capability_token: token.to_owned(),
                command: ControlCommand::Publish {
                    topic: TASKS_TOPIC.to_owned(),
                    classification: Some(Classification::Internal),
                    payload: serde_json::to_string(task).expect("serialize task"),
                },
            },
        )
        .await;

        match response {
            ControlResponse::PublishAccepted { .. } => {}
            other => panic!("expected publish accepted response, got {other:?}"),
        }
    }

    async fn publish_task_event(state: &BrokerState, token: &str, event: &TaskEvent) {
        let response = process_request(
            state,
            ControlRequest {
                capability_token: token.to_owned(),
                command: ControlCommand::Publish {
                    topic: TASK_EVENTS_TOPIC.to_owned(),
                    classification: Some(Classification::Internal),
                    payload: serde_json::to_string(event).expect("serialize task event"),
                },
            },
        )
        .await;

        match response {
            ControlResponse::PublishAccepted { .. } => {}
            other => panic!("expected publish accepted response, got {other:?}"),
        }
    }

    async fn consume_messages_from_topic(
        state: &BrokerState,
        token: &str,
        topic: &str,
        offset: u64,
        limit: usize,
    ) -> Vec<StoredMessage> {
        let response = process_request(
            state,
            ControlRequest {
                capability_token: token.to_owned(),
                command: ControlCommand::Consume {
                    topic: topic.to_owned(),
                    offset,
                    limit,
                },
            },
        )
        .await;

        match response {
            ControlResponse::Messages { messages, .. } => messages,
            other => panic!("expected messages response, got {other:?}"),
        }
    }

    async fn send_wire_request<T>(
        client: &mut Framed<T, LengthDelimitedCodec>,
        request: ControlRequest,
    ) where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        let packet = ControlWireEnvelope::Request {
            request,
            attachment_length: 0,
        }
        .encode_with_attachment(None)
        .expect("encode request packet");
        client
            .send(packet.into())
            .await
            .expect("send request packet");
    }

    async fn read_wire_stream_frame<T>(client: &mut Framed<T, LengthDelimitedCodec>) -> StreamFrame
    where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        let packet = client.next().await.expect("frame").expect("frame io");
        let (envelope, attachment) =
            ControlWireEnvelope::decode_packet(&packet).expect("decode stream packet");
        assert!(
            attachment.is_empty(),
            "stream frame should not include attachment"
        );
        match envelope {
            ControlWireEnvelope::Stream { frame } => frame,
            other => panic!("expected stream frame packet, got {other:?}"),
        }
    }

    fn example_agent_registration(agent_id: &str) -> AgentRegistration {
        AgentRegistration {
            agent_id: agent_id.to_owned(),
            display_name: "Summarizer".to_owned(),
            version: "1.0.0".to_owned(),
            summary: "Example document summarizer".to_owned(),
            skills: vec!["summarize".to_owned()],
            subscriptions: vec![format!("topic:{TASKS_TOPIC}")],
            publications: vec![format!("topic:{TASK_EVENTS_TOPIC}")],
            schemas: Vec::new(),
            endpoint: AgentEndpoint {
                transport: "local_task_worker".to_owned(),
                address: agent_id.to_owned(),
            },
            classification: Classification::Internal,
            retention_class: RetentionClass::Operational,
            ttl_seconds: Some(60),
        }
    }

    fn summarize_task(
        task_id: &str,
        path: &Path,
        max_summary_lines: usize,
        retry_policy: TaskRetryPolicy,
    ) -> TaskWorkItem {
        summarize_task_with_delay(task_id, path, max_summary_lines, retry_policy, 0)
    }

    fn summarize_task_with_delay(
        task_id: &str,
        path: &Path,
        max_summary_lines: usize,
        retry_policy: TaskRetryPolicy,
        simulate_delay_ms: u64,
    ) -> TaskWorkItem {
        TaskWorkItem {
            task_id: task_id.to_owned(),
            task_type: "summarize_document".to_owned(),
            priority: 0,
            requirements: TaskRequirements {
                skill: Some("summarize".to_owned()),
                topic: None,
                principal: None,
                preferred_agents: Vec::new(),
                avoid_agents: Vec::new(),
            },
            payload: TaskPayload::json(json!({
                "path": path,
                "max_summary_lines": max_summary_lines,
                "simulate_delay_ms": simulate_delay_ms,
            })),
            retry_policy,
            submitted_at: Utc::now(),
        }
    }

    async fn run_example_agent_loop(
        endpoint: Endpoint,
        capability_token: String,
        agent_id: String,
        output_dir: PathBuf,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut worker = AgentWorker::new(endpoint, capability_token, agent_id);

        loop {
            if shutdown.is_cancelled() {
                break;
            }

            let outcome = worker
                .run_once_with_context(|assignment, context| {
                    sample_agent_bin::handle_assignment(assignment, output_dir.clone(), context)
                })
                .await;

            match outcome {
                Ok(WorkerRunOutcome::Idle) | Err(_) => {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = sleep(StdDuration::from_millis(25)) => {}
                    }
                }
                Ok(
                    WorkerRunOutcome::Completed { .. }
                    | WorkerRunOutcome::Failed { .. }
                    | WorkerRunOutcome::Canceled { .. },
                ) => {}
            }
        }

        Ok(())
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
    async fn orchestration_loop_completes_and_retries_with_sample_agent() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let mut config = app_config(&root, public_key_path);
        config.quotas.profiles = vec![
            QuotaProfile {
                name: "operator".to_owned(),
                publish_payload_max_bytes: 64 * 1024,
                publish_requests_per_window: 200,
                publish_window_seconds: 1,
                consume_max_limit: 200,
                consume_requests_per_window: 200,
                consume_window_seconds: 1,
                backpressure_mode: BackpressureMode::Reject,
                backpressure_delay_ms: 0,
            },
            QuotaProfile {
                name: "agent".to_owned(),
                publish_payload_max_bytes: 64 * 1024,
                publish_requests_per_window: 200,
                publish_window_seconds: 1,
                consume_max_limit: 200,
                consume_requests_per_window: 200,
                consume_window_seconds: 1,
                backpressure_mode: BackpressureMode::Reject,
                backpressure_delay_ms: 0,
            },
        ];
        let state = Arc::new(build_state(&config).expect("build broker state"));
        let (_, developer_token) = issue_token(
            &issuer,
            "local:developer",
            vec![
                CapabilityScope {
                    resource: "topic:*".to_owned(),
                    actions: vec![Action::Admin, Action::Publish, Action::Consume],
                },
                CapabilityScope {
                    resource: "registry:agents*".to_owned(),
                    actions: vec![Action::Admin],
                },
            ],
        );
        let (_, agent_token) = issue_token(
            &issuer,
            "local:agent-alpha",
            vec![
                CapabilityScope {
                    resource: "topic:*".to_owned(),
                    actions: vec![Action::Publish, Action::Consume],
                },
                CapabilityScope {
                    resource: "registry:agents*".to_owned(),
                    actions: vec![Action::Admin],
                },
            ],
        );

        let endpoint = broker_endpoint(Arc::clone(&state));

        create_topic(state.as_ref(), &developer_token, TASKS_TOPIC).await;
        create_topic(state.as_ref(), &developer_token, TASK_EVENTS_TOPIC).await;

        let agent_id = "summarizer";
        sample_agent_bin::register_agent(
            &endpoint,
            &agent_token,
            example_agent_registration(agent_id),
        )
        .await
        .expect("register sample agent");

        let output_dir = root.join("var/agent/results");
        let agent_shutdown = CancellationToken::new();
        let agent_handle = tokio::spawn(run_example_agent_loop(
            endpoint.clone(),
            agent_token.clone(),
            agent_id.to_owned(),
            output_dir.clone(),
            agent_shutdown.clone(),
        ));

        let supervisor_state_path = root.join("var/orchestrator/state.json");
        let supervisor_handle = tokio::spawn(orchestrator_bin::supervise(
            endpoint.clone(),
            developer_token.clone(),
            supervisor_state_path.clone(),
            25,
            TASKS_TOPIC.to_owned(),
            TASK_EVENTS_TOPIC.to_owned(),
            50,
            25,
        ));

        let success_source = root.join("inputs/success.md");
        let retry_source = root.join("inputs/retry.md");
        fs::create_dir_all(success_source.parent().expect("inputs dir"))
            .expect("create inputs dir");
        fs::write(&success_source, "Alpha\n\nBeta\nGamma\n").expect("write success source");

        publish_task(
            state.as_ref(),
            &developer_token,
            &summarize_task(
                "task-success",
                &success_source,
                3,
                TaskRetryPolicy {
                    max_attempts: 1,
                    timeout_seconds: 10,
                    retry_delay_seconds: 1,
                },
            ),
        )
        .await;
        publish_task(
            state.as_ref(),
            &developer_token,
            &summarize_task(
                "task-retry",
                &retry_source,
                2,
                TaskRetryPolicy {
                    max_attempts: 2,
                    timeout_seconds: 10,
                    retry_delay_seconds: 2,
                },
            ),
        )
        .await;

        let mut next_offset = 0;
        let mut events = Vec::<TaskEvent>::new();
        let mut retry_source_created = false;
        let deadline = Instant::now() + StdDuration::from_secs(20);
        loop {
            let messages = consume_messages_from_topic(
                state.as_ref(),
                &developer_token,
                TASK_EVENTS_TOPIC,
                next_offset,
                20,
            )
            .await;
            for message in messages {
                next_offset = message.offset.saturating_add(1);
                let event =
                    serde_json::from_str::<TaskEvent>(&message.payload).expect("parse task event");
                events.push(event);
            }

            if !retry_source_created
                && events.iter().any(|event| {
                    event.task_id == "task-retry"
                        && event.status == TaskStatus::Failed
                        && event.attempt == 1
                })
            {
                fs::write(&retry_source, "Recovered\nDocument\n")
                    .expect("write retry source after first failure");
                retry_source_created = true;
            }

            let success_assigned = events.iter().any(|event| {
                event.task_id == "task-success"
                    && event.status == TaskStatus::Assigned
                    && event.attempt == 1
            });
            let success_completed = events.iter().any(|event| {
                event.task_id == "task-success"
                    && event.status == TaskStatus::Completed
                    && event.attempt == 1
            });
            let retry_assigned_once = events.iter().any(|event| {
                event.task_id == "task-retry"
                    && event.status == TaskStatus::Assigned
                    && event.attempt == 1
            });
            let retry_failed_once = events.iter().any(|event| {
                event.task_id == "task-retry"
                    && event.status == TaskStatus::Failed
                    && event.attempt == 1
            });
            let retry_scheduled = events.iter().any(|event| {
                event.task_id == "task-retry"
                    && event.status == TaskStatus::RetryScheduled
                    && event.attempt == 1
            });
            let retry_assigned_twice = events.iter().any(|event| {
                event.task_id == "task-retry"
                    && event.status == TaskStatus::Assigned
                    && event.attempt == 2
            });
            let retry_completed = events.iter().any(|event| {
                event.task_id == "task-retry"
                    && event.status == TaskStatus::Completed
                    && event.attempt == 2
            });
            let success_artifact_path = output_dir.join("task-success.summary.json");
            let retry_artifact_path = output_dir.join("task-retry.summary.json");

            if success_assigned
                && success_completed
                && retry_assigned_once
                && retry_failed_once
                && retry_scheduled
                && retry_assigned_twice
                && retry_completed
                && success_artifact_path.exists()
                && retry_artifact_path.exists()
            {
                break;
            }

            assert!(
                Instant::now() < deadline,
                "timed out waiting for orchestration flow; events: {events:?}"
            );
            sleep(StdDuration::from_millis(50)).await;
        }

        let success_artifact: sample_agent_bin::SummaryArtifact = serde_json::from_str(
            &fs::read_to_string(output_dir.join("task-success.summary.json"))
                .expect("read success artifact"),
        )
        .expect("parse success artifact");
        assert_eq!(success_artifact.task_id, "task-success");
        assert_eq!(success_artifact.summary, "Alpha Beta Gamma");
        assert_eq!(
            success_artifact.source_path,
            success_source.display().to_string()
        );

        let retry_artifact: sample_agent_bin::SummaryArtifact = serde_json::from_str(
            &fs::read_to_string(output_dir.join("task-retry.summary.json"))
                .expect("read retry artifact"),
        )
        .expect("parse retry artifact");
        assert_eq!(retry_artifact.task_id, "task-retry");
        assert_eq!(retry_artifact.summary, "Recovered Document");
        assert_eq!(
            retry_artifact.source_path,
            retry_source.display().to_string()
        );

        sleep(StdDuration::from_millis(100)).await;
        let orchestrator_state = expressways_orchestrator::load_state(&supervisor_state_path)
            .expect("load supervisor state");
        assert_eq!(
            orchestrator_state
                .tasks
                .get("task-success")
                .expect("tracked success task")
                .status,
            TaskStatus::Completed
        );
        assert_eq!(
            orchestrator_state
                .tasks
                .get("task-retry")
                .expect("tracked retry task")
                .status,
            TaskStatus::Completed
        );

        agent_shutdown.cancel();
        agent_handle
            .await
            .expect("agent task join")
            .expect("agent task");
        sample_agent_bin::remove_agent(&endpoint, &agent_token, agent_id)
            .await
            .expect("remove sample agent");

        supervisor_handle.abort();
        let _ = supervisor_handle.await;
    }

    #[tokio::test]
    async fn canceled_assignment_stops_sample_agent_before_completion() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let mut config = app_config(&root, public_key_path);
        config.quotas.profiles = vec![
            QuotaProfile {
                name: "operator".to_owned(),
                publish_payload_max_bytes: 64 * 1024,
                publish_requests_per_window: 200,
                publish_window_seconds: 1,
                consume_max_limit: 200,
                consume_requests_per_window: 200,
                consume_window_seconds: 1,
                backpressure_mode: BackpressureMode::Reject,
                backpressure_delay_ms: 0,
            },
            QuotaProfile {
                name: "agent".to_owned(),
                publish_payload_max_bytes: 64 * 1024,
                publish_requests_per_window: 200,
                publish_window_seconds: 1,
                consume_max_limit: 200,
                consume_requests_per_window: 200,
                consume_window_seconds: 1,
                backpressure_mode: BackpressureMode::Reject,
                backpressure_delay_ms: 0,
            },
        ];
        let state = Arc::new(build_state(&config).expect("build broker state"));
        let (_, developer_token) = issue_token(
            &issuer,
            "local:developer",
            vec![
                CapabilityScope {
                    resource: "topic:*".to_owned(),
                    actions: vec![Action::Admin, Action::Publish, Action::Consume],
                },
                CapabilityScope {
                    resource: "registry:agents*".to_owned(),
                    actions: vec![Action::Admin],
                },
            ],
        );
        let (_, agent_token) = issue_token(
            &issuer,
            "local:agent-alpha",
            vec![
                CapabilityScope {
                    resource: "topic:*".to_owned(),
                    actions: vec![Action::Publish, Action::Consume],
                },
                CapabilityScope {
                    resource: "registry:agents*".to_owned(),
                    actions: vec![Action::Admin],
                },
            ],
        );

        let endpoint = broker_endpoint(Arc::clone(&state));
        create_topic(state.as_ref(), &developer_token, TASKS_TOPIC).await;
        create_topic(state.as_ref(), &developer_token, TASK_EVENTS_TOPIC).await;

        let agent_id = "summarizer";
        sample_agent_bin::register_agent(
            &endpoint,
            &agent_token,
            example_agent_registration(agent_id),
        )
        .await
        .expect("register sample agent");

        let output_dir = root.join("var/agent/results");
        let agent_shutdown = CancellationToken::new();
        let agent_handle = tokio::spawn(run_example_agent_loop(
            endpoint.clone(),
            agent_token.clone(),
            agent_id.to_owned(),
            output_dir.clone(),
            agent_shutdown.clone(),
        ));

        let supervisor_state_path = root.join("var/orchestrator/state.json");
        let supervisor_handle = tokio::spawn(orchestrator_bin::supervise(
            endpoint.clone(),
            developer_token.clone(),
            supervisor_state_path.clone(),
            25,
            TASKS_TOPIC.to_owned(),
            TASK_EVENTS_TOPIC.to_owned(),
            50,
            25,
        ));

        let source_path = root.join("inputs/cancel.md");
        fs::create_dir_all(source_path.parent().expect("inputs dir")).expect("create inputs dir");
        fs::write(&source_path, "Alpha\nBeta\nGamma\n").expect("write cancel source");

        publish_task(
            state.as_ref(),
            &developer_token,
            &summarize_task_with_delay(
                "task-cancel",
                &source_path,
                3,
                TaskRetryPolicy {
                    max_attempts: 1,
                    timeout_seconds: 10,
                    retry_delay_seconds: 1,
                },
                1_500,
            ),
        )
        .await;

        let deadline = Instant::now() + StdDuration::from_secs(20);
        let mut next_offset = 0;
        let mut events = Vec::<TaskEvent>::new();
        let mut cancel_published = false;
        let artifact_path = output_dir.join("task-cancel.summary.json");

        loop {
            let messages = consume_messages_from_topic(
                state.as_ref(),
                &developer_token,
                TASK_EVENTS_TOPIC,
                next_offset,
                20,
            )
            .await;
            for message in messages {
                next_offset = message.offset.saturating_add(1);
                let event =
                    serde_json::from_str::<TaskEvent>(&message.payload).expect("parse task event");
                events.push(event);
            }

            if !cancel_published {
                if let Some(assignment) = events.iter().find(|event| {
                    event.task_id == "task-cancel"
                        && event.status == TaskStatus::Assigned
                        && event.agent_id.as_deref() == Some(agent_id)
                }) {
                    publish_task_event(
                        state.as_ref(),
                        &developer_token,
                        &TaskEvent {
                            event_id: Uuid::now_v7(),
                            task_id: assignment.task_id.clone(),
                            task_offset: assignment.task_offset,
                            assignment_id: assignment.assignment_id,
                            agent_id: assignment.agent_id.clone(),
                            status: TaskStatus::Canceled,
                            attempt: assignment.attempt,
                            reason: Some("operator canceled stale work".to_owned()),
                            emitted_at: Utc::now(),
                        },
                    )
                    .await;
                    cancel_published = true;
                }
            }

            let canceled = events.iter().any(|event| {
                event.task_id == "task-cancel" && event.status == TaskStatus::Canceled
            });
            let completed = events.iter().any(|event| {
                event.task_id == "task-cancel" && event.status == TaskStatus::Completed
            });

            if cancel_published && canceled && !artifact_path.exists() && !completed {
                sleep(StdDuration::from_millis(1_750)).await;
                let new_messages = consume_messages_from_topic(
                    state.as_ref(),
                    &developer_token,
                    TASK_EVENTS_TOPIC,
                    next_offset,
                    20,
                )
                .await;
                for message in new_messages {
                    let event = serde_json::from_str::<TaskEvent>(&message.payload)
                        .expect("parse post-cancel task event");
                    events.push(event);
                }

                let completed_after_wait = events.iter().any(|event| {
                    event.task_id == "task-cancel" && event.status == TaskStatus::Completed
                });
                assert!(
                    !completed_after_wait,
                    "worker should not publish stale completion after cancellation: {events:?}"
                );
                assert!(
                    !artifact_path.exists(),
                    "worker should not write artifacts for canceled work"
                );
                break;
            }

            assert!(
                Instant::now() < deadline,
                "timed out waiting for canceled task flow; events: {events:?}"
            );
            sleep(StdDuration::from_millis(50)).await;
        }

        sleep(StdDuration::from_millis(100)).await;
        let orchestrator_state = expressways_orchestrator::load_state(&supervisor_state_path)
            .expect("load supervisor state");
        assert_eq!(
            orchestrator_state
                .tasks
                .get("task-cancel")
                .expect("tracked canceled task")
                .status,
            TaskStatus::Canceled
        );

        agent_shutdown.cancel();
        agent_handle
            .await
            .expect("agent task join")
            .expect("agent task");
        sample_agent_bin::remove_agent(&endpoint, &agent_token, agent_id)
            .await
            .expect("remove sample agent");

        supervisor_handle.abort();
        let _ = supervisor_handle.await;
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
    async fn artifact_commands_store_fetch_and_report_metadata() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let config = app_config(&root, public_key_path);
        let state = build_state(&config).expect("build broker state");
        let (_, token) = issue_token(
            &issuer,
            "local:developer",
            vec![CapabilityScope {
                resource: "artifact:*".to_owned(),
                actions: vec![Action::Publish, Action::Consume],
            }],
        );

        let (put, put_attachment) = process_request_with_attachment(
            &state,
            ControlRequest {
                capability_token: token.clone(),
                command: ControlCommand::PutArtifact {
                    artifact_id: Some("artifact-1".to_owned()),
                    content_type: "application/pdf".to_owned(),
                    byte_length: 3,
                    sha256: None,
                    classification: Some(Classification::Restricted),
                    retention_class: Some(RetentionClass::Regulated),
                },
            },
            Some(b"PDF".to_vec()),
        )
        .await;
        assert!(put_attachment.is_none());

        let stored = match put {
            ControlResponse::ArtifactStored { artifact } => artifact,
            other => panic!("expected artifact stored response, got {other:?}"),
        };
        assert_eq!(stored.artifact_id, "artifact-1");
        assert_eq!(stored.content_type, "application/pdf");
        assert_eq!(stored.byte_length, 3);
        assert_eq!(stored.classification, Classification::Restricted);
        assert_eq!(stored.retention_class, RetentionClass::Regulated);
        assert!(
            stored
                .local_path
                .as_deref()
                .is_some_and(|path| Path::new(path).exists())
        );

        let stat = process_request(
            &state,
            ControlRequest {
                capability_token: token.clone(),
                command: ControlCommand::StatArtifact {
                    artifact_id: "artifact-1".to_owned(),
                },
            },
        )
        .await;
        match stat {
            ControlResponse::ArtifactMetadata { artifact } => {
                assert_eq!(artifact.artifact_id, "artifact-1");
                assert_eq!(artifact.sha256, stored.sha256);
            }
            other => panic!("expected artifact metadata response, got {other:?}"),
        }

        let (get, attachment) = process_request_with_attachment(
            &state,
            ControlRequest {
                capability_token: token,
                command: ControlCommand::GetArtifact {
                    artifact_id: "artifact-1".to_owned(),
                },
            },
            None,
        )
        .await;
        match get {
            ControlResponse::Artifact { artifact } => {
                assert_eq!(artifact.artifact_id, "artifact-1");
                assert_eq!(attachment.expect("artifact bytes"), b"PDF".to_vec());
            }
            other => panic!("expected artifact response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn artifact_upload_integrity_mismatches_are_rejected() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let config = app_config(&root, public_key_path);
        let state = build_state(&config).expect("build broker state");
        let (_, token) = issue_token(
            &issuer,
            "local:developer",
            vec![CapabilityScope {
                resource: "artifact:*".to_owned(),
                actions: vec![Action::Publish],
            }],
        );

        let (response, attachment) = process_request_with_attachment(
            &state,
            ControlRequest {
                capability_token: token,
                command: ControlCommand::PutArtifact {
                    artifact_id: Some("artifact-bad".to_owned()),
                    content_type: "application/octet-stream".to_owned(),
                    byte_length: 3,
                    sha256: Some("deadbeef".to_owned()),
                    classification: None,
                    retention_class: None,
                },
            },
            Some(b"abc".to_vec()),
        )
        .await;
        assert!(attachment.is_none());

        match response {
            ControlResponse::Error { code, message } => {
                assert_eq!(code, "integrity_error");
                assert!(message.contains("sha256 mismatch"));
            }
            other => panic!("expected integrity error response, got {other:?}"),
        }
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
    async fn startup_with_unavailable_audit_stays_servable_in_degraded_mode() {
        let root = test_root();
        fs::create_dir_all(&root).expect("create root");
        let blocker = root.join("audit-blocker");
        fs::write(&blocker, "occupied").expect("write blocker");

        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let mut config = app_config(&root, public_key_path);
        config.audit.path = blocker.join("audit.jsonl");

        let state = build_state(&config).expect("build degraded broker state");
        let (_, token) = issue_token(
            &issuer,
            "local:developer",
            vec![CapabilityScope {
                resource: BROKER_RESOURCE.to_owned(),
                actions: vec![Action::Health],
            }],
        );

        let response = process_request(
            &state,
            ControlRequest {
                capability_token: token,
                command: ControlCommand::Health,
            },
        )
        .await;

        match response {
            ControlResponse::Health { status, .. } => assert_eq!(status, "degraded"),
            other => panic!("expected health response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn startup_with_unavailable_storage_returns_degraded_service_errors() {
        let root = test_root();
        fs::create_dir_all(&root).expect("create root");
        let blocker = root.join("storage-blocker");
        fs::write(&blocker, "occupied").expect("write blocker");

        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let mut config = app_config(&root, public_key_path);
        config.server.data_dir = blocker.join("data");

        let state = build_state(&config).expect("build degraded broker state");
        let (_, token) = issue_token(
            &issuer,
            "local:developer",
            vec![
                CapabilityScope {
                    resource: BROKER_RESOURCE.to_owned(),
                    actions: vec![Action::Health, Action::Admin],
                },
                CapabilityScope {
                    resource: "topic:*".to_owned(),
                    actions: vec![Action::Publish],
                },
            ],
        );

        let health = process_request(
            &state,
            ControlRequest {
                capability_token: token.clone(),
                command: ControlCommand::Health,
            },
        )
        .await;
        match health {
            ControlResponse::Health { status, .. } => assert_eq!(status, "degraded"),
            other => panic!("expected health response, got {other:?}"),
        }

        let publish = process_request(
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
        match publish {
            ControlResponse::Error { code, message } => {
                assert_eq!(code, "service_degraded");
                assert!(message.contains("degraded mode"));
            }
            other => panic!("expected degraded error response, got {other:?}"),
        }

        let metrics = process_request(
            &state,
            ControlRequest {
                capability_token: token,
                command: ControlCommand::GetMetrics,
            },
        )
        .await;
        match metrics {
            ControlResponse::Metrics { metrics } => {
                assert_eq!(metrics.resilience.service_mode, "degraded");
                assert!(
                    metrics
                        .resilience
                        .degraded_components
                        .iter()
                        .any(|component| component.starts_with("storage:"))
                );
            }
            other => panic!("expected metrics response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn adopters_command_reports_enabled_and_available_packages() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let mut config = app_config(&root, public_key_path);
        config.adopters.enabled = vec!["storage_guard".to_owned()];

        let state = build_state(&config).expect("build broker state");
        state.adopters.probe_now(&state.service_mode);
        let (_, token) = issue_token(
            &issuer,
            "local:developer",
            vec![CapabilityScope {
                resource: BROKER_RESOURCE.to_owned(),
                actions: vec![Action::Admin],
            }],
        );

        let response = process_request(
            &state,
            ControlRequest {
                capability_token: token,
                command: ControlCommand::GetAdopters,
            },
        )
        .await;

        match response {
            ControlResponse::Adopters { adopters } => {
                assert!(adopters.iter().any(|adopter| {
                    adopter.id == "storage_guard" && adopter.enabled && adopter.status == "healthy"
                }));
                assert!(adopters.iter().any(|adopter| {
                    adopter.id == "audit_integrity"
                        && !adopter.enabled
                        && adopter.status == "inactive"
                }));
            }
            other => panic!("expected adopters response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn missing_adopter_package_can_degrade_without_blocking_service() {
        let root = test_root();
        let (issuer, public_key_path) = write_issuer(&root, "dev");
        let mut config = app_config(&root, public_key_path);
        config.adopters.enabled = vec!["not_installed".to_owned()];
        config.adopters.require_installed = false;

        let state = build_state(&config).expect("build broker state");
        state.adopters.probe_now(&state.service_mode);
        let (_, token) = issue_token(
            &issuer,
            "local:developer",
            vec![CapabilityScope {
                resource: BROKER_RESOURCE.to_owned(),
                actions: vec![Action::Admin, Action::Health],
            }],
        );

        let health = process_request(
            &state,
            ControlRequest {
                capability_token: token.clone(),
                command: ControlCommand::Health,
            },
        )
        .await;
        match health {
            ControlResponse::Health { status, .. } => assert_eq!(status, "degraded"),
            other => panic!("expected health response, got {other:?}"),
        }

        let adopters = process_request(
            &state,
            ControlRequest {
                capability_token: token.clone(),
                command: ControlCommand::GetAdopters,
            },
        )
        .await;
        match adopters {
            ControlResponse::Adopters { adopters } => {
                assert!(adopters.iter().any(|adopter| {
                    adopter.id == "not_installed" && adopter.enabled && adopter.status == "failed"
                }));
            }
            other => panic!("expected adopters response, got {other:?}"),
        }

        let metrics = process_request(
            &state,
            ControlRequest {
                capability_token: token,
                command: ControlCommand::GetMetrics,
            },
        )
        .await;
        match metrics {
            ControlResponse::Metrics { metrics } => {
                assert_eq!(metrics.resilience.service_mode, "degraded");
                assert!(metrics.adopters.iter().any(|adopter| {
                    adopter.id == "not_installed" && adopter.status == "failed"
                }));
            }
            other => panic!("expected metrics response, got {other:?}"),
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
        let mut client = Framed::new(client_stream, LengthDelimitedCodec::new());

        send_wire_request(
            &mut client,
            ControlRequest {
                capability_token: developer_token.clone(),
                command: ControlCommand::OpenAgentWatchStream {
                    query: AgentQuery::default(),
                    cursor: None,
                    max_events: 10,
                    wait_timeout_ms: 2_000,
                },
            },
        )
        .await;

        let opened = read_wire_stream_frame(&mut client).await;
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

        let event_frame = read_wire_stream_frame(&mut client).await;
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
        let mut client = Framed::new(client_stream, LengthDelimitedCodec::new());

        send_wire_request(
            &mut client,
            ControlRequest {
                capability_token: developer_token,
                command: ControlCommand::OpenAgentWatchStream {
                    query: AgentQuery::default(),
                    cursor: None,
                    max_events: 10,
                    wait_timeout_ms: 1,
                },
            },
        )
        .await;

        let opened = read_wire_stream_frame(&mut client).await;
        assert!(matches!(opened, StreamFrame::AgentWatchOpened { .. }));

        let keepalive = read_wire_stream_frame(&mut client).await;
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
        let mut client = Framed::new(client_stream, LengthDelimitedCodec::new());

        send_wire_request(
            &mut client,
            ControlRequest {
                capability_token: developer_token.clone(),
                command: ControlCommand::OpenAgentWatchStream {
                    query: AgentQuery::default(),
                    cursor: None,
                    max_events: 10,
                    wait_timeout_ms: 1,
                },
            },
        )
        .await;

        let opened = read_wire_stream_frame(&mut client).await;
        assert!(matches!(
            opened,
            StreamFrame::AgentWatchOpened { cursor: 0 }
        ));

        let keepalive_one = read_wire_stream_frame(&mut client).await;
        assert!(matches!(
            keepalive_one,
            StreamFrame::KeepAlive { cursor: 0 }
        ));

        let keepalive_two = read_wire_stream_frame(&mut client).await;
        assert!(matches!(
            keepalive_two,
            StreamFrame::KeepAlive { cursor: 0 }
        ));

        let closed = read_wire_stream_frame(&mut client).await;
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
        let mut resume_client = Framed::new(resume_client_stream, LengthDelimitedCodec::new());

        send_wire_request(
            &mut resume_client,
            ControlRequest {
                capability_token: developer_token.clone(),
                command: ControlCommand::OpenAgentWatchStream {
                    query: AgentQuery::default(),
                    cursor: Some(resume_cursor),
                    max_events: 10,
                    wait_timeout_ms: 2_000,
                },
            },
        )
        .await;

        let resumed_open = read_wire_stream_frame(&mut resume_client).await;
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

        let resumed_event = read_wire_stream_frame(&mut resume_client).await;
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
        let mut client = Framed::new(client_stream, LengthDelimitedCodec::new());

        send_wire_request(
            &mut client,
            ControlRequest {
                capability_token: developer_token.clone(),
                command: ControlCommand::OpenAgentWatchStream {
                    query: AgentQuery::default(),
                    cursor: None,
                    max_events: 10,
                    wait_timeout_ms: 2_000,
                },
            },
        )
        .await;

        let opened = read_wire_stream_frame(&mut client).await;
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
            storage
                .as_ref()
                .expect("storage should be available")
                .stats()
                .expect("storage stats")
        };
        let audit_summary = state.audit.lock().await.summary();
        let metrics = state.metrics.snapshot(
            storage_stats,
            audit_summary,
            state.service_mode.status(),
            state.service_mode.degraded_components(),
            state.adopters.snapshot(),
        );
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
