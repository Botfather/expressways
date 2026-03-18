mod config;
mod quota;

use std::path::Path;
use std::sync::Arc;

use expressways_audit::{AuditDecision, AuditOutcome, AuditSink, DraftAuditEvent};
use expressways_auth::{AuthSnapshot, CapabilityVerifier, RevocationList, VerifiedCapability};
use expressways_policy::PolicyEngine;
use expressways_protocol::{
    Action, AuthIssuerView, AuthPrincipalView, AuthRevocationView, AuthStateView, BROKER_RESOURCE,
    ControlCommand, ControlRequest, ControlResponse, TopicSpec, topic_resource,
};
use expressways_storage::{Storage, StorageConfig};
use anyhow::Context;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::{AppConfig, ServerConfig, TransportKind};
use crate::quota::QuotaManager;

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
    storage: Mutex<Storage>,
    audit: Mutex<AuditSink>,
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
    })?;
    let audit = AuditSink::new(&config.audit.path)?;
    let verifier = CapabilityVerifier::from_config(&config.auth)?;
    let quotas = QuotaManager::new(config.quotas.clone())?;
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
        storage: Mutex::new(storage),
        audit: Mutex::new(audit),
    })
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

        let response = match serde_json::from_str::<ControlRequest>(&line) {
            Ok(request) => process_request(&state, request).await,
            Err(error) => {
                warn!(error = %error, "failed to decode request");
                ControlResponse::error("invalid_request", error.to_string())
            }
        };

        let serialized = serde_json::to_string(&response)?;
        framed.send(serialized).await?;
    }

    Ok(())
}

async fn process_request(state: &BrokerState, request: ControlRequest) -> ControlResponse {
    match request.command {
        ControlCommand::Health => handle_health(state, request.capability_token).await,
        ControlCommand::GetAuthState => {
            handle_get_auth_state(state, request.capability_token).await
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
        return ControlResponse::error("audit_failure", error.to_string());
    }

    ControlResponse::Health {
        node_name: state.node_name.clone(),
        status: "ok".to_owned(),
    }
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
        return ControlResponse::error("audit_failure", error.to_string());
    }

    ControlResponse::AuthState {
        state: auth_state_view(snapshot),
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
                return ControlResponse::error("audit_failure", error.to_string());
            }

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
                return ControlResponse::error("audit_failure", error.to_string());
            }

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
    .map_err(|error| ControlResponse::error("audit_failure", error.to_string()))
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

    use expressways_auth::{
        AuthConfig, CapabilityIssuer, IssuerStatus, PrincipalKind, PrincipalRecord,
        PrincipalStatus, TrustedIssuerConfig,
    };
    use expressways_policy::{DefaultDecision, PolicyConfig, Rule};
    use expressways_protocol::{
        CapabilityClaims, CapabilityScope, Classification, ControlCommand, ControlResponse,
        RetentionClass,
    };
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    use super::*;
    use crate::config::{AuditSection, StorageSection};
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
                        principal: "local:agent-alpha".to_owned(),
                        resource: "topic:*".to_owned(),
                        actions: vec![Action::Publish, Action::Consume],
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
}
