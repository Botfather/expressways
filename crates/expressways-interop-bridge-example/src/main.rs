use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use base64::Engine as _;
use chrono::{DateTime, Utc};
use clap::{Args, Parser, ValueEnum};
use expressways_adapter_sdk::{CursorStore, stable_id};
use expressways_client::{Client, Endpoint};
use expressways_protocol::{
    Classification, ControlCommand, ControlRequest, ControlResponse,
    INTEROP_CHAT_HANDOFF_SCHEMA_VERSION, INTEROP_CHAT_HANDOFF_TASK_TYPE,
    INTEROP_CHAT_REPLIES_TOPIC, INTEROP_CHAT_REPLY_SCHEMA_VERSION, INTEROP_CHAT_REQUESTS_TOPIC,
    InteropChatAttachmentRef, InteropChatContent, InteropChatHandoffV1, InteropChatMessage,
    InteropChatReplyV1, InteropChatRouting, InteropChatSession, RetentionClass, TaskPayload,
    TaskRequirements, TaskRetryPolicy, TaskWorkItem, TopicSpec,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const DEFAULT_MAX_REQUEST_BYTES: usize = 1_048_576;
const MAX_REQUEST_BYTES_LIMIT: usize = 16 * 1_048_576;
const MAX_HTTP_HEADER_BYTES: usize = 64 * 1024;
const MAX_IDENTIFIER_BYTES: usize = 256;
const MAX_DISPLAY_NAME_BYTES: usize = 1_024;
const MAX_MESSAGE_TEXT_BYTES: usize = 1024 * 1024;
const MAX_METADATA_BYTES: usize = 256 * 1024;
const MAX_ATTACHMENTS: usize = 64;
const MAX_STRUCTURED_CONTENT: usize = 64;
const MAX_STRUCTURED_CONTENT_BYTES: usize = 64 * 1024;
const MAX_AGENT_HINTS: usize = 256;
const MAX_ROUTING_LABELS: usize = 128;
const MAX_ARTIFACT_ID_BYTES: usize = 128;
const MAX_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CONTENT_TYPE_BYTES: usize = 256;
const MAX_TASK_ATTEMPTS: u32 = 1_000;
const MAX_TASK_DURATION_SECONDS: u64 = 2_592_000;
const MIN_REQUEST_TIMEOUT_MS: u64 = 100;
const MAX_REQUEST_TIMEOUT_MS: u64 = 300_000;

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
    #[arg(long, default_value = "127.0.0.1:8891")]
    listen: String,
    #[arg(long, default_value = "/v1/webhook/handoff")]
    webhook_path: String,
    #[arg(long, default_value = "/v1/artifacts")]
    artifact_path: String,
    #[arg(long, default_value_t = DEFAULT_MAX_REQUEST_BYTES)]
    max_request_bytes: usize,
    #[arg(long, default_value_t = MAX_ARTIFACT_BYTES)]
    max_artifact_request_bytes: u64,
    #[arg(long)]
    ingress_bearer: Option<String>,
    #[arg(long, conflicts_with = "ingress_bearer")]
    ingress_bearer_file: Option<PathBuf>,
    #[arg(long, default_value_t = 64)]
    max_connections: usize,
    #[arg(long, default_value_t = 10_000)]
    request_timeout_ms: u64,
    #[arg(long, default_value = INTEROP_CHAT_REQUESTS_TOPIC)]
    tasks_topic: String,
    #[arg(long, default_value = INTEROP_CHAT_HANDOFF_TASK_TYPE)]
    task_type: String,
    #[arg(long)]
    default_skill: Option<String>,
    #[arg(long, default_value = "operational")]
    topic_retention_class: RetentionClass,
    #[arg(long, default_value = "internal")]
    topic_classification: Classification,
    #[arg(long, default_value = "internal")]
    default_classification: Classification,
    #[arg(long, default_value_t = 3)]
    default_max_attempts: u32,
    #[arg(long, default_value_t = 300)]
    default_timeout_seconds: u64,
    #[arg(long, default_value_t = 5)]
    default_retry_delay_seconds: u64,
    #[arg(long)]
    egress_url: Option<String>,
    #[arg(long, conflicts_with = "egress_bearer_file")]
    egress_bearer: Option<String>,
    #[arg(long)]
    egress_bearer_file: Option<PathBuf>,
    #[arg(long, default_value = INTEROP_CHAT_REPLIES_TOPIC)]
    replies_topic: String,
    #[arg(long, default_value = "./var/agent/interop-bridge-state.json")]
    state_path: PathBuf,
    #[arg(long, default_value_t = 1_000)]
    egress_poll_interval_ms: u64,
    #[arg(long, default_value_t = 100)]
    egress_batch_size: usize,
    #[command(flatten)]
    token: TokenArgs,
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

#[derive(Debug, Clone)]
struct BridgeRuntime {
    endpoint: Endpoint,
    capability_token: String,
    listen: String,
    webhook_path: String,
    artifact_path: String,
    max_request_bytes: usize,
    max_artifact_request_bytes: usize,
    ingress_bearer: Option<String>,
    max_connections: usize,
    request_timeout: Duration,
    tasks_topic: String,
    task_type: String,
    default_skill: Option<String>,
    topic_retention_class: RetentionClass,
    topic_classification: Classification,
    default_classification: Classification,
    default_max_attempts: u32,
    default_timeout_seconds: u64,
    default_retry_delay_seconds: u64,
    egress_url: Option<String>,
    egress_bearer: Option<String>,
    replies_topic: String,
    state_path: PathBuf,
    egress_poll_interval: Duration,
    egress_batch_size: usize,
}

#[derive(Debug, Deserialize)]
struct BridgeWebhookRequest {
    schema_version: String,
    #[serde(default)]
    correlation_id: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
    source_runtime: String,
    #[serde(default)]
    target_runtime: Option<String>,
    session: IncomingSession,
    message: IncomingMessage,
    #[serde(default)]
    routing: Option<IncomingRouting>,
    #[serde(default)]
    metadata: Option<serde_json::Value>,
    #[serde(default)]
    received_at: Option<DateTime<Utc>>,
    #[serde(default)]
    task_id: Option<String>,
    #[serde(default)]
    task_type: Option<String>,
    #[serde(default)]
    skill: Option<String>,
    #[serde(default)]
    requires_topic: Option<String>,
    #[serde(default)]
    principal: Option<String>,
    #[serde(default)]
    preferred_agents: Vec<String>,
    #[serde(default)]
    avoid_agents: Vec<String>,
    #[serde(default)]
    priority: Option<i32>,
    #[serde(default)]
    max_attempts: Option<u32>,
    #[serde(default)]
    timeout_seconds: Option<u64>,
    #[serde(default)]
    retry_delay_seconds: Option<u64>,
    #[serde(default)]
    classification: Option<Classification>,
    #[serde(default)]
    retention_class: Option<RetentionClass>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct IncomingSession {
    session_id: String,
    channel: String,
    account_id: String,
    sender_id: String,
    #[serde(default)]
    sender_display_name: Option<String>,
    #[serde(default)]
    message_id: Option<String>,
    #[serde(default)]
    reply_to_message_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IncomingMessage {
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    attachments: Vec<IncomingAttachment>,
    #[serde(default)]
    content: Vec<InteropChatContent>,
}

#[derive(Debug, Deserialize)]
struct IncomingAttachment {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    artifact_id: Option<String>,
    #[serde(default)]
    inline_base64: Option<String>,
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    byte_length: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct IncomingRouting {
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    workspace: Option<String>,
    #[serde(default)]
    skill_hint: Option<String>,
    #[serde(default)]
    labels: Vec<String>,
    #[serde(default)]
    affinity_key: Option<String>,
}

#[derive(Debug, Serialize)]
struct AcceptedIngressResponse {
    schema_version: String,
    correlation_id: String,
    task_id: String,
    task_type: String,
    topic: String,
    message_id: Uuid,
    offset: u64,
    classification: Classification,
    uploaded_artifacts: Vec<String>,
}

#[derive(Debug, Serialize)]
struct AcceptedArtifactResponse {
    artifact_id: String,
    content_type: String,
    byte_length: u64,
    sha256: String,
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

#[derive(Debug)]
struct HttpResponse {
    status_code: u16,
    reason: &'static str,
    content_type: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

#[derive(Debug)]
struct HttpError {
    status_code: u16,
    message: String,
}

impl HttpError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status_code: 400,
            message: message.into(),
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status_code: 401,
            message: message.into(),
        }
    }

    fn method_not_allowed(message: impl Into<String>) -> Self {
        Self {
            status_code: 405,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status_code: 404,
            message: message.into(),
        }
    }

    fn upstream(message: impl Into<String>) -> Self {
        Self {
            status_code: 502,
            message: message.into(),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log_level)?;
    validate_server_limits(
        cli.max_request_bytes,
        cli.max_artifact_request_bytes,
        cli.max_connections,
        cli.request_timeout_ms,
    )?;
    validate_default_retry_policy(
        cli.default_max_attempts,
        cli.default_timeout_seconds,
        cli.default_retry_delay_seconds,
    )?;
    let ingress_bearer = resolve_optional_secret(cli.ingress_bearer, cli.ingress_bearer_file)?;
    let egress_bearer = resolve_optional_secret(cli.egress_bearer, cli.egress_bearer_file)?;
    validate_egress_url(cli.egress_url.as_deref())?;
    if cli.egress_url.is_some() {
        CursorStore::open_with_legacy_field(
            &cli.state_path,
            Some(("replies_offset", &cli.replies_topic)),
        )
        .with_context(|| {
            format!(
                "durable egress state is invalid at {}",
                cli.state_path.display()
            )
        })?;
    }
    let runtime = BridgeRuntime {
        endpoint: endpoint_from_cli(cli.transport, cli.address, cli.socket)?,
        capability_token: resolve_token(cli.token)?,
        listen: cli.listen,
        webhook_path: cli.webhook_path,
        artifact_path: cli.artifact_path,
        max_request_bytes: cli.max_request_bytes,
        max_artifact_request_bytes: usize::try_from(cli.max_artifact_request_bytes)
            .context("max_artifact_request_bytes does not fit this platform")?,
        ingress_bearer,
        max_connections: cli.max_connections,
        request_timeout: Duration::from_millis(cli.request_timeout_ms),
        tasks_topic: cli.tasks_topic,
        task_type: cli.task_type,
        default_skill: cli.default_skill,
        topic_retention_class: cli.topic_retention_class,
        topic_classification: cli.topic_classification,
        default_classification: cli.default_classification,
        default_max_attempts: cli.default_max_attempts,
        default_timeout_seconds: cli.default_timeout_seconds,
        default_retry_delay_seconds: cli.default_retry_delay_seconds,
        egress_url: cli.egress_url,
        egress_bearer,
        replies_topic: cli.replies_topic,
        state_path: cli.state_path,
        egress_poll_interval: Duration::from_millis(cli.egress_poll_interval_ms.max(10)),
        egress_batch_size: cli.egress_batch_size.clamp(1, 1_000),
    };

    run_server(runtime).await
}

fn validate_server_limits(
    max_request_bytes: usize,
    max_artifact_request_bytes: u64,
    max_connections: usize,
    request_timeout_ms: u64,
) -> anyhow::Result<()> {
    if !(1024..=MAX_REQUEST_BYTES_LIMIT).contains(&max_request_bytes) {
        bail!(
            "max_request_bytes must be between 1024 and {}",
            MAX_REQUEST_BYTES_LIMIT
        );
    }
    if !(1024..=MAX_ARTIFACT_BYTES).contains(&max_artifact_request_bytes) {
        bail!("max_artifact_request_bytes must be between 1024 and {MAX_ARTIFACT_BYTES}");
    }
    if !(1..=Semaphore::MAX_PERMITS).contains(&max_connections) {
        bail!(
            "max_connections must be between 1 and {}",
            Semaphore::MAX_PERMITS
        );
    }
    if !(MIN_REQUEST_TIMEOUT_MS..=MAX_REQUEST_TIMEOUT_MS).contains(&request_timeout_ms) {
        bail!(
            "request_timeout_ms must be between {MIN_REQUEST_TIMEOUT_MS} and {MAX_REQUEST_TIMEOUT_MS}"
        );
    }
    Ok(())
}

fn validate_egress_url(value: Option<&str>) -> anyhow::Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    let url = reqwest::Url::parse(value).context("egress_url must be an absolute URL")?;
    if url.scheme() == "https" {
        return Ok(());
    }
    let loopback = url
        .host_str()
        .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"));
    if url.scheme() == "http" && loopback {
        return Ok(());
    }
    bail!("egress_url must use HTTPS, except for loopback HTTP development endpoints")
}

fn validate_default_retry_policy(
    max_attempts: u32,
    timeout_seconds: u64,
    retry_delay_seconds: u64,
) -> anyhow::Result<()> {
    if !(1..=MAX_TASK_ATTEMPTS).contains(&max_attempts) {
        bail!("default_max_attempts must be between 1 and {MAX_TASK_ATTEMPTS}");
    }
    for (field, value) in [
        ("default_timeout_seconds", timeout_seconds),
        ("default_retry_delay_seconds", retry_delay_seconds),
    ] {
        if !(1..=MAX_TASK_DURATION_SECONDS).contains(&value) {
            bail!("{field} must be between 1 and {MAX_TASK_DURATION_SECONDS}");
        }
    }
    Ok(())
}

async fn run_server(runtime: BridgeRuntime) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&runtime.listen)
        .await
        .with_context(|| format!("failed to bind bridge listener on {}", runtime.listen))?;
    let local_addr = listener.local_addr()?;
    validate_bridge_exposure(
        local_addr.ip().is_loopback(),
        runtime.ingress_bearer.as_deref(),
    )?;
    let connection_limit = Arc::new(Semaphore::new(runtime.max_connections));
    let egress_task = runtime
        .egress_url
        .as_ref()
        .map(|_| tokio::spawn(run_egress(runtime.clone())));
    info!(
        listen = %runtime.listen,
        webhook_path = %runtime.webhook_path,
        tasks_topic = %runtime.tasks_topic,
        task_type = %runtime.task_type,
        "interop bridge listening"
    );

    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (mut stream, peer) = accept?;
                let permit = match Arc::clone(&connection_limit).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        warn!(peer = %peer, "interop bridge connection limit reached; rejecting client");
                        continue;
                    }
                };
                let runtime = runtime.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    match tokio::time::timeout(
                        runtime.request_timeout,
                        handle_connection(&mut stream, runtime.clone()),
                    ).await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => warn!(peer = %peer, error = %error, "webhook request failed"),
                        Err(_) => warn!(peer = %peer, timeout_ms = runtime.request_timeout.as_millis(), "webhook request timed out"),
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                if let Some(task) = &egress_task {
                    task.abort();
                }
                info!("interop bridge shutting down");
                return Ok(());
            }
        }
    }
}

fn validate_bridge_exposure(loopback: bool, ingress_bearer: Option<&str>) -> anyhow::Result<()> {
    if !loopback && ingress_bearer.is_none() {
        bail!("ingress bearer authentication is required for non-loopback listeners");
    }
    Ok(())
}

async fn handle_connection(
    stream: &mut tokio::net::TcpStream,
    runtime: BridgeRuntime,
) -> anyhow::Result<()> {
    let response = match read_http_request(
        stream,
        runtime.max_request_bytes,
        &runtime.artifact_path,
        runtime.max_artifact_request_bytes,
    )
    .await
    {
        Ok(request) => match process_request(runtime, request).await {
            Ok(response) => response,
            Err(error) => error_json_response(error.status_code, error.message),
        },
        Err(error) => error_json_response(400, format!("invalid http request: {error}")),
    };

    let bytes = encode_http_response(&response);
    stream.write_all(&bytes).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn process_request(
    runtime: BridgeRuntime,
    request: HttpRequest,
) -> Result<HttpResponse, HttpError> {
    if !bearer_authorized(&request.headers, runtime.ingress_bearer.as_deref()) {
        return Err(HttpError::unauthorized(
            "missing or invalid Authorization bearer token",
        ));
    }

    if request.path == runtime.webhook_path {
        if request.method != "POST" {
            return Err(HttpError::method_not_allowed(
                "the webhook endpoint only supports POST",
            ));
        }
        if request.body.len() > runtime.max_request_bytes {
            return Err(HttpError::bad_request(
                "webhook request body exceeds configured limit",
            ));
        }
        let webhook =
            serde_json::from_slice::<BridgeWebhookRequest>(&request.body).map_err(|error| {
                HttpError::bad_request(format!("failed to parse webhook json: {error}"))
            })?;
        return Ok(json_response(202, &submit_webhook(runtime, webhook).await?));
    }
    if request.path == runtime.artifact_path {
        if request.method != "POST" {
            return Err(HttpError::method_not_allowed(
                "the artifact collection endpoint only supports POST",
            ));
        }
        if request.body.is_empty() || request.body.len() > runtime.max_artifact_request_bytes {
            return Err(HttpError::bad_request(
                "artifact body must be non-empty and within the configured artifact limit",
            ));
        }
        return Ok(json_response(
            202,
            &upload_artifact(runtime, request).await?,
        ));
    }
    let artifact_prefix = format!("{}/", runtime.artifact_path.trim_end_matches('/'));
    if let Some(artifact_id) = request.path.strip_prefix(&artifact_prefix) {
        if request.method != "GET" {
            return Err(HttpError::method_not_allowed(
                "artifact resources only support GET",
            ));
        }
        return download_artifact(runtime, artifact_id).await;
    }
    Err(HttpError::not_found(format!(
        "unsupported path `{}`",
        request.path
    )))
}

async fn submit_webhook(
    runtime: BridgeRuntime,
    webhook: BridgeWebhookRequest,
) -> Result<AcceptedIngressResponse, HttpError> {
    validate_webhook(&webhook)?;

    let mut client = Client::connect(runtime.endpoint.clone())
        .await
        .map_err(|error| HttpError::upstream(format!("failed to connect to broker: {error}")))?;

    ensure_topic(
        &mut client,
        &runtime.capability_token,
        &runtime.tasks_topic,
        runtime.topic_retention_class.clone(),
        runtime.topic_classification.clone(),
    )
    .await?;

    let classification = webhook
        .classification
        .clone()
        .unwrap_or_else(|| runtime.default_classification.clone());
    let artifact_retention = webhook
        .retention_class
        .clone()
        .unwrap_or_else(|| runtime.topic_retention_class.clone());

    let (attachments, uploaded_artifacts) = materialize_attachments(
        &mut client,
        &runtime.capability_token,
        &webhook.message.attachments,
        classification.clone(),
        artifact_retention,
    )
    .await?;

    let correlation_id = webhook
        .correlation_id
        .clone()
        .unwrap_or_else(|| derive_correlation_id(&webhook));
    let task_id = webhook.task_id.clone().unwrap_or_else(|| {
        deterministic_task_id(
            webhook
                .idempotency_key
                .as_deref()
                .unwrap_or(&correlation_id),
        )
    });
    let task_type = webhook
        .task_type
        .unwrap_or_else(|| runtime.task_type.clone());
    let skill = webhook
        .skill
        .or_else(|| runtime.default_skill.clone())
        .or_else(|| {
            webhook
                .routing
                .as_ref()
                .and_then(|routing| routing.skill_hint.clone())
        });
    let received_at = webhook.received_at.unwrap_or_else(Utc::now);
    let metadata = webhook.metadata.unwrap_or_else(|| serde_json::json!({}));
    let routing = webhook.routing;
    let required_agent = routing
        .as_ref()
        .and_then(|routing| routing.agent_id.clone());
    let affinity_key = routing
        .as_ref()
        .and_then(|routing| routing.affinity_key.clone())
        .or_else(|| Some(session_affinity_key(&webhook.session)));

    let payload = InteropChatHandoffV1 {
        schema_version: INTEROP_CHAT_HANDOFF_SCHEMA_VERSION.to_owned(),
        correlation_id: correlation_id.clone(),
        reply_topic: runtime.replies_topic.clone(),
        source_runtime: webhook.source_runtime,
        target_runtime: webhook.target_runtime,
        session: InteropChatSession {
            session_id: webhook.session.session_id,
            channel: webhook.session.channel,
            account_id: webhook.session.account_id,
            sender_id: webhook.session.sender_id,
            sender_display_name: webhook.session.sender_display_name,
            message_id: webhook.session.message_id,
            reply_to_message_id: webhook.session.reply_to_message_id,
        },
        message: InteropChatMessage {
            text: webhook
                .message
                .text
                .and_then(|text| (!text.trim().is_empty()).then_some(text)),
            attachments,
            content: webhook.message.content,
        },
        routing: routing.map(|routing| InteropChatRouting {
            agent_id: routing.agent_id,
            workspace: routing.workspace,
            skill_hint: routing.skill_hint,
            labels: routing.labels,
            affinity_key: routing.affinity_key,
        }),
        metadata,
        received_at,
    };

    let task = TaskWorkItem {
        task_id: task_id.clone(),
        task_type: task_type.clone(),
        priority: webhook.priority.unwrap_or(0),
        requirements: TaskRequirements {
            skill,
            topic: webhook.requires_topic,
            principal: webhook.principal,
            preferred_agents: webhook.preferred_agents,
            avoid_agents: webhook.avoid_agents,
            required_agent,
            affinity_key,
        },
        payload: TaskPayload::json(serde_json::to_value(payload).map_err(|error| {
            HttpError::bad_request(format!("payload is invalid json: {error}"))
        })?),
        retry_policy: TaskRetryPolicy {
            max_attempts: webhook.max_attempts.unwrap_or(runtime.default_max_attempts),
            timeout_seconds: webhook
                .timeout_seconds
                .unwrap_or(runtime.default_timeout_seconds),
            retry_delay_seconds: webhook
                .retry_delay_seconds
                .unwrap_or(runtime.default_retry_delay_seconds),
        },
        submitted_at: Utc::now(),
    };

    let publish_response = client
        .send(ControlRequest {
            capability_token: runtime.capability_token.clone(),
            command: ControlCommand::Publish {
                topic: runtime.tasks_topic.clone(),
                classification: Some(classification),
                payload: serde_json::to_string(&task).map_err(|error| {
                    HttpError::bad_request(format!("failed to serialize task payload: {error}"))
                })?,
            },
        })
        .await
        .map_err(|error| HttpError::upstream(format!("failed to publish task: {error}")))?;

    match publish_response {
        ControlResponse::PublishAccepted {
            message_id,
            offset,
            classification,
        } => {
            info!(
                task_id = %task_id,
                topic = %runtime.tasks_topic,
                offset,
                "submitted interop task"
            );
            Ok(AcceptedIngressResponse {
                schema_version: INTEROP_CHAT_HANDOFF_SCHEMA_VERSION.to_owned(),
                correlation_id,
                task_id,
                task_type,
                topic: runtime.tasks_topic,
                message_id,
                offset,
                classification,
                uploaded_artifacts,
            })
        }
        ControlResponse::Error { code, message } => Err(HttpError::upstream(format!(
            "broker rejected task publish: {code}: {message}"
        ))),
        other => Err(HttpError::upstream(format!(
            "unexpected broker response while publishing task: {other:?}"
        ))),
    }
}

fn session_affinity_key(session: &IncomingSession) -> String {
    format!(
        "{}:{}:{}",
        session.channel, session.account_id, session.session_id
    )
}

fn derive_correlation_id(webhook: &BridgeWebhookRequest) -> String {
    let source = format!(
        "{}:{}:{}:{}:{}",
        webhook.source_runtime,
        webhook.session.channel,
        webhook.session.account_id,
        webhook.session.session_id,
        webhook
            .session
            .message_id
            .as_deref()
            .unwrap_or("unidentified")
    );
    format!("corr-{:x}", Sha256::digest(source.as_bytes()))
}

fn deterministic_task_id(idempotency_key: &str) -> String {
    stable_id("interop", idempotency_key)
        .expect("validated idempotency key and fixed prefix produce a stable id")
}

fn validate_webhook(webhook: &BridgeWebhookRequest) -> Result<(), HttpError> {
    if webhook.schema_version != INTEROP_CHAT_HANDOFF_SCHEMA_VERSION {
        return Err(HttpError::bad_request(format!(
            "unsupported schema_version `{}`; expected `{INTEROP_CHAT_HANDOFF_SCHEMA_VERSION}`",
            webhook.schema_version
        )));
    }
    validate_required_text(
        "source_runtime",
        &webhook.source_runtime,
        MAX_IDENTIFIER_BYTES,
    )?;
    if webhook.task_id.is_none()
        && webhook.idempotency_key.is_none()
        && webhook.correlation_id.is_none()
        && webhook.session.message_id.is_none()
    {
        return Err(HttpError::bad_request(
            "one of task_id, idempotency_key, correlation_id, or session.message_id is required",
        ));
    }
    validate_optional_text(
        "target_runtime",
        webhook.target_runtime.as_deref(),
        MAX_IDENTIFIER_BYTES,
    )?;
    if webhook
        .metadata
        .as_ref()
        .is_some_and(|metadata| !metadata.is_object())
    {
        return Err(HttpError::bad_request(
            "metadata must be a JSON object when provided",
        ));
    }
    if let Some(metadata) = &webhook.metadata {
        let encoded = serde_json::to_vec(metadata)
            .map_err(|error| HttpError::bad_request(format!("metadata is invalid: {error}")))?;
        if encoded.len() > MAX_METADATA_BYTES {
            return Err(HttpError::bad_request(format!(
                "metadata exceeds {MAX_METADATA_BYTES} bytes"
            )));
        }
    }

    validate_required_text(
        "session.session_id",
        &webhook.session.session_id,
        MAX_IDENTIFIER_BYTES,
    )?;
    validate_required_text(
        "session.channel",
        &webhook.session.channel,
        MAX_IDENTIFIER_BYTES,
    )?;
    validate_required_text(
        "session.account_id",
        &webhook.session.account_id,
        MAX_IDENTIFIER_BYTES,
    )?;
    validate_required_text(
        "session.sender_id",
        &webhook.session.sender_id,
        MAX_IDENTIFIER_BYTES,
    )?;
    validate_optional_text(
        "session.sender_display_name",
        webhook.session.sender_display_name.as_deref(),
        MAX_DISPLAY_NAME_BYTES,
    )?;
    validate_optional_text(
        "session.message_id",
        webhook.session.message_id.as_deref(),
        MAX_IDENTIFIER_BYTES,
    )?;
    validate_optional_text(
        "session.reply_to_message_id",
        webhook.session.reply_to_message_id.as_deref(),
        MAX_IDENTIFIER_BYTES,
    )?;
    validate_optional_text("task_id", webhook.task_id.as_deref(), MAX_IDENTIFIER_BYTES)?;
    validate_optional_text(
        "correlation_id",
        webhook.correlation_id.as_deref(),
        MAX_IDENTIFIER_BYTES,
    )?;
    validate_optional_text(
        "idempotency_key",
        webhook.idempotency_key.as_deref(),
        MAX_IDENTIFIER_BYTES,
    )?;
    validate_optional_text(
        "task_type",
        webhook.task_type.as_deref(),
        MAX_IDENTIFIER_BYTES,
    )?;
    validate_optional_text("skill", webhook.skill.as_deref(), MAX_IDENTIFIER_BYTES)?;
    validate_optional_text(
        "requires_topic",
        webhook.requires_topic.as_deref(),
        MAX_IDENTIFIER_BYTES,
    )?;
    validate_optional_text(
        "principal",
        webhook.principal.as_deref(),
        MAX_IDENTIFIER_BYTES,
    )?;
    if webhook
        .message
        .text
        .as_ref()
        .is_some_and(|text| text.len() > MAX_MESSAGE_TEXT_BYTES)
    {
        return Err(HttpError::bad_request(format!(
            "message.text exceeds {MAX_MESSAGE_TEXT_BYTES} bytes"
        )));
    }

    if !webhook
        .message
        .text
        .as_ref()
        .is_some_and(|text| !text.trim().is_empty())
        && webhook.message.attachments.is_empty()
        && webhook.message.content.is_empty()
    {
        return Err(HttpError::bad_request(
            "message must contain text, an attachment, or structured content",
        ));
    }
    if webhook.message.attachments.len() > MAX_ATTACHMENTS {
        return Err(HttpError::bad_request(format!(
            "message has too many attachments; maximum is {MAX_ATTACHMENTS}"
        )));
    }
    if webhook.message.content.len() > MAX_STRUCTURED_CONTENT {
        return Err(HttpError::bad_request(format!(
            "message has too many structured content entries; maximum is {MAX_STRUCTURED_CONTENT}"
        )));
    }
    for (index, content) in webhook.message.content.iter().enumerate() {
        if !content.data.is_object() {
            return Err(HttpError::bad_request(format!(
                "message.content[{index}].data must be a JSON object"
            )));
        }
        let bytes = serde_json::to_vec(&content.data).map_err(|error| {
            HttpError::bad_request(format!("message.content[{index}] is invalid: {error}"))
        })?;
        if bytes.len() > MAX_STRUCTURED_CONTENT_BYTES {
            return Err(HttpError::bad_request(format!(
                "message.content[{index}].data exceeds {MAX_STRUCTURED_CONTENT_BYTES} bytes"
            )));
        }
    }
    validate_unique_text_list(
        "preferred_agents",
        &webhook.preferred_agents,
        MAX_AGENT_HINTS,
        MAX_IDENTIFIER_BYTES,
    )?;
    validate_unique_text_list(
        "avoid_agents",
        &webhook.avoid_agents,
        MAX_AGENT_HINTS,
        MAX_IDENTIFIER_BYTES,
    )?;
    let avoided = webhook.avoid_agents.iter().collect::<HashSet<_>>();
    if let Some(conflict) = webhook
        .preferred_agents
        .iter()
        .find(|agent| avoided.contains(agent))
    {
        return Err(HttpError::bad_request(format!(
            "agent `{conflict}` cannot be both preferred and avoided"
        )));
    }

    if let Some(routing) = &webhook.routing {
        validate_optional_text(
            "routing.agent_id",
            routing.agent_id.as_deref(),
            MAX_IDENTIFIER_BYTES,
        )?;
        validate_optional_text(
            "routing.workspace",
            routing.workspace.as_deref(),
            MAX_DISPLAY_NAME_BYTES,
        )?;
        validate_optional_text(
            "routing.skill_hint",
            routing.skill_hint.as_deref(),
            MAX_IDENTIFIER_BYTES,
        )?;
        validate_optional_text(
            "routing.affinity_key",
            routing.affinity_key.as_deref(),
            MAX_IDENTIFIER_BYTES,
        )?;
        validate_unique_text_list(
            "routing.labels",
            &routing.labels,
            MAX_ROUTING_LABELS,
            MAX_IDENTIFIER_BYTES,
        )?;
    }

    if webhook
        .max_attempts
        .is_some_and(|value| value == 0 || value > MAX_TASK_ATTEMPTS)
    {
        return Err(HttpError::bad_request(format!(
            "max_attempts must be between 1 and {MAX_TASK_ATTEMPTS}"
        )));
    }
    for (field, value) in [
        ("timeout_seconds", webhook.timeout_seconds),
        ("retry_delay_seconds", webhook.retry_delay_seconds),
    ] {
        if value.is_some_and(|value| value == 0 || value > MAX_TASK_DURATION_SECONDS) {
            return Err(HttpError::bad_request(format!(
                "{field} must be between 1 and {MAX_TASK_DURATION_SECONDS}"
            )));
        }
    }

    for (index, attachment) in webhook.message.attachments.iter().enumerate() {
        validate_optional_text(
            &format!("message.attachments[{index}].name"),
            attachment.name.as_deref(),
            MAX_DISPLAY_NAME_BYTES,
        )?;
        validate_optional_text(
            &format!("message.attachments[{index}].content_type"),
            attachment.content_type.as_deref(),
            MAX_CONTENT_TYPE_BYTES,
        )?;
        validate_optional_text(
            &format!("message.attachments[{index}].artifact_id"),
            attachment.artifact_id.as_deref(),
            MAX_ARTIFACT_ID_BYTES,
        )?;
        if let Some(sha256) = attachment.sha256.as_deref()
            && (sha256.len() != 64 || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit()))
        {
            return Err(HttpError::bad_request(format!(
                "message.attachments[{index}].sha256 must be 64 hexadecimal characters"
            )));
        }
        if attachment
            .byte_length
            .is_some_and(|length| length > MAX_ARTIFACT_BYTES)
        {
            return Err(HttpError::bad_request(format!(
                "message.attachments[{index}].byte_length exceeds {MAX_ARTIFACT_BYTES} bytes"
            )));
        }
        let has_artifact = attachment
            .artifact_id
            .as_deref()
            .is_some_and(|id| !id.trim().is_empty());
        let has_inline = attachment
            .inline_base64
            .as_deref()
            .is_some_and(|data| !data.trim().is_empty());
        if has_artifact == has_inline {
            return Err(HttpError::bad_request(
                "each attachment must set exactly one of artifact_id or inline_base64",
            ));
        }
    }

    Ok(())
}

fn validate_required_text(field: &str, value: &str, max_bytes: usize) -> Result<(), HttpError> {
    if value.trim().is_empty() || value.len() > max_bytes {
        return Err(HttpError::bad_request(format!(
            "{field} must contain 1..={max_bytes} bytes"
        )));
    }
    Ok(())
}

fn validate_optional_text(
    field: &str,
    value: Option<&str>,
    max_bytes: usize,
) -> Result<(), HttpError> {
    if let Some(value) = value {
        validate_required_text(field, value, max_bytes)?;
    }
    Ok(())
}

fn validate_unique_text_list(
    field: &str,
    values: &[String],
    max_items: usize,
    max_bytes: usize,
) -> Result<(), HttpError> {
    if values.len() > max_items {
        return Err(HttpError::bad_request(format!(
            "{field} contains too many values; maximum is {max_items}"
        )));
    }
    let mut unique = HashSet::with_capacity(values.len());
    for value in values {
        validate_required_text(field, value, max_bytes)?;
        if !unique.insert(value) {
            return Err(HttpError::bad_request(format!(
                "{field} contains duplicate value `{value}`"
            )));
        }
    }
    Ok(())
}

async fn upload_artifact(
    runtime: BridgeRuntime,
    request: HttpRequest,
) -> Result<AcceptedArtifactResponse, HttpError> {
    let content_type = request
        .headers
        .get("content-type")
        .cloned()
        .unwrap_or_else(|| "application/octet-stream".to_owned());
    validate_required_text("Content-Type", &content_type, MAX_CONTENT_TYPE_BYTES)?;
    let artifact_id = request.headers.get("x-artifact-id").cloned();
    validate_optional_text(
        "X-Artifact-Id",
        artifact_id.as_deref(),
        MAX_ARTIFACT_ID_BYTES,
    )?;
    let expected_sha256 = request.headers.get("x-content-sha256").cloned();
    if let Some(value) = expected_sha256.as_deref()
        && (value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return Err(HttpError::bad_request(
            "X-Content-Sha256 must be 64 hexadecimal characters",
        ));
    }
    let actual_sha256 = format!("{:x}", Sha256::digest(&request.body));
    if expected_sha256
        .as_ref()
        .is_some_and(|expected| !expected.eq_ignore_ascii_case(&actual_sha256))
    {
        return Err(HttpError::bad_request(
            "artifact sha256 does not match body",
        ));
    }

    let mut client = Client::connect(runtime.endpoint.clone())
        .await
        .map_err(|error| HttpError::upstream(format!("failed to connect to broker: {error}")))?;
    let (response, attachment) = client
        .send_with_attachment(
            ControlRequest {
                capability_token: runtime.capability_token,
                command: ControlCommand::PutArtifact {
                    artifact_id,
                    content_type,
                    byte_length: request.body.len() as u64,
                    sha256: Some(actual_sha256),
                    classification: Some(runtime.default_classification),
                    retention_class: Some(runtime.topic_retention_class),
                },
            },
            Some(request.body),
        )
        .await
        .map_err(|error| HttpError::upstream(format!("failed to store artifact: {error}")))?;
    if attachment.is_some() {
        return Err(HttpError::upstream(
            "broker returned unexpected bytes for artifact upload",
        ));
    }
    match response {
        ControlResponse::ArtifactStored { artifact } => Ok(AcceptedArtifactResponse {
            artifact_id: artifact.artifact_id,
            content_type: artifact.content_type,
            byte_length: artifact.byte_length,
            sha256: artifact.sha256,
        }),
        ControlResponse::Error { code, message } => Err(HttpError::upstream(format!(
            "broker rejected artifact upload: {code}: {message}"
        ))),
        other => Err(HttpError::upstream(format!(
            "unexpected broker response while storing artifact: {other:?}"
        ))),
    }
}

async fn download_artifact(
    runtime: BridgeRuntime,
    artifact_id: &str,
) -> Result<HttpResponse, HttpError> {
    validate_required_text("artifact_id", artifact_id, MAX_ARTIFACT_ID_BYTES)?;
    let mut client = Client::connect(runtime.endpoint)
        .await
        .map_err(|error| HttpError::upstream(format!("failed to connect to broker: {error}")))?;
    let (response, bytes) = client
        .send_with_attachment(
            ControlRequest {
                capability_token: runtime.capability_token,
                command: ControlCommand::GetArtifact {
                    artifact_id: artifact_id.to_owned(),
                },
            },
            None,
        )
        .await
        .map_err(|error| HttpError::upstream(format!("failed to fetch artifact: {error}")))?;
    match response {
        ControlResponse::Artifact { artifact } => {
            let body = bytes.ok_or_else(|| HttpError::upstream("broker omitted artifact bytes"))?;
            let actual_sha256 = format!("{:x}", Sha256::digest(&body));
            if artifact.artifact_id != artifact_id
                || artifact.byte_length != body.len() as u64
                || !artifact.sha256.eq_ignore_ascii_case(&actual_sha256)
            {
                return Err(HttpError::upstream(
                    "broker returned artifact bytes that failed integrity validation",
                ));
            }
            Ok(HttpResponse {
                status_code: 200,
                reason: status_reason(200),
                content_type: artifact.content_type,
                headers: vec![
                    ("X-Artifact-Id".to_owned(), artifact.artifact_id),
                    ("X-Content-Sha256".to_owned(), artifact.sha256),
                ],
                body,
            })
        }
        ControlResponse::Error { code, message } if code == "artifact_not_found" => {
            Err(HttpError::not_found(message))
        }
        ControlResponse::Error { code, message } => Err(HttpError::upstream(format!(
            "broker rejected artifact download: {code}: {message}"
        ))),
        other => Err(HttpError::upstream(format!(
            "unexpected broker response while fetching artifact: {other:?}"
        ))),
    }
}

async fn run_egress(runtime: BridgeRuntime) {
    let Some(egress_url) = runtime.egress_url.clone() else {
        return;
    };
    let http = match reqwest::Client::builder()
        .timeout(runtime.request_timeout)
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            warn!(error = %error, "failed to initialize egress HTTP client");
            return;
        }
    };
    let mut state = match CursorStore::open_with_legacy_field(
        &runtime.state_path,
        Some(("replies_offset", &runtime.replies_topic)),
    ) {
        Ok(state) => state,
        Err(error) => {
            warn!(error = %error, path = %runtime.state_path.display(), "failed to load durable bridge state");
            return;
        }
    };
    loop {
        match deliver_reply_batch(&runtime, &http, &egress_url, &mut state).await {
            Ok(delivered) if delivered > 0 => continue,
            Ok(_) => {}
            Err(error) => {
                let offset = state
                    .next_offset(&runtime.replies_topic)
                    .unwrap_or_default();
                warn!(error = %error, offset, "egress delivery paused; cursor not advanced")
            }
        }
        tokio::time::sleep(runtime.egress_poll_interval).await;
    }
}

async fn deliver_reply_batch(
    runtime: &BridgeRuntime,
    http: &reqwest::Client,
    egress_url: &str,
    state: &mut CursorStore,
) -> anyhow::Result<usize> {
    let mut client = Client::connect(runtime.endpoint.clone())
        .await
        .context("connect to broker for reply egress")?;
    ensure_topic(
        &mut client,
        &runtime.capability_token,
        &runtime.replies_topic,
        runtime.topic_retention_class.clone(),
        runtime.topic_classification.clone(),
    )
    .await
    .map_err(|error| anyhow::anyhow!(error.message))?;
    let response = client
        .send(ControlRequest {
            capability_token: runtime.capability_token.clone(),
            command: ControlCommand::Consume {
                topic: runtime.replies_topic.clone(),
                offset: state.next_offset(&runtime.replies_topic)?,
                limit: runtime.egress_batch_size,
            },
        })
        .await
        .context("consume reply topic")?;
    let messages = match response {
        ControlResponse::Messages { messages, .. } => messages,
        ControlResponse::Error { code, message } => {
            bail!("consume replies failed: {code}: {message}")
        }
        other => bail!("unexpected consume replies response: {other:?}"),
    };
    let mut delivered = 0;
    for stored in messages {
        let reply: InteropChatReplyV1 = serde_json::from_str(&stored.payload)
            .with_context(|| format!("invalid reply envelope at offset {}", stored.offset))?;
        validate_reply(&reply)?;
        let mut request = http
            .post(egress_url)
            .header("Idempotency-Key", &reply.delivery_id)
            .header("X-Expressways-Correlation-Id", &reply.correlation_id)
            .json(&reply);
        if let Some(bearer) = &runtime.egress_bearer {
            request = request.bearer_auth(bearer);
        }
        let response = request
            .send()
            .await
            .context("send reply to channel endpoint")?;
        if !response.status().is_success() {
            bail!(
                "channel endpoint rejected reply with HTTP {}",
                response.status()
            );
        }
        let next_offset = stored
            .offset
            .checked_add(1)
            .context("reply offset overflow")?;
        state.checkpoint(&runtime.replies_topic, next_offset)?;
        delivered += 1;
        info!(delivery_id = %reply.delivery_id, offset = stored.offset, "delivered interop reply");
    }
    Ok(delivered)
}

fn validate_reply(reply: &InteropChatReplyV1) -> anyhow::Result<()> {
    if reply.schema_version != INTEROP_CHAT_REPLY_SCHEMA_VERSION {
        bail!(
            "unsupported reply schema_version `{}`",
            reply.schema_version
        );
    }
    for (field, value) in [
        ("delivery_id", reply.delivery_id.as_str()),
        ("correlation_id", reply.correlation_id.as_str()),
        ("source_runtime", reply.source_runtime.as_str()),
        ("target_runtime", reply.target_runtime.as_str()),
        ("in_reply_to_task_id", reply.in_reply_to_task_id.as_str()),
    ] {
        if value.is_empty() || value.len() > MAX_IDENTIFIER_BYTES {
            bail!("reply {field} must contain 1..={MAX_IDENTIFIER_BYTES} bytes");
        }
    }
    if reply.message.text.is_none()
        && reply.message.attachments.is_empty()
        && reply.message.content.is_empty()
    {
        bail!("reply message must contain text, attachments, or structured content");
    }
    if reply.message.content.len() > MAX_STRUCTURED_CONTENT {
        bail!("reply message has too many structured content entries");
    }
    for content in &reply.message.content {
        if !content.data.is_object()
            || serde_json::to_vec(&content.data)?.len() > MAX_STRUCTURED_CONTENT_BYTES
        {
            bail!("reply structured content must be a bounded JSON object");
        }
    }
    Ok(())
}

async fn ensure_topic(
    client: &mut Client,
    capability_token: &str,
    topic: &str,
    retention_class: RetentionClass,
    classification: Classification,
) -> Result<(), HttpError> {
    let response = client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::CreateTopic {
                topic: TopicSpec {
                    name: topic.to_owned(),
                    retention_class,
                    default_classification: classification,
                },
            },
        })
        .await
        .map_err(|error| {
            HttpError::upstream(format!("failed to create or validate topic: {error}"))
        })?;

    match response {
        ControlResponse::TopicCreated { .. } => Ok(()),
        ControlResponse::Error { code, message } => Err(HttpError::upstream(format!(
            "broker rejected topic ensure request: {code}: {message}"
        ))),
        other => Err(HttpError::upstream(format!(
            "unexpected broker response while ensuring topic: {other:?}"
        ))),
    }
}

async fn materialize_attachments(
    client: &mut Client,
    capability_token: &str,
    attachments: &[IncomingAttachment],
    classification: Classification,
    retention_class: RetentionClass,
) -> Result<(Vec<InteropChatAttachmentRef>, Vec<String>), HttpError> {
    let mut refs = Vec::with_capacity(attachments.len());
    let mut uploaded = Vec::new();

    for attachment in attachments {
        if let Some(artifact_id) = attachment.artifact_id.clone() {
            refs.push(InteropChatAttachmentRef {
                name: attachment.name.clone(),
                content_type: attachment.content_type.clone(),
                artifact_id,
                sha256: attachment.sha256.clone(),
                byte_length: attachment.byte_length,
            });
            continue;
        }

        let inline = attachment
            .inline_base64
            .as_deref()
            .ok_or_else(|| HttpError::bad_request("missing inline attachment payload"))?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(inline)
            .map_err(|error| {
                HttpError::bad_request(format!(
                    "failed to decode inline attachment base64: {error}"
                ))
            })?;
        let actual_sha256 = format!("{:x}", Sha256::digest(&bytes));
        validate_inline_attachment_claims(attachment, &bytes, &actual_sha256)?;
        let content_type = attachment
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_owned());

        let (response, returned_attachment) = client
            .send_with_attachment(
                ControlRequest {
                    capability_token: capability_token.to_owned(),
                    command: ControlCommand::PutArtifact {
                        artifact_id: attachment.artifact_id.clone(),
                        content_type: content_type.clone(),
                        byte_length: bytes.len() as u64,
                        sha256: Some(actual_sha256),
                        classification: Some(classification.clone()),
                        retention_class: Some(retention_class.clone()),
                    },
                },
                Some(bytes),
            )
            .await
            .map_err(|error| HttpError::upstream(format!("failed to store attachment: {error}")))?;
        if returned_attachment.is_some() {
            return Err(HttpError::upstream(
                "broker returned unexpected attachment bytes for put_artifact",
            ));
        }

        match response {
            ControlResponse::ArtifactStored { artifact } => {
                uploaded.push(artifact.artifact_id.clone());
                refs.push(InteropChatAttachmentRef {
                    name: attachment.name.clone(),
                    content_type: Some(content_type),
                    artifact_id: artifact.artifact_id,
                    sha256: Some(artifact.sha256),
                    byte_length: Some(artifact.byte_length),
                });
            }
            ControlResponse::Error { code, message } => {
                return Err(HttpError::upstream(format!(
                    "broker rejected attachment upload: {code}: {message}"
                )));
            }
            other => {
                return Err(HttpError::upstream(format!(
                    "unexpected broker response while storing attachment: {other:?}"
                )));
            }
        }
    }

    Ok((refs, uploaded))
}

fn validate_inline_attachment_claims(
    attachment: &IncomingAttachment,
    bytes: &[u8],
    actual_sha256: &str,
) -> Result<(), HttpError> {
    if let Some(expected_length) = attachment.byte_length
        && expected_length != bytes.len() as u64
    {
        return Err(HttpError::bad_request(format!(
            "inline attachment byte_length mismatch: expected {expected_length}, decoded {}",
            bytes.len()
        )));
    }
    if let Some(expected_sha256) = attachment.sha256.as_deref()
        && !actual_sha256.eq_ignore_ascii_case(expected_sha256)
    {
        return Err(HttpError::bad_request(format!(
            "inline attachment sha256 mismatch: expected {expected_sha256}, decoded {actual_sha256}"
        )));
    }
    Ok(())
}

async fn read_http_request<R>(
    stream: &mut R,
    max_request_bytes: usize,
    artifact_path: &str,
    max_artifact_request_bytes: usize,
) -> anyhow::Result<HttpRequest>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let header_limit = max_request_bytes.min(MAX_HTTP_HEADER_BYTES);
    let mut buffer = Vec::with_capacity(header_limit.min(8 * 1024));
    let mut chunk = [0_u8; 8 * 1024];
    let header_end;

    loop {
        if buffer.len() == header_limit {
            bail!("request headers exceeded {header_limit} bytes");
        }
        let remaining = header_limit - buffer.len();
        let read_limit = remaining.min(chunk.len());
        let bytes = stream.read(&mut chunk[..read_limit]).await?;
        if bytes == 0 {
            bail!("connection closed before headers were complete");
        }
        buffer.extend_from_slice(&chunk[..bytes]);

        if let Some(index) = find_header_end(&buffer) {
            header_end = index;
            break;
        }
    }

    let header_raw = std::str::from_utf8(&buffer[..header_end]).context("headers are not utf-8")?;
    let (method, path, headers) = parse_request_head(header_raw)?;
    let content_length = headers
        .get("content-length")
        .map(|value| value.parse::<usize>())
        .transpose()
        .context("invalid Content-Length value")?
        .unwrap_or(0);

    let body_limit = if path == artifact_path {
        max_artifact_request_bytes
    } else {
        max_request_bytes
    };
    let body_start = checked_body_start(header_end, content_length, body_limit)?;

    let available = buffer.len().saturating_sub(body_start).min(content_length);
    let mut body = Vec::with_capacity(available);
    body.extend_from_slice(&buffer[body_start..body_start + available]);
    drop(buffer);

    while body.len() < content_length {
        let remaining = content_length - body.len();
        let read_limit = remaining.min(chunk.len());
        let bytes = stream.read(&mut chunk[..read_limit]).await?;
        if bytes == 0 {
            bail!("connection closed before request body was complete");
        }
        body.extend_from_slice(&chunk[..bytes]);
    }

    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

fn checked_body_start(
    header_end: usize,
    content_length: usize,
    max_request_bytes: usize,
) -> anyhow::Result<usize> {
    let body_start = header_end
        .checked_add(4)
        .context("request header length overflow")?;
    let request_end = body_start
        .checked_add(content_length)
        .context("Content-Length overflow")?;
    if request_end > max_request_bytes {
        bail!("request body exceeds configured max_request_bytes");
    }
    Ok(body_start)
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_request_head(head: &str) -> anyhow::Result<(String, String, HashMap<String, String>)> {
    let mut lines = head.split("\r\n");
    let request_line = lines.next().context("missing request line")?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().context("missing method")?.to_owned();
    let target = parts.next().context("missing request target")?;
    let version = parts.next().context("missing http version")?;
    if parts.next().is_some() || !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        bail!("unsupported or malformed http version");
    }
    let path = split_request_target(target);

    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (name, value) = line.split_once(':').context("malformed header line")?;
        let name = name.trim().to_ascii_lowercase();
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
            || value
                .bytes()
                .any(|byte| byte.is_ascii_control() && byte != b'\t')
            || headers.insert(name, value.trim().to_owned()).is_some()
        {
            bail!("invalid or duplicate http header");
        }
    }

    Ok((method, path, headers))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..max_len {
        difference |= usize::from(*left.get(index).unwrap_or(&0) ^ *right.get(index).unwrap_or(&0));
    }
    difference == 0
}

fn bearer_authorized(headers: &HashMap<String, String>, expected: Option<&str>) -> bool {
    let Some(expected) = expected else {
        return true;
    };
    headers
        .get("authorization")
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|provided| constant_time_eq(provided.as_bytes(), expected.as_bytes()))
}

fn split_request_target(target: &str) -> String {
    target.split_once('?').map_or_else(
        || decode_url_component(target),
        |(path, _query)| decode_url_component(path),
    )
}

fn decode_url_component(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0usize;

    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let hi = (bytes[index + 1] as char).to_digit(16);
                let lo = (bytes[index + 2] as char).to_digit(16);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    decoded.push(((hi << 4) | lo) as u8);
                    index += 3;
                } else {
                    decoded.push(bytes[index]);
                    index += 1;
                }
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8_lossy(&decoded).into_owned()
}

fn json_response<T: Serialize>(status_code: u16, value: &T) -> HttpResponse {
    match serde_json::to_vec_pretty(value) {
        Ok(body) => HttpResponse {
            status_code,
            reason: status_reason(status_code),
            content_type: "application/json; charset=utf-8".to_owned(),
            headers: Vec::new(),
            body,
        },
        Err(error) => error_json_response(500, format!("failed to encode response json: {error}")),
    }
}

fn error_json_response(status_code: u16, message: String) -> HttpResponse {
    let body = serde_json::to_vec_pretty(&serde_json::json!({ "error": message }))
        .expect("serialize error response");
    HttpResponse {
        status_code,
        reason: status_reason(status_code),
        content_type: "application/json; charset=utf-8".to_owned(),
        headers: Vec::new(),
        body,
    }
}

fn status_reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Internal Server Error",
    }
}

fn encode_http_response(response: &HttpResponse) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nContent-Security-Policy: default-src 'none'; frame-ancestors 'none'\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\nConnection: close\r\n",
            response.status_code,
            response.reason,
            response.content_type,
            response.body.len()
        )
        .as_bytes(),
    );
    for (name, value) in &response.headers {
        if name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            && !value.bytes().any(|byte| byte.is_ascii_control())
        {
            bytes.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
        }
    }
    bytes.extend_from_slice(b"\r\n");
    bytes.extend_from_slice(&response.body);
    bytes
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
        return expressways_client::normalize_capability_token(&token);
    }
    if let Some(path) = args.token_file {
        return expressways_client::read_capability_token_file(&path);
    }

    bail!("a capability token is required via --token or --token-file")
}

fn resolve_optional_secret(
    inline: Option<String>,
    file: Option<PathBuf>,
) -> anyhow::Result<Option<String>> {
    let value = match (inline, file) {
        (Some(value), None) => Some(value),
        (None, Some(path)) => Some(expressways_client::read_secret_file(&path)?),
        (None, None) => None,
        (Some(_), Some(_)) => bail!("provide either --ingress-bearer or --ingress-bearer-file"),
    };
    value
        .map(|value| {
            let value = value.trim().to_owned();
            if value.is_empty() {
                bail!("ingress bearer must not be empty");
            }
            Ok(value)
        })
        .transpose()
}

fn init_tracing(log_level: &str) -> anyhow::Result<()> {
    let filter = EnvFilter::try_new(log_level)
        .or_else(|_| EnvFilter::try_new("info"))
        .context("failed to initialize tracing filter")?;
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_request_target_discards_query() {
        let parsed = split_request_target("/v1/webhook/handoff?source=openclaw");
        assert_eq!(parsed, "/v1/webhook/handoff");
    }

    #[test]
    fn decode_percent_encoded_path() {
        let parsed = decode_url_component("/v1/webhook/space%20name");
        assert_eq!(parsed, "/v1/webhook/space name");
    }

    #[test]
    fn request_parser_rejects_duplicate_security_headers() {
        let request = "POST /v1/webhook/handoff HTTP/1.1\r\nAuthorization: Bearer one\r\nauthorization: Bearer two";
        assert!(parse_request_head(request).is_err());
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreu"));
        assert!(!constant_time_eq(b"secret", b"secret-longer"));
        assert!(validate_bridge_exposure(true, None).is_ok());
        assert!(validate_bridge_exposure(false, Some("secret")).is_ok());
        assert!(validate_bridge_exposure(false, None).is_err());
        let mut headers = HashMap::new();
        assert!(!bearer_authorized(&headers, Some("secret")));
        headers.insert("authorization".to_owned(), "Bearer secret".to_owned());
        assert!(bearer_authorized(&headers, Some("secret")));
        assert!(bearer_authorized(&HashMap::new(), None));
        let response = encode_http_response(&error_json_response(400, "bad".to_owned()));
        let response = String::from_utf8(response).expect("response is UTF-8");
        assert!(response.contains("Content-Security-Policy: default-src 'none'"));
        assert!(response.contains("X-Content-Type-Options: nosniff"));
    }

    #[test]
    fn server_limits_and_content_length_overflow_are_rejected() {
        assert!(
            validate_server_limits(1024, MAX_ARTIFACT_BYTES, 1, MIN_REQUEST_TIMEOUT_MS).is_ok()
        );
        assert!(
            validate_server_limits(1023, MAX_ARTIFACT_BYTES, 1, MIN_REQUEST_TIMEOUT_MS).is_err()
        );
        assert!(
            validate_server_limits(1024, MAX_ARTIFACT_BYTES, usize::MAX, MIN_REQUEST_TIMEOUT_MS)
                .is_err()
        );
        assert!(
            validate_server_limits(1024, MAX_ARTIFACT_BYTES, 1, MIN_REQUEST_TIMEOUT_MS - 1)
                .is_err()
        );
        assert!(
            validate_server_limits(1024, MAX_ARTIFACT_BYTES + 1, 1, MIN_REQUEST_TIMEOUT_MS)
                .is_err()
        );

        assert!(checked_body_start(10, usize::MAX, 1024).is_err());
        assert!(checked_body_start(10, 100, 50).is_err());
        assert_eq!(checked_body_start(10, 100, 1024).expect("bounded"), 14);
        assert!(validate_default_retry_policy(3, 300, 5).is_ok());
        assert!(validate_default_retry_policy(0, 300, 5).is_err());
        assert!(validate_default_retry_policy(3, u64::MAX, 5).is_err());
    }

    #[test]
    fn webhook_schema_and_fanout_are_strictly_bounded() {
        let raw = serde_json::json!({
            "schema_version": INTEROP_CHAT_HANDOFF_SCHEMA_VERSION,
            "idempotency_key": "pigeon-message-1",
            "source_runtime": "openclaw",
            "session": {
                "session_id": "session-1",
                "channel": "chat",
                "account_id": "account-1",
                "sender_id": "sender-1"
            },
            "message": { "text": "hello" }
        });
        let mut webhook: BridgeWebhookRequest =
            serde_json::from_value(raw.clone()).expect("valid webhook");
        validate_webhook(&webhook).expect("bounded webhook");

        let content_only = serde_json::json!({
            "schema_version": INTEROP_CHAT_HANDOFF_SCHEMA_VERSION,
            "idempotency_key": "pigeon-location-1",
            "source_runtime": "pigeon",
            "session": {
                "session_id": "session-1",
                "channel": "whatsapp",
                "account_id": "account-1",
                "sender_id": "sender-1"
            },
            "message": {
                "text": "   ",
                "content": [{
                    "type": "location",
                    "data": { "latitude": 28.6139, "longitude": 77.2090 }
                }]
            }
        });
        let content_only: BridgeWebhookRequest =
            serde_json::from_value(content_only).expect("parse structured content");
        validate_webhook(&content_only).expect("structured content-only handoff");

        let mut unknown = raw;
        unknown["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<BridgeWebhookRequest>(unknown).is_ok());

        webhook.preferred_agents = vec!["agent-a".to_owned(), "agent-a".to_owned()];
        assert_eq!(
            validate_webhook(&webhook)
                .expect_err("duplicate hints must fail")
                .status_code,
            400
        );

        webhook.preferred_agents.clear();
        webhook.message.attachments = (0..=MAX_ATTACHMENTS)
            .map(|index| IncomingAttachment {
                name: Some(format!("attachment-{index}")),
                content_type: None,
                artifact_id: Some(format!("artifact-{index}")),
                inline_base64: None,
                sha256: None,
                byte_length: None,
            })
            .collect();
        assert!(validate_webhook(&webhook).is_err());

        let mut inline = IncomingAttachment {
            name: None,
            content_type: None,
            artifact_id: None,
            inline_base64: Some("aGVsbG8=".to_owned()),
            sha256: Some(
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".to_owned(),
            ),
            byte_length: Some(5),
        };
        let hello_sha256 = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        validate_inline_attachment_claims(&inline, b"hello", hello_sha256)
            .expect("matching claims");
        inline.byte_length = Some(4);
        assert!(validate_inline_attachment_claims(&inline, b"hello", hello_sha256).is_err());
        inline.byte_length = Some(5);
        inline.sha256 = Some("0".repeat(64));
        assert!(validate_inline_attachment_claims(&inline, b"hello", hello_sha256).is_err());
    }

    #[test]
    fn identifiers_are_deterministic_and_an_identity_is_required() {
        assert_eq!(
            deterministic_task_id("pigeon-message-1"),
            deterministic_task_id("pigeon-message-1")
        );
        let raw = serde_json::json!({
            "schema_version": INTEROP_CHAT_HANDOFF_SCHEMA_VERSION,
            "source_runtime": "pigeon",
            "session": {
                "session_id": "session-1",
                "channel": "whatsapp",
                "account_id": "account-1",
                "sender_id": "sender-1"
            },
            "message": { "text": "hello" }
        });
        let webhook: BridgeWebhookRequest = serde_json::from_value(raw).expect("parse webhook");
        assert!(validate_webhook(&webhook).is_err());
    }

    #[test]
    fn reply_contract_and_durable_cursor_are_validated() {
        let reply = InteropChatReplyV1 {
            schema_version: INTEROP_CHAT_REPLY_SCHEMA_VERSION.to_owned(),
            delivery_id: "delivery-1".to_owned(),
            correlation_id: "correlation-1".to_owned(),
            source_runtime: "agent".to_owned(),
            target_runtime: "pigeon".to_owned(),
            session: InteropChatSession {
                session_id: "session-1".to_owned(),
                channel: "whatsapp".to_owned(),
                account_id: "account-1".to_owned(),
                sender_id: "sender-1".to_owned(),
                sender_display_name: None,
                message_id: None,
                reply_to_message_id: Some("message-1".to_owned()),
            },
            in_reply_to_task_id: "task-1".to_owned(),
            message: InteropChatMessage {
                text: Some("hello back".to_owned()),
                attachments: Vec::new(),
                content: Vec::new(),
            },
            metadata: serde_json::json!({}),
            created_at: Utc::now(),
        };
        validate_reply(&reply).expect("valid reply");

        let path = std::env::temp_dir().join(format!("expressways-bridge-{}.json", Uuid::now_v7()));
        let mut state = CursorStore::open(&path).expect("create cursor store");
        state
            .checkpoint(INTEROP_CHAT_REPLIES_TOPIC, 42)
            .expect("save cursor");
        let loaded = CursorStore::open(&path).expect("load cursor store");
        assert_eq!(
            loaded
                .next_offset(INTEROP_CHAT_REPLIES_TOPIC)
                .expect("load reply cursor"),
            42
        );
        std::fs::remove_file(path).expect("remove state");
    }

    #[tokio::test]
    async fn request_reader_grows_with_received_data_and_bounds_headers() {
        let (mut client, mut server) = tokio::io::duplex(128 * 1024);
        let request = b"POST /v1/webhook/handoff HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello";
        client.write_all(request).await.expect("write request");
        let parsed = read_http_request(
            &mut server,
            1024,
            "/v1/artifacts",
            MAX_ARTIFACT_BYTES as usize,
        )
        .await
        .expect("parse bounded request");
        assert_eq!(parsed.body, b"hello");

        let (mut client, mut server) = tokio::io::duplex(128 * 1024);
        client
            .write_all(&vec![b'x'; MAX_HTTP_HEADER_BYTES])
            .await
            .expect("write oversized header");
        let error = read_http_request(
            &mut server,
            MAX_REQUEST_BYTES_LIMIT,
            "/v1/artifacts",
            MAX_ARTIFACT_BYTES as usize,
        )
        .await
        .expect_err("oversized header must fail");
        assert!(error.to_string().contains("headers exceeded"));
    }
}
