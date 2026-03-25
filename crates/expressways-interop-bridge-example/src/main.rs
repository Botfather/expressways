use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, bail};
use base64::Engine as _;
use chrono::{DateTime, Utc};
use clap::{Args, Parser, ValueEnum};
use expressways_client::{Client, Endpoint};
use expressways_protocol::{
    Classification, ControlCommand, ControlRequest, ControlResponse,
    INTEROP_CHAT_HANDOFF_TASK_TYPE, INTEROP_CHAT_REQUESTS_TOPIC, RetentionClass, TaskPayload,
    TaskRequirements, TaskRetryPolicy, TaskWorkItem, TopicSpec,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const WEBHOOK_SCHEMA_VERSION: &str = "interop.chat.handoff.v1";
const DEFAULT_MAX_REQUEST_BYTES: usize = 1_048_576;

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
    #[arg(long, default_value_t = DEFAULT_MAX_REQUEST_BYTES)]
    max_request_bytes: usize,
    #[arg(long)]
    ingress_bearer: Option<String>,
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
    max_request_bytes: usize,
    ingress_bearer: Option<String>,
    tasks_topic: String,
    task_type: String,
    default_skill: Option<String>,
    topic_retention_class: RetentionClass,
    topic_classification: Classification,
    default_classification: Classification,
    default_max_attempts: u32,
    default_timeout_seconds: u64,
    default_retry_delay_seconds: u64,
}

#[derive(Debug, Deserialize)]
struct BridgeWebhookRequest {
    #[serde(default)]
    schema_version: Option<String>,
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
}

#[derive(Debug, Serialize)]
struct HandoffPayload {
    schema_version: String,
    source_runtime: String,
    target_runtime: Option<String>,
    session: IncomingSession,
    message: HandoffMessage,
    routing: Option<IncomingRouting>,
    metadata: serde_json::Value,
    received_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct HandoffMessage {
    text: Option<String>,
    attachments: Vec<HandoffAttachmentRef>,
}

#[derive(Debug, Serialize)]
struct HandoffAttachmentRef {
    name: Option<String>,
    content_type: Option<String>,
    artifact_id: String,
    sha256: Option<String>,
    byte_length: Option<u64>,
}

#[derive(Debug, Serialize)]
struct AcceptedIngressResponse {
    task_id: String,
    task_type: String,
    topic: String,
    message_id: Uuid,
    offset: u64,
    classification: Classification,
    uploaded_artifacts: Vec<String>,
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
    content_type: &'static str,
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
    let runtime = BridgeRuntime {
        endpoint: endpoint_from_cli(cli.transport, cli.address, cli.socket)?,
        capability_token: resolve_token(cli.token)?,
        listen: cli.listen,
        webhook_path: cli.webhook_path,
        max_request_bytes: cli.max_request_bytes.max(1024),
        ingress_bearer: cli.ingress_bearer,
        tasks_topic: cli.tasks_topic,
        task_type: cli.task_type,
        default_skill: cli.default_skill,
        topic_retention_class: cli.topic_retention_class,
        topic_classification: cli.topic_classification,
        default_classification: cli.default_classification,
        default_max_attempts: cli.default_max_attempts.max(1),
        default_timeout_seconds: cli.default_timeout_seconds.max(1),
        default_retry_delay_seconds: cli.default_retry_delay_seconds.max(1),
    };

    run_server(runtime).await
}

async fn run_server(runtime: BridgeRuntime) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&runtime.listen)
        .await
        .with_context(|| format!("failed to bind bridge listener on {}", runtime.listen))?;
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
                let runtime = runtime.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(&mut stream, runtime).await {
                        warn!(peer = %peer, error = %error, "webhook request failed");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                info!("interop bridge shutting down");
                return Ok(());
            }
        }
    }
}

async fn handle_connection(
    stream: &mut tokio::net::TcpStream,
    runtime: BridgeRuntime,
) -> anyhow::Result<()> {
    let response = match read_http_request(stream, runtime.max_request_bytes).await {
        Ok(request) => match process_request(runtime, request).await {
            Ok(accepted) => json_response(202, &accepted),
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
) -> Result<AcceptedIngressResponse, HttpError> {
    if request.method != "POST" {
        return Err(HttpError::method_not_allowed(
            "only POST requests are supported for this endpoint",
        ));
    }
    if request.path != runtime.webhook_path {
        return Err(HttpError::not_found(format!(
            "unsupported path `{}`",
            request.path
        )));
    }

    if let Some(expected_bearer) = runtime.ingress_bearer.as_deref() {
        let actual = request
            .headers
            .get("authorization")
            .cloned()
            .unwrap_or_default();
        let expected = format!("Bearer {expected_bearer}");
        if actual != expected {
            return Err(HttpError::unauthorized(
                "missing or invalid Authorization bearer token",
            ));
        }
    }

    let webhook =
        serde_json::from_slice::<BridgeWebhookRequest>(&request.body).map_err(|error| {
            HttpError::bad_request(format!("failed to parse webhook json: {error}"))
        })?;
    submit_webhook(runtime, webhook).await
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
        .unwrap_or_else(|| runtime.default_classification.clone());
    let artifact_retention = webhook
        .retention_class
        .unwrap_or_else(|| runtime.topic_retention_class.clone());

    let (attachments, uploaded_artifacts) = materialize_attachments(
        &mut client,
        &runtime.capability_token,
        &webhook.message.attachments,
        classification.clone(),
        artifact_retention,
    )
    .await?;

    let task_id = webhook
        .task_id
        .unwrap_or_else(|| Uuid::now_v7().to_string());
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

    let payload = HandoffPayload {
        schema_version: WEBHOOK_SCHEMA_VERSION.to_owned(),
        source_runtime: webhook.source_runtime,
        target_runtime: webhook.target_runtime,
        session: webhook.session,
        message: HandoffMessage {
            text: webhook.message.text,
            attachments,
        },
        routing,
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
        },
        payload: TaskPayload::json(serde_json::to_value(payload).map_err(|error| {
            HttpError::bad_request(format!("payload is invalid json: {error}"))
        })?),
        retry_policy: TaskRetryPolicy {
            max_attempts: webhook
                .max_attempts
                .unwrap_or(runtime.default_max_attempts)
                .max(1),
            timeout_seconds: webhook
                .timeout_seconds
                .unwrap_or(runtime.default_timeout_seconds)
                .max(1),
            retry_delay_seconds: webhook
                .retry_delay_seconds
                .unwrap_or(runtime.default_retry_delay_seconds)
                .max(1),
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

fn validate_webhook(webhook: &BridgeWebhookRequest) -> Result<(), HttpError> {
    if let Some(schema_version) = webhook.schema_version.as_deref()
        && schema_version != WEBHOOK_SCHEMA_VERSION
    {
        return Err(HttpError::bad_request(format!(
            "unsupported schema_version `{schema_version}`; expected `{WEBHOOK_SCHEMA_VERSION}`"
        )));
    }
    if webhook.source_runtime.trim().is_empty() {
        return Err(HttpError::bad_request("source_runtime must not be empty"));
    }
    if webhook
        .metadata
        .as_ref()
        .is_some_and(|metadata| !metadata.is_object())
    {
        return Err(HttpError::bad_request(
            "metadata must be a JSON object when provided",
        ));
    }
    if webhook.session.session_id.trim().is_empty()
        || webhook.session.channel.trim().is_empty()
        || webhook.session.account_id.trim().is_empty()
        || webhook.session.sender_id.trim().is_empty()
    {
        return Err(HttpError::bad_request(
            "session_id, channel, account_id, and sender_id are required",
        ));
    }

    for attachment in &webhook.message.attachments {
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
) -> Result<(Vec<HandoffAttachmentRef>, Vec<String>), HttpError> {
    let mut refs = Vec::with_capacity(attachments.len());
    let mut uploaded = Vec::new();

    for attachment in attachments {
        if let Some(artifact_id) = attachment.artifact_id.clone() {
            refs.push(HandoffAttachmentRef {
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
                        sha256: attachment.sha256.clone(),
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
                refs.push(HandoffAttachmentRef {
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

async fn read_http_request(
    stream: &mut tokio::net::TcpStream,
    max_request_bytes: usize,
) -> anyhow::Result<HttpRequest> {
    let mut buffer = vec![0_u8; max_request_bytes];
    let mut read = 0usize;
    let header_end;

    loop {
        if read == buffer.len() {
            bail!("request exceeded {} bytes", max_request_bytes);
        }
        let bytes = stream.read(&mut buffer[read..]).await?;
        if bytes == 0 {
            bail!("connection closed before headers were complete");
        }
        read += bytes;

        if let Some(index) = find_header_end(&buffer[..read]) {
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

    if header_end + 4 + content_length > max_request_bytes {
        bail!("request body exceeds configured max_request_bytes");
    }

    let body_start = header_end + 4;
    let available = read.saturating_sub(body_start);
    let mut body = Vec::with_capacity(content_length);
    body.extend_from_slice(&buffer[body_start..read]);

    if content_length > available {
        let remaining = content_length - available;
        let mut tail = vec![0_u8; remaining];
        stream.read_exact(&mut tail).await?;
        body.extend_from_slice(&tail);
    } else if content_length < available {
        body.truncate(content_length);
    }

    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index)
}

fn parse_request_head(head: &str) -> anyhow::Result<(String, String, HashMap<String, String>)> {
    let mut lines = head.split("\r\n");
    let request_line = lines.next().context("missing request line")?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().context("missing method")?.to_owned();
    let target = parts.next().context("missing request target")?;
    let _version = parts.next().context("missing http version")?;
    let path = split_request_target(target);

    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }

    Ok((method, path, headers))
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
            content_type: "application/json; charset=utf-8",
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
        content_type: "application/json; charset=utf-8",
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
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
            response.status_code,
            response.reason,
            response.content_type,
            response.body.len()
        )
        .as_bytes(),
    );
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
        return Ok(token);
    }
    if let Some(path) = args.token_file {
        let token = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read token file {}", path.display()))?;
        return Ok(token.trim().to_owned());
    }

    bail!("a capability token is required via --token or --token-file")
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
}
