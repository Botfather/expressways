use std::collections::VecDeque;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, bail};
use axum::body::{Bytes, to_bytes};
use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use expressways_client::{Client, Endpoint, normalize_capability_token};
use expressways_protocol::{
    AgentQuery, Classification, ControlCommand, ControlRequest, ControlResponse, RetentionClass,
    StoredMessage, TASKS_TOPIC, TaskWorkItem,
};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

const DEFAULT_JSON_LIMIT: usize = 1024 * 1024;
const DEFAULT_ARTIFACT_LIMIT: usize = 64 * 1024 * 1024;
const MAX_CONSUME_LIMIT: usize = 10_000;
const MAX_CONCURRENT_REQUESTS: usize = 4096;
const MAX_CONCURRENT_STREAMS: usize = 1024;
const MAX_REQUEST_TIMEOUT_MS: u64 = 300_000;
const MIN_STREAM_WAIT_TIMEOUT_MS: u64 = 1_000;
const MAX_STREAM_WAIT_TIMEOUT_MS: u64 = 25_000;

#[derive(Debug, Parser)]
#[command(about = "Authenticated loopback HTTP gateway for Expressways")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:8790")]
    listen: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:7766")]
    broker_address: String,
    #[arg(long, default_value_t = DEFAULT_JSON_LIMIT)]
    max_json_bytes: usize,
    #[arg(long, default_value_t = DEFAULT_ARTIFACT_LIMIT)]
    max_artifact_bytes: usize,
    #[arg(long, default_value_t = 128)]
    max_concurrent_requests: usize,
    #[arg(long, default_value_t = 64)]
    max_concurrent_streams: usize,
    #[arg(long, default_value_t = 30_000)]
    request_timeout_ms: u64,
}

#[derive(Clone)]
struct AppState {
    broker: Endpoint,
    max_artifact_bytes: usize,
    request_timeout: Duration,
    request_slots: Arc<Semaphore>,
    stream_slots: Arc<Semaphore>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishBody {
    payload: Value,
    #[serde(default)]
    classification: Option<Classification>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsumeQuery {
    #[serde(default)]
    offset: u64,
    #[serde(default = "default_consume_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TopicEventsQuery {
    offset: Option<u64>,
    #[serde(default = "default_consume_limit")]
    limit: usize,
    #[serde(default = "default_stream_wait_timeout_ms")]
    wait_timeout_ms: u64,
}

struct TopicStreamState {
    app: AppState,
    token: String,
    topic: String,
    offset: u64,
    limit: usize,
    wait_timeout_ms: u64,
    pending: VecDeque<StoredMessage>,
    terminal: bool,
    _permit: OwnedSemaphorePermit,
}

#[derive(Debug, Deserialize)]
struct AgentQueryParams {
    skill: Option<String>,
    topic: Option<String>,
    principal: Option<String>,
    #[serde(default)]
    include_stale: bool,
}

#[derive(Debug, Serialize)]
struct GatewayErrorBody {
    error: GatewayErrorDetail,
}

#[derive(Debug, Serialize)]
struct GatewayErrorDetail {
    code: String,
    message: String,
}

#[derive(Debug)]
struct GatewayError {
    status: StatusCode,
    code: String,
    message: String,
}

impl GatewayError {
    fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
        }
    }

    fn bad_request(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message)
    }
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(GatewayErrorBody {
                error: GatewayErrorDetail {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    validate_cli(&cli)?;

    let state = AppState {
        broker: Endpoint::Tcp(cli.broker_address.clone()),
        max_artifact_bytes: cli.max_artifact_bytes,
        request_timeout: Duration::from_millis(cli.request_timeout_ms),
        request_slots: Arc::new(Semaphore::new(cli.max_concurrent_requests)),
        stream_slots: Arc::new(Semaphore::new(cli.max_concurrent_streams)),
    };
    let artifact_limit = cli.max_artifact_bytes;
    let app = Router::new()
        .route("/v1/health", get(health))
        .route("/v1/topics/{topic}/messages", post(publish).get(consume))
        .route("/v1/topics/{topic}/events", get(stream_topic))
        .route("/v1/tasks", post(submit_task))
        .route("/v1/agents", get(list_agents))
        .route(
            "/v1/artifacts",
            post(put_artifact).layer(DefaultBodyLimit::max(artifact_limit)),
        )
        .route("/v1/artifacts/{artifact_id}", get(get_artifact))
        .layer(DefaultBodyLimit::max(cli.max_json_bytes))
        .layer(middleware::from_fn(security_headers))
        .with_state(state);

    let listener = TcpListener::bind(cli.listen)
        .await
        .with_context(|| format!("failed to bind HTTP gateway at {}", cli.listen))?;
    info!(listen = %cli.listen, broker = %cli.broker_address, "Expressways HTTP gateway listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("HTTP gateway stopped unexpectedly")
}

fn validate_cli(cli: &Cli) -> anyhow::Result<()> {
    if !cli.listen.ip().is_loopback() {
        bail!(
            "HTTP gateway must bind to a loopback address; place an authenticated TLS reverse proxy in front for remote access"
        );
    }
    if cli.max_json_bytes == 0 || cli.max_json_bytes > DEFAULT_ARTIFACT_LIMIT {
        bail!("max_json_bytes must be between 1 and {DEFAULT_ARTIFACT_LIMIT}");
    }
    if cli.max_artifact_bytes == 0 || cli.max_artifact_bytes > DEFAULT_ARTIFACT_LIMIT {
        bail!("max_artifact_bytes must be between 1 and {DEFAULT_ARTIFACT_LIMIT}");
    }
    if cli.max_concurrent_requests == 0 || cli.max_concurrent_requests > MAX_CONCURRENT_REQUESTS {
        bail!("max_concurrent_requests must be between 1 and {MAX_CONCURRENT_REQUESTS}");
    }
    if cli.max_concurrent_streams == 0 || cli.max_concurrent_streams > MAX_CONCURRENT_STREAMS {
        bail!("max_concurrent_streams must be between 1 and {MAX_CONCURRENT_STREAMS}");
    }
    if cli.request_timeout_ms == 0 || cli.request_timeout_ms > MAX_REQUEST_TIMEOUT_MS {
        bail!("request_timeout_ms must be between 1 and {MAX_REQUEST_TIMEOUT_MS}");
    }
    Ok(())
}

async fn health(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ControlResponse>, GatewayError> {
    proxy_json(&state, &headers, ControlCommand::Health).await
}

async fn publish(
    State(state): State<AppState>,
    Path(topic): Path<String>,
    headers: HeaderMap,
    Json(body): Json<PublishBody>,
) -> Result<Json<ControlResponse>, GatewayError> {
    validate_path_identifier("topic", &topic)?;
    let payload = match body.payload {
        Value::String(value) => value,
        value => serde_json::to_string(&value).map_err(|error| {
            GatewayError::bad_request("invalid_payload", format!("invalid JSON payload: {error}"))
        })?,
    };
    proxy_json(
        &state,
        &headers,
        ControlCommand::Publish {
            topic,
            classification: body.classification,
            payload,
        },
    )
    .await
}

async fn consume(
    State(state): State<AppState>,
    Path(topic): Path<String>,
    Query(query): Query<ConsumeQuery>,
    headers: HeaderMap,
) -> Result<Json<ControlResponse>, GatewayError> {
    validate_path_identifier("topic", &topic)?;
    validate_consume_limit(query.limit)?;
    proxy_json(
        &state,
        &headers,
        ControlCommand::Consume {
            topic,
            offset: query.offset,
            limit: query.limit,
        },
    )
    .await
}

async fn stream_topic(
    State(state): State<AppState>,
    Path(topic): Path<String>,
    Query(query): Query<TopicEventsQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, GatewayError> {
    validate_path_identifier("topic", &topic)?;
    validate_consume_limit(query.limit)?;
    if !(MIN_STREAM_WAIT_TIMEOUT_MS..=MAX_STREAM_WAIT_TIMEOUT_MS).contains(&query.wait_timeout_ms) {
        return Err(GatewayError::bad_request(
            "invalid_wait_timeout",
            format!(
                "wait_timeout_ms must be between {MIN_STREAM_WAIT_TIMEOUT_MS} and {MAX_STREAM_WAIT_TIMEOUT_MS}"
            ),
        ));
    }
    let offset = resolve_stream_offset(query.offset, &headers)?;
    let token = bearer_token(&headers)?;
    let permit = state
        .stream_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            GatewayError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "stream_capacity_exceeded",
                "the HTTP gateway has reached its concurrent event stream limit",
            )
        })?;

    // Authenticate, authorize, and seed the stream before sending HTTP 200.
    let (response, attachment) = send(
        &state,
        token.clone(),
        ControlCommand::Consume {
            topic: topic.clone(),
            offset,
            limit: query.limit,
        },
        None,
    )
    .await?;
    if attachment.is_some() {
        return Err(upstream_protocol_error(
            "unexpected broker response attachment",
        ));
    }
    let (messages, next_offset) = extract_messages(response, &topic)?;
    let stream_state = TopicStreamState {
        app: state,
        token,
        topic,
        offset: next_offset,
        limit: query.limit,
        wait_timeout_ms: query.wait_timeout_ms,
        pending: messages.into(),
        terminal: false,
        _permit: permit,
    };
    let stream = futures_util::stream::unfold(stream_state, next_topic_event);
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}

async fn next_topic_event(
    mut stream: TopicStreamState,
) -> Option<(Result<Event, Infallible>, TopicStreamState)> {
    if stream.terminal {
        return None;
    }
    loop {
        if let Some(message) = stream.pending.pop_front() {
            let event = match serde_json::to_string(&message) {
                Ok(data) => Event::default()
                    .event("message")
                    .id(message.offset.to_string())
                    .data(data),
                Err(error) => {
                    stream.terminal = true;
                    sse_error_event("serialization_failed", &error.to_string())
                }
            };
            return Some((Ok(event), stream));
        }

        let result = send(
            &stream.app,
            stream.token.clone(),
            ControlCommand::WatchTopic {
                topic: stream.topic.clone(),
                offset: stream.offset,
                limit: stream.limit,
                wait_timeout_ms: stream.wait_timeout_ms,
            },
            None,
        )
        .await;
        match result {
            Ok((response, None)) => match extract_messages(response, &stream.topic) {
                Ok((messages, next_offset)) => {
                    stream.offset = next_offset;
                    stream.pending.extend(messages);
                }
                Err(error) => {
                    stream.terminal = true;
                    let event = sse_error_event(&error.code, &error.message);
                    return Some((Ok(event), stream));
                }
            },
            Ok((_, Some(_))) => {
                stream.terminal = true;
                let event = sse_error_event(
                    "broker_protocol_error",
                    "unexpected broker response attachment",
                );
                return Some((Ok(event), stream));
            }
            Err(error) => {
                stream.terminal = true;
                let event = sse_error_event(&error.code, &error.message);
                return Some((Ok(event), stream));
            }
        }
    }
}

fn extract_messages(
    response: ControlResponse,
    expected_topic: &str,
) -> Result<(Vec<StoredMessage>, u64), GatewayError> {
    match response {
        ControlResponse::Messages {
            topic,
            messages,
            next_offset,
        } if topic == expected_topic => Ok((messages, next_offset)),
        ControlResponse::Messages { .. } => Err(upstream_protocol_error(
            "broker returned messages for an unexpected topic",
        )),
        ControlResponse::Error { code, message } => Err(broker_error(code, message)),
        _ => Err(upstream_protocol_error(
            "broker returned an unexpected consume response",
        )),
    }
}

fn resolve_stream_offset(
    requested_offset: Option<u64>,
    headers: &HeaderMap,
) -> Result<u64, GatewayError> {
    if let Some(offset) = requested_offset {
        return Ok(offset);
    }
    let Some(last_event_id) = unique_optional_header(headers, "last-event-id")? else {
        return Ok(0);
    };
    let last_offset = last_event_id.parse::<u64>().map_err(|_| {
        GatewayError::bad_request(
            "invalid_last_event_id",
            "Last-Event-ID must be an unsigned topic offset",
        )
    })?;
    last_offset.checked_add(1).ok_or_else(|| {
        GatewayError::bad_request(
            "invalid_last_event_id",
            "Last-Event-ID cannot advance beyond the offset range",
        )
    })
}

fn sse_error_event(code: &str, message: &str) -> Event {
    let data = serde_json::to_string(&GatewayErrorBody {
        error: GatewayErrorDetail {
            code: code.to_owned(),
            message: message.to_owned(),
        },
    })
    .unwrap_or_else(|_| {
        "{\"error\":{\"code\":\"serialization_failed\",\"message\":\"failed to encode stream error\"}}"
            .to_owned()
    });
    Event::default().event("error").data(data)
}

async fn submit_task(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(task): Json<TaskWorkItem>,
) -> Result<Json<ControlResponse>, GatewayError> {
    let classification = parse_optional_header::<Classification>(&headers, "x-classification")?;
    let payload = serde_json::to_string(&task).map_err(|error| {
        GatewayError::bad_request("invalid_task", format!("failed to encode task: {error}"))
    })?;
    proxy_json(
        &state,
        &headers,
        ControlCommand::Publish {
            topic: TASKS_TOPIC.to_owned(),
            classification,
            payload,
        },
    )
    .await
}

async fn list_agents(
    State(state): State<AppState>,
    Query(query): Query<AgentQueryParams>,
    headers: HeaderMap,
) -> Result<Json<ControlResponse>, GatewayError> {
    proxy_json(
        &state,
        &headers,
        ControlCommand::ListAgents {
            query: AgentQuery {
                skill: query.skill,
                topic: query.topic,
                principal: query.principal,
                include_stale: query.include_stale,
            },
        },
    )
    .await
}

async fn put_artifact(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Request,
) -> Result<Json<ControlResponse>, GatewayError> {
    let token = bearer_token(&headers)?;
    let content_type = required_header(&headers, header::CONTENT_TYPE.as_str())?;
    let artifact_id = optional_header(&headers, "x-artifact-id")?;
    let claimed_sha256 = optional_header(&headers, "x-content-sha256")?;
    let classification = parse_optional_header::<Classification>(&headers, "x-classification")?;
    let retention_class = parse_optional_header::<RetentionClass>(&headers, "x-retention-class")?;
    let bytes = to_bytes(request.into_body(), state.max_artifact_bytes)
        .await
        .map_err(|error| {
            GatewayError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "artifact_too_large",
                format!("artifact body exceeds the configured limit: {error}"),
            )
        })?;
    if bytes.is_empty() {
        return Err(GatewayError::bad_request(
            "empty_artifact",
            "artifact body must not be empty",
        ));
    }
    let actual_sha256 = format!("{:x}", Sha256::digest(&bytes));
    if let Some(claimed) = claimed_sha256.as_deref()
        && !claimed.eq_ignore_ascii_case(&actual_sha256)
    {
        return Err(GatewayError::bad_request(
            "sha256_mismatch",
            "X-Content-Sha256 does not match the request body",
        ));
    }
    let command = ControlCommand::PutArtifact {
        artifact_id,
        content_type,
        byte_length: bytes.len() as u64,
        sha256: Some(actual_sha256),
        classification,
        retention_class,
    };
    let (response, attachment) = send(&state, token, command, Some(bytes.to_vec())).await?;
    if attachment.is_some() {
        return Err(upstream_protocol_error(
            "unexpected artifact response attachment",
        ));
    }
    response_json(response)
}

async fn get_artifact(
    State(state): State<AppState>,
    Path(artifact_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, GatewayError> {
    validate_path_identifier("artifact_id", &artifact_id)?;
    let token = bearer_token(&headers)?;
    let (response, attachment) = send(
        &state,
        token,
        ControlCommand::GetArtifact { artifact_id },
        None,
    )
    .await?;
    match response {
        ControlResponse::Artifact { artifact } => {
            let bytes = attachment.ok_or_else(|| {
                upstream_protocol_error("broker omitted the artifact response body")
            })?;
            let mut response_headers = HeaderMap::new();
            insert_header(
                &mut response_headers,
                header::CONTENT_TYPE,
                &artifact.content_type,
            )?;
            insert_header_name(
                &mut response_headers,
                "x-artifact-id",
                &artifact.artifact_id,
            )?;
            insert_header_name(&mut response_headers, "x-content-sha256", &artifact.sha256)?;
            insert_header_name(
                &mut response_headers,
                "x-classification",
                &artifact.classification.to_string(),
            )?;
            insert_header_name(
                &mut response_headers,
                "x-retention-class",
                &artifact.retention_class.to_string(),
            )?;
            Ok((StatusCode::OK, response_headers, Bytes::from(bytes)).into_response())
        }
        ControlResponse::Error { code, message } => Err(broker_error(code, message)),
        _ => Err(upstream_protocol_error(
            "broker returned an unexpected artifact response",
        )),
    }
}

async fn proxy_json(
    state: &AppState,
    headers: &HeaderMap,
    command: ControlCommand,
) -> Result<Json<ControlResponse>, GatewayError> {
    let token = bearer_token(headers)?;
    let (response, attachment) = send(state, token, command, None).await?;
    if attachment.is_some() {
        return Err(upstream_protocol_error(
            "unexpected broker response attachment",
        ));
    }
    response_json(response)
}

async fn send(
    state: &AppState,
    token: String,
    command: ControlCommand,
    attachment: Option<Vec<u8>>,
) -> Result<(ControlResponse, Option<Vec<u8>>), GatewayError> {
    let permit = state
        .request_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            GatewayError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "gateway_busy",
                "the HTTP gateway has reached its concurrent broker request limit",
            )
        })?;
    let operation = async {
        let mut client = Client::connect(state.broker.clone())
            .await
            .map_err(|error| {
                error!(error = %error, "failed to connect to broker");
                GatewayError::new(
                    StatusCode::BAD_GATEWAY,
                    "broker_unavailable",
                    "failed to connect to the Expressways broker",
                )
            })?;
        client
            .send_with_attachment(
                ControlRequest {
                    capability_token: token,
                    command,
                },
                attachment,
            )
            .await
            .map_err(|error| {
                error!(error = %error, "broker request failed");
                GatewayError::new(
                    StatusCode::BAD_GATEWAY,
                    "broker_request_failed",
                    "the Expressways broker request failed",
                )
            })
    };
    let result = timeout(state.request_timeout, operation)
        .await
        .map_err(|_| {
            GatewayError::new(
                StatusCode::GATEWAY_TIMEOUT,
                "broker_timeout",
                "the Expressways broker did not respond before the gateway timeout",
            )
        });
    drop(permit);
    result?
}

fn response_json(response: ControlResponse) -> Result<Json<ControlResponse>, GatewayError> {
    match response {
        ControlResponse::Error { code, message } => Err(broker_error(code, message)),
        response => Ok(Json(response)),
    }
}

fn broker_error(code: String, message: String) -> GatewayError {
    let status = match code.as_str() {
        "authentication_failed" | "invalid_capability" => StatusCode::UNAUTHORIZED,
        "policy_denied" | "capability_denied" => StatusCode::FORBIDDEN,
        "not_found" | "topic_not_found" | "artifact_not_found" => StatusCode::NOT_FOUND,
        "quota_exceeded" => StatusCode::TOO_MANY_REQUESTS,
        "service_degraded" => StatusCode::SERVICE_UNAVAILABLE,
        code if code.starts_with("invalid_") => StatusCode::BAD_REQUEST,
        _ => StatusCode::BAD_GATEWAY,
    };
    GatewayError::new(status, code, message)
}

fn bearer_token(headers: &HeaderMap) -> Result<String, GatewayError> {
    let value =
        unique_optional_header(headers, header::AUTHORIZATION.as_str())?.ok_or_else(|| {
            GatewayError::new(
                StatusCode::UNAUTHORIZED,
                "missing_authorization",
                "missing Authorization bearer capability",
            )
        })?;
    let token = value.strip_prefix("Bearer ").ok_or_else(|| {
        GatewayError::new(
            StatusCode::UNAUTHORIZED,
            "invalid_authorization",
            "Authorization must use the Bearer scheme",
        )
    })?;
    normalize_capability_token(token).map_err(|_| {
        GatewayError::new(
            StatusCode::UNAUTHORIZED,
            "invalid_authorization",
            "Bearer capability token is invalid",
        )
    })
}

fn required_header(headers: &HeaderMap, name: &str) -> Result<String, GatewayError> {
    optional_header(headers, name)?.ok_or_else(|| {
        GatewayError::bad_request("missing_header", format!("missing required header {name}"))
    })
}

fn optional_header(headers: &HeaderMap, name: &str) -> Result<Option<String>, GatewayError> {
    unique_optional_header(headers, name)
}

fn unique_optional_header(headers: &HeaderMap, name: &str) -> Result<Option<String>, GatewayError> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(GatewayError::bad_request(
            "duplicate_header",
            format!("{name} must not be repeated"),
        ));
    }
    value
        .to_str()
        .map(|value| Some(value.to_owned()))
        .map_err(|_| GatewayError::bad_request("invalid_header", format!("invalid {name} header")))
}

fn parse_optional_header<T>(headers: &HeaderMap, name: &str) -> Result<Option<T>, GatewayError>
where
    T: FromStr<Err = String>,
{
    optional_header(headers, name)?
        .map(|value| {
            value.parse().map_err(|error| {
                GatewayError::bad_request("invalid_header", format!("invalid {name}: {error}"))
            })
        })
        .transpose()
}

fn validate_path_identifier(label: &str, value: &str) -> Result<(), GatewayError> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        return Err(GatewayError::bad_request(
            "invalid_path_parameter",
            format!("{label} is empty, too long, or contains control characters"),
        ));
    }
    Ok(())
}

fn validate_consume_limit(limit: usize) -> Result<(), GatewayError> {
    if limit == 0 || limit > MAX_CONSUME_LIMIT {
        return Err(GatewayError::bad_request(
            "invalid_limit",
            format!("limit must be between 1 and {MAX_CONSUME_LIMIT}"),
        ));
    }
    Ok(())
}

fn insert_header(
    headers: &mut HeaderMap,
    name: HeaderName,
    value: &str,
) -> Result<(), GatewayError> {
    let value = HeaderValue::from_str(value)
        .map_err(|_| upstream_protocol_error("broker returned invalid response metadata"))?;
    headers.insert(name, value);
    Ok(())
}

fn insert_header_name(
    headers: &mut HeaderMap,
    name: &'static str,
    value: &str,
) -> Result<(), GatewayError> {
    insert_header(headers, HeaderName::from_static(name), value)
}

fn upstream_protocol_error(message: impl Into<String>) -> GatewayError {
    GatewayError::new(StatusCode::BAD_GATEWAY, "broker_protocol_error", message)
}

async fn security_headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static("default-src 'none'; frame-ancestors 'none'"),
    );
    headers.insert(
        HeaderName::from_static("referrer-policy"),
        HeaderValue::from_static("no-referrer"),
    );
    response
}

fn default_consume_limit() -> usize {
    100
}

fn default_stream_wait_timeout_ms() -> u64 {
    25_000
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!(error = %error, "failed to listen for shutdown signal");
    }
    info!("HTTP gateway shutdown requested");
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use axum::body::Body;
    use chrono::Utc;
    use expressways_client::CustomEndpoint;
    use expressways_protocol::{ArtifactMetadata, ControlWireEnvelope};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    type ObservedRequests = Arc<Mutex<Vec<(ControlRequest, Option<Vec<u8>>)>>>;

    fn fake_broker(response: ControlResponse) -> (Endpoint, ObservedRequests) {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_for_connector = Arc::clone(&observed);
        let endpoint = Endpoint::Custom(CustomEndpoint::new("fake-broker", move || {
            let response = response.clone();
            let observed = Arc::clone(&observed_for_connector);
            async move {
                let (client, mut server) = tokio::io::duplex(1024 * 1024);
                tokio::spawn(async move {
                    let frame_length = server.read_u32().await.expect("request length") as usize;
                    let mut frame = vec![0_u8; frame_length];
                    server.read_exact(&mut frame).await.expect("request frame");
                    let (envelope, attachment) =
                        ControlWireEnvelope::decode_packet(&frame).expect("request packet");
                    let request = match envelope {
                        ControlWireEnvelope::Request { request, .. } => request,
                        other => panic!("unexpected request envelope: {other:?}"),
                    };
                    observed
                        .lock()
                        .expect("observed lock")
                        .push((request, (!attachment.is_empty()).then_some(attachment)));

                    let envelope = ControlWireEnvelope::Response {
                        response,
                        attachment_length: 0,
                    };
                    let packet = envelope
                        .encode_with_attachment(None)
                        .expect("response packet");
                    server
                        .write_u32(packet.len() as u32)
                        .await
                        .expect("response length");
                    server.write_all(&packet).await.expect("response frame");
                });
                Ok(Box::new(client) as expressways_client::BoxedClientIo)
            }
        }));
        (endpoint, observed)
    }

    fn bearer_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer caller-capability"),
        );
        headers
    }

    #[test]
    fn rejects_non_loopback_listener() {
        let cli = Cli {
            listen: "0.0.0.0:8790".parse().expect("socket"),
            broker_address: "127.0.0.1:7766".to_owned(),
            max_json_bytes: DEFAULT_JSON_LIMIT,
            max_artifact_bytes: DEFAULT_ARTIFACT_LIMIT,
            max_concurrent_requests: 128,
            max_concurrent_streams: 64,
            request_timeout_ms: 30_000,
        };
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn extracts_and_bounds_bearer_capabilities() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer signed-token"),
        );
        assert_eq!(bearer_token(&headers).expect("token"), "signed-token");
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic nope"),
        );
        assert!(bearer_token(&headers).is_err());
    }

    #[test]
    fn maps_broker_denials_to_http_statuses() {
        assert_eq!(
            broker_error("authentication_failed".to_owned(), "no".to_owned()).status,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            broker_error("policy_denied".to_owned(), "no".to_owned()).status,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            broker_error("quota_exceeded".to_owned(), "no".to_owned()).status,
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[test]
    fn stream_resume_prefers_explicit_offset_and_advances_last_event_id() {
        let mut headers = HeaderMap::new();
        headers.insert("last-event-id", HeaderValue::from_static("41"));
        assert_eq!(resolve_stream_offset(None, &headers).expect("resume"), 42);
        assert_eq!(
            resolve_stream_offset(Some(7), &headers).expect("explicit offset"),
            7
        );
        headers.insert("last-event-id", HeaderValue::from_static("invalid"));
        assert!(resolve_stream_offset(None, &headers).is_err());
    }

    #[test]
    fn stream_rejects_mismatched_broker_topics() {
        let error = extract_messages(
            ControlResponse::Messages {
                topic: "other".to_owned(),
                messages: Vec::new(),
                next_offset: 0,
            },
            "expected",
        )
        .expect_err("topic mismatch");
        assert_eq!(error.code, "broker_protocol_error");
    }

    #[tokio::test]
    async fn health_forwards_the_callers_capability_to_the_broker() {
        let (broker, observed) = fake_broker(ControlResponse::Health {
            node_name: "local".to_owned(),
            status: "healthy".to_owned(),
        });
        let state = AppState {
            broker,
            max_artifact_bytes: DEFAULT_ARTIFACT_LIMIT,
            request_timeout: Duration::from_secs(1),
            request_slots: Arc::new(Semaphore::new(1)),
            stream_slots: Arc::new(Semaphore::new(1)),
        };
        let response = health(State(state), bearer_headers())
            .await
            .expect("health response");
        assert!(matches!(response.0, ControlResponse::Health { .. }));
        let observed = observed.lock().expect("observed lock");
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].0.capability_token, "caller-capability");
        assert!(matches!(observed[0].0.command, ControlCommand::Health));
    }

    #[tokio::test]
    async fn broker_requests_are_time_bounded() {
        let broker = Endpoint::Custom(CustomEndpoint::new("slow-broker", || async {
            let (client, server) = tokio::io::duplex(1024);
            tokio::spawn(async move {
                let _server = server;
                tokio::time::sleep(Duration::from_secs(1)).await;
            });
            Ok(Box::new(client) as expressways_client::BoxedClientIo)
        }));
        let state = AppState {
            broker,
            max_artifact_bytes: DEFAULT_ARTIFACT_LIMIT,
            request_timeout: Duration::from_millis(5),
            request_slots: Arc::new(Semaphore::new(1)),
            stream_slots: Arc::new(Semaphore::new(1)),
        };
        let error = health(State(state), bearer_headers())
            .await
            .expect_err("slow broker should time out");
        assert_eq!(error.status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(error.code, "broker_timeout");
    }

    #[tokio::test]
    async fn artifact_upload_forwards_raw_bytes_and_verified_digest() {
        let body = b"binary artifact";
        let digest = format!("{:x}", Sha256::digest(body));
        let (broker, observed) = fake_broker(ControlResponse::ArtifactStored {
            artifact: ArtifactMetadata {
                artifact_id: "artifact-1".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                byte_length: body.len() as u64,
                sha256: digest.clone(),
                classification: Classification::Internal,
                retention_class: RetentionClass::Operational,
                created_at: Utc::now(),
                principal: "local:test".to_owned(),
                local_path: None,
            },
        });
        let state = AppState {
            broker,
            max_artifact_bytes: DEFAULT_ARTIFACT_LIMIT,
            request_timeout: Duration::from_secs(1),
            request_slots: Arc::new(Semaphore::new(1)),
            stream_slots: Arc::new(Semaphore::new(1)),
        };
        let mut headers = bearer_headers();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        headers.insert(
            HeaderName::from_static("x-content-sha256"),
            HeaderValue::from_str(&digest).expect("digest header"),
        );
        let request = Request::new(Body::from(body.as_slice()));
        let response = put_artifact(State(state), headers, request)
            .await
            .expect("artifact response");
        assert!(matches!(response.0, ControlResponse::ArtifactStored { .. }));

        let observed = observed.lock().expect("observed lock");
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].1.as_deref(), Some(body.as_slice()));
        match &observed[0].0.command {
            ControlCommand::PutArtifact {
                byte_length,
                sha256,
                ..
            } => {
                assert_eq!(*byte_length, body.len() as u64);
                assert_eq!(sha256.as_deref(), Some(digest.as_str()));
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }
}
