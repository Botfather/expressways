use std::time::{Duration, Instant};

use futures_util::StreamExt;
use reqwest::{Client as HttpClient, RequestBuilder, StatusCode};
use tokio::time::sleep;

use crate::model::{SessionRole, SessionTurn};

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderToolCall {
    pub name: String,
    pub args: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProviderStep {
    Respond { text: String },
    ToolCalls { calls: Vec<ProviderToolCall> },
}

pub const DEFAULT_PROVIDER_MAX_ATTEMPTS: usize = 3;
pub const DEFAULT_PROVIDER_BASE_BACKOFF_MS: u64 = 200;
pub const DEFAULT_PROVIDER_MAX_BACKOFF_MS: u64 = 2_000;
pub const DEFAULT_PROVIDER_JITTER_MS: u64 = 75;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderRetryConfig {
    pub max_attempts: usize,
    pub base_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub jitter_ms: u64,
}

impl ProviderRetryConfig {
    pub fn normalized(self) -> Self {
        let max_attempts = self.max_attempts.max(1);
        let base_backoff_ms = self.base_backoff_ms.max(1);
        let max_backoff_ms = self.max_backoff_ms.max(base_backoff_ms);
        Self {
            max_attempts,
            base_backoff_ms,
            max_backoff_ms,
            jitter_ms: self.jitter_ms,
        }
    }
}

impl Default for ProviderRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_PROVIDER_MAX_ATTEMPTS,
            base_backoff_ms: DEFAULT_PROVIDER_BASE_BACKOFF_MS,
            max_backoff_ms: DEFAULT_PROVIDER_MAX_BACKOFF_MS,
            jitter_ms: DEFAULT_PROVIDER_JITTER_MS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderInvocationStats {
    pub attempts: usize,
    pub elapsed_ms: u64,
}

impl ProviderInvocationStats {
    fn from_elapsed(attempts: usize, elapsed: Duration) -> Self {
        let elapsed_ms = elapsed.as_millis().min(u64::MAX as u128) as u64;
        Self {
            attempts: attempts.max(1),
            elapsed_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderInvocation {
    pub step: ProviderStep,
    pub stats: ProviderInvocationStats,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderInvocationError {
    pub message: String,
    pub stats: ProviderInvocationStats,
}

impl std::fmt::Display for ProviderInvocationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}

impl std::error::Error for ProviderInvocationError {}

#[derive(Debug, Clone)]
pub struct ProviderRequest {
    pub latest_user_text: String,
    pub history: Vec<SessionTurn>,
    pub memory_summary: Option<String>,
    pub pending_tool_result: Option<ToolResultContext>,
}

#[derive(Debug, Clone)]
pub struct ToolResultContext {
    pub tool_name: String,
    pub result: serde_json::Value,
}

pub trait Provider {
    fn next_step(&mut self, request: &ProviderRequest) -> ProviderStep;
}

#[derive(Debug, Clone)]
pub enum ProviderBackend {
    RuleBased,
    OpenAi(OpenAiConfig),
    Anthropic(AnthropicConfig),
}

#[derive(Debug, Clone)]
pub struct OpenAiConfig {
    pub api_base: String,
    pub model: String,
    pub api_key: Option<String>,
    pub timeout: Duration,
    pub retry: ProviderRetryConfig,
    pub system_prompt: String,
    pub http_client: HttpClient,
}

impl OpenAiConfig {
    pub fn default_api_base() -> String {
        "https://api.openai.com/v1".to_owned()
    }

    pub fn default_system_prompt() -> String {
        "You are a Nanobot-style runtime running on Expressways. Reply normally unless a listed function is required. If a function is required, return a tool call with valid JSON arguments.".to_owned()
    }
}

#[derive(Debug, Clone)]
pub struct AnthropicConfig {
    pub api_base: String,
    pub model: String,
    pub api_key: Option<String>,
    pub timeout: Duration,
    pub retry: ProviderRetryConfig,
    pub system_prompt: String,
    pub api_version: String,
    pub http_client: HttpClient,
}

impl AnthropicConfig {
    pub fn default_api_base() -> String {
        "https://api.anthropic.com".to_owned()
    }

    pub fn default_api_version() -> String {
        "2023-06-01".to_owned()
    }

    pub fn default_system_prompt() -> String {
        OpenAiConfig::default_system_prompt()
    }
}

#[derive(Debug, Default, Clone)]
pub struct RuleBasedProvider;

impl Provider for RuleBasedProvider {
    fn next_step(&mut self, request: &ProviderRequest) -> ProviderStep {
        if let Some(tool_result) = &request.pending_tool_result {
            let rendered = serde_json::to_string_pretty(&tool_result.result)
                .unwrap_or_else(|_| tool_result.result.to_string());
            return ProviderStep::Respond {
                text: format!("Tool `{}` result:\n{}", tool_result.tool_name, rendered),
            };
        }

        if let Some((name, args)) = parse_tool_command(&request.latest_user_text) {
            return ProviderStep::ToolCalls {
                calls: vec![ProviderToolCall { name, args }],
            };
        }

        if let Some(prompt) = parse_spawn_command(&request.latest_user_text) {
            return ProviderStep::ToolCalls {
                calls: vec![ProviderToolCall {
                    name: "spawn_subagent".to_owned(),
                    args: serde_json::json!({ "prompt": prompt }),
                }],
            };
        }

        let mut text = request.latest_user_text.trim().to_owned();
        if text.is_empty() {
            text = "No text provided in message payload.".to_owned();
        }

        let memory_suffix = request
            .memory_summary
            .as_deref()
            .map(str::trim)
            .filter(|summary| !summary.is_empty())
            .map(|summary| format!("\n\nMemory: {summary}"))
            .unwrap_or_default();

        let history_hint = if request.history.is_empty() {
            "fresh session"
        } else {
            "continued session"
        };

        ProviderStep::Respond {
            text: format!("({history_hint}) {text}{memory_suffix}"),
        }
    }
}

async fn send_json_with_retry(
    provider_label: &str,
    retry: ProviderRetryConfig,
    build_request: impl Fn() -> RequestBuilder,
) -> Result<(serde_json::Value, ProviderInvocationStats), ProviderInvocationError> {
    let mut last_error = None;
    let started = Instant::now();
    let retry = retry.normalized();

    for attempt in 1..=retry.max_attempts {
        let response = build_request().send().await;
        match response {
            Ok(response) => {
                if response.status().is_success() {
                    return response
                        .json::<serde_json::Value>()
                        .await
                        .map(|payload| {
                            (
                                payload,
                                ProviderInvocationStats::from_elapsed(attempt, started.elapsed()),
                            )
                        })
                        .map_err(|error| ProviderInvocationError {
                            message: format!("failed to parse {provider_label} response: {error}"),
                            stats: ProviderInvocationStats::from_elapsed(
                                attempt,
                                started.elapsed(),
                            ),
                        });
                }

                let status = response.status();
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "<unavailable>".to_owned());
                let message = format!("{provider_label} endpoint returned HTTP {status}: {body}");
                if attempt < retry.max_attempts && is_retryable_status(status) {
                    last_error = Some(message);
                    sleep(provider_retry_backoff(retry, attempt)).await;
                    continue;
                }
                return Err(ProviderInvocationError {
                    message,
                    stats: ProviderInvocationStats::from_elapsed(attempt, started.elapsed()),
                });
            }
            Err(error) => {
                let message = format!("{provider_label} request failed: {error}");
                if attempt < retry.max_attempts && is_retryable_transport_error(&error) {
                    last_error = Some(message);
                    sleep(provider_retry_backoff(retry, attempt)).await;
                    continue;
                }
                return Err(ProviderInvocationError {
                    message,
                    stats: ProviderInvocationStats::from_elapsed(attempt, started.elapsed()),
                });
            }
        }
    }

    Err(ProviderInvocationError {
        message: last_error
            .unwrap_or_else(|| format!("{provider_label} request failed after retries")),
        stats: ProviderInvocationStats::from_elapsed(retry.max_attempts, started.elapsed()),
    })
}

async fn send_stream_with_retry(
    provider_label: &str,
    retry: ProviderRetryConfig,
    build_request: impl Fn() -> RequestBuilder,
) -> Result<(reqwest::Response, usize, Instant), ProviderInvocationError> {
    let mut last_error = None;
    let started = Instant::now();
    let retry = retry.normalized();

    for attempt in 1..=retry.max_attempts {
        match build_request().send().await {
            Ok(response) => {
                if response.status().is_success() {
                    return Ok((response, attempt, started));
                }
                let status = response.status();
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "<unavailable>".to_owned());
                let message = format!("{provider_label} endpoint returned HTTP {status}: {body}");
                if attempt < retry.max_attempts && is_retryable_status(status) {
                    last_error = Some(message);
                    sleep(provider_retry_backoff(retry, attempt)).await;
                    continue;
                }
                return Err(ProviderInvocationError {
                    message,
                    stats: ProviderInvocationStats::from_elapsed(attempt, started.elapsed()),
                });
            }
            Err(error) => {
                let message = format!("{provider_label} request failed: {error}");
                if attempt < retry.max_attempts && is_retryable_transport_error(&error) {
                    last_error = Some(message);
                    sleep(provider_retry_backoff(retry, attempt)).await;
                    continue;
                }
                return Err(ProviderInvocationError {
                    message,
                    stats: ProviderInvocationStats::from_elapsed(attempt, started.elapsed()),
                });
            }
        }
    }

    Err(ProviderInvocationError {
        message: last_error
            .unwrap_or_else(|| format!("{provider_label} request failed after retries")),
        stats: ProviderInvocationStats::from_elapsed(retry.max_attempts, started.elapsed()),
    })
}

fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

fn is_retryable_transport_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect() || error.is_request() || error.is_body()
}

fn provider_retry_backoff(retry: ProviderRetryConfig, attempt: usize) -> Duration {
    let shift = attempt.saturating_sub(1).min(6) as u32;
    let scaled = retry.base_backoff_ms.saturating_mul(1u64 << shift);
    let bounded = scaled.min(retry.max_backoff_ms);
    Duration::from_millis(bounded.saturating_add(provider_jitter_ms(retry.jitter_ms)))
}

fn provider_jitter_ms(max: u64) -> u64 {
    if max == 0 {
        return 0;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos() as u64)
        .unwrap_or(0);
    now % (max + 1)
}

pub async fn openai_next_step(
    request: &ProviderRequest,
    config: &OpenAiConfig,
) -> Result<ProviderInvocation, ProviderInvocationError> {
    let endpoint = join_api_endpoint(&config.api_base, "chat/completions");
    let messages = build_openai_messages(request, &config.system_prompt);
    let body = serde_json::json!({
        "model": config.model,
        "messages": messages,
        "tools": openai_tool_definitions(),
        "tool_choice": "auto",
        "stream": false
    });
    let auth_token = config
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .strip_prefix("Bearer ")
                .map(str::trim)
                .unwrap_or(value)
                .to_owned()
        });
    let (payload, stats) = send_json_with_retry("OpenAI", config.retry, || {
        let mut req = config.http_client.post(endpoint.clone()).json(&body);
        if let Some(token) = auth_token.as_deref() {
            req = req.bearer_auth(token);
        }
        req
    })
    .await?;

    let step = parse_openai_response(&payload)
        .map_err(|message| ProviderInvocationError { message, stats })?;
    Ok(ProviderInvocation { step, stats })
}

#[derive(Debug, Default, Clone)]
struct OpenAiToolCallAccum {
    name: String,
    arguments: String,
}

#[derive(Debug, Default, Clone)]
struct AnthropicToolUseAccum {
    name: String,
    input_json: String,
}

pub async fn openai_next_step_streaming(
    request: &ProviderRequest,
    config: &OpenAiConfig,
    mut on_text_chunk: impl FnMut(&str),
) -> Result<ProviderInvocation, ProviderInvocationError> {
    let endpoint = join_api_endpoint(&config.api_base, "chat/completions");
    let messages = build_openai_messages(request, &config.system_prompt);
    let body = serde_json::json!({
        "model": config.model,
        "messages": messages,
        "tools": openai_tool_definitions(),
        "tool_choice": "auto",
        "stream": true
    });
    let auth_token = config
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .strip_prefix("Bearer ")
                .map(str::trim)
                .unwrap_or(value)
                .to_owned()
        });
    let (response, attempts, started) = send_stream_with_retry("OpenAI", config.retry, || {
        let mut req = config.http_client.post(endpoint.clone()).json(&body);
        if let Some(token) = auth_token.as_deref() {
            req = req.bearer_auth(token);
        }
        req
    })
    .await?;
    let mut text = String::new();
    let mut tool_calls = std::collections::BTreeMap::<u64, OpenAiToolCallAccum>::new();
    parse_openai_sse_stream(response, &mut text, &mut tool_calls, &mut on_text_chunk).await?;

    let step = openai_stream_result_to_step(text, tool_calls).map_err(|message| {
        ProviderInvocationError {
            message,
            stats: ProviderInvocationStats::from_elapsed(attempts, started.elapsed()),
        }
    })?;
    Ok(ProviderInvocation {
        step,
        stats: ProviderInvocationStats::from_elapsed(attempts, started.elapsed()),
    })
}

pub async fn anthropic_next_step(
    request: &ProviderRequest,
    config: &AnthropicConfig,
) -> Result<ProviderInvocation, ProviderInvocationError> {
    let endpoint = join_api_endpoint(&config.api_base, "messages");
    let body = serde_json::json!({
        "model": config.model,
        "max_tokens": 1024,
        "system": config.system_prompt,
        "messages": build_anthropic_messages(request),
        "tools": anthropic_tool_definitions(),
        "tool_choice": { "type": "auto" }
    });
    let api_key = config
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let api_version = config.api_version.trim().to_owned();

    let (payload, stats) = send_json_with_retry("Anthropic", config.retry, || {
        let mut req = config
            .http_client
            .post(endpoint.clone())
            .header("anthropic-version", api_version.as_str())
            .json(&body);
        if let Some(key) = api_key.as_deref() {
            req = req.header("x-api-key", key);
        }
        req
    })
    .await?;

    let step = parse_anthropic_response(&payload)
        .map_err(|message| ProviderInvocationError { message, stats })?;
    Ok(ProviderInvocation { step, stats })
}

pub async fn anthropic_next_step_streaming(
    request: &ProviderRequest,
    config: &AnthropicConfig,
    mut on_text_chunk: impl FnMut(&str),
) -> Result<ProviderInvocation, ProviderInvocationError> {
    let endpoint = join_api_endpoint(&config.api_base, "messages");
    let body = serde_json::json!({
        "model": config.model,
        "max_tokens": 1024,
        "system": config.system_prompt,
        "messages": build_anthropic_messages(request),
        "tools": anthropic_tool_definitions(),
        "tool_choice": { "type": "auto" },
        "stream": true
    });
    let api_key = config
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let api_version = config.api_version.trim().to_owned();

    let (response, attempts, started) = send_stream_with_retry("Anthropic", config.retry, || {
        let mut req = config
            .http_client
            .post(endpoint.clone())
            .header("anthropic-version", api_version.as_str())
            .json(&body);
        if let Some(key) = api_key.as_deref() {
            req = req.header("x-api-key", key);
        }
        req
    })
    .await?;

    let mut text = String::new();
    let mut tool_uses = std::collections::BTreeMap::<u64, AnthropicToolUseAccum>::new();
    parse_anthropic_sse_stream(response, &mut text, &mut tool_uses, &mut on_text_chunk).await?;

    let step = anthropic_stream_result_to_step(text, tool_uses).map_err(|message| {
        ProviderInvocationError {
            message,
            stats: ProviderInvocationStats::from_elapsed(attempts, started.elapsed()),
        }
    })?;
    Ok(ProviderInvocation {
        step,
        stats: ProviderInvocationStats::from_elapsed(attempts, started.elapsed()),
    })
}

async fn parse_openai_sse_stream(
    response: reqwest::Response,
    text: &mut String,
    tool_calls: &mut std::collections::BTreeMap<u64, OpenAiToolCallAccum>,
    on_text_chunk: &mut impl FnMut(&str),
) -> Result<(), ProviderInvocationError> {
    let mut bytes_stream = response.bytes_stream();
    let mut buffer = String::new();
    let mut event_data = Vec::<String>::new();
    let mut done = false;

    while let Some(next) = bytes_stream.next().await {
        let bytes = next.map_err(|error| ProviderInvocationError {
            message: format!("failed to read OpenAI stream bytes: {error}"),
            stats: ProviderInvocationStats {
                attempts: 1,
                elapsed_ms: 0,
            },
        })?;
        let chunk = String::from_utf8_lossy(&bytes);
        buffer.push_str(&chunk);

        while let Some(line_end) = buffer.find('\n') {
            let mut line = buffer[..line_end].to_owned();
            buffer.drain(..=line_end);
            if line.ends_with('\r') {
                line.pop();
            }
            if line.is_empty() {
                if !event_data.is_empty() {
                    let payload = event_data.join("\n");
                    event_data.clear();
                    if payload.trim() == "[DONE]" {
                        done = true;
                        break;
                    }
                    let value: serde_json::Value =
                        serde_json::from_str(&payload).map_err(|error| {
                            ProviderInvocationError {
                                message: format!("failed to parse OpenAI SSE chunk: {error}"),
                                stats: ProviderInvocationStats {
                                    attempts: 1,
                                    elapsed_ms: 0,
                                },
                            }
                        })?;
                    apply_openai_stream_payload(&value, text, tool_calls, on_text_chunk).map_err(
                        |message| ProviderInvocationError {
                            message,
                            stats: ProviderInvocationStats {
                                attempts: 1,
                                elapsed_ms: 0,
                            },
                        },
                    )?;
                }
                continue;
            }
            if line.starts_with(':') {
                continue;
            }
            if let Some(data) = line.strip_prefix("data:") {
                event_data.push(data.trim_start().to_owned());
            }
        }

        if done {
            break;
        }
    }

    if !event_data.is_empty() {
        let payload = event_data.join("\n");
        if payload.trim() != "[DONE]" {
            let value: serde_json::Value =
                serde_json::from_str(&payload).map_err(|error| ProviderInvocationError {
                    message: format!("failed to parse trailing OpenAI SSE chunk: {error}"),
                    stats: ProviderInvocationStats {
                        attempts: 1,
                        elapsed_ms: 0,
                    },
                })?;
            apply_openai_stream_payload(&value, text, tool_calls, on_text_chunk).map_err(
                |message| ProviderInvocationError {
                    message,
                    stats: ProviderInvocationStats {
                        attempts: 1,
                        elapsed_ms: 0,
                    },
                },
            )?;
        }
    }

    Ok(())
}

async fn parse_anthropic_sse_stream(
    response: reqwest::Response,
    text: &mut String,
    tool_uses: &mut std::collections::BTreeMap<u64, AnthropicToolUseAccum>,
    on_text_chunk: &mut impl FnMut(&str),
) -> Result<(), ProviderInvocationError> {
    let mut bytes_stream = response.bytes_stream();
    let mut buffer = String::new();
    let mut event_data = Vec::<String>::new();
    let mut done = false;

    while let Some(next) = bytes_stream.next().await {
        let bytes = next.map_err(|error| ProviderInvocationError {
            message: format!("failed to read Anthropic stream bytes: {error}"),
            stats: ProviderInvocationStats {
                attempts: 1,
                elapsed_ms: 0,
            },
        })?;
        let chunk = String::from_utf8_lossy(&bytes);
        buffer.push_str(&chunk);

        while let Some(line_end) = buffer.find('\n') {
            let mut line = buffer[..line_end].to_owned();
            buffer.drain(..=line_end);
            if line.ends_with('\r') {
                line.pop();
            }
            if line.is_empty() {
                if !event_data.is_empty() {
                    let payload = event_data.join("\n");
                    event_data.clear();
                    if payload.trim() == "[DONE]" {
                        done = true;
                        break;
                    }
                    let value: serde_json::Value =
                        serde_json::from_str(&payload).map_err(|error| {
                            ProviderInvocationError {
                                message: format!("failed to parse Anthropic SSE chunk: {error}"),
                                stats: ProviderInvocationStats {
                                    attempts: 1,
                                    elapsed_ms: 0,
                                },
                            }
                        })?;
                    let should_stop =
                        apply_anthropic_stream_payload(&value, text, tool_uses, on_text_chunk)
                            .map_err(|message| ProviderInvocationError {
                                message,
                                stats: ProviderInvocationStats {
                                    attempts: 1,
                                    elapsed_ms: 0,
                                },
                            })?;
                    if should_stop {
                        done = true;
                        break;
                    }
                }
                continue;
            }
            if line.starts_with(':') {
                continue;
            }
            if let Some(data) = line.strip_prefix("data:") {
                event_data.push(data.trim_start().to_owned());
            }
        }

        if done {
            break;
        }
    }

    if !event_data.is_empty() {
        let payload = event_data.join("\n");
        if payload.trim() != "[DONE]" {
            let value: serde_json::Value =
                serde_json::from_str(&payload).map_err(|error| ProviderInvocationError {
                    message: format!("failed to parse trailing Anthropic SSE chunk: {error}"),
                    stats: ProviderInvocationStats {
                        attempts: 1,
                        elapsed_ms: 0,
                    },
                })?;
            let _ = apply_anthropic_stream_payload(&value, text, tool_uses, on_text_chunk)
                .map_err(|message| ProviderInvocationError {
                    message,
                    stats: ProviderInvocationStats {
                        attempts: 1,
                        elapsed_ms: 0,
                    },
                })?;
        }
    }

    Ok(())
}

fn apply_openai_stream_payload(
    payload: &serde_json::Value,
    text: &mut String,
    tool_calls: &mut std::collections::BTreeMap<u64, OpenAiToolCallAccum>,
    on_text_chunk: &mut impl FnMut(&str),
) -> Result<(), String> {
    let Some(choices) = payload.get("choices").and_then(serde_json::Value::as_array) else {
        return Ok(());
    };
    for choice in choices {
        let Some(delta) = choice.get("delta") else {
            continue;
        };

        if let Some(content_text) = delta.get("content").and_then(serde_json::Value::as_str) {
            if !content_text.is_empty() {
                text.push_str(content_text);
                on_text_chunk(content_text);
            }
        } else if let Some(parts) = delta.get("content").and_then(serde_json::Value::as_array) {
            for part in parts {
                if let Some(content_text) = part.get("text").and_then(serde_json::Value::as_str) {
                    if !content_text.is_empty() {
                        text.push_str(content_text);
                        on_text_chunk(content_text);
                    }
                } else if let Some(content_text) = part.as_str()
                    && !content_text.is_empty()
                {
                    text.push_str(content_text);
                    on_text_chunk(content_text);
                }
            }
        }

        let Some(tool_call_parts) = delta
            .get("tool_calls")
            .and_then(serde_json::Value::as_array)
        else {
            continue;
        };
        for part in tool_call_parts {
            let index = part
                .get("index")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(tool_calls.len() as u64);
            let accum = tool_calls.entry(index).or_default();
            if let Some(name) = part
                .get("function")
                .and_then(|function| function.get("name"))
                .and_then(serde_json::Value::as_str)
            {
                accum.name.push_str(name);
            }
            if let Some(arguments) = part
                .get("function")
                .and_then(|function| function.get("arguments"))
                .and_then(serde_json::Value::as_str)
            {
                accum.arguments.push_str(arguments);
            }
        }
    }
    Ok(())
}

fn apply_anthropic_stream_payload(
    payload: &serde_json::Value,
    text: &mut String,
    tool_uses: &mut std::collections::BTreeMap<u64, AnthropicToolUseAccum>,
    on_text_chunk: &mut impl FnMut(&str),
) -> Result<bool, String> {
    let payload_type = payload
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();

    match payload_type {
        "message_stop" => return Ok(true),
        "error" => {
            let message = payload
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("Anthropic streaming error")
                .to_owned();
            return Err(message);
        }
        "content_block_start" => {
            let index = payload
                .get("index")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(tool_uses.len() as u64);
            let Some(block) = payload.get("content_block") else {
                return Ok(false);
            };
            let block_type = block
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if block_type == "text" {
                if let Some(chunk) = block.get("text").and_then(serde_json::Value::as_str)
                    && !chunk.is_empty()
                {
                    text.push_str(chunk);
                    on_text_chunk(chunk);
                }
                return Ok(false);
            }
            if block_type != "tool_use" {
                return Ok(false);
            }

            let accum = tool_uses.entry(index).or_default();
            if let Some(name) = block.get("name").and_then(serde_json::Value::as_str) {
                accum.name = name.to_owned();
            }
            if let Some(input) = block.get("input") {
                let serialized = serde_json::to_string(input).unwrap_or_else(|_| "{}".to_owned());
                if !serialized.trim().is_empty() && serialized != "{}" {
                    accum.input_json = serialized;
                }
            }
            return Ok(false);
        }
        "content_block_delta" => {
            let index = payload
                .get("index")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(tool_uses.len() as u64);
            let Some(delta) = payload.get("delta") else {
                return Ok(false);
            };
            let delta_type = delta
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();

            if delta_type == "text_delta" {
                if let Some(chunk) = delta.get("text").and_then(serde_json::Value::as_str)
                    && !chunk.is_empty()
                {
                    text.push_str(chunk);
                    on_text_chunk(chunk);
                }
                return Ok(false);
            }

            if delta_type == "input_json_delta" {
                let partial = delta
                    .get("partial_json")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                if !partial.is_empty() {
                    let accum = tool_uses.entry(index).or_default();
                    accum.input_json.push_str(partial);
                }
                return Ok(false);
            }

            return Ok(false);
        }
        _ => {}
    }

    Ok(false)
}

fn openai_stream_result_to_step(
    text: String,
    tool_calls: std::collections::BTreeMap<u64, OpenAiToolCallAccum>,
) -> Result<ProviderStep, String> {
    if !tool_calls.is_empty() {
        let mut calls = Vec::new();
        for (_index, call) in tool_calls {
            if call.name.trim().is_empty() {
                return Err("OpenAI streamed tool call missing function name".to_owned());
            }
            let raw_args = call.arguments.trim();
            let args = if raw_args.is_empty() {
                serde_json::json!({})
            } else {
                serde_json::from_str::<serde_json::Value>(raw_args).unwrap_or_else(|_| {
                    serde_json::json!({
                        "raw_arguments": raw_args
                    })
                })
            };
            calls.push(ProviderToolCall {
                name: call.name,
                args,
            });
        }
        return Ok(ProviderStep::ToolCalls { calls });
    }

    if text.trim().is_empty() {
        return Err("OpenAI streaming response content was empty".to_owned());
    }

    Ok(ProviderStep::Respond { text })
}

fn anthropic_stream_result_to_step(
    text: String,
    tool_uses: std::collections::BTreeMap<u64, AnthropicToolUseAccum>,
) -> Result<ProviderStep, String> {
    if !tool_uses.is_empty() {
        let mut calls = Vec::new();
        for (_index, tool_use) in tool_uses {
            if tool_use.name.trim().is_empty() {
                return Err("Anthropic streamed tool_use block missing name".to_owned());
            }
            let raw_input = tool_use.input_json.trim();
            let args = if raw_input.is_empty() {
                serde_json::json!({})
            } else {
                serde_json::from_str::<serde_json::Value>(raw_input).unwrap_or_else(|_| {
                    serde_json::json!({
                        "raw_arguments": raw_input
                    })
                })
            };
            calls.push(ProviderToolCall {
                name: tool_use.name,
                args,
            });
        }
        return Ok(ProviderStep::ToolCalls { calls });
    }

    if text.trim().is_empty() {
        return Err("Anthropic streaming response content was empty".to_owned());
    }

    Ok(ProviderStep::Respond { text })
}

fn build_openai_messages(request: &ProviderRequest, system_prompt: &str) -> Vec<serde_json::Value> {
    let mut messages = Vec::new();
    messages.push(serde_json::json!({
        "role": "system",
        "content": system_prompt,
    }));

    if let Some(summary) = request
        .memory_summary
        .as_deref()
        .map(str::trim)
        .filter(|summary| !summary.is_empty())
    {
        messages.push(serde_json::json!({
            "role": "system",
            "content": format!("Session memory: {summary}")
        }));
    }

    let mut last_tool_call_id: Option<String> = None;
    for (index, turn) in request.history.iter().enumerate() {
        match turn.role {
            SessionRole::User => {
                if let Some(text) = turn.text.as_deref()
                    && !text.trim().is_empty()
                {
                    messages.push(serde_json::json!({
                        "role": "user",
                        "content": text
                    }));
                }
            }
            SessionRole::Assistant => {
                if let Some(text) = turn.text.as_deref()
                    && !text.trim().is_empty()
                {
                    messages.push(serde_json::json!({
                        "role": "assistant",
                        "content": text
                    }));
                }
            }
            SessionRole::ToolCall => {
                if let Some(tool_name) = turn.tool_name.as_deref() {
                    let tool_call_id = format!("call_{}", index);
                    last_tool_call_id = Some(tool_call_id.clone());
                    messages.push(serde_json::json!({
                        "role": "assistant",
                        "content": serde_json::Value::Null,
                        "tool_calls": [{
                            "id": tool_call_id,
                            "type": "function",
                            "function": {
                                "name": tool_name,
                                "arguments": turn
                                    .tool_args
                                    .as_ref()
                                    .map_or_else(|| "{}".to_owned(), serde_json::Value::to_string)
                            }
                        }]
                    }));
                }
            }
            SessionRole::ToolResult => {
                if let Some(tool_name) = turn.tool_name.as_deref() {
                    let tool_call_id = last_tool_call_id
                        .take()
                        .unwrap_or_else(|| format!("call_result_{}", index));
                    messages.push(serde_json::json!({
                        "role": "tool",
                        "tool_call_id": tool_call_id,
                        "name": tool_name,
                        "content": turn
                            .tool_result
                            .as_ref()
                            .map_or_else(|| "null".to_owned(), serde_json::Value::to_string)
                    }));
                }
            }
            SessionRole::System => {
                if let Some(text) = turn.text.as_deref()
                    && !text.trim().is_empty()
                {
                    messages.push(serde_json::json!({
                        "role": "system",
                        "content": text
                    }));
                }
            }
        }
    }

    if messages.len() == 1 {
        messages.push(serde_json::json!({
            "role": "user",
            "content": request.latest_user_text
        }));
    }

    messages
}

fn build_anthropic_messages(request: &ProviderRequest) -> Vec<serde_json::Value> {
    let mut messages = Vec::new();
    let mut last_tool_use_id: Option<String> = None;
    for (index, turn) in request.history.iter().enumerate() {
        match turn.role {
            SessionRole::User => {
                if let Some(text) = turn.text.as_deref()
                    && !text.trim().is_empty()
                {
                    messages.push(serde_json::json!({
                        "role": "user",
                        "content": text
                    }));
                }
            }
            SessionRole::Assistant => {
                if let Some(text) = turn.text.as_deref()
                    && !text.trim().is_empty()
                {
                    messages.push(serde_json::json!({
                        "role": "assistant",
                        "content": text
                    }));
                }
            }
            SessionRole::ToolCall => {
                if let Some(tool_name) = turn.tool_name.as_deref() {
                    let tool_use_id = format!("toolu_{}", index);
                    last_tool_use_id = Some(tool_use_id.clone());
                    messages.push(serde_json::json!({
                        "role": "assistant",
                        "content": [{
                            "type": "tool_use",
                            "id": tool_use_id,
                            "name": tool_name,
                            "input": turn
                                .tool_args
                                .clone()
                                .unwrap_or_else(|| serde_json::json!({}))
                        }]
                    }));
                }
            }
            SessionRole::ToolResult => {
                if turn.tool_name.is_some() {
                    let tool_use_id = last_tool_use_id
                        .take()
                        .unwrap_or_else(|| format!("toolu_result_{}", index));
                    messages.push(serde_json::json!({
                        "role": "user",
                        "content": [{
                            "type": "tool_result",
                            "tool_use_id": tool_use_id,
                            "content": turn
                                .tool_result
                                .as_ref()
                                .map_or_else(|| "null".to_owned(), serde_json::Value::to_string)
                        }]
                    }));
                }
            }
            SessionRole::System => {
                if let Some(text) = turn.text.as_deref()
                    && !text.trim().is_empty()
                {
                    messages.push(serde_json::json!({
                        "role": "assistant",
                        "content": format!("[system] {text}")
                    }));
                }
            }
        }
    }

    if messages.is_empty() {
        messages.push(serde_json::json!({
            "role": "user",
            "content": request.latest_user_text
        }));
    }

    if let Some(summary) = request
        .memory_summary
        .as_deref()
        .map(str::trim)
        .filter(|summary| !summary.is_empty())
    {
        messages.push(serde_json::json!({
            "role": "user",
            "content": format!("Session memory: {summary}")
        }));
    }

    messages
}

#[derive(Debug, Clone)]
struct CanonicalToolSchema {
    name: &'static str,
    description: &'static str,
    input_schema: serde_json::Value,
}

fn canonical_tool_schemas() -> Vec<CanonicalToolSchema> {
    vec![
        CanonicalToolSchema {
            name: "echo",
            description: "Echo arbitrary JSON payload back to the caller.",
            input_schema: serde_json::json!({
                "type": "object",
                "additionalProperties": true
            }),
        },
        CanonicalToolSchema {
            name: "read_file",
            description: "Read a UTF-8 preview from an allowed workspace path.",
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Absolute or workspace-relative file path." },
                    "max_bytes": { "type": "integer", "minimum": 1, "maximum": 1048576 }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
        },
        CanonicalToolSchema {
            name: "exec",
            description: "Run a guarded local program from the allowlist.",
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "program": { "type": "string" },
                    "args": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "timeout_ms": { "type": "integer", "minimum": 100, "maximum": 30000 }
                },
                "required": ["program"],
                "additionalProperties": false
            }),
        },
        CanonicalToolSchema {
            name: "spawn_subagent",
            description: "Queue a subagent prompt into the inbound topic for asynchronous handling.",
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "prompt": { "type": "string" },
                    "session_id": { "type": "string" }
                },
                "required": ["prompt"],
                "additionalProperties": false
            }),
        },
    ]
}

fn openai_tool_definitions() -> serde_json::Value {
    serde_json::Value::Array(
        canonical_tool_schemas()
            .into_iter()
            .map(|tool| {
                serde_json::json!({
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": tool.input_schema,
                    }
                })
            })
            .collect(),
    )
}

fn anthropic_tool_definitions() -> serde_json::Value {
    serde_json::Value::Array(
        canonical_tool_schemas()
            .into_iter()
            .map(|tool| {
                serde_json::json!({
                    "name": tool.name,
                    "description": tool.description,
                    "input_schema": tool.input_schema,
                })
            })
            .collect(),
    )
}

fn parse_openai_response(payload: &serde_json::Value) -> Result<ProviderStep, String> {
    let message = payload
        .get("choices")
        .and_then(serde_json::Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .ok_or_else(|| "OpenAI response did not contain choices[0].message".to_owned())?;

    if let Some(tool_calls) = message
        .get("tool_calls")
        .and_then(serde_json::Value::as_array)
    {
        let mut calls = Vec::new();
        for tool_call in tool_calls {
            let name = tool_call
                .get("function")
                .and_then(|function| function.get("name"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "OpenAI tool call missing function.name".to_owned())?
                .to_owned();
            let arguments_value = tool_call
                .get("function")
                .and_then(|function| function.get("arguments"))
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let args = match arguments_value {
                serde_json::Value::String(raw) => {
                    if raw.trim().is_empty() {
                        serde_json::json!({})
                    } else {
                        serde_json::from_str::<serde_json::Value>(&raw).unwrap_or_else(|_| {
                            serde_json::json!({
                                "raw_arguments": raw
                            })
                        })
                    }
                }
                other => other,
            };
            calls.push(ProviderToolCall { name, args });
        }

        if !calls.is_empty() {
            return Ok(ProviderStep::ToolCalls { calls });
        }
    }

    let text = extract_content(message.get("content"))?;
    if text.trim().is_empty() {
        return Err("OpenAI response content was empty".to_owned());
    }

    Ok(ProviderStep::Respond { text })
}

fn parse_anthropic_response(payload: &serde_json::Value) -> Result<ProviderStep, String> {
    let content = payload
        .get("content")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "Anthropic response did not contain content array".to_owned())?;

    let mut calls = Vec::new();
    for block in content {
        let block_type = block
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if block_type != "tool_use" {
            continue;
        }

        let name = block
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "Anthropic tool_use block missing name".to_owned())?
            .to_owned();
        let args = block
            .get("input")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        calls.push(ProviderToolCall { name, args });
    }

    if !calls.is_empty() {
        return Ok(ProviderStep::ToolCalls { calls });
    }

    let mut text_parts = Vec::new();
    for block in content {
        if block
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            != "text"
        {
            continue;
        }
        if let Some(text) = block.get("text").and_then(serde_json::Value::as_str)
            && !text.trim().is_empty()
        {
            text_parts.push(text.to_owned());
        }
    }

    if text_parts.is_empty() {
        return Err("Anthropic response did not contain text or tool_use content".to_owned());
    }

    Ok(ProviderStep::Respond {
        text: text_parts.join("\n"),
    })
}

fn extract_content(content: Option<&serde_json::Value>) -> Result<String, String> {
    let Some(content) = content else {
        return Ok(String::new());
    };

    if let Some(text) = content.as_str() {
        return Ok(text.to_owned());
    }

    if let Some(parts) = content.as_array() {
        let mut text_parts = Vec::new();
        for part in parts {
            if let Some(text) = part.get("text").and_then(serde_json::Value::as_str) {
                if !text.trim().is_empty() {
                    text_parts.push(text.to_owned());
                }
                continue;
            }
            if let Some(text) = part.as_str()
                && !text.trim().is_empty()
            {
                text_parts.push(text.to_owned());
            }
        }
        return Ok(text_parts.join("\n"));
    }

    Err("unsupported OpenAI content format".to_owned())
}

fn join_api_endpoint(base: &str, endpoint: &str) -> String {
    let base = base.trim().trim_end_matches('/');
    if base.ends_with("/v1") {
        format!("{base}/{endpoint}")
    } else {
        format!("{base}/v1/{endpoint}")
    }
}

fn parse_tool_command(input: &str) -> Option<(String, serde_json::Value)> {
    let trimmed = input.trim();
    let rest = trimmed.strip_prefix("/tool ")?;
    let (name, args_raw) = match rest.split_once(' ') {
        Some((name, args_raw)) => (name.trim(), args_raw.trim()),
        None => (rest.trim(), "{}"),
    };
    if name.is_empty() {
        return None;
    }
    let args = serde_json::from_str::<serde_json::Value>(args_raw).ok()?;
    Some((name.to_owned(), args))
}

fn parse_spawn_command(input: &str) -> Option<String> {
    let trimmed = input.trim();
    let prompt = trimmed.strip_prefix("/spawn ")?;
    let prompt = prompt.trim();
    if prompt.is_empty() {
        None
    } else {
        Some(prompt.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[derive(Clone, Debug)]
    struct MockHttpResponse {
        status: u16,
        body: String,
    }

    fn make_test_request() -> ProviderRequest {
        ProviderRequest {
            latest_user_text: "hello".to_owned(),
            history: Vec::new(),
            memory_summary: None,
            pending_tool_result: None,
        }
    }

    async fn spawn_mock_http_server(
        responses: Vec<MockHttpResponse>,
    ) -> (
        String,
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<Result<(), String>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock server");
        let address = listener.local_addr().expect("read listener address");
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_count_task = Arc::clone(&request_count);

        let handle = tokio::spawn(async move {
            for (index, response) in responses.into_iter().enumerate() {
                let accept_result =
                    tokio::time::timeout(Duration::from_secs(3), listener.accept()).await;
                let (mut socket, _) = match accept_result {
                    Ok(Ok(connection)) => connection,
                    Ok(Err(error)) => return Err(format!("mock server accept failed: {error}")),
                    Err(_) => {
                        return Err(format!(
                            "mock server timed out waiting for request {}",
                            index + 1
                        ));
                    }
                };
                read_http_request(&mut socket)
                    .await
                    .map_err(|error| format!("mock server failed reading request: {error}"))?;
                request_count_task.fetch_add(1, Ordering::SeqCst);

                let reason = match response.status {
                    200 => "OK",
                    429 => "Too Many Requests",
                    500 => "Internal Server Error",
                    _ => "OK",
                };
                let payload = response.body;
                let wire_response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response.status,
                    reason,
                    payload.len(),
                    payload
                );
                socket
                    .write_all(wire_response.as_bytes())
                    .await
                    .map_err(|error| format!("mock server failed writing response: {error}"))?;
                let _ = socket.shutdown().await;
            }
            Ok(())
        });

        (format!("http://{address}"), request_count, handle)
    }

    async fn read_http_request(socket: &mut tokio::net::TcpStream) -> std::io::Result<()> {
        let mut bytes = Vec::with_capacity(2048);
        let mut chunk = [0u8; 1024];
        let mut header_end = None;
        let mut expected_len = None;

        loop {
            let read = socket.read(&mut chunk).await?;
            if read == 0 {
                return Ok(());
            }
            bytes.extend_from_slice(&chunk[..read]);

            if header_end.is_none() {
                header_end = find_header_end(&bytes);
                if let Some(end) = header_end {
                    expected_len = Some(end.saturating_add(parse_content_length(&bytes[..end])));
                }
            }

            if let Some(total_len) = expected_len
                && bytes.len() >= total_len
            {
                return Ok(());
            }
        }
    }

    fn find_header_end(bytes: &[u8]) -> Option<usize> {
        bytes
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| index + 4)
    }

    fn parse_content_length(headers: &[u8]) -> usize {
        let text = String::from_utf8_lossy(headers);
        for line in text.lines() {
            let mut parts = line.splitn(2, ':');
            let Some(name) = parts.next() else {
                continue;
            };
            if !name.trim().eq_ignore_ascii_case("content-length") {
                continue;
            }
            let Some(value) = parts.next() else {
                continue;
            };
            if let Ok(length) = value.trim().parse::<usize>() {
                return length;
            }
        }
        0
    }

    #[test]
    fn parses_tool_command_with_json() {
        let parsed = parse_tool_command("/tool read_file {\"path\":\"README.md\"}");
        let Some((name, args)) = parsed else {
            panic!("expected command");
        };
        assert_eq!(name, "read_file");
        assert_eq!(args["path"], "README.md");
    }

    #[test]
    fn parses_spawn_command() {
        let parsed = parse_spawn_command(" /spawn summarize this ");
        assert_eq!(parsed.as_deref(), Some("summarize this"));
    }

    #[test]
    fn provider_responds_after_tool_result() {
        let mut provider = RuleBasedProvider;
        let step = provider.next_step(&ProviderRequest {
            latest_user_text: String::new(),
            history: Vec::new(),
            memory_summary: None,
            pending_tool_result: Some(ToolResultContext {
                tool_name: "echo".to_owned(),
                result: serde_json::json!({"ok":true}),
            }),
        });

        match step {
            ProviderStep::Respond { text } => {
                assert!(text.contains("echo"));
                assert!(text.contains("\"ok\": true"));
            }
            other => panic!("expected respond step, got {other:?}"),
        }
    }

    #[test]
    fn openai_stream_payload_accumulates_text_chunks() {
        let mut text = String::new();
        let mut tool_calls = std::collections::BTreeMap::new();
        let mut chunks = Vec::<String>::new();

        let first = serde_json::json!({
            "choices": [{
                "delta": {
                    "content": "hello "
                }
            }]
        });
        let second = serde_json::json!({
            "choices": [{
                "delta": {
                    "content": "world"
                }
            }]
        });

        apply_openai_stream_payload(&first, &mut text, &mut tool_calls, &mut |chunk| {
            chunks.push(chunk.to_owned());
        })
        .expect("apply first payload");
        apply_openai_stream_payload(&second, &mut text, &mut tool_calls, &mut |chunk| {
            chunks.push(chunk.to_owned());
        })
        .expect("apply second payload");

        assert_eq!(text, "hello world");
        assert_eq!(chunks, vec!["hello ".to_owned(), "world".to_owned()]);
        assert!(tool_calls.is_empty());
    }

    #[test]
    fn openai_stream_payload_accumulates_tool_call_arguments() {
        let mut text = String::new();
        let mut tool_calls = std::collections::BTreeMap::new();
        let mut chunks = Vec::<String>::new();

        let first = serde_json::json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "function": {
                            "name": "read_file",
                            "arguments": "{\"path\":\"READ"
                        }
                    }]
                }
            }]
        });
        let second = serde_json::json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "function": {
                            "arguments": "ME.md\"}"
                        }
                    }]
                }
            }]
        });

        apply_openai_stream_payload(&first, &mut text, &mut tool_calls, &mut |chunk| {
            chunks.push(chunk.to_owned());
        })
        .expect("apply first payload");
        apply_openai_stream_payload(&second, &mut text, &mut tool_calls, &mut |chunk| {
            chunks.push(chunk.to_owned());
        })
        .expect("apply second payload");

        assert!(chunks.is_empty());
        let step = openai_stream_result_to_step(text, tool_calls).expect("stream result to step");
        match step {
            ProviderStep::ToolCalls { calls } => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].name, "read_file");
                assert_eq!(calls[0].args["path"], "README.md");
            }
            other => panic!("expected streamed tool call, got {other:?}"),
        }
    }

    #[test]
    fn anthropic_stream_payload_accumulates_text_chunks() {
        let mut text = String::new();
        let mut tool_uses = std::collections::BTreeMap::new();
        let mut chunks = Vec::<String>::new();

        let first = serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "text_delta",
                "text": "hello "
            }
        });
        let second = serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "text_delta",
                "text": "anthropic"
            }
        });

        let done =
            apply_anthropic_stream_payload(&first, &mut text, &mut tool_uses, &mut |chunk| {
                chunks.push(chunk.to_owned());
            })
            .expect("apply first payload");
        assert!(!done);
        let done =
            apply_anthropic_stream_payload(&second, &mut text, &mut tool_uses, &mut |chunk| {
                chunks.push(chunk.to_owned());
            })
            .expect("apply second payload");
        assert!(!done);

        assert_eq!(text, "hello anthropic");
        assert_eq!(chunks, vec!["hello ".to_owned(), "anthropic".to_owned()]);
        assert!(tool_uses.is_empty());
    }

    #[test]
    fn anthropic_stream_payload_accumulates_tool_use_input_json() {
        let mut text = String::new();
        let mut tool_uses = std::collections::BTreeMap::new();
        let mut chunks = Vec::<String>::new();

        let start = serde_json::json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {
                "type": "tool_use",
                "name": "read_file"
            }
        });
        let first = serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "input_json_delta",
                "partial_json": "{\"path\":\"READ"
            }
        });
        let second = serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "input_json_delta",
                "partial_json": "ME.md\"}"
            }
        });

        apply_anthropic_stream_payload(&start, &mut text, &mut tool_uses, &mut |chunk| {
            chunks.push(chunk.to_owned());
        })
        .expect("apply start payload");
        apply_anthropic_stream_payload(&first, &mut text, &mut tool_uses, &mut |chunk| {
            chunks.push(chunk.to_owned());
        })
        .expect("apply first payload");
        apply_anthropic_stream_payload(&second, &mut text, &mut tool_uses, &mut |chunk| {
            chunks.push(chunk.to_owned());
        })
        .expect("apply second payload");

        assert!(chunks.is_empty());
        let step = anthropic_stream_result_to_step(text, tool_uses).expect("stream result to step");
        match step {
            ProviderStep::ToolCalls { calls } => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].name, "read_file");
                assert_eq!(calls[0].args["path"], "README.md");
            }
            other => panic!("expected streamed tool call, got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_response_prefers_tool_call() {
        let payload = serde_json::json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {
                            "name": "read_file",
                            "arguments": "{\"path\":\"README.md\"}"
                        }
                    }],
                    "content": null
                }
            }]
        });

        let step = parse_openai_response(&payload).expect("parse response");
        match step {
            ProviderStep::ToolCalls { calls } => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].name, "read_file");
                assert_eq!(calls[0].args["path"], "README.md");
            }
            other => panic!("expected tool call, got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_response_supports_multiple_tool_calls() {
        let payload = serde_json::json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {
                                "name": "read_file",
                                "arguments": "{\"path\":\"README.md\"}"
                            }
                        },
                        {
                            "id": "call_2",
                            "type": "function",
                            "function": {
                                "name": "exec",
                                "arguments": "{\"program\":\"ls\"}"
                            }
                        }
                    ],
                    "content": null
                }
            }]
        });

        let step = parse_openai_response(&payload).expect("parse response");
        match step {
            ProviderStep::ToolCalls { calls } => {
                assert_eq!(calls.len(), 2);
                assert_eq!(calls[0].name, "read_file");
                assert_eq!(calls[1].name, "exec");
            }
            other => panic!("expected tool calls, got {other:?}"),
        }
    }

    #[test]
    fn parse_openai_response_reads_text_content() {
        let payload = serde_json::json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "hello world",
                    "tool_calls": null
                }
            }]
        });
        let step = parse_openai_response(&payload).expect("parse response");
        match step {
            ProviderStep::Respond { text } => assert_eq!(text, "hello world"),
            other => panic!("expected response step, got {other:?}"),
        }
    }

    #[test]
    fn parse_anthropic_response_prefers_tool_use() {
        let payload = serde_json::json!({
            "content": [{
                "type": "tool_use",
                "id": "toolu_1",
                "name": "read_file",
                "input": {
                    "path": "README.md"
                }
            }]
        });

        let step = parse_anthropic_response(&payload).expect("parse response");
        match step {
            ProviderStep::ToolCalls { calls } => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].name, "read_file");
                assert_eq!(calls[0].args["path"], "README.md");
            }
            other => panic!("expected tool call, got {other:?}"),
        }
    }

    #[test]
    fn parse_anthropic_response_supports_multiple_tool_uses() {
        let payload = serde_json::json!({
            "content": [
                {
                    "type": "tool_use",
                    "id": "toolu_1",
                    "name": "read_file",
                    "input": { "path": "README.md" }
                },
                {
                    "type": "tool_use",
                    "id": "toolu_2",
                    "name": "exec",
                    "input": { "program": "ls" }
                }
            ]
        });

        let step = parse_anthropic_response(&payload).expect("parse response");
        match step {
            ProviderStep::ToolCalls { calls } => {
                assert_eq!(calls.len(), 2);
                assert_eq!(calls[0].name, "read_file");
                assert_eq!(calls[1].name, "exec");
            }
            other => panic!("expected tool calls, got {other:?}"),
        }
    }

    #[test]
    fn parse_anthropic_response_reads_text_content() {
        let payload = serde_json::json!({
            "content": [{
                "type": "text",
                "text": "hello from anthropic"
            }]
        });

        let step = parse_anthropic_response(&payload).expect("parse response");
        match step {
            ProviderStep::Respond { text } => assert_eq!(text, "hello from anthropic"),
            other => panic!("expected response step, got {other:?}"),
        }
    }

    #[test]
    fn retryable_status_classification_matches_transient_codes() {
        assert!(is_retryable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_retryable_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_retryable_status(StatusCode::BAD_REQUEST));
        assert!(!is_retryable_status(StatusCode::UNAUTHORIZED));
    }

    #[test]
    fn retry_config_normalizes_invalid_values() {
        let normalized = ProviderRetryConfig {
            max_attempts: 0,
            base_backoff_ms: 0,
            max_backoff_ms: 0,
            jitter_ms: 11,
        }
        .normalized();
        assert_eq!(normalized.max_attempts, 1);
        assert_eq!(normalized.base_backoff_ms, 1);
        assert_eq!(normalized.max_backoff_ms, 1);
        assert_eq!(normalized.jitter_ms, 11);
    }

    #[tokio::test]
    async fn openai_request_retries_and_succeeds_after_transient_error() {
        let (api_base, request_count, server_task) = spawn_mock_http_server(vec![
            MockHttpResponse {
                status: 429,
                body: serde_json::json!({ "error": { "message": "rate limit" } }).to_string(),
            },
            MockHttpResponse {
                status: 200,
                body: serde_json::json!({
                    "choices": [{
                        "message": {
                            "role": "assistant",
                            "content": "retry succeeded"
                        }
                    }]
                })
                .to_string(),
            },
        ])
        .await;

        let config = OpenAiConfig {
            api_base,
            model: "gpt-5-mini".to_owned(),
            api_key: Some("test-openai-key".to_owned()),
            timeout: Duration::from_secs(5),
            retry: ProviderRetryConfig::default(),
            system_prompt: OpenAiConfig::default_system_prompt(),
            http_client: HttpClient::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("build http client"),
        };

        let invocation = openai_next_step(&make_test_request(), &config)
            .await
            .expect("openai request should eventually succeed");
        assert_eq!(invocation.stats.attempts, 2);
        match invocation.step {
            ProviderStep::Respond { text } => assert_eq!(text, "retry succeeded"),
            other => panic!("expected respond step, got {other:?}"),
        }

        let server_result = server_task.await.expect("join mock server task");
        assert!(
            server_result.is_ok(),
            "mock server failed: {server_result:?}"
        );
        assert_eq!(request_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn anthropic_request_fails_after_retry_budget_exhausted() {
        let (api_base, request_count, server_task) = spawn_mock_http_server(vec![
            MockHttpResponse {
                status: 500,
                body: serde_json::json!({ "error": { "message": "boom-1" } }).to_string(),
            },
            MockHttpResponse {
                status: 500,
                body: serde_json::json!({ "error": { "message": "boom-2" } }).to_string(),
            },
            MockHttpResponse {
                status: 500,
                body: serde_json::json!({ "error": { "message": "boom-3" } }).to_string(),
            },
        ])
        .await;

        let config = AnthropicConfig {
            api_base,
            model: "claude-sonnet-4-5".to_owned(),
            api_key: Some("test-anthropic-key".to_owned()),
            timeout: Duration::from_secs(5),
            retry: ProviderRetryConfig::default(),
            system_prompt: AnthropicConfig::default_system_prompt(),
            api_version: AnthropicConfig::default_api_version(),
            http_client: HttpClient::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("build http client"),
        };

        let error = anthropic_next_step(&make_test_request(), &config)
            .await
            .expect_err("anthropic request should fail after retries");
        assert!(
            error
                .message
                .contains("Anthropic endpoint returned HTTP 500")
        );
        assert_eq!(error.stats.attempts, config.retry.normalized().max_attempts);

        let server_result = server_task.await.expect("join mock server task");
        assert!(
            server_result.is_ok(),
            "mock server failed: {server_result:?}"
        );
        assert_eq!(
            request_count.load(Ordering::SeqCst),
            config.retry.normalized().max_attempts
        );
    }
}
