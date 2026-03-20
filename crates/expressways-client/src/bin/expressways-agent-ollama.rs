use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, ValueEnum};
use expressways_client::{
    AgentWorker, AgentWorkerState, AssignedTask, Client, Endpoint, TaskExecutionContext,
    WorkerRunOutcome,
};
use expressways_protocol::{
    AgentEndpoint, AgentRegistration, Classification, ControlCommand, ControlRequest,
    ControlResponse, RetentionClass, TASK_EVENTS_TOPIC, TASKS_TOPIC,
};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, value_enum, default_value_t = TransportKind::Tcp)]
    transport: TransportKind,
    #[arg(long, default_value = "127.0.0.1:7766")]
    address: String,
    #[arg(long, default_value = "./tmp/expressways.sock")]
    socket: PathBuf,
    #[arg(long, default_value = "ollama-chat-agent")]
    agent_id: String,
    #[arg(long)]
    display_name: Option<String>,
    #[arg(long, default_value = env!("CARGO_PKG_VERSION"))]
    version: String,
    #[arg(long, default_value = "Ollama chat worker powered by AgentWorker")]
    summary: String,
    #[arg(long = "skill")]
    skills: Vec<String>,
    #[arg(long = "subscribe")]
    subscriptions: Vec<String>,
    #[arg(long = "publish-topic")]
    publications: Vec<String>,
    #[arg(long, default_value = "local_ollama_worker")]
    endpoint_transport: String,
    #[arg(long)]
    endpoint_address: Option<String>,
    #[arg(long, default_value = "internal")]
    classification: Classification,
    #[arg(long, default_value = "operational")]
    retention_class: RetentionClass,
    #[arg(long, default_value_t = 120)]
    ttl_seconds: u64,
    #[arg(long, default_value = "./var/agent/ollama-agent-state.json")]
    state_path: PathBuf,
    #[arg(long, default_value = "./var/agent/ollama-results")]
    output_dir: PathBuf,
    #[arg(long, default_value = TASKS_TOPIC)]
    tasks_topic: String,
    #[arg(long, default_value = TASK_EVENTS_TOPIC)]
    task_events_topic: String,
    #[arg(long, default_value_t = 50)]
    batch_limit: usize,
    #[arg(long, default_value_t = 500)]
    poll_interval_ms: u64,
    #[arg(long, default_value_t = 30)]
    heartbeat_interval_seconds: u64,
    #[arg(long, default_value_t = false)]
    once: bool,
    #[arg(long, default_value = "http://127.0.0.1:11434")]
    ollama_url: String,
    #[arg(long, default_value = "llama3.2")]
    default_model: String,
    #[arg(long, default_value_t = 120)]
    request_timeout_seconds: u64,
    #[arg(long, default_value = "ollama_results")]
    results_topic: String,
    #[arg(long, default_value = "internal")]
    results_classification: Classification,
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
struct HandlerConfig {
    endpoint: Endpoint,
    capability_token: String,
    ollama_url: String,
    default_model: String,
    output_dir: PathBuf,
    request_timeout_seconds: u64,
    results_topic: String,
    results_classification: Classification,
}

#[derive(Debug, Deserialize)]
struct OllamaChatPayload {
    prompt: String,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    system: Option<String>,
    #[serde(default)]
    temperature: Option<f32>,
    #[serde(default)]
    max_tokens: Option<u32>,
    #[serde(default)]
    output_path: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct OllamaArtifact {
    task_id: String,
    assignment_id: String,
    agent_id: String,
    task_type: String,
    model: String,
    prompt: String,
    response: String,
    generated_at: DateTime<Utc>,
    elapsed_ms: u128,
    output_path: String,
}

#[derive(Debug)]
struct OllamaChatResult {
    model: String,
    response: String,
}

#[derive(Debug, Deserialize)]
struct OllamaChatResponse {
    model: String,
    message: OllamaMessage,
}

#[derive(Debug, Deserialize)]
struct OllamaMessage {
    content: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let endpoint = endpoint_from_cli(cli.transport, cli.address.clone(), cli.socket.clone())?;
    let capability_token = resolve_token(cli.token.clone())?;
    let registration = registration_from_cli(&cli);
    let worker_state = load_worker_state(&cli.state_path)?;
    let shutdown = CancellationToken::new();
    let handler_config = HandlerConfig {
        endpoint: endpoint.clone(),
        capability_token: capability_token.clone(),
        ollama_url: cli.ollama_url.clone(),
        default_model: cli.default_model.clone(),
        output_dir: cli.output_dir.clone(),
        request_timeout_seconds: cli.request_timeout_seconds.max(1),
        results_topic: cli.results_topic.clone(),
        results_classification: cli.results_classification.clone(),
    };

    register_agent(&endpoint, &capability_token, registration.clone()).await?;
    log_json(json!({
        "timestamp": Utc::now(),
        "event": "agent_registered",
        "agent_id": registration.agent_id,
        "skills": registration.skills,
        "subscriptions": registration.subscriptions,
        "publications": registration.publications,
    }));

    let heartbeat_handle = tokio::spawn(run_heartbeat_loop(
        endpoint.clone(),
        capability_token.clone(),
        registration.agent_id.clone(),
        shutdown.clone(),
        Duration::from_secs(cli.heartbeat_interval_seconds.max(1)),
    ));
    let signal_handle = if cli.once {
        None
    } else {
        Some(tokio::spawn(run_shutdown_listener(shutdown.clone())))
    };

    let mut worker = AgentWorker::new(
        endpoint.clone(),
        capability_token.clone(),
        registration.agent_id.clone(),
    )
    .with_topics(cli.tasks_topic.clone(), cli.task_events_topic.clone())
    .with_batch_limit(cli.batch_limit.max(1))
    .with_state(worker_state);

    let run_result = if cli.once {
        let outcome =
            run_worker_iteration(&mut worker, &cli.state_path, handler_config.clone()).await?;
        log_worker_outcome(&outcome);
        Ok(())
    } else {
        run_worker_loop(
            &mut worker,
            &cli.state_path,
            shutdown.clone(),
            Duration::from_millis(cli.poll_interval_ms.max(1)),
            handler_config,
        )
        .await
    };

    shutdown.cancel();
    await_task("heartbeat", heartbeat_handle).await;
    if let Some(signal_handle) = signal_handle {
        signal_handle.abort();
        await_task("signal_listener", signal_handle).await;
    }

    if let Err(error) = remove_agent(&endpoint, &capability_token, &registration.agent_id).await {
        log_json(json!({
            "timestamp": Utc::now(),
            "event": "agent_remove_failed",
            "agent_id": registration.agent_id,
            "error": error.to_string(),
        }));
    } else {
        log_json(json!({
            "timestamp": Utc::now(),
            "event": "agent_removed",
            "agent_id": registration.agent_id,
        }));
    }

    run_result
}

async fn run_worker_loop(
    worker: &mut AgentWorker,
    state_path: &Path,
    shutdown: CancellationToken,
    poll_interval: Duration,
    handler_config: HandlerConfig,
) -> anyhow::Result<()> {
    loop {
        let delay_after_iteration =
            match run_worker_iteration(worker, state_path, handler_config.clone()).await {
                Ok(outcome) => {
                    log_worker_outcome(&outcome);
                    matches!(outcome, WorkerRunOutcome::Idle)
                }
                Err(error) => {
                    log_json(json!({
                        "timestamp": Utc::now(),
                        "event": "worker_iteration_failed",
                        "error": error.to_string(),
                    }));
                    true
                }
            };

        if shutdown.is_cancelled() {
            break;
        }

        if delay_after_iteration {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(poll_interval) => {}
            }
        }
    }

    Ok(())
}

async fn run_worker_iteration(
    worker: &mut AgentWorker,
    state_path: &Path,
    handler_config: HandlerConfig,
) -> anyhow::Result<WorkerRunOutcome> {
    let result = worker
        .run_once_with_context(|assignment, context| async move {
            handle_assignment(assignment, handler_config, context).await
        })
        .await;
    save_worker_state(state_path, worker.state())?;
    result.map_err(Into::into)
}

async fn handle_assignment(
    assignment: AssignedTask,
    config: HandlerConfig,
    context: TaskExecutionContext,
) -> Result<(), String> {
    match assignment.task.task_type.as_str() {
        "ollama_chat" | "chat" => handle_ollama_chat(assignment, config, context).await,
        other => Err(format!("unsupported task_type `{other}`")),
    }
}

async fn handle_ollama_chat(
    assignment: AssignedTask,
    config: HandlerConfig,
    context: TaskExecutionContext,
) -> Result<(), String> {
    let payload: OllamaChatPayload = assignment
        .decode_payload_json()
        .map_err(|error| format!("invalid ollama_chat payload: {error}"))?;
    abort_if_cancelled(&context)?;

    let model = payload
        .model
        .clone()
        .unwrap_or_else(|| config.default_model.clone());
    let output_path = resolve_output_path(
        &config.output_dir,
        &assignment.task.task_id,
        payload.output_path.as_ref(),
    );

    let start = Instant::now();
    let ollama_result = call_ollama(
        &config.ollama_url,
        config.request_timeout_seconds,
        &model,
        &payload.prompt,
        payload.system.as_deref(),
        payload.temperature,
        payload.max_tokens,
        &context,
    )
    .await?;

    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    abort_if_cancelled(&context)?;

    let artifact = OllamaArtifact {
        task_id: assignment.task.task_id.clone(),
        assignment_id: assignment
            .assignment
            .assignment_id
            .unwrap_or_else(uuid::Uuid::nil)
            .to_string(),
        agent_id: assignment
            .assignment
            .agent_id
            .clone()
            .unwrap_or_else(|| "unknown".to_owned()),
        task_type: assignment.task.task_type.clone(),
        model: ollama_result.model,
        prompt: payload.prompt,
        response: ollama_result.response,
        generated_at: Utc::now(),
        elapsed_ms: start.elapsed().as_millis(),
        output_path: output_path.display().to_string(),
    };

    let rendered = serde_json::to_vec_pretty(&artifact)
        .map_err(|error| format!("failed to render ollama artifact: {error}"))?;
    abort_if_cancelled(&context)?;

    tokio::fs::write(&output_path, rendered)
        .await
        .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;
    abort_if_cancelled(&context)?;

    publish_result_artifact(&config, &artifact)
        .await
        .map_err(|error| format!("failed to publish result artifact: {error}"))?;

    log_json(json!({
        "timestamp": Utc::now(),
        "event": "ollama_response_written",
        "task_id": artifact.task_id,
        "assignment_id": artifact.assignment_id,
        "model": artifact.model,
        "output_path": artifact.output_path,
        "results_topic": config.results_topic,
    }));

    Ok(())
}

async fn call_ollama(
    ollama_url: &str,
    timeout_seconds: u64,
    model: &str,
    prompt: &str,
    system: Option<&str>,
    temperature: Option<f32>,
    max_tokens: Option<u32>,
    context: &TaskExecutionContext,
) -> Result<OllamaChatResult, String> {
    abort_if_cancelled(context)?;

    let endpoint = format!("{}/api/chat", ollama_url.trim_end_matches('/'));
    let mut messages = Vec::new();
    if let Some(system_prompt) = system {
        messages.push(json!({
            "role": "system",
            "content": system_prompt,
        }));
    }
    messages.push(json!({
        "role": "user",
        "content": prompt,
    }));

    let mut body = json!({
        "model": model,
        "messages": messages,
        "stream": false,
    });

    if let Some(tokens) = max_tokens {
        body["options"] = json!({ "num_predict": tokens });
    }
    if let Some(temp) = temperature {
        let options = body
            .get_mut("options")
            .and_then(serde_json::Value::as_object_mut);
        if let Some(options) = options {
            options.insert("temperature".to_owned(), json!(temp));
        } else {
            body["options"] = json!({ "temperature": temp });
        }
    }

    let http = HttpClient::builder()
        .timeout(Duration::from_secs(timeout_seconds.max(1)))
        .build()
        .map_err(|error| format!("failed to build HTTP client: {error}"))?;

    let request = http.post(endpoint).json(&body);

    let response = tokio::select! {
        _ = context.cancelled() => return Err(cancellation_reason(context)),
        result = request.send() => result,
    }
    .map_err(|error| format!("ollama request failed: {error}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unavailable>".to_owned());
        return Err(format!("ollama returned HTTP {status}: {body}"));
    }

    let parsed: OllamaChatResponse = response
        .json()
        .await
        .map_err(|error| format!("failed to parse ollama response: {error}"))?;

    if parsed.message.content.trim().is_empty() {
        return Err("ollama response was empty".to_owned());
    }

    Ok(OllamaChatResult {
        model: parsed.model,
        response: parsed.message.content,
    })
}

fn abort_if_cancelled(context: &TaskExecutionContext) -> Result<(), String> {
    if context.is_cancelled() {
        return Err(cancellation_reason(context));
    }

    Ok(())
}

fn cancellation_reason(context: &TaskExecutionContext) -> String {
    match context.invalidation() {
        Some(invalidation) => match invalidation.reason {
            Some(reason) => format!(
                "assignment invalidated as `{}`: {reason}",
                invalidation.status
            ),
            None => format!("assignment invalidated as `{}`", invalidation.status),
        },
        None => "assignment canceled".to_owned(),
    }
}

fn resolve_output_path(output_dir: &Path, task_id: &str, explicit: Option<&PathBuf>) -> PathBuf {
    match explicit {
        Some(path) => path.clone(),
        None => output_dir.join(format!("{task_id}.ollama.json")),
    }
}

fn registration_from_cli(cli: &Cli) -> AgentRegistration {
    let display_name = cli
        .display_name
        .clone()
        .unwrap_or_else(|| cli.agent_id.clone());
    let endpoint_address = cli
        .endpoint_address
        .clone()
        .unwrap_or_else(|| cli.agent_id.clone());
    let skills = if cli.skills.is_empty() {
        vec!["chat".to_owned(), "ollama".to_owned()]
    } else {
        cli.skills.clone()
    };
    let subscriptions = if cli.subscriptions.is_empty() {
        vec![format!("topic:{}", cli.tasks_topic)]
    } else {
        cli.subscriptions.clone()
    };
    let publications = if cli.publications.is_empty() {
        vec![
            format!("topic:{}", cli.task_events_topic),
            format!("topic:{}", cli.results_topic),
        ]
    } else {
        cli.publications.clone()
    };

    AgentRegistration {
        agent_id: cli.agent_id.clone(),
        display_name,
        version: cli.version.clone(),
        summary: cli.summary.clone(),
        skills,
        subscriptions,
        publications,
        schemas: Vec::new(),
        endpoint: AgentEndpoint {
            transport: cli.endpoint_transport.clone(),
            address: endpoint_address,
        },
        classification: cli.classification.clone(),
        retention_class: cli.retention_class.clone(),
        ttl_seconds: Some(cli.ttl_seconds.max(1)),
    }
}

async fn publish_result_artifact(
    config: &HandlerConfig,
    artifact: &OllamaArtifact,
) -> anyhow::Result<()> {
    let mut client = Client::connect(config.endpoint.clone()).await?;
    let payload = serde_json::to_string(artifact)?;

    match client
        .send(ControlRequest {
            capability_token: config.capability_token.clone(),
            command: ControlCommand::Publish {
                topic: config.results_topic.clone(),
                classification: Some(config.results_classification.clone()),
                payload,
            },
        })
        .await?
    {
        ControlResponse::PublishAccepted { .. } => Ok(()),
        ControlResponse::Error { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected publish response: {other:?}"),
    }
}

async fn register_agent(
    endpoint: &Endpoint,
    capability_token: &str,
    registration: AgentRegistration,
) -> anyhow::Result<()> {
    let mut client = Client::connect(endpoint.clone()).await?;
    match client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::RegisterAgent { registration },
        })
        .await?
    {
        ControlResponse::AgentRegistered { .. } => Ok(()),
        ControlResponse::Error { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected register-agent response: {other:?}"),
    }
}

async fn heartbeat_agent(
    endpoint: &Endpoint,
    capability_token: &str,
    agent_id: &str,
) -> anyhow::Result<()> {
    let mut client = Client::connect(endpoint.clone()).await?;
    match client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::HeartbeatAgent {
                agent_id: agent_id.to_owned(),
            },
        })
        .await?
    {
        ControlResponse::AgentHeartbeat { .. } => Ok(()),
        ControlResponse::Error { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected heartbeat-agent response: {other:?}"),
    }
}

async fn remove_agent(
    endpoint: &Endpoint,
    capability_token: &str,
    agent_id: &str,
) -> anyhow::Result<()> {
    let mut client = Client::connect(endpoint.clone()).await?;
    match client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::RemoveAgent {
                agent_id: agent_id.to_owned(),
            },
        })
        .await?
    {
        ControlResponse::AgentRemoved { .. } => Ok(()),
        ControlResponse::Error { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected remove-agent response: {other:?}"),
    }
}

async fn run_heartbeat_loop(
    endpoint: Endpoint,
    capability_token: String,
    agent_id: String,
    shutdown: CancellationToken,
    interval: Duration,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
        if let Err(error) = heartbeat_agent(&endpoint, &capability_token, &agent_id).await {
            log_json(json!({
                "timestamp": Utc::now(),
                "event": "heartbeat_failed",
                "agent_id": agent_id,
                "error": error.to_string(),
            }));
        }
    }
}

async fn run_shutdown_listener(shutdown: CancellationToken) {
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            log_json(json!({
                "timestamp": Utc::now(),
                "event": "shutdown_requested",
            }));
            shutdown.cancel();
        }
        Err(error) => {
            log_json(json!({
                "timestamp": Utc::now(),
                "event": "shutdown_listener_failed",
                "error": error.to_string(),
            }));
            shutdown.cancel();
        }
    }
}

async fn await_task<T>(name: &str, handle: tokio::task::JoinHandle<T>) {
    if let Err(error) = handle.await {
        if !error.is_cancelled() {
            log_json(json!({
                "timestamp": Utc::now(),
                "event": "background_task_join_error",
                "task": name,
                "error": error.to_string(),
            }));
        }
    }
}

fn load_worker_state(path: &Path) -> anyhow::Result<AgentWorkerState> {
    if !path.exists() {
        return Ok(AgentWorkerState::default());
    }

    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read worker state {}", path.display()))?;
    serde_json::from_str(&raw).context("failed to parse worker state")
}

fn save_worker_state(path: &Path, state: &AgentWorkerState) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let rendered = serde_json::to_vec_pretty(state)?;
    std::fs::write(path, rendered)
        .with_context(|| format!("failed to write worker state {}", path.display()))?;
    Ok(())
}

fn log_worker_outcome(outcome: &WorkerRunOutcome) {
    match outcome {
        WorkerRunOutcome::Idle => log_json(json!({
            "timestamp": Utc::now(),
            "event": "worker_idle",
        })),
        WorkerRunOutcome::Completed {
            task_id,
            assignment_id,
        } => log_json(json!({
            "timestamp": Utc::now(),
            "event": "task_completed",
            "task_id": task_id,
            "assignment_id": assignment_id,
        })),
        WorkerRunOutcome::Failed {
            task_id,
            assignment_id,
            reason,
        } => log_json(json!({
            "timestamp": Utc::now(),
            "event": "task_failed",
            "task_id": task_id,
            "assignment_id": assignment_id,
            "reason": reason,
        })),
        WorkerRunOutcome::Canceled {
            task_id,
            assignment_id,
            status,
            reason,
        } => log_json(json!({
            "timestamp": Utc::now(),
            "event": "task_canceled",
            "task_id": task_id,
            "assignment_id": assignment_id,
            "status": status,
            "reason": reason,
        })),
    }
}

fn log_json(value: serde_json::Value) {
    println!(
        "{}",
        serde_json::to_string(&value).expect("serialize structured log")
    );
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
