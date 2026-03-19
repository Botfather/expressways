use std::path::{Path, PathBuf};
use std::time::Duration;

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
    #[arg(long, default_value = "example-summarizer")]
    agent_id: String,
    #[arg(long)]
    display_name: Option<String>,
    #[arg(long, default_value = env!("CARGO_PKG_VERSION"))]
    version: String,
    #[arg(long, default_value = "Example local task agent backed by AgentWorker")]
    summary: String,
    #[arg(long = "skill")]
    skills: Vec<String>,
    #[arg(long = "subscribe")]
    subscriptions: Vec<String>,
    #[arg(long = "publish-topic")]
    publications: Vec<String>,
    #[arg(long, default_value = "local_task_worker")]
    endpoint_transport: String,
    #[arg(long)]
    endpoint_address: Option<String>,
    #[arg(long, default_value = "internal")]
    classification: Classification,
    #[arg(long, default_value = "operational")]
    retention_class: RetentionClass,
    #[arg(long, default_value_t = 120)]
    ttl_seconds: u64,
    #[arg(long, default_value = "./var/agent/example-agent-state.json")]
    state_path: PathBuf,
    #[arg(long, default_value = "./var/agent/results")]
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

#[derive(Debug, Deserialize)]
struct SummarizeDocumentPayload {
    path: PathBuf,
    output_path: Option<PathBuf>,
    #[serde(default = "default_summary_lines")]
    max_summary_lines: usize,
    #[serde(default)]
    simulate_delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SummaryArtifact {
    pub(crate) task_id: String,
    pub(crate) assignment_id: String,
    pub(crate) agent_id: String,
    pub(crate) task_type: String,
    pub(crate) source_path: String,
    pub(crate) output_path: String,
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) line_count: usize,
    pub(crate) word_count: usize,
    pub(crate) char_count: usize,
    pub(crate) summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TextSummary {
    line_count: usize,
    word_count: usize,
    char_count: usize,
    summary: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let endpoint = endpoint_from_cli(cli.transport, cli.address.clone(), cli.socket.clone())?;
    let capability_token = resolve_token(cli.token.clone())?;
    let registration = registration_from_cli(&cli);
    let worker_state = load_worker_state(&cli.state_path)?;
    let shutdown = CancellationToken::new();

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
        let outcome = run_worker_iteration(&mut worker, &cli.output_dir, &cli.state_path).await?;
        log_worker_outcome(&outcome);
        Ok(())
    } else {
        run_worker_loop(
            &mut worker,
            &cli.output_dir,
            &cli.state_path,
            shutdown.clone(),
            Duration::from_millis(cli.poll_interval_ms.max(1)),
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
    output_dir: &Path,
    state_path: &Path,
    shutdown: CancellationToken,
    poll_interval: Duration,
) -> anyhow::Result<()> {
    loop {
        let delay_after_iteration = match run_worker_iteration(worker, output_dir, state_path).await
        {
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
    output_dir: &Path,
    state_path: &Path,
) -> anyhow::Result<WorkerRunOutcome> {
    let output_dir = output_dir.to_path_buf();
    let result = worker
        .run_once_with_context(|assignment, context| async move {
            handle_assignment(assignment, output_dir, context).await
        })
        .await;
    save_worker_state(state_path, worker.state())?;
    result.map_err(Into::into)
}

pub(crate) async fn handle_assignment(
    assignment: AssignedTask,
    output_dir: PathBuf,
    context: TaskExecutionContext,
) -> Result<(), String> {
    match assignment.task.task_type.as_str() {
        "summarize_document" => handle_summarize_document(assignment, output_dir, context).await,
        other => Err(format!("unsupported task_type `{other}`")),
    }
}

async fn handle_summarize_document(
    assignment: AssignedTask,
    output_dir: PathBuf,
    context: TaskExecutionContext,
) -> Result<(), String> {
    let payload: SummarizeDocumentPayload = assignment
        .decode_payload_json()
        .map_err(|error| format!("invalid summarize_document payload: {error}"))?;
    abort_if_cancelled(&context)?;

    let source_text = tokio::fs::read_to_string(&payload.path)
        .await
        .map_err(|error| format!("failed to read {}: {error}", payload.path.display()))?;
    abort_if_cancelled(&context)?;
    sleep_with_cancellation(&context, Duration::from_millis(payload.simulate_delay_ms)).await?;

    let summary = summarize_text(&source_text, payload.max_summary_lines);
    let output_path = resolve_output_path(
        &output_dir,
        &assignment.task.task_id,
        payload.output_path.as_ref(),
    );

    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    abort_if_cancelled(&context)?;

    let artifact = SummaryArtifact {
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
        source_path: payload.path.display().to_string(),
        output_path: output_path.display().to_string(),
        generated_at: Utc::now(),
        line_count: summary.line_count,
        word_count: summary.word_count,
        char_count: summary.char_count,
        summary: summary.summary,
    };

    let rendered = serde_json::to_vec_pretty(&artifact)
        .map_err(|error| format!("failed to render summary artifact: {error}"))?;
    abort_if_cancelled(&context)?;
    tokio::fs::write(&output_path, rendered)
        .await
        .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;
    abort_if_cancelled(&context)?;

    log_json(json!({
        "timestamp": Utc::now(),
        "event": "summary_written",
        "task_id": artifact.task_id,
        "assignment_id": artifact.assignment_id,
        "output_path": artifact.output_path,
        "source_path": artifact.source_path,
    }));
    Ok(())
}

async fn sleep_with_cancellation(
    context: &TaskExecutionContext,
    duration: Duration,
) -> Result<(), String> {
    if duration.is_zero() {
        return Ok(());
    }

    tokio::select! {
        _ = context.cancelled() => Err(cancellation_reason(context)),
        _ = tokio::time::sleep(duration) => Ok(()),
    }
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

fn summarize_text(text: &str, max_summary_lines: usize) -> TextSummary {
    let summary_lines = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(max_summary_lines.max(1))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let line_count = text.lines().count();
    let word_count = text.split_whitespace().count();
    let char_count = text.chars().count();
    let summary = if summary_lines.is_empty() {
        "(empty document)".to_owned()
    } else {
        summary_lines.join(" ")
    };

    TextSummary {
        line_count,
        word_count,
        char_count,
        summary,
    }
}

fn resolve_output_path(output_dir: &Path, task_id: &str, explicit: Option<&PathBuf>) -> PathBuf {
    match explicit {
        Some(path) => path.clone(),
        None => output_dir.join(format!("{task_id}.summary.json")),
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
        vec!["summarize".to_owned()]
    } else {
        cli.skills.clone()
    };
    let subscriptions = if cli.subscriptions.is_empty() {
        vec![format!("topic:{}", cli.tasks_topic)]
    } else {
        cli.subscriptions.clone()
    };
    let publications = if cli.publications.is_empty() {
        vec![format!("topic:{}", cli.task_events_topic)]
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

pub(crate) async fn register_agent(
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

pub(crate) async fn heartbeat_agent(
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

pub(crate) async fn remove_agent(
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

fn default_summary_lines() -> usize {
    3
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_from_cli_defaults_to_task_topics() {
        let cli = Cli::parse_from(["expressways-agent-example", "--token", "signed-token"]);
        let registration = registration_from_cli(&cli);

        assert_eq!(registration.agent_id, "example-summarizer");
        assert_eq!(registration.display_name, "example-summarizer");
        assert_eq!(registration.skills, vec!["summarize"]);
        assert_eq!(registration.subscriptions, vec!["topic:tasks"]);
        assert_eq!(registration.publications, vec!["topic:task_events"]);
        assert_eq!(registration.endpoint.transport, "local_task_worker");
        assert_eq!(registration.endpoint.address, "example-summarizer");
        assert_eq!(registration.ttl_seconds, Some(120));
    }

    #[test]
    fn summarize_text_prefers_first_non_empty_lines() {
        let summary = summarize_text("\nAlpha\n\nBeta\nGamma\nDelta\n", 3);
        assert_eq!(
            summary,
            TextSummary {
                line_count: 6,
                word_count: 4,
                char_count: 25,
                summary: "Alpha Beta Gamma".to_owned(),
            }
        );
    }

    #[test]
    fn resolve_output_path_defaults_to_task_scoped_file() {
        let output_dir = PathBuf::from("./var/agent/results");
        let path = resolve_output_path(&output_dir, "task-42", None);
        assert_eq!(path, output_dir.join("task-42.summary.json"));
    }

    #[tokio::test]
    async fn sleep_with_cancellation_returns_context_reason() {
        let context = TaskExecutionContext::default();
        context.cancellation_token().cancel();

        let error = sleep_with_cancellation(&context, Duration::from_millis(10))
            .await
            .expect_err("sleep should stop on cancellation");
        assert!(error.contains("assignment canceled"));
    }
}
