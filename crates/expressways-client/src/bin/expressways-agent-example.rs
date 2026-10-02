use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, ValueEnum};
use expressways_client::{
    AgentWorker, AssignedTask, Client, Endpoint, TaskExecutionContext, WorkerRunOutcome,
    load_agent_worker_state, read_bounded_utf8_file, save_agent_worker_state, write_contained_file,
};
use expressways_protocol::{
    AgentEndpoint, AgentRegistration, Classification, ControlCommand, ControlRequest,
    ControlResponse, RetentionClass, TASK_EVENTS_TOPIC, TASKS_TOPIC,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_util::sync::CancellationToken;

const MAX_SUMMARIZE_INPUT_BYTES: u64 = 16 * 1024 * 1024;

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
    #[arg(long, default_value = "./var/agent/incoming")]
    input_dir: PathBuf,
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
    let worker_state = load_agent_worker_state(&cli.state_path)?;
    let shutdown = CancellationToken::new();
    std::fs::create_dir_all(&cli.input_dir)
        .with_context(|| format!("failed to create {}", cli.input_dir.display()))?;
    std::fs::create_dir_all(&cli.output_dir)
        .with_context(|| format!("failed to create {}", cli.output_dir.display()))?;

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
        let outcome = run_worker_iteration(
            &mut worker,
            &cli.input_dir,
            &cli.output_dir,
            &cli.state_path,
        )
        .await?;
        log_worker_outcome(&outcome);
        Ok(())
    } else {
        run_worker_loop(
            &mut worker,
            &cli.input_dir,
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
    input_dir: &Path,
    output_dir: &Path,
    state_path: &Path,
    shutdown: CancellationToken,
    poll_interval: Duration,
) -> anyhow::Result<()> {
    loop {
        let delay_after_iteration =
            match run_worker_iteration(worker, input_dir, output_dir, state_path).await {
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
    input_dir: &Path,
    output_dir: &Path,
    state_path: &Path,
) -> anyhow::Result<WorkerRunOutcome> {
    let input_dir = input_dir.to_path_buf();
    let output_dir = output_dir.to_path_buf();
    let result = worker
        .run_once_with_context(|assignment, context| async move {
            handle_assignment(assignment, input_dir, output_dir, context).await
        })
        .await;
    save_agent_worker_state(state_path, worker.state())?;
    result.map_err(Into::into)
}

pub(crate) async fn handle_assignment(
    assignment: AssignedTask,
    input_dir: PathBuf,
    output_dir: PathBuf,
    context: TaskExecutionContext,
) -> Result<(), String> {
    match assignment.task.task_type.as_str() {
        "summarize_document" => {
            handle_summarize_document(assignment, input_dir, output_dir, context).await
        }
        other => Err(format!("unsupported task_type `{other}`")),
    }
}

async fn handle_summarize_document(
    assignment: AssignedTask,
    input_dir: PathBuf,
    output_dir: PathBuf,
    context: TaskExecutionContext,
) -> Result<(), String> {
    let payload: SummarizeDocumentPayload = assignment
        .decode_payload_json()
        .map_err(|error| format!("invalid summarize_document payload: {error}"))?;
    abort_if_cancelled(&context)?;

    let source_path = resolve_input_path(&input_dir, &payload.path).await?;
    let source_text = read_bounded_utf8_file(&source_path, MAX_SUMMARIZE_INPUT_BYTES)
        .await
        .map_err(|error| format!("invalid summary input: {error}"))?;
    abort_if_cancelled(&context)?;
    sleep_with_cancellation(&context, Duration::from_millis(payload.simulate_delay_ms)).await?;

    let summary = summarize_text(&source_text, payload.max_summary_lines);
    let output_path = resolve_output_path(
        &output_dir,
        &assignment.task.task_id,
        payload.output_path.as_ref(),
    )?;

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
        source_path: source_path.display().to_string(),
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
    write_contained_file(&output_dir, &output_path, &rendered)
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

async fn resolve_input_path(input_dir: &Path, requested: &Path) -> Result<PathBuf, String> {
    let root = tokio::fs::canonicalize(input_dir).await.map_err(|error| {
        format!(
            "failed to resolve input root {}: {error}",
            input_dir.display()
        )
    })?;
    let candidate = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        root.join(requested)
    };
    let resolved = tokio::fs::canonicalize(&candidate)
        .await
        .map_err(|error| format!("failed to resolve input {}: {error}", candidate.display()))?;
    if !resolved.starts_with(&root) {
        return Err(format!(
            "input path {} escapes configured root {}",
            requested.display(),
            input_dir.display()
        ));
    }
    Ok(resolved)
}

fn resolve_output_path(
    output_dir: &Path,
    task_id: &str,
    explicit: Option<&PathBuf>,
) -> Result<PathBuf, String> {
    let relative = match explicit {
        Some(path) => path.clone(),
        None => PathBuf::from(format!("{}.summary.json", safe_filename(task_id))),
    };
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
    {
        return Err(format!(
            "output path {} must be a relative path contained by {}",
            relative.display(),
            output_dir.display()
        ));
    }
    Ok(output_dir.join(relative))
}

fn safe_filename(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "task".to_owned()
    } else {
        sanitized
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
    if let Err(error) = handle.await
        && !error.is_cancelled()
    {
        log_json(json!({
            "timestamp": Utc::now(),
            "event": "background_task_join_error",
            "task": name,
            "error": error.to_string(),
        }));
    }
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
        return expressways_client::normalize_capability_token(&token);
    }
    if let Some(path) = args.token_file {
        return expressways_client::read_capability_token_file(&path);
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
        let path = resolve_output_path(&output_dir, "task-42", None).expect("safe output path");
        assert_eq!(path, output_dir.join("task-42.summary.json"));
    }

    #[test]
    fn resolve_output_path_rejects_absolute_and_parent_paths() {
        let output_dir = PathBuf::from("./var/agent/results");
        for path in [PathBuf::from("/tmp/stolen"), PathBuf::from("../stolen")] {
            assert!(resolve_output_path(&output_dir, "task-42", Some(&path)).is_err());
        }
    }

    #[tokio::test]
    async fn resolve_input_path_rejects_paths_outside_configured_root() {
        let root = std::env::temp_dir().join(format!("expressways-input-{}", uuid::Uuid::now_v7()));
        tokio::fs::create_dir_all(&root).await.expect("create root");
        let outside = root
            .parent()
            .expect("parent")
            .join(format!("outside-document-{}.txt", uuid::Uuid::now_v7()));
        tokio::fs::write(&outside, "secret")
            .await
            .expect("write outside");

        let error = resolve_input_path(&root, &outside)
            .await
            .expect_err("outside path must be rejected");
        assert!(error.contains("escapes configured root"));

        let _ = tokio::fs::remove_file(outside).await;
        let _ = tokio::fs::remove_dir(root).await;
    }

    #[tokio::test]
    async fn bounded_summary_input_rejects_oversized_and_non_utf8_files() {
        let oversized =
            std::env::temp_dir().join(format!("expressways-summary-{}.txt", uuid::Uuid::now_v7()));
        std::fs::File::create(&oversized)
            .expect("create summary input")
            .set_len(5)
            .expect("size summary input");
        assert!(read_bounded_utf8_file(&oversized, 4).await.is_err());
        tokio::fs::remove_file(&oversized)
            .await
            .expect("remove oversized input");

        let binary =
            std::env::temp_dir().join(format!("expressways-summary-{}.txt", uuid::Uuid::now_v7()));
        tokio::fs::write(&binary, [0xff, 0xfe])
            .await
            .expect("write binary input");
        let error = read_bounded_utf8_file(&binary, 4)
            .await
            .expect_err("binary input must be rejected");
        assert!(error.to_string().contains("UTF-8"));
        tokio::fs::remove_file(binary)
            .await
            .expect("remove binary input");
    }

    #[tokio::test]
    async fn bounded_summary_input_accepts_regular_utf8_files() {
        let path =
            std::env::temp_dir().join(format!("expressways-summary-{}.txt", uuid::Uuid::now_v7()));
        tokio::fs::write(&path, "hello")
            .await
            .expect("write summary input");

        assert_eq!(
            read_bounded_utf8_file(&path, 5)
                .await
                .expect("read summary input"),
            "hello"
        );
        tokio::fs::remove_file(path)
            .await
            .expect("remove summary input");
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
