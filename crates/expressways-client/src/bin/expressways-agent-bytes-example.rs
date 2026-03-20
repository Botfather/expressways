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
    #[arg(long, default_value = "example-bytes-agent")]
    agent_id: String,
    #[arg(long)]
    display_name: Option<String>,
    #[arg(long, default_value = env!("CARGO_PKG_VERSION"))]
    version: String,
    #[arg(
        long,
        default_value = "Example binary payload inspector backed by AgentWorker"
    )]
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
    #[arg(long, default_value = "./var/agent/example-bytes-agent-state.json")]
    state_path: PathBuf,
    #[arg(long, default_value = "./var/agent/blob-results")]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BlobInspectionArtifact {
    task_id: String,
    assignment_id: String,
    agent_id: String,
    task_type: String,
    payload_kind: String,
    artifact_id: Option<String>,
    content_type: Option<String>,
    byte_length: usize,
    preview_hex: String,
    utf8_preview: Option<String>,
    source_path: Option<String>,
    declared_byte_length: Option<u64>,
    declared_sha256: Option<String>,
    output_path: String,
    generated_at: DateTime<Utc>,
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

async fn handle_assignment(
    assignment: AssignedTask,
    output_dir: PathBuf,
    context: TaskExecutionContext,
) -> Result<(), String> {
    match assignment.task.task_type.as_str() {
        "inspect_blob" => handle_inspect_blob(assignment, output_dir, context).await,
        other => Err(format!("unsupported task_type `{other}`")),
    }
}

async fn handle_inspect_blob(
    assignment: AssignedTask,
    output_dir: PathBuf,
    context: TaskExecutionContext,
) -> Result<(), String> {
    abort_if_cancelled(&context)?;
    let bytes = assignment
        .read_payload_bytes()
        .await
        .map_err(|error| format!("failed to load payload bytes: {error}"))?;
    abort_if_cancelled(&context)?;

    if let Some(parent) = output_dir.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    tokio::fs::create_dir_all(&output_dir)
        .await
        .map_err(|error| format!("failed to create {}: {error}", output_dir.display()))?;
    abort_if_cancelled(&context)?;

    let output_path = resolve_output_path(&output_dir, &assignment.task.task_id);
    let artifact = build_artifact(&assignment, &bytes, &output_path);
    let rendered = serde_json::to_vec_pretty(&artifact)
        .map_err(|error| format!("failed to render artifact: {error}"))?;
    abort_if_cancelled(&context)?;

    tokio::fs::write(&output_path, rendered)
        .await
        .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;
    abort_if_cancelled(&context)?;

    log_json(json!({
        "timestamp": Utc::now(),
        "event": "blob_inspected",
        "task_id": artifact.task_id,
        "assignment_id": artifact.assignment_id,
        "payload_kind": artifact.payload_kind,
        "content_type": artifact.content_type,
        "byte_length": artifact.byte_length,
        "output_path": artifact.output_path,
    }));
    Ok(())
}

fn build_artifact(
    assignment: &AssignedTask,
    bytes: &[u8],
    output_path: &Path,
) -> BlobInspectionArtifact {
    let file_ref = assignment.payload_file_ref();
    let artifact_ref = assignment.payload_artifact_ref();
    BlobInspectionArtifact {
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
        payload_kind: assignment.payload_kind().to_owned(),
        artifact_id: artifact_ref
            .as_ref()
            .map(|descriptor| descriptor.artifact_id.clone()),
        content_type: assignment.payload_content_type().map(ToOwned::to_owned),
        byte_length: bytes.len(),
        preview_hex: hex::encode(bytes.iter().take(24).copied().collect::<Vec<_>>()),
        utf8_preview: utf8_preview(bytes),
        source_path: file_ref
            .as_ref()
            .map(|descriptor| descriptor.path.display().to_string())
            .or_else(|| {
                artifact_ref
                    .as_ref()
                    .and_then(|descriptor| descriptor.local_path.as_ref())
                    .map(|path| path.display().to_string())
            }),
        declared_byte_length: file_ref
            .as_ref()
            .and_then(|descriptor| descriptor.byte_length)
            .or_else(|| {
                artifact_ref
                    .as_ref()
                    .and_then(|descriptor| descriptor.byte_length)
            }),
        declared_sha256: file_ref
            .as_ref()
            .and_then(|descriptor| descriptor.sha256.clone())
            .or_else(|| {
                artifact_ref
                    .as_ref()
                    .and_then(|descriptor| descriptor.sha256.clone())
            }),
        output_path: output_path.display().to_string(),
        generated_at: Utc::now(),
    }
}

fn utf8_preview(bytes: &[u8]) -> Option<String> {
    let preview = &bytes[..bytes.len().min(120)];
    std::str::from_utf8(preview)
        .ok()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn resolve_output_path(output_dir: &Path, task_id: &str) -> PathBuf {
    output_dir.join(format!("{task_id}.blob.json"))
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
        vec!["binary".to_owned()]
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

#[cfg(test)]
mod tests {
    use super::*;
    use expressways_protocol::{
        TaskPayload, TaskRequirements, TaskRetryPolicy, TaskStatus, TaskWorkItem,
    };

    #[test]
    fn registration_from_cli_defaults_to_binary_topics() {
        let cli = Cli::parse_from(["expressways-agent-bytes-example", "--token", "signed-token"]);
        let registration = registration_from_cli(&cli);

        assert_eq!(registration.agent_id, "example-bytes-agent");
        assert_eq!(registration.display_name, "example-bytes-agent");
        assert_eq!(registration.skills, vec!["binary"]);
        assert_eq!(registration.subscriptions, vec!["topic:tasks"]);
        assert_eq!(registration.publications, vec!["topic:task_events"]);
    }

    #[test]
    fn build_artifact_preserves_file_ref_metadata_and_preview() {
        let assignment = assigned_task(
            "task-blob",
            TaskPayload::file_ref(
                "./var/agent/incoming/report.pdf",
                Some("application/pdf".to_owned()),
                Some(2048),
                Some("abc123".to_owned()),
            ),
        );
        let output_path = PathBuf::from("./var/agent/blob-results/task-blob.blob.json");
        let artifact = build_artifact(&assignment, b"%PDF sample", &output_path);

        assert_eq!(artifact.payload_kind, "file_ref");
        assert_eq!(artifact.content_type.as_deref(), Some("application/pdf"));
        assert_eq!(artifact.byte_length, 11);
        assert_eq!(
            artifact.source_path.as_deref(),
            Some("./var/agent/incoming/report.pdf")
        );
        assert_eq!(artifact.declared_byte_length, Some(2048));
        assert_eq!(artifact.declared_sha256.as_deref(), Some("abc123"));
        assert!(artifact.preview_hex.starts_with("25504446"));
    }

    #[test]
    fn build_artifact_preserves_artifact_ref_metadata_and_preview() {
        let assignment = assigned_task(
            "task-artifact",
            TaskPayload::artifact_ref(
                "artifact-1",
                Some("application/pdf".to_owned()),
                Some(4096),
                Some("def456".to_owned()),
                Some("./var/data/artifacts/blobs/artifact-1.blob".to_owned()),
            ),
        );
        let output_path = PathBuf::from("./var/agent/blob-results/task-artifact.blob.json");
        let artifact = build_artifact(&assignment, b"%PDF sample", &output_path);

        assert_eq!(artifact.payload_kind, "artifact_ref");
        assert_eq!(artifact.artifact_id.as_deref(), Some("artifact-1"));
        assert_eq!(
            artifact.source_path.as_deref(),
            Some("./var/data/artifacts/blobs/artifact-1.blob")
        );
        assert_eq!(artifact.declared_byte_length, Some(4096));
        assert_eq!(artifact.declared_sha256.as_deref(), Some("def456"));
        assert!(artifact.preview_hex.starts_with("25504446"));
    }

    #[test]
    fn utf8_preview_returns_none_for_binary_payloads() {
        assert_eq!(utf8_preview(&[0xff, 0x00, 0x7f]), None);
        assert_eq!(utf8_preview(b"hello world"), Some("hello world".to_owned()));
    }

    fn assigned_task(task_id: &str, payload: TaskPayload) -> AssignedTask {
        let task = TaskWorkItem {
            task_id: task_id.to_owned(),
            task_type: "inspect_blob".to_owned(),
            priority: 0,
            requirements: TaskRequirements::default(),
            payload,
            retry_policy: TaskRetryPolicy::default(),
            submitted_at: Utc::now(),
        };
        AssignedTask {
            assignment: expressways_protocol::TaskEvent {
                event_id: uuid::Uuid::now_v7(),
                task_id: task.task_id.clone(),
                task_offset: Some(4),
                assignment_id: Some(uuid::Uuid::now_v7()),
                agent_id: Some("bytes-agent".to_owned()),
                status: TaskStatus::Assigned,
                attempt: 1,
                reason: None,
                emitted_at: Utc::now(),
            },
            task_message: expressways_protocol::StoredMessage {
                message_id: uuid::Uuid::now_v7(),
                topic: TASKS_TOPIC.to_owned(),
                offset: 4,
                timestamp: Utc::now(),
                producer: "local:test".to_owned(),
                classification: Classification::Internal,
                payload: serde_json::to_string(&task).expect("serialize task"),
            },
            task,
        }
    }
}
