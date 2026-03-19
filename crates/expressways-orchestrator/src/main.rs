use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, bail};
use chrono::Utc;
use clap::{Args, Parser, Subcommand, ValueEnum};
use expressways_client::{Client, Endpoint};
use expressways_orchestrator::{
    AssignmentRequirements, OrchestratorState, TaskEventApplyOutcome, TaskListQuery, apply_event,
    apply_snapshot, apply_task_event_message, ingest_task, list_tasks, load_state,
    orchestrator_metrics, plan_assignment_event_with_reason, plan_cancel_event,
    plan_exhausted_event, plan_requeue_event, plan_retry_event, plan_timeout_event,
    query_from_requirements, ready_task_ids, record_local_task_event, retry_decision_task_ids,
    save_state, select_agent_with_reason, show_task, task_can_retry, timed_out_task_ids,
};
use expressways_protocol::{
    Classification, ControlCommand, ControlRequest, ControlResponse, RetentionClass, StoredMessage,
    StreamFrame, TASK_EVENTS_TOPIC, TASKS_TOPIC, TaskEvent, TaskStatus, TaskWorkItem, TopicSpec,
};
use serde::Serialize;
use tokio::time::{MissedTickBehavior, interval, sleep};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

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
    #[command(subcommand)]
    command: Command,
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

#[derive(Debug, Subcommand)]
enum Command {
    Supervise {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = "./var/orchestrator/state.json")]
        state_path: PathBuf,
        #[arg(long, default_value_t = 250)]
        retry_delay_ms: u64,
        #[arg(long, default_value = TASKS_TOPIC)]
        tasks_topic: String,
        #[arg(long, default_value = TASK_EVENTS_TOPIC)]
        task_events_topic: String,
        #[arg(long, default_value_t = 100)]
        consume_limit: usize,
        #[arg(long, default_value_t = 250)]
        poll_interval_ms: u64,
    },
    Assign {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = "./var/orchestrator/state.json")]
        state_path: PathBuf,
        #[arg(long)]
        task_id: Option<String>,
        #[arg(long)]
        task_offset: Option<u64>,
        #[arg(long)]
        skill: Option<String>,
        #[arg(long)]
        topic: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long, default_value = TASK_EVENTS_TOPIC)]
        decision_topic: String,
        #[arg(long, default_value_t = true)]
        bootstrap: bool,
    },
    ShowState {
        #[arg(long, default_value = "./var/orchestrator/state.json")]
        state_path: PathBuf,
    },
    ShowMetrics {
        #[arg(long, default_value = "./var/orchestrator/state.json")]
        state_path: PathBuf,
    },
    ListTasks {
        #[arg(long, default_value = "./var/orchestrator/state.json")]
        state_path: PathBuf,
        #[arg(long)]
        status: Option<TaskStatus>,
        #[arg(long)]
        skill: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
    },
    ShowTask {
        #[arg(long, default_value = "./var/orchestrator/state.json")]
        state_path: PathBuf,
        #[arg(long)]
        task_id: String,
    },
    ShowTaskHistory {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long)]
        task_id: String,
        #[arg(long, default_value = TASK_EVENTS_TOPIC)]
        task_events_topic: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 100)]
        batch_size: usize,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    TailTaskEvents {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = TASK_EVENTS_TOPIC)]
        task_events_topic: String,
        #[arg(long)]
        task_id: Option<String>,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 100)]
        batch_size: usize,
        #[arg(long, default_value_t = 1_000)]
        poll_interval_ms: u64,
        #[arg(long)]
        limit: Option<usize>,
    },
    RequeueTask {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = "./var/orchestrator/state.json")]
        state_path: PathBuf,
        #[arg(long)]
        task_id: String,
        #[arg(long, default_value = TASK_EVENTS_TOPIC)]
        decision_topic: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = true)]
        bootstrap: bool,
    },
    CancelTask {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = "./var/orchestrator/state.json")]
        state_path: PathBuf,
        #[arg(long)]
        task_id: String,
        #[arg(long, default_value = TASK_EVENTS_TOPIC)]
        decision_topic: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = true)]
        bootstrap: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log_level)?;
    let endpoint = endpoint_from_cli(cli.transport, cli.address, cli.socket)?;

    match cli.command {
        Command::Supervise {
            token,
            state_path,
            retry_delay_ms,
            tasks_topic,
            task_events_topic,
            consume_limit,
            poll_interval_ms,
        } => {
            let capability_token = resolve_token(token)?;
            supervise(
                endpoint,
                capability_token,
                state_path,
                retry_delay_ms,
                tasks_topic,
                task_events_topic,
                consume_limit.max(1),
                poll_interval_ms.max(1),
            )
            .await
        }
        Command::Assign {
            token,
            state_path,
            task_id,
            task_offset,
            skill,
            topic,
            principal,
            decision_topic,
            bootstrap,
        } => {
            let capability_token = resolve_token(token)?;
            let requirements = AssignmentRequirements {
                skill,
                topic,
                principal,
                preferred_agents: Vec::new(),
                avoid_agents: Vec::new(),
            };
            assign(
                endpoint,
                capability_token,
                state_path,
                task_id,
                task_offset,
                requirements,
                decision_topic,
                bootstrap,
            )
            .await
        }
        Command::ShowState { state_path } => {
            let state = load_state(&state_path)?;
            println!("{}", serde_json::to_string_pretty(&state)?);
            Ok(())
        }
        Command::ShowMetrics { state_path } => {
            let state = load_state(&state_path)?;
            let metrics = orchestrator_metrics(&state, Utc::now());
            println!("{}", serde_json::to_string_pretty(&metrics)?);
            Ok(())
        }
        Command::ListTasks {
            state_path,
            status,
            skill,
            agent_id,
            limit,
        } => {
            let state = load_state(&state_path)?;
            let tasks = list_tasks(
                &state,
                &TaskListQuery {
                    status,
                    skill,
                    agent_id,
                    limit: limit.map(|value| value.max(1)),
                },
            );
            println!("{}", serde_json::to_string_pretty(&tasks)?);
            Ok(())
        }
        Command::ShowTask {
            state_path,
            task_id,
        } => {
            let state = load_state(&state_path)?;
            let task = show_task(&state, &task_id, Utc::now())
                .with_context(|| format!("task `{task_id}` was not found in orchestrator state"))?;
            println!("{}", serde_json::to_string_pretty(&task)?);
            Ok(())
        }
        Command::ShowTaskHistory {
            token,
            task_id,
            task_events_topic,
            offset,
            batch_size,
            limit,
        } => {
            let capability_token = resolve_token(token)?;
            show_task_history(
                endpoint,
                capability_token,
                task_id,
                task_events_topic,
                offset,
                batch_size.max(1),
                limit.max(1),
            )
            .await
        }
        Command::TailTaskEvents {
            token,
            task_events_topic,
            task_id,
            offset,
            batch_size,
            poll_interval_ms,
            limit,
        } => {
            let capability_token = resolve_token(token)?;
            tail_task_events(
                endpoint,
                capability_token,
                task_events_topic,
                task_id,
                offset,
                batch_size.max(1),
                poll_interval_ms.max(1),
                limit.map(|value| value.max(1)),
            )
            .await
        }
        Command::RequeueTask {
            token,
            state_path,
            task_id,
            decision_topic,
            reason,
            bootstrap,
        } => {
            let capability_token = resolve_token(token)?;
            requeue_task(
                endpoint,
                capability_token,
                state_path,
                task_id,
                decision_topic,
                reason,
                bootstrap,
            )
            .await
        }
        Command::CancelTask {
            token,
            state_path,
            task_id,
            decision_topic,
            reason,
            bootstrap,
        } => {
            let capability_token = resolve_token(token)?;
            cancel_task(
                endpoint,
                capability_token,
                state_path,
                task_id,
                decision_topic,
                reason,
                bootstrap,
            )
            .await
        }
    }
}

fn init_tracing(log_level: &str) -> anyhow::Result<()> {
    let filter = EnvFilter::try_new(log_level)
        .or_else(|_| EnvFilter::try_new(format!("expressways_orchestrator={log_level}")))
        .context("invalid log level")?;

    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(false)
        .with_span_list(false)
        .init();

    Ok(())
}

fn log_state_metrics(
    reason: &'static str,
    state_path: &std::path::Path,
    state: &OrchestratorState,
) {
    let metrics = orchestrator_metrics(state, Utc::now());
    info!(
        reason,
        state_path = %state_path.display(),
        cursor = metrics.source_cursor,
        task_offset = metrics.task_offset,
        task_event_offset = metrics.task_event_offset,
        agent_count = metrics.agent_count,
        task_count = metrics.task_count,
        pending = metrics.tasks.pending,
        assigned = metrics.tasks.assigned,
        completed = metrics.tasks.completed,
        failed = metrics.tasks.failed,
        retry_scheduled = metrics.tasks.retry_scheduled,
        timed_out = metrics.tasks.timed_out,
        exhausted = metrics.tasks.exhausted,
        canceled = metrics.tasks.canceled,
        retry_count = metrics.tasks.retry_count,
        oldest_in_flight_age_seconds = metrics.tasks.oldest_in_flight_age_seconds.unwrap_or_default(),
        has_in_flight = metrics.tasks.oldest_in_flight_age_seconds.is_some(),
        pending_task_event_acks = metrics.pending_task_event_acks,
        "orchestrator metrics snapshot"
    );
}

pub(crate) async fn supervise(
    endpoint: Endpoint,
    capability_token: String,
    state_path: PathBuf,
    retry_delay_ms: u64,
    tasks_topic: String,
    task_events_topic: String,
    consume_limit: usize,
    poll_interval_ms: u64,
) -> anyhow::Result<()> {
    let existing_state = match load_state(&state_path) {
        Ok(state) => Some(state),
        Err(error) => {
            warn!(
                error = %error,
                state_path = %state_path.display(),
                "orchestrator state missing or invalid; bootstrapping fresh state"
            );
            None
        }
    };
    let mut state = bootstrap_state(&endpoint, &capability_token, existing_state).await?;
    let mut topic_client = Client::connect(endpoint.clone()).await?;
    ensure_orchestration_topics(
        &mut topic_client,
        &capability_token,
        &tasks_topic,
        &task_events_topic,
    )
    .await?;
    drop(topic_client);

    if synchronize_tasks(
        &endpoint,
        &capability_token,
        &mut state,
        &tasks_topic,
        &task_events_topic,
        consume_limit,
    )
    .await?
    {
        save_state(&state_path, &state)?;
    } else {
        save_state(&state_path, &state)?;
    }
    log_state_metrics("bootstrap_saved", &state_path, &state);

    loop {
        let client = match Client::connect(endpoint.clone()).await {
            Ok(client) => client,
            Err(error) => {
                warn!(error = %error, "failed to connect orchestrator watch stream");
                sleep(Duration::from_millis(retry_delay_ms)).await;
                continue;
            }
        };
        let mut stream = client.into_stream();
        let opened = stream
            .open(ControlRequest {
                capability_token: capability_token.clone(),
                command: ControlCommand::OpenAgentWatchStream {
                    query: query_from_requirements(&AssignmentRequirements::default(), true),
                    cursor: Some(state.source_cursor),
                    max_events: 100,
                    wait_timeout_ms: 30_000,
                },
            })
            .await;
        let opened = match opened {
            Ok(frame) => frame,
            Err(error) => {
                warn!(error = %error, "failed to open orchestrator watch stream");
                sleep(Duration::from_millis(retry_delay_ms)).await;
                continue;
            }
        };

        match opened {
            StreamFrame::AgentWatchOpened { cursor } => {
                state.source_cursor = cursor;
                info!(cursor, "orchestrator watch stream opened");
            }
            StreamFrame::StreamError { code, message } => {
                warn!(code, message, "orchestrator stream open rejected");
                if code == "watch_cursor_expired" {
                    state = bootstrap_state(&endpoint, &capability_token, Some(state)).await?;
                    save_state(&state_path, &state)?;
                }
                sleep(Duration::from_millis(retry_delay_ms)).await;
                continue;
            }
            other => {
                bail!("unexpected opening frame for orchestrator supervisor: {other:?}");
            }
        }

        let mut poll = interval(Duration::from_millis(poll_interval_ms));
        poll.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut should_reconnect = false;
        loop {
            tokio::select! {
                _ = poll.tick() => {
                    match synchronize_tasks(
                        &endpoint,
                        &capability_token,
                        &mut state,
                        &tasks_topic,
                        &task_events_topic,
                        consume_limit,
                    ).await {
                        Ok(true) => {
                            save_state(&state_path, &state)?;
                            log_state_metrics("task_state_synchronized", &state_path, &state);
                        }
                        Ok(false) => {}
                        Err(error) => {
                            warn!(error = %error, "failed to synchronize task topics");
                        }
                    }
                }
                frame = stream.next_frame() => {
                    let Some(frame) = frame? else {
                        warn!("orchestrator watch stream ended unexpectedly; reconnecting");
                        break;
                    };

                    match frame {
                        StreamFrame::RegistryEvents { events, cursor } => {
                            for event in events {
                                apply_event(&mut state, event);
                            }
                            state.source_cursor = cursor;
                            save_state(&state_path, &state)?;
                            log_state_metrics("registry_events_applied", &state_path, &state);
                        }
                        StreamFrame::KeepAlive { cursor } => {
                            state.source_cursor = cursor;
                        }
                        StreamFrame::StreamClosed { cursor, reason } => {
                            state.source_cursor = cursor;
                            info!(cursor, reason, "orchestrator watch stream closed; reconnecting");
                            should_reconnect = true;
                            break;
                        }
                        StreamFrame::StreamError { code, message } => {
                            warn!(code, message, "orchestrator stream error");
                            if code == "watch_cursor_expired" {
                                state = bootstrap_state(&endpoint, &capability_token, Some(state)).await?;
                                save_state(&state_path, &state)?;
                            }
                            should_reconnect = true;
                            break;
                        }
                        StreamFrame::AgentWatchOpened { .. } => {}
                    }
                }
            }
        }

        if !should_reconnect {
            warn!("orchestrator watch stream ended without a terminal frame; reconnecting");
        }
        sleep(Duration::from_millis(retry_delay_ms)).await;
    }
}

async fn assign(
    endpoint: Endpoint,
    capability_token: String,
    state_path: PathBuf,
    task_id: Option<String>,
    task_offset: Option<u64>,
    requirements: AssignmentRequirements,
    decision_topic: String,
    bootstrap: bool,
) -> anyhow::Result<()> {
    let state =
        load_or_bootstrap_state(&endpoint, &capability_token, &state_path, bootstrap).await?;

    let selected = select_agent_with_reason(&state, &requirements, 0, Utc::now())
        .context("no eligible agent matched the assignment requirements")?;
    let task_id = task_id.unwrap_or_else(|| format!("manual-{}", Uuid::now_v7()));
    let event = state
        .tasks
        .contains_key(&task_id)
        .then(|| {
            plan_assignment_event_with_reason(
                &state,
                &task_id,
                selected.agent,
                Utc::now(),
                Some(selected.reason.clone()),
            )
        })
        .flatten()
        .unwrap_or_else(|| {
            build_manual_assignment_event(
                task_id,
                task_offset,
                selected.agent,
                Some(format!(
                    "operator manually assigned agent `{}`",
                    selected.agent.agent_id
                )),
            )
        });

    publish_stateful_task_event(
        endpoint,
        capability_token,
        state_path,
        decision_topic,
        state,
        event,
    )
    .await
}

async fn requeue_task(
    endpoint: Endpoint,
    capability_token: String,
    state_path: PathBuf,
    task_id: String,
    decision_topic: String,
    reason: Option<String>,
    bootstrap: bool,
) -> anyhow::Result<()> {
    let state =
        load_or_bootstrap_state(&endpoint, &capability_token, &state_path, bootstrap).await?;
    let task = state
        .tasks
        .get(&task_id)
        .with_context(|| format!("task `{task_id}` was not found in orchestrator state"))?;

    if matches!(task.status, TaskStatus::Completed | TaskStatus::Canceled) {
        bail!(
            "task `{task_id}` is already terminal as `{}` and cannot be requeued",
            task.status
        );
    }
    if !task_can_retry(&state, &task_id) {
        bail!("task `{task_id}` has exhausted its retry budget and cannot be requeued safely");
    }

    let event = plan_requeue_event(
        &state,
        &task_id,
        Utc::now(),
        Some(reason.unwrap_or_else(|| "operator requested task requeue".to_owned())),
    )
    .with_context(|| format!("task `{task_id}` could not be requeued"))?;

    publish_stateful_task_event(
        endpoint,
        capability_token,
        state_path,
        decision_topic,
        state,
        event,
    )
    .await
}

async fn show_task_history(
    endpoint: Endpoint,
    capability_token: String,
    task_id: String,
    task_events_topic: String,
    offset: u64,
    batch_size: usize,
    limit: usize,
) -> anyhow::Result<()> {
    let history = fetch_task_history(
        endpoint,
        capability_token,
        &task_events_topic,
        &task_id,
        offset,
        batch_size,
        limit,
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&history)?);
    Ok(())
}

async fn tail_task_events(
    endpoint: Endpoint,
    capability_token: String,
    task_events_topic: String,
    task_id: Option<String>,
    offset: u64,
    batch_size: usize,
    poll_interval_ms: u64,
    limit: Option<usize>,
) -> anyhow::Result<()> {
    let mut client = Client::connect(endpoint).await?;
    let mut next_offset = offset;
    let mut emitted = 0usize;

    loop {
        let messages = consume_messages(
            &mut client,
            &capability_token,
            &task_events_topic,
            next_offset,
            batch_size,
        )
        .await?;

        if messages.is_empty() {
            sleep(Duration::from_millis(poll_interval_ms)).await;
            continue;
        }

        for message in messages {
            next_offset = message.offset.saturating_add(1);
            let Some(entry) = decode_task_event_entry(&message, task_id.as_deref()) else {
                continue;
            };

            println!("{}", serde_json::to_string(&entry)?);
            emitted += 1;

            if limit.is_some_and(|max_events| emitted >= max_events) {
                return Ok(());
            }
        }
    }
}

async fn cancel_task(
    endpoint: Endpoint,
    capability_token: String,
    state_path: PathBuf,
    task_id: String,
    decision_topic: String,
    reason: Option<String>,
    bootstrap: bool,
) -> anyhow::Result<()> {
    let state =
        load_or_bootstrap_state(&endpoint, &capability_token, &state_path, bootstrap).await?;
    let task = state
        .tasks
        .get(&task_id)
        .with_context(|| format!("task `{task_id}` was not found in orchestrator state"))?;

    if matches!(task.status, TaskStatus::Completed | TaskStatus::Canceled) {
        bail!(
            "task `{task_id}` is already terminal as `{}` and cannot be canceled",
            task.status
        );
    }

    let event = plan_cancel_event(
        &state,
        &task_id,
        Utc::now(),
        Some(reason.unwrap_or_else(|| "operator canceled task".to_owned())),
    )
    .with_context(|| format!("task `{task_id}` could not be canceled"))?;

    publish_stateful_task_event(
        endpoint,
        capability_token,
        state_path,
        decision_topic,
        state,
        event,
    )
    .await
}

async fn load_or_bootstrap_state(
    endpoint: &Endpoint,
    capability_token: &str,
    state_path: &std::path::Path,
    bootstrap: bool,
) -> anyhow::Result<OrchestratorState> {
    match load_state(state_path) {
        Ok(state) => Ok(state),
        Err(error) if bootstrap => {
            warn!(
                error = %error,
                "orchestrator state missing or invalid; bootstrapping from broker"
            );
            let state = bootstrap_state(endpoint, capability_token, None).await?;
            save_state(state_path, &state)?;
            Ok(state)
        }
        Err(error) => Err(error),
    }
}

async fn publish_stateful_task_event(
    endpoint: Endpoint,
    capability_token: String,
    state_path: PathBuf,
    decision_topic: String,
    state: OrchestratorState,
    event: TaskEvent,
) -> anyhow::Result<()> {
    let mut next_state = state.clone();
    let mut client = Client::connect(endpoint).await?;
    ensure_topic(
        &mut client,
        &capability_token,
        &decision_topic,
        RetentionClass::Operational,
        Classification::Internal,
    )
    .await?;
    let published = publish_task_event(
        &mut client,
        &capability_token,
        &decision_topic,
        &event,
        Some(&mut next_state),
    )
    .await?;

    save_state(&state_path, &next_state)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "event": event,
            "event_topic": decision_topic,
            "message_id": published.message_id,
            "offset": published.offset,
            "classification": published.classification,
            "state_path": state_path,
        }))?
    );
    Ok(())
}

async fn bootstrap_state(
    endpoint: &Endpoint,
    capability_token: &str,
    existing_state: Option<OrchestratorState>,
) -> anyhow::Result<OrchestratorState> {
    let mut client = Client::connect(endpoint.clone()).await?;
    let response = client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::ListAgents {
                query: query_from_requirements(&AssignmentRequirements::default(), true),
            },
        })
        .await?;

    match response {
        ControlResponse::Agents { agents, cursor } => {
            let mut state = existing_state.unwrap_or_default();
            apply_snapshot(&mut state, agents, cursor);
            Ok(state)
        }
        ControlResponse::Error { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected bootstrap response: {other:?}"),
    }
}

async fn ensure_orchestration_topics(
    client: &mut Client,
    capability_token: &str,
    tasks_topic: &str,
    task_events_topic: &str,
) -> anyhow::Result<()> {
    ensure_topic(
        client,
        capability_token,
        tasks_topic,
        RetentionClass::Operational,
        Classification::Internal,
    )
    .await?;
    ensure_topic(
        client,
        capability_token,
        task_events_topic,
        RetentionClass::Operational,
        Classification::Internal,
    )
    .await?;
    Ok(())
}

async fn ensure_topic(
    client: &mut Client,
    capability_token: &str,
    topic: &str,
    retention_class: RetentionClass,
    default_classification: Classification,
) -> anyhow::Result<()> {
    let response = client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::CreateTopic {
                topic: TopicSpec {
                    name: topic.to_owned(),
                    retention_class,
                    default_classification,
                },
            },
        })
        .await?;

    match response {
        ControlResponse::TopicCreated { .. } => Ok(()),
        ControlResponse::Error { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected create-topic response: {other:?}"),
    }
}

async fn synchronize_tasks(
    endpoint: &Endpoint,
    capability_token: &str,
    state: &mut OrchestratorState,
    tasks_topic: &str,
    task_events_topic: &str,
    consume_limit: usize,
) -> anyhow::Result<bool> {
    let mut client = Client::connect(endpoint.clone()).await?;
    let mut changed = false;

    changed |= drain_tasks(
        &mut client,
        capability_token,
        state,
        tasks_topic,
        consume_limit,
    )
    .await?;
    changed |= drain_task_events(
        &mut client,
        capability_token,
        state,
        task_events_topic,
        consume_limit,
    )
    .await?;

    for task_id in timed_out_task_ids(state, Utc::now()) {
        if let Some(event) = plan_timeout_event(
            state,
            &task_id,
            Utc::now(),
            Some("assignment timed out before an agent completion event arrived".to_owned()),
        ) {
            let published = publish_task_event(
                &mut client,
                capability_token,
                task_events_topic,
                &event,
                Some(state),
            )
            .await?;
            changed = true;
            info!(
                task_id = %task_id,
                offset = published.offset,
                attempt = event.attempt,
                status = %event.status,
                "orchestrator published timeout event"
            );
        }
    }

    for task_id in retry_decision_task_ids(state) {
        let next_event = if task_can_retry(state, &task_id) {
            plan_retry_event(
                state,
                &task_id,
                Utc::now(),
                Some("task queued for a safe reassignment attempt".to_owned()),
            )
        } else {
            plan_exhausted_event(
                state,
                &task_id,
                Utc::now(),
                Some("task retry budget exhausted".to_owned()),
            )
        };

        if let Some(event) = next_event {
            let published = publish_task_event(
                &mut client,
                capability_token,
                task_events_topic,
                &event,
                Some(state),
            )
            .await?;
            changed = true;
            info!(
                task_id = %task_id,
                offset = published.offset,
                attempt = event.attempt,
                status = %event.status,
                "orchestrator published lifecycle follow-up event"
            );
        }
    }

    for task_id in ready_task_ids(state, Utc::now()) {
        let Some(task) = state.tasks.get(&task_id) else {
            continue;
        };
        let requirements = task.work_item.requirements.clone();
        let Some(selection) =
            select_agent_with_reason(state, &requirements, task.work_item.priority, Utc::now())
        else {
            continue;
        };
        let agent_id = selection.agent.agent_id.clone();
        let Some(event) = plan_assignment_event_with_reason(
            state,
            &task_id,
            selection.agent,
            Utc::now(),
            Some(selection.reason.clone()),
        ) else {
            continue;
        };

        let published = publish_task_event(
            &mut client,
            capability_token,
            task_events_topic,
            &event,
            Some(state),
        )
        .await?;
        changed = true;
        info!(
            task_id = %task_id,
            agent_id = %agent_id,
            offset = published.offset,
            attempt = event.attempt,
            reason = event.reason.as_deref().unwrap_or(""),
            "orchestrator published task assignment"
        );
    }

    Ok(changed)
}

async fn drain_tasks(
    client: &mut Client,
    capability_token: &str,
    state: &mut OrchestratorState,
    topic: &str,
    consume_limit: usize,
) -> anyhow::Result<bool> {
    let mut changed = false;
    let mut next_offset = state.task_offset;

    loop {
        let messages =
            consume_messages(client, capability_token, topic, next_offset, consume_limit).await?;
        if messages.is_empty() {
            break;
        }

        for message in &messages {
            match serde_json::from_str::<TaskWorkItem>(&message.payload) {
                Ok(work_item) => {
                    let task_id = work_item.task_id.clone();
                    if ingest_task(state, work_item, message.offset) {
                        changed = true;
                        info!(task_id = %task_id, offset = message.offset, "orchestrator consumed task work item");
                    } else {
                        warn!(task_id = %task_id, offset = message.offset, "orchestrator skipped duplicate task id");
                    }
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        topic = %topic,
                        offset = message.offset,
                        "orchestrator skipped invalid task payload"
                    );
                }
            }
        }

        next_offset = messages
            .last()
            .map(|message| message.offset.saturating_add(1))
            .unwrap_or(next_offset);
        if next_offset > state.task_offset {
            state.task_offset = next_offset;
            changed = true;
        }

        if messages.len() < consume_limit {
            break;
        }
    }

    Ok(changed)
}

async fn drain_task_events(
    client: &mut Client,
    capability_token: &str,
    state: &mut OrchestratorState,
    topic: &str,
    consume_limit: usize,
) -> anyhow::Result<bool> {
    let mut changed = false;
    let mut next_offset = state.task_event_offset;

    loop {
        let messages =
            consume_messages(client, capability_token, topic, next_offset, consume_limit).await?;
        if messages.is_empty() {
            break;
        }

        for message in &messages {
            match serde_json::from_str::<TaskEvent>(&message.payload) {
                Ok(event) => match apply_task_event_message(state, event.clone()) {
                    TaskEventApplyOutcome::Applied => {
                        changed = true;
                        info!(
                            task_id = %event.task_id,
                            offset = message.offset,
                            status = %event.status,
                            attempt = event.attempt,
                            "orchestrator applied task lifecycle event"
                        );
                    }
                    TaskEventApplyOutcome::SkippedLocalEcho => {}
                    TaskEventApplyOutcome::SkippedMissingTask => {
                        warn!(
                            task_id = %event.task_id,
                            offset = message.offset,
                            status = %event.status,
                            "orchestrator ignored task event for an unknown task"
                        );
                    }
                    TaskEventApplyOutcome::SkippedMalformed => {
                        warn!(
                            task_id = %event.task_id,
                            offset = message.offset,
                            status = %event.status,
                            "orchestrator ignored malformed task event"
                        );
                    }
                    TaskEventApplyOutcome::SkippedStaleAssignment => {
                        warn!(
                            task_id = %event.task_id,
                            offset = message.offset,
                            status = %event.status,
                            "orchestrator ignored stale task event"
                        );
                    }
                },
                Err(error) => {
                    warn!(
                        error = %error,
                        topic = %topic,
                        offset = message.offset,
                        "orchestrator skipped invalid task-event payload"
                    );
                }
            }
        }

        next_offset = messages
            .last()
            .map(|message| message.offset.saturating_add(1))
            .unwrap_or(next_offset);
        if next_offset > state.task_event_offset {
            state.task_event_offset = next_offset;
            changed = true;
        }

        if messages.len() < consume_limit {
            break;
        }
    }

    Ok(changed)
}

struct PublishReceipt {
    message_id: Uuid,
    offset: u64,
    classification: Classification,
}

async fn publish_task_event(
    client: &mut Client,
    capability_token: &str,
    topic: &str,
    event: &TaskEvent,
    mut state: Option<&mut OrchestratorState>,
) -> anyhow::Result<PublishReceipt> {
    let response = client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::Publish {
                topic: topic.to_owned(),
                classification: Some(Classification::Internal),
                payload: serde_json::to_string(event)?,
            },
        })
        .await?;

    match response {
        ControlResponse::PublishAccepted {
            message_id,
            offset,
            classification,
        } => {
            if let Some(state) = &mut state {
                let _ = record_local_task_event(state, event.clone());
            }

            Ok(PublishReceipt {
                message_id,
                offset,
                classification,
            })
        }
        ControlResponse::Error { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected task-event publish response: {other:?}"),
    }
}

async fn consume_messages(
    client: &mut Client,
    capability_token: &str,
    topic: &str,
    offset: u64,
    limit: usize,
) -> anyhow::Result<Vec<StoredMessage>> {
    let response = client
        .send(ControlRequest {
            capability_token: capability_token.to_owned(),
            command: ControlCommand::Consume {
                topic: topic.to_owned(),
                offset,
                limit,
            },
        })
        .await?;

    match response {
        ControlResponse::Messages { messages, .. } => Ok(messages),
        ControlResponse::Error { code, message } => bail!("{code}: {message}"),
        other => bail!("unexpected consume response: {other:?}"),
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct TaskEventHistoryEntry {
    message_id: Uuid,
    topic: String,
    offset: u64,
    timestamp: chrono::DateTime<Utc>,
    producer: String,
    classification: Classification,
    event: TaskEvent,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct TaskHistoryView {
    task_id: String,
    topic: String,
    start_offset: u64,
    next_offset: u64,
    returned: usize,
    events: Vec<TaskEventHistoryEntry>,
}

async fn fetch_task_history(
    endpoint: Endpoint,
    capability_token: String,
    topic: &str,
    task_id: &str,
    offset: u64,
    batch_size: usize,
    limit: usize,
) -> anyhow::Result<TaskHistoryView> {
    let mut client = Client::connect(endpoint).await?;
    let mut next_offset = offset;
    let mut entries = VecDeque::new();

    loop {
        let messages = consume_messages(
            &mut client,
            &capability_token,
            topic,
            next_offset,
            batch_size,
        )
        .await?;
        if messages.is_empty() {
            break;
        }

        for message in messages {
            next_offset = message.offset.saturating_add(1);
            let Some(entry) = decode_task_event_entry(&message, Some(task_id)) else {
                continue;
            };

            entries.push_back(entry);
            while entries.len() > limit {
                let _ = entries.pop_front();
            }
        }
    }

    Ok(TaskHistoryView {
        task_id: task_id.to_owned(),
        topic: topic.to_owned(),
        start_offset: offset,
        next_offset,
        returned: entries.len(),
        events: entries.into_iter().collect(),
    })
}

fn decode_task_event_entry(
    message: &StoredMessage,
    task_id_filter: Option<&str>,
) -> Option<TaskEventHistoryEntry> {
    let event = serde_json::from_str::<TaskEvent>(&message.payload).ok()?;
    if task_id_filter.is_some_and(|task_id| event.task_id != task_id) {
        return None;
    }

    Some(TaskEventHistoryEntry {
        message_id: message.message_id,
        topic: message.topic.clone(),
        offset: message.offset,
        timestamp: message.timestamp,
        producer: message.producer.clone(),
        classification: message.classification.clone(),
        event,
    })
}

fn build_manual_assignment_event(
    task_id: String,
    task_offset: Option<u64>,
    agent: &expressways_protocol::AgentCard,
    reason: Option<String>,
) -> TaskEvent {
    TaskEvent {
        event_id: Uuid::now_v7(),
        task_id,
        task_offset,
        assignment_id: Some(Uuid::now_v7()),
        agent_id: Some(agent.agent_id.clone()),
        status: TaskStatus::Assigned,
        attempt: 1,
        reason,
        emitted_at: Utc::now(),
    }
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
    use super::{TaskHistoryView, decode_task_event_entry};
    use chrono::Utc;
    use expressways_protocol::{Classification, StoredMessage, TaskEvent, TaskStatus};
    use uuid::Uuid;

    #[test]
    fn decode_task_event_entry_filters_other_tasks() {
        let event = TaskEvent {
            event_id: Uuid::now_v7(),
            task_id: "task-1".to_owned(),
            task_offset: Some(4),
            assignment_id: Some(Uuid::now_v7()),
            agent_id: Some("agent-a".to_owned()),
            status: TaskStatus::Assigned,
            attempt: 1,
            reason: None,
            emitted_at: Utc::now(),
        };
        let message = stored_message(&event, 9);

        let entry = decode_task_event_entry(&message, Some("task-1")).expect("matching task");
        assert_eq!(entry.offset, 9);
        assert_eq!(entry.event, event);
        assert!(decode_task_event_entry(&message, Some("task-2")).is_none());
    }

    #[test]
    fn decode_task_event_entry_skips_invalid_payloads() {
        let message = StoredMessage {
            message_id: Uuid::now_v7(),
            topic: "task_events".to_owned(),
            offset: 3,
            timestamp: Utc::now(),
            producer: "local:test".to_owned(),
            classification: Classification::Internal,
            payload: "{\"not\":\"a_task_event\"}".to_owned(),
        };

        assert!(decode_task_event_entry(&message, None).is_none());
    }

    #[test]
    fn task_history_view_serializes_compact_metadata() {
        let history = TaskHistoryView {
            task_id: "task-1".to_owned(),
            topic: "task_events".to_owned(),
            start_offset: 4,
            next_offset: 8,
            returned: 0,
            events: Vec::new(),
        };

        let payload = serde_json::to_value(&history).expect("serialize history");
        assert_eq!(payload["task_id"], "task-1");
        assert_eq!(payload["start_offset"], 4);
        assert_eq!(payload["next_offset"], 8);
        assert_eq!(payload["returned"], 0);
    }

    fn stored_message(event: &TaskEvent, offset: u64) -> StoredMessage {
        StoredMessage {
            message_id: Uuid::now_v7(),
            topic: "task_events".to_owned(),
            offset,
            timestamp: Utc::now(),
            producer: "local:test".to_owned(),
            classification: Classification::Internal,
            payload: serde_json::to_string(event).expect("serialize event"),
        }
    }
}
