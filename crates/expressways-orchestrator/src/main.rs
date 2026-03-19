use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, bail};
use chrono::Utc;
use clap::{Args, Parser, Subcommand, ValueEnum};
use expressways_client::{Client, Endpoint};
use expressways_orchestrator::{
    AssignmentRequirements, OrchestratorState, TaskEventApplyOutcome, TaskListQuery, TaskListSort,
    TaskSummaryView, apply_event, apply_snapshot, apply_task_event_message, ingest_task,
    list_tasks, load_state, orchestrator_metrics, plan_assignment_event_with_reason,
    plan_cancel_event, plan_exhausted_event, plan_requeue_event, plan_retry_event,
    plan_timeout_event, query_from_requirements, ready_task_ids, record_local_task_event,
    retry_decision_task_ids, save_state, select_agent_with_reason, show_task, task_can_retry,
    timed_out_task_ids,
};
use expressways_protocol::{
    Classification, ControlCommand, ControlRequest, ControlResponse, RetentionClass, StoredMessage,
    StreamFrame, TASK_EVENTS_TOPIC, TASKS_TOPIC, TaskEvent, TaskStatus, TaskWorkItem, TopicSpec,
};
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TaskListSortKind {
    Offset,
    Priority,
    Age,
    Retries,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TaskListOutputKind {
    Json,
    Jsonl,
    Table,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TaskEventOutputKind {
    Json,
    Jsonl,
    Table,
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
        #[arg(long, value_enum, default_value_t = TaskListSortKind::Offset)]
        sort_by: TaskListSortKind,
        #[arg(long, value_enum, default_value_t = TaskListOutputKind::Json)]
        output: TaskListOutputKind,
        #[arg(long)]
        limit: Option<usize>,
    },
    WatchTasks {
        #[arg(long, default_value = "./var/orchestrator/state.json")]
        state_path: PathBuf,
        #[arg(long)]
        status: Option<TaskStatus>,
        #[arg(long)]
        skill: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long, value_enum, default_value_t = TaskListSortKind::Offset)]
        sort_by: TaskListSortKind,
        #[arg(long, value_enum, default_value_t = TaskListOutputKind::Table)]
        output: TaskListOutputKind,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, default_value_t = 1_000)]
        refresh_interval_ms: u64,
        #[arg(long)]
        iterations: Option<usize>,
    },
    ServeDashboard {
        #[command(flatten)]
        token: TokenArgs,
        #[arg(long, default_value = "./var/orchestrator/state.json")]
        state_path: PathBuf,
        #[arg(long, default_value = "127.0.0.1:8787")]
        listen: String,
        #[arg(long, default_value = TASK_EVENTS_TOPIC)]
        task_events_topic: String,
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
        #[arg(long)]
        status: Option<TaskStatus>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long)]
        assignment_id: Option<Uuid>,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 100)]
        batch_size: usize,
        #[arg(long, value_enum, default_value_t = TaskEventOutputKind::Json)]
        output: TaskEventOutputKind,
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
        #[arg(long)]
        status: Option<TaskStatus>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long)]
        assignment_id: Option<Uuid>,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 100)]
        batch_size: usize,
        #[arg(long, default_value_t = 1_000)]
        poll_interval_ms: u64,
        #[arg(long, value_enum, default_value_t = TaskEventOutputKind::Jsonl)]
        output: TaskEventOutputKind,
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
            sort_by,
            output,
            limit,
        } => {
            let state = load_state(&state_path)?;
            let tasks = list_tasks(
                &state,
                &build_task_list_query(status, skill, agent_id, sort_by, limit),
            );
            print!("{}", render_task_list(&tasks, output)?);
            Ok(())
        }
        Command::WatchTasks {
            state_path,
            status,
            skill,
            agent_id,
            sort_by,
            output,
            limit,
            refresh_interval_ms,
            iterations,
        } => {
            watch_tasks(
                state_path,
                build_task_list_query(status, skill, agent_id, sort_by, limit),
                output,
                refresh_interval_ms.max(1),
                iterations,
            )
            .await
        }
        Command::ServeDashboard {
            token,
            state_path,
            listen,
            task_events_topic,
        } => {
            let capability_token = resolve_token(token)?;
            serve_dashboard(
                endpoint,
                capability_token,
                state_path,
                task_events_topic,
                listen,
            )
            .await
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
            status,
            agent_id,
            assignment_id,
            offset,
            batch_size,
            output,
            limit,
        } => {
            let capability_token = resolve_token(token)?;
            show_task_history(
                endpoint,
                capability_token,
                TaskEventFilter {
                    task_id: Some(task_id),
                    status,
                    agent_id,
                    assignment_id,
                },
                task_events_topic,
                offset,
                batch_size.max(1),
                output,
                limit.max(1),
            )
            .await
        }
        Command::TailTaskEvents {
            token,
            task_events_topic,
            task_id,
            status,
            agent_id,
            assignment_id,
            offset,
            batch_size,
            poll_interval_ms,
            output,
            limit,
        } => {
            let capability_token = resolve_token(token)?;
            tail_task_events(
                endpoint,
                capability_token,
                task_events_topic,
                TaskEventFilter {
                    task_id,
                    status,
                    agent_id,
                    assignment_id,
                },
                offset,
                batch_size.max(1),
                poll_interval_ms.max(1),
                output,
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
    filter: TaskEventFilter,
    task_events_topic: String,
    offset: u64,
    batch_size: usize,
    output: TaskEventOutputKind,
    limit: usize,
) -> anyhow::Result<()> {
    let _ = filter
        .task_id
        .as_deref()
        .context("show-task-history requires a task_id filter")?;
    let history = fetch_task_history(
        endpoint,
        capability_token,
        &task_events_topic,
        &filter,
        offset,
        batch_size,
        limit,
    )
    .await?;
    print!("{}", render_task_history(&history, output)?);
    Ok(())
}

async fn watch_tasks(
    state_path: PathBuf,
    query: TaskListQuery,
    output: TaskListOutputKind,
    refresh_interval_ms: u64,
    iterations: Option<usize>,
) -> anyhow::Result<()> {
    let mut ticker = interval(Duration::from_millis(refresh_interval_ms));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut rendered_iterations = 0usize;

    loop {
        ticker.tick().await;
        let state = load_state(&state_path)?;
        let tasks = list_tasks(&state, &query);
        let frame = render_watch_task_frame(&state_path, &query, &tasks, output, Utc::now())?;
        print!("{frame}");
        rendered_iterations += 1;

        if iterations.is_some_and(|max_iterations| rendered_iterations >= max_iterations) {
            return Ok(());
        }
    }
}

async fn serve_dashboard(
    endpoint: Endpoint,
    capability_token: String,
    state_path: PathBuf,
    task_events_topic: String,
    listen: String,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&listen)
        .await
        .with_context(|| format!("failed to bind dashboard listener on {listen}"))?;
    let context = DashboardContext {
        endpoint,
        capability_token,
        state_path,
        task_events_topic,
    };

    info!(listen = %listen, "dashboard server listening");

    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (mut stream, peer) = accept?;
                let context = context.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_dashboard_connection(&mut stream, context).await {
                        warn!(peer = %peer, error = %error, "dashboard request failed");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                info!("dashboard server shutting down");
                return Ok(());
            }
        }
    }
}

async fn tail_task_events(
    endpoint: Endpoint,
    capability_token: String,
    task_events_topic: String,
    filter: TaskEventFilter,
    offset: u64,
    batch_size: usize,
    poll_interval_ms: u64,
    output: TaskEventOutputKind,
    limit: Option<usize>,
) -> anyhow::Result<()> {
    let mut client = Client::connect(endpoint).await?;
    let mut next_offset = offset;
    let mut emitted = 0usize;
    let mut table_header_rendered = false;

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
            let Some(entry) = decode_task_event_entry(&message) else {
                continue;
            };
            if !task_event_matches_filter(&entry, &filter) {
                continue;
            };

            print!(
                "{}",
                render_task_event_entry(&entry, output, &mut table_header_rendered)?
            );
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TaskEventFilter {
    task_id: Option<String>,
    status: Option<TaskStatus>,
    agent_id: Option<String>,
    assignment_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
struct DashboardContext {
    endpoint: Endpoint,
    capability_token: String,
    state_path: PathBuf,
    task_events_topic: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpRequestHead {
    method: String,
    path: String,
    query: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DashboardRoute {
    Index,
    Metrics,
    Tasks,
    Task { task_id: String },
    TaskHistory { task_id: String },
    Favicon,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpResponse {
    status_code: u16,
    reason: &'static str,
    content_type: &'static str,
    body: Vec<u8>,
}

async fn fetch_task_history(
    endpoint: Endpoint,
    capability_token: String,
    topic: &str,
    filter: &TaskEventFilter,
    offset: u64,
    batch_size: usize,
    limit: usize,
) -> anyhow::Result<TaskHistoryView> {
    let task_id = filter
        .task_id
        .as_deref()
        .context("task history requires a task_id filter")?;
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
            let Some(entry) = decode_task_event_entry(&message) else {
                continue;
            };
            if !task_event_matches_filter(&entry, filter) {
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

async fn handle_dashboard_connection(
    stream: &mut tokio::net::TcpStream,
    context: DashboardContext,
) -> anyhow::Result<()> {
    let request = read_http_request_head(stream).await?;
    let response = serve_dashboard_request(&context, &request).await;
    stream.write_all(&encode_http_response(&response)).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn serve_dashboard_request(
    context: &DashboardContext,
    request: &HttpRequestHead,
) -> HttpResponse {
    if request.method != "GET" {
        return text_response(405, "Method Not Allowed", "only GET is supported");
    }

    match route_dashboard_path(&request.path) {
        Some(DashboardRoute::Index) => html_response(render_dashboard_html(&context.state_path)),
        Some(DashboardRoute::Metrics) => match load_state(&context.state_path) {
            Ok(state) => json_response(&orchestrator_metrics(&state, Utc::now())),
            Err(error) => error_response(500, format!("failed to load state: {error}")),
        },
        Some(DashboardRoute::Tasks) => match load_state(&context.state_path) {
            Ok(state) => match dashboard_tasks_payload(&state, &request.query) {
                Ok(tasks) => json_response(&tasks),
                Err(error) => error_response(400, error.to_string()),
            },
            Err(error) => error_response(500, format!("failed to load state: {error}")),
        },
        Some(DashboardRoute::Task { task_id }) => match load_state(&context.state_path) {
            Ok(state) => match dashboard_task_payload(&state, &task_id) {
                Ok(task) => json_response(&task),
                Err(error) => error_response(404, error.to_string()),
            },
            Err(error) => error_response(500, format!("failed to load state: {error}")),
        },
        Some(DashboardRoute::TaskHistory { task_id }) => {
            match dashboard_history_request(&request.query, task_id) {
                Ok(history_request) => match fetch_task_history(
                    context.endpoint.clone(),
                    context.capability_token.clone(),
                    &context.task_events_topic,
                    &history_request.filter,
                    history_request.offset,
                    history_request.batch_size,
                    history_request.limit,
                )
                .await
                {
                    Ok(history) => json_response(&history),
                    Err(error) => error_response(500, error.to_string()),
                },
                Err(error) => error_response(400, error.to_string()),
            }
        }
        Some(DashboardRoute::Favicon) => HttpResponse {
            status_code: 204,
            reason: "No Content",
            content_type: "text/plain; charset=utf-8",
            body: Vec::new(),
        },
        None => error_response(
            404,
            format!("unsupported dashboard path `{}`", request.path),
        ),
    }
}

fn dashboard_tasks_payload(
    state: &OrchestratorState,
    query: &HashMap<String, String>,
) -> anyhow::Result<Vec<TaskSummaryView>> {
    Ok(list_tasks(state, &task_list_query_from_params(query)?))
}

fn dashboard_task_payload(
    state: &OrchestratorState,
    task_id: &str,
) -> anyhow::Result<expressways_orchestrator::TaskDetailView> {
    show_task(state, task_id, Utc::now())
        .with_context(|| format!("task `{task_id}` was not found in orchestrator state"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DashboardHistoryRequest {
    filter: TaskEventFilter,
    offset: u64,
    batch_size: usize,
    limit: usize,
}

fn dashboard_history_request(
    query: &HashMap<String, String>,
    task_id: String,
) -> anyhow::Result<DashboardHistoryRequest> {
    Ok(DashboardHistoryRequest {
        filter: TaskEventFilter {
            task_id: Some(task_id),
            status: optional_query_param(query, "status")?
                .map(|value| value.parse())
                .transpose()
                .map_err(|error: String| anyhow::anyhow!(error))?,
            agent_id: query.get("agent_id").cloned(),
            assignment_id: optional_query_param(query, "assignment_id")?
                .map(|value| {
                    Uuid::parse_str(&value)
                        .with_context(|| format!("invalid assignment_id `{value}`"))
                })
                .transpose()?,
        },
        offset: parse_query_u64(query, "offset")?.unwrap_or(0),
        batch_size: parse_query_usize(query, "batch_size")?
            .unwrap_or(100)
            .max(1),
        limit: parse_query_usize(query, "limit")?.unwrap_or(20).max(1),
    })
}

fn task_list_query_from_params(query: &HashMap<String, String>) -> anyhow::Result<TaskListQuery> {
    let sort_by = optional_query_param(query, "sort_by")?
        .map(|value| parse_task_list_sort_kind(&value))
        .transpose()?
        .unwrap_or(TaskListSortKind::Offset);

    Ok(build_task_list_query(
        optional_query_param(query, "status")?
            .map(|value| value.parse())
            .transpose()
            .map_err(|error: String| anyhow::anyhow!(error))?,
        query.get("skill").cloned(),
        query.get("agent_id").cloned(),
        sort_by,
        parse_query_usize(query, "limit")?,
    ))
}

fn parse_task_list_sort_kind(value: &str) -> anyhow::Result<TaskListSortKind> {
    match value {
        "offset" => Ok(TaskListSortKind::Offset),
        "priority" => Ok(TaskListSortKind::Priority),
        "age" => Ok(TaskListSortKind::Age),
        "retries" => Ok(TaskListSortKind::Retries),
        other => bail!("unsupported sort_by `{other}`"),
    }
}

fn parse_query_usize(query: &HashMap<String, String>, key: &str) -> anyhow::Result<Option<usize>> {
    optional_query_param(query, key)?
        .map(|value| {
            value
                .parse::<usize>()
                .with_context(|| format!("invalid `{key}` value `{value}`"))
        })
        .transpose()
}

fn parse_query_u64(query: &HashMap<String, String>, key: &str) -> anyhow::Result<Option<u64>> {
    optional_query_param(query, key)?
        .map(|value| {
            value
                .parse::<u64>()
                .with_context(|| format!("invalid `{key}` value `{value}`"))
        })
        .transpose()
}

fn optional_query_param(
    query: &HashMap<String, String>,
    key: &str,
) -> anyhow::Result<Option<String>> {
    Ok(query.get(key).cloned().filter(|value| !value.is_empty()))
}

async fn read_http_request_head(
    stream: &mut tokio::net::TcpStream,
) -> anyhow::Result<HttpRequestHead> {
    let mut buffer = vec![0_u8; 8192];
    let mut read = 0usize;

    loop {
        if read == buffer.len() {
            bail!("http request header exceeded 8192 bytes");
        }
        let bytes = stream.read(&mut buffer[read..]).await?;
        if bytes == 0 {
            bail!("connection closed before request headers were received");
        }
        read += bytes;
        if buffer[..read]
            .windows(4)
            .any(|window| window == b"\r\n\r\n")
        {
            break;
        }
    }

    let request = String::from_utf8_lossy(&buffer[..read]);
    parse_http_request_head(&request).context("invalid http request")
}

fn parse_http_request_head(request: &str) -> Option<HttpRequestHead> {
    let request_line = request.lines().next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_owned();
    let target = parts.next()?;
    let _version = parts.next()?;
    let (path, query) = split_request_target(target);

    Some(HttpRequestHead {
        method,
        path,
        query,
    })
}

fn split_request_target(target: &str) -> (String, HashMap<String, String>) {
    let mut parts = target.splitn(2, '?');
    let path = decode_url_component(parts.next().unwrap_or("/"));
    let mut query = HashMap::new();

    if let Some(query_string) = parts.next() {
        for pair in query_string.split('&').filter(|pair| !pair.is_empty()) {
            let mut kv = pair.splitn(2, '=');
            let key = decode_url_component(kv.next().unwrap_or_default());
            let value = decode_url_component(kv.next().unwrap_or_default());
            query.insert(key, value);
        }
    }

    (path, query)
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

fn route_dashboard_path(path: &str) -> Option<DashboardRoute> {
    match path {
        "/" => Some(DashboardRoute::Index),
        "/favicon.ico" => Some(DashboardRoute::Favicon),
        "/api/metrics" => Some(DashboardRoute::Metrics),
        "/api/tasks" => Some(DashboardRoute::Tasks),
        _ => {
            let segments = path
                .split('/')
                .filter(|segment| !segment.is_empty())
                .collect::<Vec<_>>();
            match segments.as_slice() {
                ["api", "tasks", task_id] => Some(DashboardRoute::Task {
                    task_id: (*task_id).to_owned(),
                }),
                ["api", "tasks", task_id, "history"] => Some(DashboardRoute::TaskHistory {
                    task_id: (*task_id).to_owned(),
                }),
                _ => None,
            }
        }
    }
}

fn json_response<T: Serialize>(value: &T) -> HttpResponse {
    match serde_json::to_vec_pretty(value) {
        Ok(body) => HttpResponse {
            status_code: 200,
            reason: "OK",
            content_type: "application/json; charset=utf-8",
            body,
        },
        Err(error) => error_response(500, format!("failed to serialize json response: {error}")),
    }
}

fn html_response(body: String) -> HttpResponse {
    HttpResponse {
        status_code: 200,
        reason: "OK",
        content_type: "text/html; charset=utf-8",
        body: body.into_bytes(),
    }
}

fn text_response(status_code: u16, reason: &'static str, body: &str) -> HttpResponse {
    HttpResponse {
        status_code,
        reason,
        content_type: "text/plain; charset=utf-8",
        body: body.as_bytes().to_vec(),
    }
}

fn error_response(status_code: u16, message: String) -> HttpResponse {
    HttpResponse {
        status_code,
        reason: match status_code {
            400 => "Bad Request",
            404 => "Not Found",
            405 => "Method Not Allowed",
            _ => "Internal Server Error",
        },
        content_type: "application/json; charset=utf-8",
        body: serde_json::to_vec_pretty(&serde_json::json!({ "error": message }))
            .expect("serialize dashboard error"),
    }
}

fn encode_http_response(response: &HttpResponse) -> Vec<u8> {
    let mut encoded = Vec::new();
    write!(
        &mut encoded,
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        response.status_code,
        response.reason,
        response.content_type,
        response.body.len(),
    )
    .expect("encode response head");
    encoded.extend_from_slice(&response.body);
    encoded
}

fn decode_task_event_entry(message: &StoredMessage) -> Option<TaskEventHistoryEntry> {
    let event = serde_json::from_str::<TaskEvent>(&message.payload).ok()?;

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

fn task_event_matches_filter(entry: &TaskEventHistoryEntry, filter: &TaskEventFilter) -> bool {
    if filter
        .task_id
        .as_deref()
        .is_some_and(|task_id| entry.event.task_id != task_id)
    {
        return false;
    }
    if filter
        .status
        .is_some_and(|status| entry.event.status != status)
    {
        return false;
    }
    if filter
        .agent_id
        .as_deref()
        .is_some_and(|agent_id| entry.event.agent_id.as_deref() != Some(agent_id))
    {
        return false;
    }
    if filter
        .assignment_id
        .is_some_and(|assignment_id| entry.event.assignment_id != Some(assignment_id))
    {
        return false;
    }

    true
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

fn build_task_list_query(
    status: Option<TaskStatus>,
    skill: Option<String>,
    agent_id: Option<String>,
    sort_by: TaskListSortKind,
    limit: Option<usize>,
) -> TaskListQuery {
    TaskListQuery {
        status,
        skill,
        agent_id,
        sort_by: task_list_sort(sort_by),
        limit: limit.map(|value| value.max(1)),
    }
}

fn task_list_sort(sort: TaskListSortKind) -> TaskListSort {
    match sort {
        TaskListSortKind::Offset => TaskListSort::Offset,
        TaskListSortKind::Priority => TaskListSort::Priority,
        TaskListSortKind::Age => TaskListSort::Age,
        TaskListSortKind::Retries => TaskListSort::Retries,
    }
}

fn render_task_list(
    tasks: &[TaskSummaryView],
    output: TaskListOutputKind,
) -> anyhow::Result<String> {
    match output {
        TaskListOutputKind::Json => Ok(format!("{}\n", serde_json::to_string_pretty(tasks)?)),
        TaskListOutputKind::Jsonl => {
            let mut rendered = String::new();
            for task in tasks {
                writeln!(&mut rendered, "{}", serde_json::to_string(task)?)
                    .expect("write to string");
            }
            Ok(rendered)
        }
        TaskListOutputKind::Table => Ok(render_task_table(tasks)),
    }
}

fn render_dashboard_html(state_path: &std::path::Path) -> String {
    let state_path = state_path.display();
    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Expressways Dashboard</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
    
    :root {{
      --bg: #f8fafc;
      --panel: #ffffff;
      --ink: #0f172a;
      --muted: #64748b;
      --border: #e2e8f0;
      --accent: #2563eb;
      --accent-hover: #1d4ed8;
      --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.05);
      --shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
      --font-sans: 'Inter', system-ui, sans-serif;
      --font-mono: 'JetBrains Mono', monospace;
    }}
    
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    
    body {{
      min-height: 100vh;
      background-color: var(--bg);
      color: var(--ink);
      font-family: var(--font-sans);
      -webkit-font-smoothing: antialiased;
      line-height: 1.5;
      padding-bottom: 40px;
    }}

    .container {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 0 24px;
    }}

    /* Top Navigation */
    .header {{
      background: var(--panel);
      border-bottom: 1px solid var(--border);
      padding: 16px 0;
      margin-bottom: 32px;
      position: sticky;
      top: 0;
      z-index: 50;
      box-shadow: var(--shadow-sm);
    }}
    
    .header-content {{
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}
    
    .brand {{
      display: flex;
      align-items: center;
      gap: 12px;
    }}
    
    .brand-icon {{
      width: 32px;
      height: 32px;
      background: var(--ink);
      border-radius: 8px;
      display: flex;
      align-items: center;
      justify-content: center;
      color: white;
      font-weight: 700;
      font-size: 18px;
    }}

    .brand h1 {{
      font-size: 18px;
      font-weight: 600;
      color: var(--ink);
      margin: 0;
    }}

    .header-meta {{
      display: flex;
      gap: 24px;
      font-size: 13px;
    }}
    
    .meta-item {{
      display: flex;
      flex-direction: column;
      align-items: flex-end;
    }}
    
    .meta-label {{ color: var(--muted); }}
    .meta-value {{ font-family: var(--font-mono); font-weight: 500; }}

    /* Cards */
    .panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 12px;
      box-shadow: var(--shadow-sm);
    }}

    /* Metrics Grid */
    .metrics-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 16px;
      margin-bottom: 32px;
    }}

    .metric-card {{
      padding: 20px;
      display: flex;
      flex-direction: column;
    }}

    .metric-label {{
      font-size: 13px;
      font-weight: 500;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}

    .metric-value {{
      font-size: 32px;
      font-weight: 700;
      color: var(--ink);
      margin: 8px 0 4px 0;
      line-height: 1;
    }}

    .metric-hint {{
      font-size: 13px;
      color: var(--muted);
    }}

    /* Filters Bar */
    .filters {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      padding: 16px;
      margin-bottom: 24px;
      align-items: flex-end;
      background: #ffffff;
      border: 1px solid var(--border);
      border-radius: 12px;
      box-shadow: var(--shadow-sm);
    }}

    .filter-group {{
      display: flex;
      flex-direction: column;
      gap: 6px;
      flex: 1;
      min-width: 160px;
    }}
    
    .filter-group.small {{
      min-width: 100px;
      flex: 0 1 auto;
    }}

    .filter-group label {{
      font-size: 13px;
      font-weight: 500;
      color: var(--ink);
    }}

    .filter-control {{
      padding: 8px 12px;
      border: 1px solid var(--border);
      border-radius: 8px;
      font-family: inherit;
      font-size: 14px;
      background: #fafafa;
      color: var(--ink);
      transition: all 0.2s;
      outline: none;
      height: 38px;
    }}
    
    .filter-control:focus {{
      border-color: var(--accent);
      background: #fff;
      box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1);
    }}

    .btn-primary {{
      background: var(--ink);
      color: white;
      border: none;
      padding: 8px 16px;
      border-radius: 8px;
      font-weight: 500;
      cursor: pointer;
      font-family: inherit;
      font-size: 14px;
      height: 38px;
      transition: background 0.2s;
      white-space: nowrap;
    }}
    
    .btn-primary:hover {{ background: #334155; }}

    /* Main Content Layout */
    .content-grid {{
      display: grid;
      grid-template-columns: minmax(0, 1.4fr) minmax(360px, 1fr);
      gap: 24px;
      align-items: start;
    }}

    /* Typography in panels */
    .panel-header {{
      padding: 16px 20px;
      border-bottom: 1px solid var(--border);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}
    
    .panel-header h2 {{
      font-size: 16px;
      font-weight: 600;
      margin: 0;
    }}
    
    .panel-header .badge {{
      background: var(--bg);
      padding: 4px 10px;
      border-radius: 99px;
      font-size: 12px;
      font-weight: 500;
      color: var(--muted);
      border: 1px solid var(--border);
    }}

    /* Table Styles */
    .table-container {{
      overflow-x: auto;
    }}
    
    table {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      text-align: left;
    }}
    
    th {{
      padding: 12px 20px;
      font-size: 12px;
      font-weight: 600;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      border-bottom: 1px solid var(--border);
      background: #fafafa;
    }}
    
    td {{
      padding: 14px 20px;
      font-size: 14px;
      border-bottom: 1px solid var(--border);
      vertical-align: top;
    }}

    tbody tr {{
      transition: background 0.15s;
      cursor: pointer;
    }}
    
    tbody tr:hover {{ background: #f8fafc; }}
    tbody tr.active {{ 
      background: #eff6ff; 
      position: relative;
    }}
    
    tbody tr.active td:first-child::before {{
      content: '';
      position: absolute;
      left: 0;
      top: 0;
      bottom: 0;
      width: 3px;
      background: var(--accent);
    }}

    .task-id-cell strong {{
      display: block;
      font-family: var(--font-mono);
      font-weight: 500;
      margin-bottom: 4px;
    }}
    
    .task-id-cell span {{
      color: var(--muted);
      font-size: 12px;
    }}

    /* Status Badges */
    .status-badge {{
      display: inline-flex;
      align-items: center;
      padding: 4px 10px;
      border-radius: 6px;
      font-size: 12px;
      font-weight: 600;
      line-height: 1;
      text-transform: capitalize;
    }}
    
    .status-completed {{ background: #dcfce7; color: #166534; border: 1px solid #bbf7d0; }}
    .status-failed, .status-exhausted, .status-canceled {{ background: #fee2e2; color: #991b1b; border: 1px solid #fecaca; }}
    .status-pending {{ background: #f1f5f9; color: #475569; border: 1px solid #e2e8f0; }}
    .status-assigned {{ background: #dbeafe; color: #1e40af; border: 1px solid #bfdbfe; }}
    .status-retry_scheduled, .status-timed_out {{ background: #fef3c7; color: #9a3412; border: 1px solid #fde68a; }}

    /* Detail View */
    .detail-section {{
      padding: 20px;
    }}

    .detail-grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      margin-bottom: 20px;
    }}
    
    .detail-item {{
      background: #fafafa;
      padding: 12px 16px;
      border-radius: 8px;
      border: 1px solid var(--border);
    }}
    
    .detail-item strong {{
      display: block;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 4px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    
    .detail-item span {{
      font-size: 14px;
      font-family: var(--font-mono);
    }}

    .payload-block {{
      background: #1e293b;
      color: #e2e8f0;
      padding: 16px;
      border-radius: 8px;
      font-family: var(--font-mono);
      font-size: 13px;
      overflow-x: auto;
      line-height: 1.5;
    }}

    /* History Timeline */
    .history-section {{
      padding: 20px;
      border-top: 1px solid var(--border);
    }}
    
    .history-list {{
      display: flex;
      flex-direction: column;
      gap: 16px;
      position: relative;
    }}
    
    .history-list::before {{
      content: '';
      position: absolute;
      left: 11px;
      top: 8px;
      bottom: 8px;
      width: 2px;
      background: var(--border);
      z-index: 1;
    }}

    .history-item {{
      display: flex;
      gap: 16px;
      position: relative;
      z-index: 2;
    }}
    
    .history-marker {{
      width: 24px;
      height: 24px;
      border-radius: 50%;
      background: var(--panel);
      border: 2px solid var(--border);
      flex-shrink: 0;
      display: flex;
      align-items: center;
      justify-content: center;
    }}
    
    .history-item.status-assigned .history-marker {{ border-color: #3b82f6; background: #eff6ff; }}
    .history-item.status-completed .history-marker {{ border-color: #22c55e; background: #f0fdf4; }}
    .history-item.status-failed .history-marker {{ border-color: #ef4444; background: #fef2f2; }}
    
    .history-content {{
      background: #fafafa;
      border: 1px solid var(--border);
      padding: 12px 16px;
      border-radius: 8px;
      flex: 1;
    }}
    
    .history-header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 8px;
    }}
    
    .history-header strong {{ font-size: 14px; text-transform: capitalize; padding:0; background:transparent; border:none; }}
    .history-time {{ font-size: 12px; color: var(--muted); }}
    
    .history-meta {{
      font-size: 13px;
      color: var(--muted);
      margin-bottom: 8px;
    }}
    
    .history-reason {{
      font-size: 13px;
      color: var(--ink);
      font-style: italic;
    }}

    .empty-state {{
      padding: 40px 20px;
      text-align: center;
      color: var(--muted);
      font-size: 14px;
    }}

    @media (max-width: 1024px) {{
      .content-grid {{ grid-template-columns: 1fr; }}
      .detail-grid {{ grid-template-columns: 1fr; }}
      .header-meta {{ display: none; }}
    }}
  </style>
</head>
<body>
  
  <header class="header">
    <div class="container header-content">
      <div class="brand">
        <div class="brand-icon">E</div>
        <h1>Expressways</h1>
      </div>
      <div class="header-meta">
        <div class="meta-item">
          <span class="meta-label">State path</span>
          <span class="meta-value" title="{state_path}">{state_path}</span>
        </div>
        <div class="meta-item">
          <span class="meta-label">Refresh</span>
          <span class="meta-value" id="last-updated">waiting...</span>
        </div>
      </div>
    </div>
  </header>

  <main class="container">
    
    <div class="metrics-grid" id="metrics">
      <!-- Metics inject here -->
    </div>

    <div class="filters">
      <div class="filter-group">
        <label>Status</label>
        <select id="status-filter" class="filter-control">
          <option value="">Any</option>
          <option value="pending">Pending</option>
          <option value="assigned">Assigned</option>
          <option value="completed">Completed</option>
          <option value="failed">Failed</option>
          <option value="retry_scheduled">Retry Scheduled</option>
          <option value="timed_out">Timed Out</option>
          <option value="exhausted">Exhausted</option>
          <option value="canceled">Canceled</option>
        </select>
      </div>
      <div class="filter-group">
        <label>Skill</label>
        <input id="skill-filter" class="filter-control" placeholder="e.g. summarize">
      </div>
      <div class="filter-group">
        <label>Agent</label>
        <input id="agent-filter" class="filter-control" placeholder="agent id">
      </div>
      <div class="filter-group small">
        <label>Sort</label>
        <select id="sort-filter" class="filter-control">
          <option value="priority">Priority</option>
          <option value="offset">Offset</option>
          <option value="age">Age</option>
          <option value="retries">Retries</option>
        </select>
      </div>
      <div class="filter-group small">
        <label>Limit</label>
        <input id="limit-filter" class="filter-control" type="number" min="1" value="25">
      </div>
      <button id="refresh-button" class="btn-primary" type="button">Refresh Data</button>
    </div>

    <div class="content-grid">
      <div class="panel">
        <div class="panel-header">
          <h2>Task Queue</h2>
          <span class="badge" id="queue-summary">0 tasks</span>
        </div>
        <div class="table-container">
          <table>
            <thead>
              <tr>
                <th>Task ID</th>
                <th>Status</th>
                <th>Priority</th>
                <th>Attempts</th>
                <th>Agent</th>
                <th>Reason</th>
              </tr>
            </thead>
            <tbody id="tasks-body"></tbody>
          </table>
        </div>
        <div class="empty-state" id="tasks-empty" hidden>
          No tasks match the current active filters.
        </div>
      </div>

      <div class="panel">
        <div class="panel-header">
          <h2>Task Detail</h2>
          <span class="badge" id="detail-summary">Select a task</span>
        </div>
        <div id="detail" class="detail-section"></div>
        
        <div class="panel-header" style="border-top: 1px solid var(--border);">
          <h2>Lifecycle History</h2>
        </div>
        <div id="history" class="history-section"></div>
      </div>
    </div>
  </main>

  <script>
    const state = {{
      selectedTaskId: null,
      refreshHandle: null,
    }};

    const statusFilter = document.getElementById("status-filter");
    const skillFilter = document.getElementById("skill-filter");
    const agentFilter = document.getElementById("agent-filter");
    const sortFilter = document.getElementById("sort-filter");
    const limitFilter = document.getElementById("limit-filter");

    function buildQuery() {{
      const params = new URLSearchParams();
      if (statusFilter.value) params.set("status", statusFilter.value);
      if (skillFilter.value.trim()) params.set("skill", skillFilter.value.trim());
      if (agentFilter.value.trim()) params.set("agent_id", agentFilter.value.trim());
      if (sortFilter.value) params.set("sort_by", sortFilter.value);
      if (limitFilter.value) params.set("limit", limitFilter.value);
      return params.toString();
    }}

    async function fetchJson(path) {{
      const response = await fetch(path, {{ cache: "no-store", headers: {{ "Accept": "application/json" }} }});
      if (!response.ok) {{
        let message = `request failed (${{response.status}})`;
        try {{
          const payload = await response.json();
          message = payload.error || message;
        }} catch (_e) {{}}
        throw new Error(message);
      }}
      return response.json();
    }}

    function formatDate(value) {{
      if (!value) return "n/a";
      return new Date(value).toLocaleString();
    }}
    
    function formatTime(value) {{
      if (!value) return "";
      return new Date(value).toLocaleTimeString();
    }}

    function escapeHtml(value) {{
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }}
    
    function getStatusClass(status) {{
      const s = String(status || "").toLowerCase();
      if (['completed'].includes(s)) return 'status-completed';
      if (['failed', 'exhausted', 'canceled'].includes(s)) return 'status-failed';
      if (['pending'].includes(s)) return 'status-pending';
      if (['assigned'].includes(s)) return 'status-assigned';
      if (['retry_scheduled', 'timed_out'].includes(s)) return 'status-retry_scheduled';
      return 'status-pending';
    }}

    function renderMetrics(metrics) {{
      const cards = [
        ["Agents Connected", metrics.agent_count, `${{metrics.pending_task_event_acks}} pending acks`],
        ["Tasks Evaluated", metrics.task_count, `Offset ${{metrics.task_offset}}`],
        ["In-flight Assigned", metrics.tasks.assigned, metrics.tasks.oldest_in_flight_age_seconds == null ? "None" : `Oldest: ${{metrics.tasks.oldest_in_flight_age_seconds}}s`],
        ["Completed", metrics.tasks.completed, `${{metrics.tasks.retry_count}} total retries`],
        ["Pressure", metrics.tasks.retry_scheduled + metrics.tasks.timed_out + metrics.tasks.exhausted, `Retrying: ${{metrics.tasks.retry_scheduled}}`],
      ];
      
      document.getElementById("metrics").innerHTML = cards.map(([label, value, hint]) => `
        <div class="panel metric-card">
          <div class="metric-label">${{escapeHtml(label)}}</div>
          <div class="metric-value">${{value}}</div>
          <div class="metric-hint">${{escapeHtml(hint)}}</div>
        </div>
      `).join("");
    }}

    function renderTasks(tasks) {{
      const tbody = document.getElementById("tasks-body");
      const empty = document.getElementById("tasks-empty");
      document.getElementById("queue-summary").textContent = `${{tasks.length}} / ${{limitFilter.value}}`;
      empty.hidden = tasks.length !== 0;

      if (!state.selectedTaskId || !tasks.some((task) => task.task_id === state.selectedTaskId)) {{
        state.selectedTaskId = tasks[0]?.task_id || null;
      }}

      tbody.innerHTML = tasks.map((task) => `
        <tr data-task-id="${{escapeHtml(task.task_id)}}" class="${{task.task_id === state.selectedTaskId ? "active" : ""}}">
          <td class="task-id-cell">
            <strong>${{escapeHtml(task.task_id)}}</strong>
            <span>${{escapeHtml(task.task_type)}}</span>
          </td>
          <td><span class="status-badge ${{getStatusClass(task.status)}}">${{escapeHtml(task.status)}}</span></td>
          <td>${{task.priority}}</td>
          <td>${{task.attempts}} / ${{task.max_attempts}}</td>
          <td><span style="font-family: var(--font-mono); font-size: 13px;">${{escapeHtml(task.active_agent_id || "-")}}</span></td>
          <td style="color: var(--muted); font-size: 13px;">${{escapeHtml(task.last_assignment_reason || "-")}}</td>
        </tr>
      `).join("");

      for (const row of tbody.querySelectorAll("tr")) {{
        row.addEventListener("click", () => {{
          document.querySelectorAll("#tasks-body tr").forEach(r => r.classList.remove("active"));
          row.classList.add("active");
          state.selectedTaskId = row.dataset.taskId;
          refreshDetail().catch(renderError);
        }});
      }}
    }}

    function renderDetail(task) {{
      const summary = document.getElementById("detail-summary");
      const host = document.getElementById("detail");
      
      if (!task) {{
        summary.textContent = "No Selection";
        host.innerHTML = '<div class="empty-state">Select a task from the queue to view its payload and status.</div>';
        return;
      }}

      summary.textContent = `Priority ${{task.priority}}`;
      
      host.innerHTML = `
        <div class="detail-grid">
          <div class="detail-item">
            <strong>Task Context</strong>
            <span>${{escapeHtml(task.task_type)}}</span>
          </div>
          <div class="detail-item">
            <strong>Lease Agent</strong>
            <span>${{escapeHtml(task.active_assignment?.agent_id || "None")}}</span>
          </div>
          <div class="detail-item">
            <strong>Assignment Reason</strong>
            <span>${{escapeHtml(task.last_assignment_reason || "None")}}</span>
          </div>
          <div class="detail-item">
            <strong>Failure State</strong>
            <span style="color: ${{task.last_error ? 'var(--error)' : 'inherit'}}">${{escapeHtml(task.last_error || "Clean")}}</span>
          </div>
        </div>
        <strong>Payload</strong>
        <div class="payload-block" style="margin-top: 8px;">${{escapeHtml(JSON.stringify(task.payload, null, 2))}}</div>
      `;
    }}

    function renderHistory(history) {{
      const host = document.getElementById("history");
      if (!history || !history.events || history.events.length === 0) {{
        host.innerHTML = '<div class="empty-state">No lifecycle events recorded for this task yet.</div>';
        return;
      }}

      host.innerHTML = `<div class="history-list">${{history.events.map((entry) => `
        <div class="history-item ${{getStatusClass(entry.event.status)}}">
          <div class="history-marker"></div>
          <div class="history-content">
            <div class="history-header">
              <strong class="${{getStatusClass(entry.event.status)}}">${{escapeHtml(entry.event.status)}}</strong>
              <span class="history-time">${{formatDate(entry.timestamp)}}</span>
            </div>
            <div class="history-meta">
              Agent: ${{(escapeHtml(entry.event.agent_id) || "<em>none</em>")}} | 
              Attempt: ${{entry.event.attempt}} | 
              Offset: ${{entry.offset}}
            </div>
            ${{entry.event.reason ? `<div class="history-reason">${{escapeHtml(entry.event.reason)}}</div>` : ''}}
          </div>
        </div>
      `).join("")}}</div>`;
    }}

    function renderError(error) {{
      console.error(error);
      const detail = document.getElementById("detail");
      if (detail) {{
         detail.innerHTML = `<div class="empty-state" style="color:var(--error);">Failed to load data: ${{escapeHtml(error.message)}}</div>`;
      }}
    }}

    async function refreshDetail() {{
      if (!state.selectedTaskId) {{
        renderDetail(null);
        renderHistory(null);
        return;
      }}
      const [detail, history] = await Promise.all([
        fetchJson(`/api/tasks/${{encodeURIComponent(state.selectedTaskId)}}`),
        fetchJson(`/api/tasks/${{encodeURIComponent(state.selectedTaskId)}}/history?limit=10`)
      ]);
      renderDetail(detail);
      renderHistory(history);
    }}

    async function refreshAll() {{
      const query = buildQuery();
      const [metrics, tasks] = await Promise.all([
        fetchJson("/api/metrics"),
        fetchJson(`/api/tasks${{query ? `?${{query}}` : ""}}`)
      ]);
      renderMetrics(metrics);
      renderTasks(tasks);
      await refreshDetail();
      document.getElementById("last-updated").textContent = formatTime(new Date());
    }}

    document.getElementById("refresh-button").addEventListener("click", () => {{
      refreshAll().catch(renderError);
    }});

    for (const element of [statusFilter, skillFilter, agentFilter, sortFilter, limitFilter]) {{
      element.addEventListener("change", () => refreshAll().catch(renderError));
    }}

    refreshAll().catch(renderError);
    state.refreshHandle = window.setInterval(() => {{
      refreshAll().catch(renderError);
    }}, 2000);
  </script>
</body>
</html>
"##
    )
}

fn render_watch_task_frame(
    state_path: &std::path::Path,
    query: &TaskListQuery,
    tasks: &[TaskSummaryView],
    output: TaskListOutputKind,
    rendered_at: chrono::DateTime<Utc>,
) -> anyhow::Result<String> {
    let mut rendered = String::new();
    write!(&mut rendered, "\x1b[2J\x1b[H").expect("write clear");
    writeln!(
        &mut rendered,
        "Watching tasks from {} at {}",
        state_path.display(),
        rendered_at.to_rfc3339()
    )
    .expect("write header");
    writeln!(
        &mut rendered,
        "Filters: status={} skill={} agent={} sort_by={:?} limit={}",
        query
            .status
            .map(|status| status.to_string())
            .unwrap_or_else(|| "any".to_owned()),
        query.skill.as_deref().unwrap_or("any"),
        query.agent_id.as_deref().unwrap_or("any"),
        query.sort_by,
        query
            .limit
            .map(|value| value.to_string())
            .unwrap_or_else(|| "all".to_owned()),
    )
    .expect("write filters");
    rendered.push('\n');
    rendered.push_str(&render_task_list(tasks, output)?);
    if !rendered.ends_with('\n') {
        rendered.push('\n');
    }
    Ok(rendered)
}

fn render_task_history(
    history: &TaskHistoryView,
    output: TaskEventOutputKind,
) -> anyhow::Result<String> {
    match output {
        TaskEventOutputKind::Json => Ok(format!("{}\n", serde_json::to_string_pretty(history)?)),
        TaskEventOutputKind::Jsonl | TaskEventOutputKind::Table => {
            render_task_event_entries(&history.events, output)
        }
    }
}

fn render_task_event_entries(
    entries: &[TaskEventHistoryEntry],
    output: TaskEventOutputKind,
) -> anyhow::Result<String> {
    let mut rendered = String::new();
    let mut table_header_rendered = false;
    for entry in entries {
        rendered.push_str(&render_task_event_entry(
            entry,
            output,
            &mut table_header_rendered,
        )?);
    }
    Ok(rendered)
}

fn render_task_event_entry(
    entry: &TaskEventHistoryEntry,
    output: TaskEventOutputKind,
    table_header_rendered: &mut bool,
) -> anyhow::Result<String> {
    match output {
        TaskEventOutputKind::Json => Ok(format!("{}\n", serde_json::to_string_pretty(entry)?)),
        TaskEventOutputKind::Jsonl => Ok(format!("{}\n", serde_json::to_string(entry)?)),
        TaskEventOutputKind::Table => Ok(render_task_event_table_row(entry, table_header_rendered)),
    }
}

fn render_task_table(tasks: &[TaskSummaryView]) -> String {
    let mut rendered = String::new();
    writeln!(
        &mut rendered,
        "{:<18} {:<12} {:>4} {:>5} {:<14} {:<12} {}",
        "TASK_ID", "STATUS", "PRI", "TRY", "AGENT", "SKILL", "REASON"
    )
    .expect("write header");

    for task in tasks {
        writeln!(
            &mut rendered,
            "{:<18} {:<12} {:>4} {:>5} {:<14} {:<12} {}",
            truncate_cell(&task.task_id, 18),
            task.status,
            task.priority,
            format!("{}/{}", task.attempts, task.max_attempts),
            truncate_cell(task.active_agent_id.as_deref().unwrap_or("-"), 14),
            truncate_cell(task.skill.as_deref().unwrap_or("-"), 12),
            task.last_assignment_reason.as_deref().unwrap_or("-"),
        )
        .expect("write row");
    }

    rendered
}

fn render_task_event_table_row(
    entry: &TaskEventHistoryEntry,
    table_header_rendered: &mut bool,
) -> String {
    let mut rendered = String::new();
    if !*table_header_rendered {
        writeln!(
            &mut rendered,
            "{:<8} {:<16} {:<18} {:<14} {:>5} {:<12} {}",
            "OFFSET", "STATUS", "TASK_ID", "AGENT", "TRY", "ASSIGNMENT", "REASON"
        )
        .expect("write header");
        *table_header_rendered = true;
    }

    writeln!(
        &mut rendered,
        "{:<8} {:<16} {:<18} {:<14} {:>5} {:<12} {}",
        entry.offset,
        entry.event.status,
        truncate_cell(&entry.event.task_id, 18),
        truncate_cell(entry.event.agent_id.as_deref().unwrap_or("-"), 14),
        entry.event.attempt,
        truncate_cell(
            &entry
                .event
                .assignment_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            12,
        ),
        entry.event.reason.as_deref().unwrap_or("-"),
    )
    .expect("write row");

    rendered
}

fn truncate_cell(value: &str, max_len: usize) -> String {
    if value.chars().count() <= max_len {
        return value.to_owned();
    }

    let truncated = value
        .chars()
        .take(max_len.saturating_sub(1))
        .collect::<String>();
    format!("{truncated}~")
}

#[cfg(test)]
mod tests {
    use super::{
        DashboardRoute, TaskEventFilter, TaskEventOutputKind, TaskHistoryView, TaskListOutputKind,
        TaskListSortKind, build_task_list_query, dashboard_history_request,
        decode_task_event_entry, parse_http_request_head, render_dashboard_html,
        render_task_event_entry, render_task_history, render_task_list, render_watch_task_frame,
        route_dashboard_path, split_request_target, task_event_matches_filter,
        task_list_query_from_params,
    };
    use chrono::Utc;
    use expressways_orchestrator::{TaskListSort, TaskSummaryView};
    use expressways_protocol::{Classification, StoredMessage, TaskEvent, TaskStatus};
    use std::collections::HashMap;
    use std::path::Path;
    use uuid::Uuid;

    #[test]
    fn decode_task_event_entry_parses_event_metadata() {
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

        let entry = decode_task_event_entry(&message).expect("matching task");
        assert_eq!(entry.offset, 9);
        assert_eq!(entry.event, event);
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

        assert!(decode_task_event_entry(&message).is_none());
    }

    #[test]
    fn task_event_filter_matches_task_status_agent_and_assignment() {
        let assignment_id = Uuid::now_v7();
        let event = TaskEvent {
            event_id: Uuid::now_v7(),
            task_id: "task-1".to_owned(),
            task_offset: Some(4),
            assignment_id: Some(assignment_id),
            agent_id: Some("agent-a".to_owned()),
            status: TaskStatus::Assigned,
            attempt: 1,
            reason: Some("scheduler picked agent-a".to_owned()),
            emitted_at: Utc::now(),
        };
        let entry = decode_task_event_entry(&stored_message(&event, 9)).expect("entry");

        assert!(task_event_matches_filter(
            &entry,
            &TaskEventFilter {
                task_id: Some("task-1".to_owned()),
                status: Some(TaskStatus::Assigned),
                agent_id: Some("agent-a".to_owned()),
                assignment_id: Some(assignment_id),
            }
        ));
        assert!(!task_event_matches_filter(
            &entry,
            &TaskEventFilter {
                status: Some(TaskStatus::Completed),
                ..TaskEventFilter::default()
            }
        ));
        assert!(!task_event_matches_filter(
            &entry,
            &TaskEventFilter {
                agent_id: Some("agent-b".to_owned()),
                ..TaskEventFilter::default()
            }
        ));
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

    #[test]
    fn render_task_history_supports_json_jsonl_and_table() {
        let event = TaskEvent {
            event_id: Uuid::now_v7(),
            task_id: "task-1".to_owned(),
            task_offset: Some(4),
            assignment_id: Some(Uuid::nil()),
            agent_id: Some("alpha".to_owned()),
            status: TaskStatus::Assigned,
            attempt: 2,
            reason: Some("scheduler picked alpha".to_owned()),
            emitted_at: Utc::now(),
        };
        let history = TaskHistoryView {
            task_id: "task-1".to_owned(),
            topic: "task_events".to_owned(),
            start_offset: 4,
            next_offset: 10,
            returned: 1,
            events: vec![decode_task_event_entry(&stored_message(&event, 9)).expect("entry")],
        };

        let json = render_task_history(&history, TaskEventOutputKind::Json).expect("json");
        assert!(json.contains("\"task_id\": \"task-1\""));
        assert!(json.contains("\"returned\": 1"));

        let jsonl = render_task_history(&history, TaskEventOutputKind::Jsonl).expect("jsonl");
        assert!(jsonl.contains("\"task_id\":\"task-1\""));
        assert!(jsonl.ends_with('\n'));

        let table = render_task_history(&history, TaskEventOutputKind::Table).expect("table");
        assert!(table.contains("OFFSET"));
        assert!(table.contains("assigned"));
        assert!(table.contains("scheduler picked alpha"));
    }

    #[test]
    fn render_task_list_supports_jsonl_and_table() {
        let tasks = vec![TaskSummaryView {
            task_id: "task-1".to_owned(),
            task_type: "summarize_document".to_owned(),
            task_offset: 4,
            priority: 50,
            status: TaskStatus::Assigned,
            attempts: 2,
            max_attempts: 4,
            submitted_at: Utc::now(),
            last_event_at: Utc::now(),
            skill: Some("summarize".to_owned()),
            active_agent_id: Some("alpha".to_owned()),
            assignment_id: Some(Uuid::nil()),
            last_assignment_reason: Some("scheduler picked alpha".to_owned()),
            next_retry_at: None,
            last_error: None,
        }];

        let jsonl = render_task_list(&tasks, TaskListOutputKind::Jsonl).expect("jsonl");
        assert!(jsonl.contains("\"task_id\":\"task-1\""));
        assert!(jsonl.ends_with('\n'));

        let table = render_task_list(&tasks, TaskListOutputKind::Table).expect("table");
        assert!(table.contains("TASK_ID"));
        assert!(table.contains("task-1"));
        assert!(table.contains("scheduler picked alpha"));
    }

    #[test]
    fn render_watch_task_frame_includes_filters_and_table() {
        let tasks = vec![TaskSummaryView {
            task_id: "task-1".to_owned(),
            task_type: "summarize_document".to_owned(),
            task_offset: 4,
            priority: 50,
            status: TaskStatus::Assigned,
            attempts: 2,
            max_attempts: 4,
            submitted_at: Utc::now(),
            last_event_at: Utc::now(),
            skill: Some("summarize".to_owned()),
            active_agent_id: Some("alpha".to_owned()),
            assignment_id: Some(Uuid::nil()),
            last_assignment_reason: Some("scheduler picked alpha".to_owned()),
            next_retry_at: None,
            last_error: None,
        }];
        let query = build_task_list_query(
            Some(TaskStatus::Assigned),
            Some("summarize".to_owned()),
            Some("alpha".to_owned()),
            TaskListSortKind::Priority,
            Some(5),
        );

        let frame = render_watch_task_frame(
            Path::new("./var/orchestrator/state.json"),
            &query,
            &tasks,
            TaskListOutputKind::Table,
            Utc::now(),
        )
        .expect("frame");

        assert!(frame.starts_with("\u{1b}[2J\u{1b}[H"));
        assert!(frame.contains("Watching tasks from ./var/orchestrator/state.json"));
        assert!(frame.contains("status=assigned"));
        assert!(frame.contains("skill=summarize"));
        assert!(frame.contains("agent=alpha"));
        assert!(frame.contains("sort_by=Priority"));
        assert!(frame.contains("TASK_ID"));
        assert!(frame.contains("task-1"));
    }

    #[test]
    fn parse_http_request_head_extracts_path_and_query() {
        let request = "GET /api/tasks/task-1/history?status=assigned&agent_id=alpha HTTP/1.1\r\nHost: localhost\r\n\r\n";

        let parsed = parse_http_request_head(request).expect("request");
        assert_eq!(parsed.method, "GET");
        assert_eq!(parsed.path, "/api/tasks/task-1/history");
        assert_eq!(
            parsed.query.get("status").map(String::as_str),
            Some("assigned")
        );
        assert_eq!(
            parsed.query.get("agent_id").map(String::as_str),
            Some("alpha")
        );
    }

    #[test]
    fn split_request_target_decodes_percent_encoded_components() {
        let (path, query) = split_request_target("/api/tasks/task%201?skill=summarize+doc");
        assert_eq!(path, "/api/tasks/task 1");
        assert_eq!(
            query.get("skill").map(String::as_str),
            Some("summarize doc")
        );
    }

    #[test]
    fn route_dashboard_path_matches_history_endpoint() {
        assert_eq!(route_dashboard_path("/"), Some(DashboardRoute::Index));
        assert_eq!(
            route_dashboard_path("/api/tasks/task-1/history"),
            Some(DashboardRoute::TaskHistory {
                task_id: "task-1".to_owned()
            })
        );
        assert_eq!(route_dashboard_path("/nope"), None);
    }

    #[test]
    fn task_list_query_from_params_uses_filters_and_sort() {
        let mut query = HashMap::new();
        query.insert("status".to_owned(), "assigned".to_owned());
        query.insert("skill".to_owned(), "summarize".to_owned());
        query.insert("agent_id".to_owned(), "alpha".to_owned());
        query.insert("sort_by".to_owned(), "priority".to_owned());
        query.insert("limit".to_owned(), "7".to_owned());

        let parsed = task_list_query_from_params(&query).expect("query");
        assert_eq!(parsed.status, Some(TaskStatus::Assigned));
        assert_eq!(parsed.skill.as_deref(), Some("summarize"));
        assert_eq!(parsed.agent_id.as_deref(), Some("alpha"));
        assert_eq!(parsed.sort_by, TaskListSort::Priority);
        assert_eq!(parsed.limit, Some(7));
    }

    #[test]
    fn dashboard_history_request_parses_filters_and_pagination() {
        let assignment_id = Uuid::now_v7();
        let mut query = HashMap::new();
        query.insert("status".to_owned(), "completed".to_owned());
        query.insert("agent_id".to_owned(), "alpha".to_owned());
        query.insert("assignment_id".to_owned(), assignment_id.to_string());
        query.insert("offset".to_owned(), "12".to_owned());
        query.insert("batch_size".to_owned(), "50".to_owned());
        query.insert("limit".to_owned(), "6".to_owned());

        let parsed = dashboard_history_request(&query, "task-7".to_owned()).expect("history");
        assert_eq!(parsed.filter.task_id.as_deref(), Some("task-7"));
        assert_eq!(parsed.filter.status, Some(TaskStatus::Completed));
        assert_eq!(parsed.filter.agent_id.as_deref(), Some("alpha"));
        assert_eq!(parsed.filter.assignment_id, Some(assignment_id));
        assert_eq!(parsed.offset, 12);
        assert_eq!(parsed.batch_size, 50);
        assert_eq!(parsed.limit, 6);
    }

    #[test]
    fn render_dashboard_html_mentions_api_surfaces() {
        let html = render_dashboard_html(Path::new("./var/orchestrator/state.json"));
        assert!(html.contains("Expressways Dashboard"));
        assert!(html.contains("/api/metrics"));
        assert!(html.contains("/api/tasks"));
        assert!(
            html.contains(
                "/api/tasks/${encodeURIComponent(state.selectedTaskId)}/history?limit=10"
            )
        );
    }

    #[test]
    fn render_task_event_entry_supports_jsonl_and_table() {
        let event = TaskEvent {
            event_id: Uuid::now_v7(),
            task_id: "task-1".to_owned(),
            task_offset: Some(4),
            assignment_id: Some(Uuid::nil()),
            agent_id: Some("alpha".to_owned()),
            status: TaskStatus::Assigned,
            attempt: 2,
            reason: Some("scheduler picked alpha".to_owned()),
            emitted_at: Utc::now(),
        };
        let entry = decode_task_event_entry(&stored_message(&event, 9)).expect("entry");

        let mut header_rendered = false;
        let jsonl =
            render_task_event_entry(&entry, TaskEventOutputKind::Jsonl, &mut header_rendered)
                .expect("jsonl");
        assert!(jsonl.contains("\"task_id\":\"task-1\""));
        assert!(jsonl.ends_with('\n'));
        assert!(!header_rendered);

        let table =
            render_task_event_entry(&entry, TaskEventOutputKind::Table, &mut header_rendered)
                .expect("table");
        assert!(table.contains("OFFSET"));
        assert!(table.contains("assigned"));
        assert!(table.contains("scheduler picked alpha"));
        assert!(header_rendered);
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
