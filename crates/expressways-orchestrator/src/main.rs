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
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Expressways Dashboard</title>
  <style>
    :root {{
      --bg: #f3efe4;
      --panel: rgba(255, 252, 245, 0.92);
      --ink: #1d2935;
      --muted: #6f7f8e;
      --line: rgba(29, 41, 53, 0.12);
      --accent: #ca7b2b;
      --accent-deep: #8c4b10;
      --ok: #2c6a4c;
      --warn: #9a5b17;
      --shadow: 0 20px 45px rgba(39, 35, 26, 0.12);
      font-family: "IBM Plex Sans", "Avenir Next", "Segoe UI", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      background:
        radial-gradient(circle at top left, rgba(202, 123, 43, 0.16), transparent 30rem),
        radial-gradient(circle at bottom right, rgba(44, 106, 76, 0.12), transparent 28rem),
        var(--bg);
      color: var(--ink);
    }}
    .shell {{
      max-width: 1380px;
      margin: 0 auto;
      padding: 24px;
    }}
    .hero {{
      display: grid;
      gap: 18px;
      grid-template-columns: minmax(0, 1.7fr) minmax(280px, 0.9fr);
      margin-bottom: 20px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 22px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(12px);
    }}
    .hero-copy {{
      padding: 28px;
    }}
    .eyebrow {{
      margin: 0 0 10px;
      font-size: 12px;
      letter-spacing: 0.18em;
      text-transform: uppercase;
      color: var(--accent-deep);
    }}
    h1 {{
      margin: 0;
      font-size: clamp(2rem, 4vw, 3.5rem);
      line-height: 0.95;
    }}
    .lede {{
      margin: 14px 0 0;
      max-width: 60ch;
      color: var(--muted);
      font-size: 1rem;
      line-height: 1.6;
    }}
    .hero-meta {{
      padding: 24px;
      display: grid;
      gap: 14px;
      align-content: start;
    }}
    .hero-meta strong {{
      display: block;
      font-size: 0.78rem;
      letter-spacing: 0.1em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 6px;
    }}
    .hero-meta code {{
      display: block;
      word-break: break-all;
      font-size: 0.86rem;
      color: var(--ink);
    }}
    .controls, .metrics, .content {{
      display: grid;
      gap: 16px;
      margin-bottom: 18px;
    }}
    .controls {{
      grid-template-columns: repeat(4, minmax(0, 1fr)) auto;
      align-items: end;
      padding: 18px;
    }}
    label {{
      display: grid;
      gap: 8px;
      font-size: 0.84rem;
      color: var(--muted);
    }}
    select, input, button {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 12px 14px;
      background: rgba(255, 255, 255, 0.72);
      color: var(--ink);
      font: inherit;
    }}
    button {{
      width: auto;
      min-width: 140px;
      background: linear-gradient(135deg, var(--accent), #d79756);
      color: white;
      border: none;
      cursor: pointer;
      font-weight: 700;
    }}
    .metrics {{
      grid-template-columns: repeat(5, minmax(0, 1fr));
    }}
    .metric-card {{
      padding: 18px;
    }}
    .metric-card .label {{
      font-size: 0.78rem;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.09em;
    }}
    .metric-card .value {{
      margin-top: 10px;
      font-size: 2rem;
      font-weight: 700;
    }}
    .metric-card .hint {{
      margin-top: 8px;
      color: var(--muted);
      font-size: 0.9rem;
    }}
    .content {{
      grid-template-columns: minmax(0, 1.35fr) minmax(320px, 0.95fr);
      align-items: start;
    }}
    .panel-head {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: baseline;
      padding: 20px 22px 0;
    }}
    .panel-head h2 {{
      margin: 0;
      font-size: 1.1rem;
    }}
    .panel-head span {{
      color: var(--muted);
      font-size: 0.9rem;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    thead th {{
      font-size: 0.76rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
      text-align: left;
      padding: 16px 22px 12px;
      border-bottom: 1px solid var(--line);
    }}
    tbody td {{
      padding: 14px 22px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }}
    tbody tr {{
      cursor: pointer;
      transition: background 120ms ease, transform 120ms ease;
    }}
    tbody tr:hover {{
      background: rgba(202, 123, 43, 0.08);
    }}
    tbody tr.active {{
      background: rgba(202, 123, 43, 0.14);
    }}
    .status {{
      display: inline-flex;
      padding: 5px 10px;
      border-radius: 999px;
      font-size: 0.77rem;
      font-weight: 700;
      background: rgba(29, 41, 53, 0.08);
    }}
    .detail, .history {{
      padding: 0 22px 22px;
    }}
    .detail-grid {{
      display: grid;
      gap: 12px;
      margin-top: 16px;
    }}
    .detail-item {{
      padding: 14px 16px;
      border-radius: 16px;
      background: rgba(255, 255, 255, 0.64);
      border: 1px solid var(--line);
    }}
    .detail-item strong {{
      display: block;
      margin-bottom: 6px;
      font-size: 0.76rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }}
    .detail-pre {{
      margin: 16px 0 0;
      max-height: 240px;
      overflow: auto;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: #f9f5ed;
      padding: 16px;
      font-size: 0.86rem;
      line-height: 1.45;
    }}
    .history-list {{
      display: grid;
      gap: 12px;
      margin-top: 14px;
    }}
    .history-card {{
      padding: 14px 16px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.68);
    }}
    .history-card header {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 8px;
      font-size: 0.82rem;
      color: var(--muted);
    }}
    .history-card strong {{
      color: var(--ink);
      font-size: 0.92rem;
    }}
    .empty {{
      color: var(--muted);
      padding: 18px 22px 24px;
    }}
    @media (max-width: 1024px) {{
      .hero, .content, .metrics, .controls {{
        grid-template-columns: 1fr;
      }}
      .shell {{
        padding: 16px;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="panel hero-copy">
        <p class="eyebrow">Expressways</p>
        <h1>Local orchestration dashboard</h1>
        <p class="lede">A small browser surface for queue health, scheduler rationale, and task lifecycle history, backed by the persisted orchestrator state and broker event log.</p>
      </div>
      <aside class="panel hero-meta">
        <div>
          <strong>State path</strong>
          <code>{state_path}</code>
        </div>
        <div>
          <strong>Refresh</strong>
          <span id="last-updated">waiting for first poll</span>
        </div>
        <div>
          <strong>Selected task</strong>
          <span id="selected-task-label">none</span>
        </div>
      </aside>
    </section>

    <section class="panel controls">
      <label>Status
        <select id="status-filter">
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
      </label>
      <label>Skill
        <input id="skill-filter" placeholder="summarize">
      </label>
      <label>Agent
        <input id="agent-filter" placeholder="agent id">
      </label>
      <label>Sort
        <select id="sort-filter">
          <option value="priority">Priority</option>
          <option value="offset">Offset</option>
          <option value="age">Age</option>
          <option value="retries">Retries</option>
        </select>
      </label>
      <label>Limit
        <input id="limit-filter" type="number" min="1" value="25">
      </label>
      <button id="refresh-button" type="button">Refresh now</button>
    </section>

    <section class="metrics" id="metrics"></section>

    <section class="content">
      <div class="panel">
        <div class="panel-head">
          <h2>Queue</h2>
          <span id="queue-summary">0 tasks</span>
        </div>
        <div style="overflow:auto;">
          <table>
            <thead>
              <tr>
                <th>Task</th>
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
        <div class="empty" id="tasks-empty" hidden>No tasks match the current filter.</div>
      </div>

      <div class="panel">
        <div class="panel-head">
          <h2>Task detail</h2>
          <span id="detail-summary">select a task</span>
        </div>
        <div class="detail" id="detail"></div>
        <div class="panel-head">
          <h2>History</h2>
          <span>Latest lifecycle events</span>
        </div>
        <div class="history" id="history"></div>
      </div>
    </section>
  </div>

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
      const response = await fetch(path, {{ cache: "no-store" }});
      if (!response.ok) {{
        let message = `request failed (${{response.status}})`;
        try {{
          const payload = await response.json();
          message = payload.error || message;
        }} catch (_error) {{}}
        throw new Error(message);
      }}
      return response.json();
    }}

    function formatDate(value) {{
      if (!value) return "n/a";
      return new Date(value).toLocaleString();
    }}

    function escapeHtml(value) {{
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }}

    function renderMetrics(metrics) {{
      const cards = [
        ["Agents", metrics.agent_count, `${{metrics.pending_task_event_acks}} pending event acks`],
        ["Tasks", metrics.task_count, `cursor ${{metrics.task_offset}} / events ${{metrics.task_event_offset}}`],
        ["Assigned", metrics.tasks.assigned, metrics.tasks.oldest_in_flight_age_seconds == null ? "no in-flight lease" : `oldest lease ${{metrics.tasks.oldest_in_flight_age_seconds}}s`],
        ["Completed", metrics.tasks.completed, `${{metrics.tasks.retry_count}} retries observed`],
        ["Pressure", metrics.tasks.retry_scheduled + metrics.tasks.timed_out + metrics.tasks.exhausted, `${{metrics.tasks.retry_scheduled}} retrying / ${{metrics.tasks.exhausted}} exhausted`],
      ];
      document.getElementById("metrics").innerHTML = cards.map(([label, value, hint]) => `
        <article class="panel metric-card">
          <div class="label">${{label}}</div>
          <div class="value">${{value}}</div>
          <div class="hint">${{hint}}</div>
        </article>
      `).join("");
    }}

    function renderTasks(tasks) {{
      const tbody = document.getElementById("tasks-body");
      const empty = document.getElementById("tasks-empty");
      document.getElementById("queue-summary").textContent = `${{tasks.length}} task${{tasks.length === 1 ? "" : "s"}}`;
      empty.hidden = tasks.length !== 0;

      if (!state.selectedTaskId || !tasks.some((task) => task.task_id === state.selectedTaskId)) {{
        state.selectedTaskId = tasks[0]?.task_id || null;
      }}
      document.getElementById("selected-task-label").textContent = state.selectedTaskId || "none";

      tbody.innerHTML = tasks.map((task) => `
        <tr data-task-id="${{escapeHtml(task.task_id)}}" class="${{task.task_id === state.selectedTaskId ? "active" : ""}}">
          <td><strong>${{escapeHtml(task.task_id)}}</strong><br><span style="color:var(--muted)">${{escapeHtml(task.task_type)}}</span></td>
          <td><span class="status">${{escapeHtml(task.status)}}</span></td>
          <td>${{task.priority}}</td>
          <td>${{task.attempts}} / ${{task.max_attempts}}</td>
          <td>${{escapeHtml(task.active_agent_id || "-")}}</td>
          <td>${{escapeHtml(task.last_assignment_reason || "-")}}</td>
        </tr>
      `).join("");

      for (const row of tbody.querySelectorAll("tr")) {{
        row.addEventListener("click", () => {{
          state.selectedTaskId = row.dataset.taskId;
          refreshDetail().catch(renderError);
          renderTasks(tasks);
        }});
      }}
    }}

    function renderDetail(task) {{
      document.getElementById("detail-summary").textContent = task ? `${{task.status}} · priority ${{task.priority}}` : "select a task";
      if (!task) {{
        document.getElementById("detail").innerHTML = '<div class="empty">Pick a task from the queue to inspect its payload, current lease, and retry state.</div>';
        return;
      }}

      document.getElementById("detail").innerHTML = `
        <div class="detail-grid">
          <div class="detail-item"><strong>Task</strong>${{escapeHtml(task.task_id)}} · ${{escapeHtml(task.task_type)}}</div>
          <div class="detail-item"><strong>Lease</strong>${{escapeHtml(task.active_assignment?.agent_id || "-")}}${{task.active_assignment_age_seconds == null ? "" : ` · ${{task.active_assignment_age_seconds}}s old`}}</div>
          <div class="detail-item"><strong>Scheduler reason</strong>${{escapeHtml(task.last_assignment_reason || "-")}}</div>
          <div class="detail-item"><strong>Failure state</strong>${{escapeHtml(task.last_error || "-")}}</div>
        </div>
        <pre class="detail-pre">${{escapeHtml(JSON.stringify(task.payload, null, 2))}}</pre>
      `;
    }}

    function renderHistory(history) {{
      const host = document.getElementById("history");
      if (!history || !history.events || history.events.length === 0) {{
        host.innerHTML = '<div class="empty">No lifecycle events yet for the selected task.</div>';
        return;
      }}

      host.innerHTML = `<div class="history-list">${{history.events.map((entry) => `
        <article class="history-card">
          <header>
            <span>#${{entry.offset}} · ${{formatDate(entry.timestamp)}}</span>
            <span>${{escapeHtml(entry.producer)}}</span>
          </header>
          <strong>${{escapeHtml(entry.event.status)}}</strong>
          <div style="margin-top:8px;color:var(--muted);font-size:0.9rem;">
            agent=${{escapeHtml(entry.event.agent_id || "-")}} · attempt=${{entry.event.attempt}} · assignment=${{escapeHtml(entry.event.assignment_id || "-")}}
          </div>
          <div style="margin-top:10px;">${{escapeHtml(entry.event.reason || "-")}}</div>
        </article>
      `).join("")}}</div>`;
    }}

    function renderError(error) {{
      document.getElementById("detail").innerHTML = `<div class="empty">${{escapeHtml(error.message || String(error))}}</div>`;
    }}

    async function refreshDetail() {{
      if (!state.selectedTaskId) {{
        renderDetail(null);
        renderHistory(null);
        return;
      }}
      const [detail, history] = await Promise.all([
        fetchJson(`/api/tasks/${{encodeURIComponent(state.selectedTaskId)}}`),
        fetchJson(`/api/tasks/${{encodeURIComponent(state.selectedTaskId)}}/history?limit=8`)
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
      document.getElementById("last-updated").textContent = new Date().toLocaleTimeString();
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
"#
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
        assert!(html.contains("Local orchestration dashboard"));
        assert!(html.contains("/api/metrics"));
        assert!(html.contains("/api/tasks"));
        assert!(
            html.contains("/api/tasks/${encodeURIComponent(state.selectedTaskId)}/history?limit=8")
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
