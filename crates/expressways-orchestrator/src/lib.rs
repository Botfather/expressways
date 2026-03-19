use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use expressways_protocol::{
    AgentCard, AgentQuery, RegistryEvent, RegistryEventKind, TaskEvent, TaskPayload,
    TaskRequirements, TaskStatus, TaskWorkItem,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type AssignmentRequirements = TaskRequirements;

#[derive(Debug, Clone)]
pub struct AgentSelection<'a> {
    pub agent: &'a AgentCard,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentSchedulingScore {
    avoid_penalty: u8,
    active_load: usize,
    preference_penalty: u8,
    historical_assignments: u64,
    last_assigned_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorState {
    #[serde(default = "default_observed_at")]
    pub observed_at: DateTime<Utc>,
    #[serde(default)]
    pub source_cursor: u64,
    #[serde(default)]
    pub task_offset: u64,
    #[serde(default)]
    pub task_event_offset: u64,
    #[serde(default)]
    pub agents: BTreeMap<String, AgentCard>,
    #[serde(default)]
    pub tasks: BTreeMap<String, TaskRecord>,
    #[serde(default)]
    pub assignments: BTreeMap<String, AgentAssignmentStats>,
    #[serde(default)]
    pub pending_task_event_acks: BTreeSet<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TaskLifecycleMetrics {
    pub pending: usize,
    pub assigned: usize,
    pub completed: usize,
    pub failed: usize,
    pub retry_scheduled: usize,
    pub timed_out: usize,
    pub exhausted: usize,
    pub canceled: usize,
    pub retry_count: u64,
    pub oldest_in_flight_age_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrchestratorMetricsView {
    pub observed_at: DateTime<Utc>,
    pub source_cursor: u64,
    pub task_offset: u64,
    pub task_event_offset: u64,
    pub agent_count: usize,
    pub task_count: usize,
    pub pending_task_event_acks: usize,
    pub tasks: TaskLifecycleMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskListSort {
    Offset,
    Priority,
    Age,
    Retries,
}

impl Default for TaskListSort {
    fn default() -> Self {
        Self::Offset
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TaskListQuery {
    pub status: Option<TaskStatus>,
    pub skill: Option<String>,
    pub agent_id: Option<String>,
    #[serde(default)]
    pub sort_by: TaskListSort,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskSummaryView {
    pub task_id: String,
    pub task_type: String,
    pub task_offset: u64,
    pub priority: i32,
    pub status: TaskStatus,
    pub attempts: u32,
    pub max_attempts: u32,
    pub submitted_at: DateTime<Utc>,
    pub last_event_at: DateTime<Utc>,
    pub skill: Option<String>,
    pub active_agent_id: Option<String>,
    pub assignment_id: Option<Uuid>,
    pub last_assignment_reason: Option<String>,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskDetailView {
    pub task_id: String,
    pub task_type: String,
    pub task_offset: u64,
    pub priority: i32,
    pub status: TaskStatus,
    pub attempts: u32,
    pub max_attempts: u32,
    pub retry_count: u32,
    pub submitted_at: DateTime<Utc>,
    pub last_event_at: DateTime<Utc>,
    pub requirements: TaskRequirements,
    pub payload: TaskPayload,
    pub active_assignment: Option<TaskLease>,
    pub active_assignment_age_seconds: Option<u64>,
    pub last_assignment_reason: Option<String>,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentAssignmentStats {
    pub assignment_count: u64,
    pub last_assigned_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub work_item: TaskWorkItem,
    pub task_offset: u64,
    pub status: TaskStatus,
    pub attempts: u32,
    pub last_event_at: DateTime<Utc>,
    pub active_assignment: Option<TaskLease>,
    #[serde(default)]
    pub last_assignment_reason: Option<String>,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskLease {
    pub assignment_id: Uuid,
    pub agent_id: String,
    pub assigned_at: DateTime<Utc>,
    pub timeout_at: DateTime<Utc>,
    pub attempt: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskEventApplyOutcome {
    Applied,
    SkippedLocalEcho,
    SkippedMissingTask,
    SkippedMalformed,
    SkippedStaleAssignment,
}

impl Default for OrchestratorState {
    fn default() -> Self {
        Self {
            observed_at: Utc::now(),
            source_cursor: 0,
            task_offset: 0,
            task_event_offset: 0,
            agents: BTreeMap::new(),
            tasks: BTreeMap::new(),
            assignments: BTreeMap::new(),
            pending_task_event_acks: BTreeSet::new(),
        }
    }
}

pub fn orchestrator_metrics(
    state: &OrchestratorState,
    now: DateTime<Utc>,
) -> OrchestratorMetricsView {
    let mut tasks = TaskLifecycleMetrics::default();

    for task in state.tasks.values() {
        match task.status {
            TaskStatus::Pending => tasks.pending += 1,
            TaskStatus::Assigned => tasks.assigned += 1,
            TaskStatus::Completed => tasks.completed += 1,
            TaskStatus::Failed => tasks.failed += 1,
            TaskStatus::RetryScheduled => tasks.retry_scheduled += 1,
            TaskStatus::TimedOut => tasks.timed_out += 1,
            TaskStatus::Exhausted => tasks.exhausted += 1,
            TaskStatus::Canceled => tasks.canceled += 1,
        }

        tasks.retry_count = tasks
            .retry_count
            .saturating_add(u64::from(task.attempts.saturating_sub(1)));

        if let Some(assignment) = &task.active_assignment {
            let age_seconds = now
                .signed_duration_since(assignment.assigned_at)
                .num_seconds()
                .max(0) as u64;
            tasks.oldest_in_flight_age_seconds = Some(
                tasks
                    .oldest_in_flight_age_seconds
                    .map_or(age_seconds, |current| current.max(age_seconds)),
            );
        }
    }

    OrchestratorMetricsView {
        observed_at: state.observed_at,
        source_cursor: state.source_cursor,
        task_offset: state.task_offset,
        task_event_offset: state.task_event_offset,
        agent_count: state.agents.len(),
        task_count: state.tasks.len(),
        pending_task_event_acks: state.pending_task_event_acks.len(),
        tasks,
    }
}

pub fn list_tasks(state: &OrchestratorState, query: &TaskListQuery) -> Vec<TaskSummaryView> {
    let mut tasks = state
        .tasks
        .values()
        .filter(|task| task_matches_query(task, query))
        .map(task_summary_view)
        .collect::<Vec<_>>();
    tasks.sort_by(|left, right| compare_task_summaries(left, right, &query.sort_by));

    if let Some(limit) = query.limit {
        tasks.truncate(limit.max(1));
    }

    tasks
}

pub fn show_task(
    state: &OrchestratorState,
    task_id: &str,
    now: DateTime<Utc>,
) -> Option<TaskDetailView> {
    state
        .tasks
        .get(task_id)
        .map(|task| task_detail_view(task, now))
}

pub fn apply_snapshot(state: &mut OrchestratorState, agents: Vec<AgentCard>, cursor: u64) {
    state.source_cursor = cursor;
    state.observed_at = Utc::now();
    state.agents = agents
        .into_iter()
        .map(|agent| (agent.agent_id.clone(), agent))
        .collect();
    state
        .assignments
        .retain(|agent_id, _| state.agents.contains_key(agent_id));
}

pub fn apply_event(state: &mut OrchestratorState, event: RegistryEvent) {
    state.source_cursor = state.source_cursor.max(event.sequence);
    state.observed_at = event.timestamp;
    match event.kind {
        RegistryEventKind::Registered | RegistryEventKind::Heartbeated => {
            state.agents.insert(event.card.agent_id.clone(), event.card);
        }
        RegistryEventKind::Removed | RegistryEventKind::CleanedUp => {
            state.agents.remove(&event.card.agent_id);
        }
    }
    state
        .assignments
        .retain(|agent_id, _| state.agents.contains_key(agent_id));
}

pub fn ingest_task(
    state: &mut OrchestratorState,
    work_item: TaskWorkItem,
    task_offset: u64,
) -> bool {
    if state.tasks.contains_key(&work_item.task_id) {
        return false;
    }

    let task_id = work_item.task_id.clone();
    state.observed_at = state.observed_at.max(work_item.submitted_at);
    state.tasks.insert(
        task_id,
        TaskRecord {
            last_event_at: work_item.submitted_at,
            task_offset,
            work_item,
            status: TaskStatus::Pending,
            attempts: 0,
            active_assignment: None,
            last_assignment_reason: None,
            next_retry_at: None,
            last_error: None,
        },
    );
    true
}

pub fn select_agent<'a>(
    state: &'a OrchestratorState,
    requirements: &AssignmentRequirements,
    now: DateTime<Utc>,
) -> Option<&'a AgentCard> {
    select_agent_with_reason(state, requirements, 0, now).map(|selection| selection.agent)
}

pub fn select_agent_with_reason<'a>(
    state: &'a OrchestratorState,
    requirements: &AssignmentRequirements,
    task_priority: i32,
    now: DateTime<Utc>,
) -> Option<AgentSelection<'a>> {
    state
        .agents
        .values()
        .filter(|agent| agent.expires_at > now)
        .filter(|agent| matches_requirements(agent, requirements))
        .map(|agent| (agent, agent_scheduling_score(state, requirements, agent)))
        .min_by(|(left_agent, left_score), (right_agent, right_score)| {
            scheduling_score_key(left_score, left_agent)
                .cmp(&scheduling_score_key(right_score, right_agent))
        })
        .map(|(agent, score)| AgentSelection {
            agent,
            reason: format_assignment_reason(requirements, task_priority, agent, &score),
        })
}

pub fn query_from_requirements(
    requirements: &AssignmentRequirements,
    include_stale: bool,
) -> AgentQuery {
    AgentQuery {
        skill: requirements.skill.clone(),
        topic: requirements.topic.clone(),
        principal: requirements.principal.clone(),
        include_stale,
    }
}

pub fn ready_task_ids(state: &OrchestratorState, now: DateTime<Utc>) -> Vec<String> {
    let mut tasks = state
        .tasks
        .values()
        .filter(|task| task_ready_for_assignment(task, now))
        .map(|task| {
            (
                task.work_item.priority,
                task.task_offset,
                task.work_item.task_id.clone(),
            )
        })
        .collect::<Vec<_>>();
    tasks.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    tasks.into_iter().map(|(_, _, task_id)| task_id).collect()
}

pub fn timed_out_task_ids(state: &OrchestratorState, now: DateTime<Utc>) -> Vec<String> {
    let mut tasks = state
        .tasks
        .values()
        .filter(|task| {
            task.status == TaskStatus::Assigned
                && task
                    .active_assignment
                    .as_ref()
                    .is_some_and(|assignment| assignment.timeout_at <= now)
        })
        .map(|task| {
            (
                task.active_assignment
                    .as_ref()
                    .map(|assignment| assignment.timeout_at)
                    .unwrap_or(task.last_event_at),
                task.work_item.task_id.clone(),
            )
        })
        .collect::<Vec<_>>();
    tasks.sort();
    tasks.into_iter().map(|(_, task_id)| task_id).collect()
}

pub fn retry_decision_task_ids(state: &OrchestratorState) -> Vec<String> {
    let mut tasks = state
        .tasks
        .values()
        .filter(|task| matches!(task.status, TaskStatus::Failed | TaskStatus::TimedOut))
        .map(|task| (task.task_offset, task.work_item.task_id.clone()))
        .collect::<Vec<_>>();
    tasks.sort();
    tasks.into_iter().map(|(_, task_id)| task_id).collect()
}

pub fn task_can_retry(state: &OrchestratorState, task_id: &str) -> bool {
    state
        .tasks
        .get(task_id)
        .is_some_and(|task| task.attempts < max_attempts(task))
}

pub fn plan_assignment_event(
    state: &OrchestratorState,
    task_id: &str,
    agent: &AgentCard,
    decided_at: DateTime<Utc>,
) -> Option<TaskEvent> {
    plan_assignment_event_with_reason(state, task_id, agent, decided_at, None)
}

pub fn plan_assignment_event_with_reason(
    state: &OrchestratorState,
    task_id: &str,
    agent: &AgentCard,
    decided_at: DateTime<Utc>,
    reason: Option<String>,
) -> Option<TaskEvent> {
    let task = state.tasks.get(task_id)?;
    Some(TaskEvent {
        event_id: Uuid::now_v7(),
        task_id: task_id.to_owned(),
        task_offset: Some(task.task_offset),
        assignment_id: Some(Uuid::now_v7()),
        agent_id: Some(agent.agent_id.clone()),
        status: TaskStatus::Assigned,
        attempt: task.attempts.saturating_add(1),
        reason,
        emitted_at: decided_at,
    })
}

pub fn plan_timeout_event(
    state: &OrchestratorState,
    task_id: &str,
    emitted_at: DateTime<Utc>,
    reason: Option<String>,
) -> Option<TaskEvent> {
    let task = state.tasks.get(task_id)?;
    let assignment = task.active_assignment.as_ref()?;
    Some(TaskEvent {
        event_id: Uuid::now_v7(),
        task_id: task_id.to_owned(),
        task_offset: Some(task.task_offset),
        assignment_id: Some(assignment.assignment_id),
        agent_id: Some(assignment.agent_id.clone()),
        status: TaskStatus::TimedOut,
        attempt: task.attempts,
        reason,
        emitted_at,
    })
}

pub fn plan_retry_event(
    state: &OrchestratorState,
    task_id: &str,
    emitted_at: DateTime<Utc>,
    reason: Option<String>,
) -> Option<TaskEvent> {
    let task = state.tasks.get(task_id)?;
    Some(TaskEvent {
        event_id: Uuid::now_v7(),
        task_id: task_id.to_owned(),
        task_offset: Some(task.task_offset),
        assignment_id: None,
        agent_id: None,
        status: TaskStatus::RetryScheduled,
        attempt: task.attempts,
        reason,
        emitted_at,
    })
}

pub fn plan_requeue_event(
    state: &OrchestratorState,
    task_id: &str,
    emitted_at: DateTime<Utc>,
    reason: Option<String>,
) -> Option<TaskEvent> {
    let task = state.tasks.get(task_id)?;
    let active_assignment = task.active_assignment.as_ref();
    Some(TaskEvent {
        event_id: Uuid::now_v7(),
        task_id: task_id.to_owned(),
        task_offset: Some(task.task_offset),
        assignment_id: active_assignment.map(|assignment| assignment.assignment_id),
        agent_id: active_assignment.map(|assignment| assignment.agent_id.clone()),
        status: TaskStatus::Pending,
        attempt: task.attempts,
        reason,
        emitted_at,
    })
}

pub fn plan_exhausted_event(
    state: &OrchestratorState,
    task_id: &str,
    emitted_at: DateTime<Utc>,
    reason: Option<String>,
) -> Option<TaskEvent> {
    let task = state.tasks.get(task_id)?;
    Some(TaskEvent {
        event_id: Uuid::now_v7(),
        task_id: task_id.to_owned(),
        task_offset: Some(task.task_offset),
        assignment_id: None,
        agent_id: None,
        status: TaskStatus::Exhausted,
        attempt: task.attempts,
        reason,
        emitted_at,
    })
}

pub fn plan_cancel_event(
    state: &OrchestratorState,
    task_id: &str,
    emitted_at: DateTime<Utc>,
    reason: Option<String>,
) -> Option<TaskEvent> {
    let task = state.tasks.get(task_id)?;
    let active_assignment = task.active_assignment.as_ref();
    Some(TaskEvent {
        event_id: Uuid::now_v7(),
        task_id: task_id.to_owned(),
        task_offset: Some(task.task_offset),
        assignment_id: active_assignment.map(|assignment| assignment.assignment_id),
        agent_id: active_assignment.map(|assignment| assignment.agent_id.clone()),
        status: TaskStatus::Canceled,
        attempt: task.attempts,
        reason,
        emitted_at,
    })
}

pub fn record_local_task_event(
    state: &mut OrchestratorState,
    event: TaskEvent,
) -> TaskEventApplyOutcome {
    state.pending_task_event_acks.insert(event.event_id);
    let outcome = apply_task_event_inner(state, &event);
    if outcome != TaskEventApplyOutcome::Applied {
        state.pending_task_event_acks.remove(&event.event_id);
    }
    outcome
}

pub fn apply_task_event_message(
    state: &mut OrchestratorState,
    event: TaskEvent,
) -> TaskEventApplyOutcome {
    if state.pending_task_event_acks.remove(&event.event_id) {
        return TaskEventApplyOutcome::SkippedLocalEcho;
    }

    apply_task_event_inner(state, &event)
}

pub fn load_state(path: &Path) -> anyhow::Result<OrchestratorState> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read orchestrator state {}", path.display()))?;
    serde_json::from_str(&raw).context("failed to parse orchestrator state")
}

pub fn save_state(path: &Path, state: &OrchestratorState) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let rendered = serde_json::to_vec_pretty(state)?;
    std::fs::write(path, rendered)
        .with_context(|| format!("failed to write orchestrator state {}", path.display()))?;
    Ok(())
}

fn apply_task_event_inner(
    state: &mut OrchestratorState,
    event: &TaskEvent,
) -> TaskEventApplyOutcome {
    state.observed_at = state.observed_at.max(event.emitted_at);

    let mut assigned_agent = None;
    let outcome = {
        let Some(task) = state.tasks.get_mut(&event.task_id) else {
            return TaskEventApplyOutcome::SkippedMissingTask;
        };

        task.last_event_at = event.emitted_at;
        task.attempts = task.attempts.max(event.attempt);
        if let Some(reason) = &event.reason {
            task.last_error = Some(reason.clone());
        }

        match event.status {
            TaskStatus::Pending => {
                if (event.assignment_id.is_some() || event.agent_id.is_some())
                    && !matches_current_assignment(task, event)
                {
                    return TaskEventApplyOutcome::SkippedStaleAssignment;
                }

                task.status = TaskStatus::Pending;
                task.active_assignment = None;
                task.next_retry_at = None;
                TaskEventApplyOutcome::Applied
            }
            TaskStatus::Assigned => {
                let (Some(assignment_id), Some(agent_id)) = (event.assignment_id, &event.agent_id)
                else {
                    return TaskEventApplyOutcome::SkippedMalformed;
                };

                task.status = TaskStatus::Assigned;
                task.active_assignment = Some(TaskLease {
                    assignment_id,
                    agent_id: agent_id.clone(),
                    assigned_at: event.emitted_at,
                    timeout_at: event.emitted_at + timeout_duration(task),
                    attempt: event.attempt,
                });
                task.last_assignment_reason = event.reason.clone();
                task.next_retry_at = None;
                task.last_error = None;
                assigned_agent = Some(agent_id.clone());
                TaskEventApplyOutcome::Applied
            }
            TaskStatus::Completed => {
                if !matches_current_assignment(task, event) {
                    return TaskEventApplyOutcome::SkippedStaleAssignment;
                }

                task.status = TaskStatus::Completed;
                task.active_assignment = None;
                task.next_retry_at = None;
                task.last_error = None;
                TaskEventApplyOutcome::Applied
            }
            TaskStatus::Failed => {
                if !matches_current_assignment(task, event) {
                    return TaskEventApplyOutcome::SkippedStaleAssignment;
                }

                task.status = TaskStatus::Failed;
                task.active_assignment = None;
                task.next_retry_at = None;
                TaskEventApplyOutcome::Applied
            }
            TaskStatus::RetryScheduled => {
                task.status = TaskStatus::RetryScheduled;
                task.active_assignment = None;
                task.next_retry_at = Some(event.emitted_at + retry_delay_duration(task));
                TaskEventApplyOutcome::Applied
            }
            TaskStatus::TimedOut => {
                if !matches_current_assignment(task, event) {
                    return TaskEventApplyOutcome::SkippedStaleAssignment;
                }

                task.status = TaskStatus::TimedOut;
                task.active_assignment = None;
                task.next_retry_at = None;
                TaskEventApplyOutcome::Applied
            }
            TaskStatus::Exhausted => {
                task.status = TaskStatus::Exhausted;
                task.active_assignment = None;
                task.next_retry_at = None;
                TaskEventApplyOutcome::Applied
            }
            TaskStatus::Canceled => {
                if (event.assignment_id.is_some() || event.agent_id.is_some())
                    && !matches_current_assignment(task, event)
                {
                    return TaskEventApplyOutcome::SkippedStaleAssignment;
                }

                task.status = TaskStatus::Canceled;
                task.active_assignment = None;
                task.next_retry_at = None;
                TaskEventApplyOutcome::Applied
            }
        }
    };

    if let Some(agent_id) = assigned_agent {
        note_agent_assignment(state, &agent_id, event.emitted_at);
    }

    outcome
}

fn note_agent_assignment(
    state: &mut OrchestratorState,
    agent_id: &str,
    assigned_at: DateTime<Utc>,
) {
    let stats = state.assignments.entry(agent_id.to_owned()).or_default();
    stats.assignment_count = stats.assignment_count.saturating_add(1);
    stats.last_assigned_at = Some(assigned_at);
}

fn task_ready_for_assignment(task: &TaskRecord, now: DateTime<Utc>) -> bool {
    if task.attempts >= max_attempts(task) {
        return false;
    }

    match task.status {
        TaskStatus::Pending => true,
        TaskStatus::RetryScheduled => task.next_retry_at.is_none_or(|retry_at| retry_at <= now),
        _ => false,
    }
}

fn max_attempts(task: &TaskRecord) -> u32 {
    task.work_item.retry_policy.max_attempts.max(1)
}

fn timeout_duration(task: &TaskRecord) -> Duration {
    duration_from_seconds(task.work_item.retry_policy.timeout_seconds)
}

fn retry_delay_duration(task: &TaskRecord) -> Duration {
    duration_from_seconds(task.work_item.retry_policy.retry_delay_seconds)
}

fn duration_from_seconds(seconds: u64) -> Duration {
    Duration::seconds(seconds.min(i64::MAX as u64) as i64)
}

fn matches_current_assignment(task: &TaskRecord, event: &TaskEvent) -> bool {
    let Some(active_assignment) = &task.active_assignment else {
        return false;
    };

    event.assignment_id == Some(active_assignment.assignment_id)
        && event
            .agent_id
            .as_deref()
            .is_some_and(|agent_id| agent_id == active_assignment.agent_id)
}

fn matches_requirements(agent: &AgentCard, requirements: &AssignmentRequirements) -> bool {
    matches_optional(&requirements.principal, &agent.principal)
        && matches_skill(requirements.skill.as_deref(), &agent.skills)
        && matches_topic(
            requirements.topic.as_deref(),
            &agent.subscriptions,
            &agent.publications,
        )
}

fn active_assignment_count(state: &OrchestratorState, agent_id: &str) -> usize {
    state
        .tasks
        .values()
        .filter(|task| {
            task.status == TaskStatus::Assigned
                && task
                    .active_assignment
                    .as_ref()
                    .is_some_and(|assignment| assignment.agent_id == agent_id)
        })
        .count()
}

fn agent_scheduling_score(
    state: &OrchestratorState,
    requirements: &AssignmentRequirements,
    agent: &AgentCard,
) -> AgentSchedulingScore {
    let stats = state
        .assignments
        .get(&agent.agent_id)
        .cloned()
        .unwrap_or_default();
    AgentSchedulingScore {
        avoid_penalty: affinity_avoid_penalty(requirements, &agent.agent_id),
        active_load: active_assignment_count(state, &agent.agent_id),
        preference_penalty: affinity_preference_penalty(requirements, &agent.agent_id),
        historical_assignments: stats.assignment_count,
        last_assigned_at: stats.last_assigned_at,
    }
}

fn scheduling_score_key<'a>(
    score: &AgentSchedulingScore,
    agent: &'a AgentCard,
) -> (u8, usize, u8, u64, Option<DateTime<Utc>>, &'a str) {
    (
        score.avoid_penalty,
        score.active_load,
        score.preference_penalty,
        score.historical_assignments,
        score.last_assigned_at,
        agent.agent_id.as_str(),
    )
}

fn format_assignment_reason(
    requirements: &AssignmentRequirements,
    task_priority: i32,
    agent: &AgentCard,
    score: &AgentSchedulingScore,
) -> String {
    let preferred_match = requirements
        .preferred_agents
        .iter()
        .any(|preferred| preferred == &agent.agent_id);
    let avoided_match = requirements
        .avoid_agents
        .iter()
        .any(|avoided| avoided == &agent.agent_id);
    let last_assigned_at = score
        .last_assigned_at
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_else(|| "never".to_owned());

    format!(
        "scheduler selected agent `{}` with priority={}, preferred_match={}, avoid_match={}, active_load={}, historical_assignments={}, last_assigned_at={}",
        agent.agent_id,
        task_priority,
        preferred_match,
        avoided_match,
        score.active_load,
        score.historical_assignments,
        last_assigned_at
    )
}

fn affinity_preference_penalty(requirements: &AssignmentRequirements, agent_id: &str) -> u8 {
    if requirements.preferred_agents.is_empty() {
        return 0;
    }

    if requirements
        .preferred_agents
        .iter()
        .any(|preferred| preferred == agent_id)
    {
        0
    } else {
        1
    }
}

fn affinity_avoid_penalty(requirements: &AssignmentRequirements, agent_id: &str) -> u8 {
    requirements
        .avoid_agents
        .iter()
        .any(|avoided| avoided == agent_id) as u8
}

fn task_matches_query(task: &TaskRecord, query: &TaskListQuery) -> bool {
    if query.status.is_some_and(|status| task.status != status) {
        return false;
    }

    if query.skill.as_ref().is_some_and(|skill| {
        task.work_item
            .requirements
            .skill
            .as_ref()
            .is_none_or(|task_skill| !task_skill.eq_ignore_ascii_case(skill))
    }) {
        return false;
    }

    if query.agent_id.as_ref().is_some_and(|agent_id| {
        task.active_assignment
            .as_ref()
            .is_none_or(|assignment| assignment.agent_id != *agent_id)
    }) {
        return false;
    }

    true
}

fn task_summary_view(task: &TaskRecord) -> TaskSummaryView {
    TaskSummaryView {
        task_id: task.work_item.task_id.clone(),
        task_type: task.work_item.task_type.clone(),
        task_offset: task.task_offset,
        priority: task.work_item.priority,
        status: task.status,
        attempts: task.attempts,
        max_attempts: max_attempts(task),
        submitted_at: task.work_item.submitted_at,
        last_event_at: task.last_event_at,
        skill: task.work_item.requirements.skill.clone(),
        active_agent_id: task
            .active_assignment
            .as_ref()
            .map(|assignment| assignment.agent_id.clone()),
        assignment_id: task
            .active_assignment
            .as_ref()
            .map(|assignment| assignment.assignment_id),
        last_assignment_reason: task.last_assignment_reason.clone(),
        next_retry_at: task.next_retry_at,
        last_error: task.last_error.clone(),
    }
}

fn compare_task_summaries(
    left: &TaskSummaryView,
    right: &TaskSummaryView,
    sort_by: &TaskListSort,
) -> std::cmp::Ordering {
    match sort_by {
        TaskListSort::Offset => left
            .task_offset
            .cmp(&right.task_offset)
            .then_with(|| left.task_id.cmp(&right.task_id)),
        TaskListSort::Priority => right
            .priority
            .cmp(&left.priority)
            .then_with(|| left.task_offset.cmp(&right.task_offset))
            .then_with(|| left.task_id.cmp(&right.task_id)),
        TaskListSort::Age => left
            .submitted_at
            .cmp(&right.submitted_at)
            .then_with(|| left.task_offset.cmp(&right.task_offset))
            .then_with(|| left.task_id.cmp(&right.task_id)),
        TaskListSort::Retries => right
            .attempts
            .saturating_sub(1)
            .cmp(&left.attempts.saturating_sub(1))
            .then_with(|| right.priority.cmp(&left.priority))
            .then_with(|| left.task_offset.cmp(&right.task_offset))
            .then_with(|| left.task_id.cmp(&right.task_id)),
    }
}

fn task_detail_view(task: &TaskRecord, now: DateTime<Utc>) -> TaskDetailView {
    TaskDetailView {
        task_id: task.work_item.task_id.clone(),
        task_type: task.work_item.task_type.clone(),
        task_offset: task.task_offset,
        priority: task.work_item.priority,
        status: task.status,
        attempts: task.attempts,
        max_attempts: max_attempts(task),
        retry_count: task.attempts.saturating_sub(1),
        submitted_at: task.work_item.submitted_at,
        last_event_at: task.last_event_at,
        requirements: task.work_item.requirements.clone(),
        payload: task.work_item.payload.clone(),
        active_assignment: task.active_assignment.clone(),
        active_assignment_age_seconds: task.active_assignment.as_ref().map(|assignment| {
            now.signed_duration_since(assignment.assigned_at)
                .num_seconds()
                .max(0) as u64
        }),
        last_assignment_reason: task.last_assignment_reason.clone(),
        next_retry_at: task.next_retry_at,
        last_error: task.last_error.clone(),
    }
}

fn matches_optional(expected: &Option<String>, actual: &str) -> bool {
    match expected {
        Some(expected) => expected == actual,
        None => true,
    }
}

fn matches_skill(expected: Option<&str>, skills: &[String]) -> bool {
    match expected {
        Some(expected) => skills
            .iter()
            .any(|skill| skill.eq_ignore_ascii_case(expected)),
        None => true,
    }
}

fn matches_topic(
    expected: Option<&str>,
    subscriptions: &[String],
    publications: &[String],
) -> bool {
    match expected {
        Some(expected) => {
            subscriptions.iter().any(|topic| topic == expected)
                || publications.iter().any(|topic| topic == expected)
        }
        None => true,
    }
}

fn default_observed_at() -> DateTime<Utc> {
    Utc::now()
}

#[cfg(test)]
mod tests {
    use expressways_protocol::{
        AgentEndpoint, Classification, RetentionClass, TaskEvent, TaskRetryPolicy,
    };
    use serde_json::json;

    use super::*;

    fn card(agent_id: &str, skill: &str) -> AgentCard {
        let now = Utc::now();
        AgentCard {
            agent_id: agent_id.to_owned(),
            principal: "local:agent-orchestrator".to_owned(),
            display_name: agent_id.to_owned(),
            version: "1.0.0".to_owned(),
            summary: "test".to_owned(),
            skills: vec![skill.to_owned()],
            subscriptions: vec!["topic:tasks".to_owned()],
            publications: vec!["topic:results".to_owned()],
            schemas: Vec::new(),
            endpoint: AgentEndpoint {
                transport: "control_tcp".to_owned(),
                address: format!("127.0.0.1:{}", 8800 + agent_id.len()),
            },
            classification: Classification::Internal,
            retention_class: RetentionClass::Operational,
            ttl_seconds: 300,
            updated_at: now,
            last_seen_at: now,
            expires_at: now + Duration::seconds(300),
        }
    }

    fn task(task_id: &str, skill: &str, max_attempts: u32) -> TaskWorkItem {
        task_with_hints(task_id, skill, max_attempts, 0, Vec::new(), Vec::new())
    }

    fn task_with_hints(
        task_id: &str,
        skill: &str,
        max_attempts: u32,
        priority: i32,
        preferred_agents: Vec<&str>,
        avoid_agents: Vec<&str>,
    ) -> TaskWorkItem {
        TaskWorkItem {
            task_id: task_id.to_owned(),
            task_type: "summarize_document".to_owned(),
            priority,
            requirements: AssignmentRequirements {
                skill: Some(skill.to_owned()),
                topic: None,
                principal: None,
                preferred_agents: preferred_agents.into_iter().map(str::to_owned).collect(),
                avoid_agents: avoid_agents.into_iter().map(str::to_owned).collect(),
            },
            payload: TaskPayload::json(json!({ "path": "notes.md" })),
            retry_policy: TaskRetryPolicy {
                max_attempts,
                timeout_seconds: 60,
                retry_delay_seconds: 30,
            },
            submitted_at: Utc::now(),
        }
    }

    #[test]
    fn selection_prefers_never_assigned_then_least_recently_assigned() {
        let mut state = OrchestratorState::default();
        apply_snapshot(
            &mut state,
            vec![card("alpha", "summarize"), card("beta", "summarize")],
            4,
        );
        assert!(ingest_task(&mut state, task("task-1", "summarize", 3), 0));

        let requirements = AssignmentRequirements {
            skill: Some("summarize".to_owned()),
            topic: None,
            principal: None,
            preferred_agents: Vec::new(),
            avoid_agents: Vec::new(),
        };

        let first = select_agent(&state, &requirements, Utc::now())
            .expect("first agent")
            .clone();
        assert_eq!(first.agent_id, "alpha");

        let first_event =
            plan_assignment_event(&state, "task-1", &first, Utc::now()).expect("assignment");
        assert_eq!(
            record_local_task_event(&mut state, first_event.clone()),
            TaskEventApplyOutcome::Applied
        );
        assert_eq!(
            apply_task_event_message(&mut state, first_event),
            TaskEventApplyOutcome::SkippedLocalEcho
        );

        let second = select_agent(&state, &requirements, Utc::now()).expect("second agent");
        assert_eq!(second.agent_id, "beta");
    }

    #[test]
    fn selection_prefers_lower_active_load_before_history() {
        let mut state = OrchestratorState::default();
        let alpha = card("alpha", "summarize");
        let beta = card("beta", "summarize");
        apply_snapshot(&mut state, vec![alpha.clone(), beta.clone()], 4);
        assert!(ingest_task(
            &mut state,
            task("task-busy", "summarize", 3),
            0
        ));

        let busy_assignment =
            plan_assignment_event(&state, "task-busy", &alpha, Utc::now()).expect("assignment");
        assert_eq!(
            record_local_task_event(&mut state, busy_assignment.clone()),
            TaskEventApplyOutcome::Applied
        );
        assert_eq!(
            apply_task_event_message(&mut state, busy_assignment),
            TaskEventApplyOutcome::SkippedLocalEcho
        );

        state.assignments.insert(
            "beta".to_owned(),
            AgentAssignmentStats {
                assignment_count: 50,
                last_assigned_at: Some(Utc::now()),
            },
        );

        let requirements = AssignmentRequirements {
            skill: Some("summarize".to_owned()),
            topic: None,
            principal: None,
            preferred_agents: Vec::new(),
            avoid_agents: Vec::new(),
        };

        let selected = select_agent(&state, &requirements, Utc::now()).expect("selected agent");
        assert_eq!(selected.agent_id, "beta");
    }

    #[test]
    fn selection_applies_affinity_preferences_and_avoidance() {
        let mut state = OrchestratorState::default();
        apply_snapshot(
            &mut state,
            vec![
                card("alpha", "summarize"),
                card("beta", "summarize"),
                card("gamma", "summarize"),
            ],
            4,
        );

        let preferred = AssignmentRequirements {
            skill: Some("summarize".to_owned()),
            topic: None,
            principal: None,
            preferred_agents: vec!["gamma".to_owned()],
            avoid_agents: vec!["alpha".to_owned()],
        };
        let selected = select_agent(&state, &preferred, Utc::now()).expect("preferred agent");
        assert_eq!(selected.agent_id, "gamma");

        let avoid_only = AssignmentRequirements {
            skill: Some("summarize".to_owned()),
            topic: None,
            principal: None,
            preferred_agents: Vec::new(),
            avoid_agents: vec!["alpha".to_owned()],
        };
        let selected = select_agent(&state, &avoid_only, Utc::now()).expect("non-avoided agent");
        assert_ne!(selected.agent_id, "alpha");
    }

    #[test]
    fn selection_reason_mentions_priority_load_and_affinity() {
        let mut state = OrchestratorState::default();
        let alpha = card("alpha", "summarize");
        let beta = card("beta", "summarize");
        apply_snapshot(&mut state, vec![alpha.clone(), beta.clone()], 4);
        assert!(ingest_task(
            &mut state,
            task_with_hints("task-1", "summarize", 3, 50, vec!["beta"], vec!["alpha"]),
            0
        ));

        let selection = select_agent_with_reason(
            &state,
            &state
                .tasks
                .get("task-1")
                .expect("task")
                .work_item
                .requirements,
            50,
            Utc::now(),
        )
        .expect("selection");

        assert_eq!(selection.agent.agent_id, "beta");
        assert!(selection.reason.contains("priority=50"));
        assert!(selection.reason.contains("preferred_match=true"));
        assert!(selection.reason.contains("avoid_match=false"));
        assert!(selection.reason.contains("active_load=0"));
    }

    #[test]
    fn ready_tasks_prioritize_higher_priority_before_offset() {
        let mut state = OrchestratorState::default();
        assert!(ingest_task(
            &mut state,
            task_with_hints("task-low", "summarize", 3, -5, Vec::new(), Vec::new()),
            0
        ));
        assert!(ingest_task(
            &mut state,
            task_with_hints("task-high", "summarize", 3, 50, Vec::new(), Vec::new()),
            1
        ));
        assert!(ingest_task(
            &mut state,
            task_with_hints("task-mid", "summarize", 3, 10, Vec::new(), Vec::new()),
            2
        ));

        assert_eq!(
            ready_task_ids(&state, Utc::now()),
            vec![
                "task-high".to_owned(),
                "task-mid".to_owned(),
                "task-low".to_owned()
            ]
        );
    }

    #[test]
    fn removed_events_prune_agents_and_assignment_stats() {
        let mut state = OrchestratorState::default();
        let alpha = card("alpha", "summarize");
        apply_snapshot(&mut state, vec![alpha.clone()], 2);
        assert!(ingest_task(&mut state, task("task-1", "summarize", 3), 0));

        let decision =
            plan_assignment_event(&state, "task-1", &alpha, Utc::now()).expect("assignment");
        assert_eq!(
            record_local_task_event(&mut state, decision),
            TaskEventApplyOutcome::Applied
        );
        assert!(state.assignments.contains_key("alpha"));

        apply_event(
            &mut state,
            RegistryEvent {
                sequence: 3,
                timestamp: Utc::now(),
                kind: RegistryEventKind::Removed,
                card: alpha,
            },
        );

        assert!(!state.agents.contains_key("alpha"));
        assert!(!state.assignments.contains_key("alpha"));
        assert_eq!(state.source_cursor, 3);
    }

    #[test]
    fn stale_completion_is_ignored_after_reassignment() {
        let mut state = OrchestratorState::default();
        let alpha = card("alpha", "summarize");
        let beta = card("beta", "summarize");
        apply_snapshot(&mut state, vec![alpha.clone(), beta.clone()], 7);
        assert!(ingest_task(&mut state, task("task-1", "summarize", 3), 0));

        let assigned_at = Utc::now();
        let first_assignment =
            plan_assignment_event(&state, "task-1", &alpha, assigned_at).expect("assignment");
        let first_assignment_id = first_assignment.assignment_id.expect("assignment id");
        assert_eq!(
            record_local_task_event(&mut state, first_assignment),
            TaskEventApplyOutcome::Applied
        );

        let timeout_event = plan_timeout_event(
            &state,
            "task-1",
            assigned_at + Duration::seconds(61),
            Some("assignment lease expired".to_owned()),
        )
        .expect("timeout event");
        assert_eq!(
            record_local_task_event(&mut state, timeout_event),
            TaskEventApplyOutcome::Applied
        );

        let retry_event = plan_retry_event(
            &state,
            "task-1",
            assigned_at + Duration::seconds(61),
            Some("retrying task after timeout".to_owned()),
        )
        .expect("retry event");
        assert_eq!(
            record_local_task_event(&mut state, retry_event),
            TaskEventApplyOutcome::Applied
        );

        let second_assignment =
            plan_assignment_event(&state, "task-1", &beta, assigned_at + Duration::seconds(62))
                .expect("second assignment");
        assert_eq!(
            record_local_task_event(&mut state, second_assignment),
            TaskEventApplyOutcome::Applied
        );

        let stale_completion = TaskEvent {
            event_id: Uuid::now_v7(),
            task_id: "task-1".to_owned(),
            task_offset: Some(0),
            assignment_id: Some(first_assignment_id),
            agent_id: Some("alpha".to_owned()),
            status: TaskStatus::Completed,
            attempt: 1,
            reason: None,
            emitted_at: assigned_at + Duration::seconds(63),
        };
        assert_eq!(
            apply_task_event_message(&mut state, stale_completion),
            TaskEventApplyOutcome::SkippedStaleAssignment
        );

        let tracked = state.tasks.get("task-1").expect("tracked task");
        assert_eq!(tracked.status, TaskStatus::Assigned);
        assert_eq!(
            tracked
                .active_assignment
                .as_ref()
                .expect("active assignment")
                .agent_id,
            "beta"
        );
    }

    #[test]
    fn failures_retry_until_attempt_budget_is_exhausted() {
        let mut state = OrchestratorState::default();
        let alpha = card("alpha", "summarize");
        let beta = card("beta", "summarize");
        apply_snapshot(&mut state, vec![alpha.clone(), beta.clone()], 8);
        assert!(ingest_task(&mut state, task("task-1", "summarize", 2), 0));

        let assigned_at = Utc::now();
        let first_assignment =
            plan_assignment_event(&state, "task-1", &alpha, assigned_at).expect("assignment");
        let first_assignment_id = first_assignment.assignment_id.expect("assignment id");
        assert_eq!(
            record_local_task_event(&mut state, first_assignment),
            TaskEventApplyOutcome::Applied
        );

        let failed = TaskEvent {
            event_id: Uuid::now_v7(),
            task_id: "task-1".to_owned(),
            task_offset: Some(0),
            assignment_id: Some(first_assignment_id),
            agent_id: Some("alpha".to_owned()),
            status: TaskStatus::Failed,
            attempt: 1,
            reason: Some("agent reported failure".to_owned()),
            emitted_at: assigned_at + Duration::seconds(5),
        };
        assert_eq!(
            apply_task_event_message(&mut state, failed),
            TaskEventApplyOutcome::Applied
        );
        assert_eq!(retry_decision_task_ids(&state), vec!["task-1".to_owned()]);

        let retry_event = plan_retry_event(
            &state,
            "task-1",
            assigned_at + Duration::seconds(5),
            Some("retry scheduled after failure".to_owned()),
        )
        .expect("retry event");
        assert_eq!(
            record_local_task_event(&mut state, retry_event),
            TaskEventApplyOutcome::Applied
        );
        assert!(ready_task_ids(&state, assigned_at + Duration::seconds(34)).is_empty());
        assert_eq!(
            ready_task_ids(&state, assigned_at + Duration::seconds(35)),
            vec!["task-1".to_owned()]
        );

        let second_assignment =
            plan_assignment_event(&state, "task-1", &beta, assigned_at + Duration::seconds(35))
                .expect("second assignment");
        assert_eq!(
            record_local_task_event(&mut state, second_assignment),
            TaskEventApplyOutcome::Applied
        );

        let timeout_event = plan_timeout_event(
            &state,
            "task-1",
            assigned_at + Duration::seconds(96),
            Some("assignment timed out".to_owned()),
        )
        .expect("timeout event");
        assert_eq!(
            record_local_task_event(&mut state, timeout_event),
            TaskEventApplyOutcome::Applied
        );
        assert_eq!(retry_decision_task_ids(&state), vec!["task-1".to_owned()]);
        assert!(!task_can_retry(&state, "task-1"));

        let exhausted_event = plan_exhausted_event(
            &state,
            "task-1",
            assigned_at + Duration::seconds(96),
            Some("retry budget exhausted".to_owned()),
        )
        .expect("exhausted event");
        assert_eq!(
            record_local_task_event(&mut state, exhausted_event),
            TaskEventApplyOutcome::Applied
        );

        let tracked = state.tasks.get("task-1").expect("tracked task");
        assert_eq!(tracked.status, TaskStatus::Exhausted);
        assert!(ready_task_ids(&state, assigned_at + Duration::seconds(200)).is_empty());
    }

    #[test]
    fn operator_requeue_returns_assigned_task_to_pending_queue() {
        let mut state = OrchestratorState::default();
        let alpha = card("alpha", "summarize");
        apply_snapshot(&mut state, vec![alpha.clone()], 4);
        assert!(ingest_task(&mut state, task("task-1", "summarize", 3), 0));

        let assigned_at = Utc::now() - Duration::seconds(10);
        let assigned =
            plan_assignment_event(&state, "task-1", &alpha, assigned_at).expect("assignment");
        assert_eq!(
            record_local_task_event(&mut state, assigned),
            TaskEventApplyOutcome::Applied
        );

        let requeue_event = plan_requeue_event(
            &state,
            "task-1",
            assigned_at + Duration::seconds(5),
            Some("operator requested requeue".to_owned()),
        )
        .expect("requeue event");
        assert_eq!(
            record_local_task_event(&mut state, requeue_event),
            TaskEventApplyOutcome::Applied
        );

        let tracked = state.tasks.get("task-1").expect("tracked task");
        assert_eq!(tracked.status, TaskStatus::Pending);
        assert!(tracked.active_assignment.is_none());
        assert_eq!(tracked.attempts, 1);
        assert_eq!(
            ready_task_ids(&state, assigned_at + Duration::seconds(6)),
            vec!["task-1".to_owned()]
        );
    }

    #[test]
    fn operator_cancel_marks_task_terminal_and_stale_cancel_is_ignored() {
        let mut state = OrchestratorState::default();
        let alpha = card("alpha", "summarize");
        let beta = card("beta", "summarize");
        apply_snapshot(&mut state, vec![alpha.clone(), beta.clone()], 5);
        assert!(ingest_task(&mut state, task("task-1", "summarize", 3), 0));

        let assigned_at = Utc::now();
        let first_assignment =
            plan_assignment_event(&state, "task-1", &alpha, assigned_at).expect("assignment");
        let first_assignment_id = first_assignment.assignment_id.expect("assignment id");
        assert_eq!(
            record_local_task_event(&mut state, first_assignment),
            TaskEventApplyOutcome::Applied
        );

        let requeue_event = plan_requeue_event(
            &state,
            "task-1",
            assigned_at + Duration::seconds(1),
            Some("requeue first assignment".to_owned()),
        )
        .expect("requeue");
        assert_eq!(
            record_local_task_event(&mut state, requeue_event),
            TaskEventApplyOutcome::Applied
        );

        let second_assignment =
            plan_assignment_event(&state, "task-1", &beta, assigned_at + Duration::seconds(2))
                .expect("second assignment");
        assert_eq!(
            record_local_task_event(&mut state, second_assignment),
            TaskEventApplyOutcome::Applied
        );

        let stale_cancel = TaskEvent {
            event_id: Uuid::now_v7(),
            task_id: "task-1".to_owned(),
            task_offset: Some(0),
            assignment_id: Some(first_assignment_id),
            agent_id: Some("alpha".to_owned()),
            status: TaskStatus::Canceled,
            attempt: 1,
            reason: Some("stale cancel".to_owned()),
            emitted_at: assigned_at + Duration::seconds(3),
        };
        assert_eq!(
            apply_task_event_message(&mut state, stale_cancel),
            TaskEventApplyOutcome::SkippedStaleAssignment
        );

        let cancel_event = plan_cancel_event(
            &state,
            "task-1",
            assigned_at + Duration::seconds(4),
            Some("operator canceled task".to_owned()),
        )
        .expect("cancel event");
        assert_eq!(
            record_local_task_event(&mut state, cancel_event),
            TaskEventApplyOutcome::Applied
        );

        let tracked = state.tasks.get("task-1").expect("tracked task");
        assert_eq!(tracked.status, TaskStatus::Canceled);
        assert!(tracked.active_assignment.is_none());
        assert!(ready_task_ids(&state, assigned_at + Duration::seconds(5)).is_empty());
    }

    #[test]
    fn list_tasks_filters_and_sorts_by_offset() {
        let mut state = OrchestratorState::default();
        let alpha = card("alpha", "summarize");
        apply_snapshot(&mut state, vec![alpha.clone()], 5);

        assert!(ingest_task(&mut state, task("task-2", "summarize", 3), 2));
        assert!(ingest_task(&mut state, task("task-1", "classify", 3), 1));
        assert!(ingest_task(&mut state, task("task-3", "summarize", 3), 3));

        let assigned_at = Utc::now() - Duration::seconds(30);
        let assigned = plan_assignment_event_with_reason(
            &state,
            "task-2",
            &alpha,
            assigned_at,
            Some("scheduler selected agent `alpha` with priority=0".to_owned()),
        )
        .expect("assigned event");
        assert_eq!(
            record_local_task_event(&mut state, assigned),
            TaskEventApplyOutcome::Applied
        );

        let retry_event = TaskEvent {
            event_id: Uuid::now_v7(),
            task_id: "task-3".to_owned(),
            task_offset: Some(3),
            assignment_id: None,
            agent_id: None,
            status: TaskStatus::RetryScheduled,
            attempt: 2,
            reason: Some("retry later".to_owned()),
            emitted_at: Utc::now(),
        };
        assert_eq!(
            record_local_task_event(&mut state, retry_event),
            TaskEventApplyOutcome::Applied
        );

        let all = list_tasks(&state, &TaskListQuery::default());
        assert_eq!(
            all.iter()
                .map(|task| task.task_id.as_str())
                .collect::<Vec<_>>(),
            vec!["task-1", "task-2", "task-3"]
        );

        let assigned_only = list_tasks(
            &state,
            &TaskListQuery {
                status: Some(TaskStatus::Assigned),
                skill: None,
                agent_id: Some("alpha".to_owned()),
                sort_by: TaskListSort::Offset,
                limit: None,
            },
        );
        assert_eq!(assigned_only.len(), 1);
        assert_eq!(assigned_only[0].task_id, "task-2");
        assert_eq!(assigned_only[0].active_agent_id.as_deref(), Some("alpha"));
        assert_eq!(
            assigned_only[0].last_assignment_reason.as_deref(),
            Some("scheduler selected agent `alpha` with priority=0")
        );

        let summarize_limited = list_tasks(
            &state,
            &TaskListQuery {
                status: None,
                skill: Some("summarize".to_owned()),
                agent_id: None,
                sort_by: TaskListSort::Offset,
                limit: Some(1),
            },
        );
        assert_eq!(summarize_limited.len(), 1);
        assert_eq!(summarize_limited[0].task_id, "task-2");
    }

    #[test]
    fn list_tasks_can_sort_by_priority_age_and_retries() {
        let mut state = OrchestratorState::default();
        assert!(ingest_task(
            &mut state,
            task_with_hints("task-low", "summarize", 3, -5, Vec::new(), Vec::new()),
            0
        ));
        assert!(ingest_task(
            &mut state,
            task_with_hints("task-high", "summarize", 3, 50, Vec::new(), Vec::new()),
            1
        ));
        assert!(ingest_task(
            &mut state,
            task_with_hints("task-mid", "summarize", 3, 10, Vec::new(), Vec::new()),
            2
        ));

        state
            .tasks
            .get_mut("task-low")
            .expect("task-low")
            .work_item
            .submitted_at = Utc::now() - Duration::seconds(300);
        state
            .tasks
            .get_mut("task-high")
            .expect("task-high")
            .work_item
            .submitted_at = Utc::now() - Duration::seconds(100);
        state
            .tasks
            .get_mut("task-mid")
            .expect("task-mid")
            .work_item
            .submitted_at = Utc::now() - Duration::seconds(200);

        state.tasks.get_mut("task-low").expect("task-low").attempts = 3;
        state
            .tasks
            .get_mut("task-high")
            .expect("task-high")
            .attempts = 1;
        state.tasks.get_mut("task-mid").expect("task-mid").attempts = 2;

        let by_priority = list_tasks(
            &state,
            &TaskListQuery {
                sort_by: TaskListSort::Priority,
                ..TaskListQuery::default()
            },
        );
        assert_eq!(
            by_priority
                .iter()
                .map(|task| task.task_id.as_str())
                .collect::<Vec<_>>(),
            vec!["task-high", "task-mid", "task-low"]
        );

        let by_age = list_tasks(
            &state,
            &TaskListQuery {
                sort_by: TaskListSort::Age,
                ..TaskListQuery::default()
            },
        );
        assert_eq!(
            by_age
                .iter()
                .map(|task| task.task_id.as_str())
                .collect::<Vec<_>>(),
            vec!["task-low", "task-mid", "task-high"]
        );

        let by_retries = list_tasks(
            &state,
            &TaskListQuery {
                sort_by: TaskListSort::Retries,
                ..TaskListQuery::default()
            },
        );
        assert_eq!(
            by_retries
                .iter()
                .map(|task| task.task_id.as_str())
                .collect::<Vec<_>>(),
            vec!["task-low", "task-mid", "task-high"]
        );
    }

    #[test]
    fn show_task_returns_assignment_retry_and_payload_details() {
        let mut state = OrchestratorState::default();
        let alpha = card("alpha", "summarize");
        apply_snapshot(&mut state, vec![alpha.clone()], 6);
        assert!(ingest_task(&mut state, task("task-1", "summarize", 4), 1));

        let assigned_at = Utc::now() - Duration::seconds(90);
        let assigned = plan_assignment_event_with_reason(
            &state,
            "task-1",
            &alpha,
            assigned_at,
            Some("scheduler selected agent `alpha` with priority=50".to_owned()),
        )
        .expect("assigned event");
        let assignment_id = assigned.assignment_id.expect("assignment id");
        assert_eq!(
            record_local_task_event(&mut state, assigned),
            TaskEventApplyOutcome::Applied
        );

        let detail =
            show_task(&state, "task-1", assigned_at + Duration::seconds(90)).expect("task detail");
        assert_eq!(detail.task_id, "task-1");
        assert_eq!(detail.status, TaskStatus::Assigned);
        assert_eq!(detail.max_attempts, 4);
        assert_eq!(detail.retry_count, 0);
        assert_eq!(detail.requirements.skill.as_deref(), Some("summarize"));
        assert_eq!(
            detail.payload,
            TaskPayload::json(json!({ "path": "notes.md" }))
        );
        assert_eq!(
            detail
                .active_assignment
                .as_ref()
                .expect("active assignment")
                .assignment_id,
            assignment_id
        );
        assert_eq!(detail.active_assignment_age_seconds, Some(90));
        assert_eq!(
            detail.last_assignment_reason.as_deref(),
            Some("scheduler selected agent `alpha` with priority=50")
        );
        assert_eq!(show_task(&state, "missing-task", Utc::now()), None);
    }

    #[test]
    fn orchestrator_metrics_summarize_task_lifecycle_and_oldest_assignment_age() {
        let mut state = OrchestratorState::default();
        let alpha = card("alpha", "summarize");
        let beta = card("beta", "summarize");
        apply_snapshot(&mut state, vec![alpha.clone(), beta.clone()], 9);

        assert!(ingest_task(
            &mut state,
            task("pending-task", "summarize", 3),
            0
        ));
        assert!(ingest_task(
            &mut state,
            task("assigned-task", "summarize", 3),
            1
        ));
        assert!(ingest_task(
            &mut state,
            task("completed-task", "summarize", 3),
            2
        ));
        assert!(ingest_task(
            &mut state,
            task("failed-task", "summarize", 3),
            3
        ));
        assert!(ingest_task(
            &mut state,
            task("retry-task", "summarize", 3),
            4
        ));
        assert!(ingest_task(
            &mut state,
            task("timed-out-task", "summarize", 3),
            5
        ));
        assert!(ingest_task(
            &mut state,
            task("exhausted-task", "summarize", 3),
            6
        ));
        assert!(ingest_task(
            &mut state,
            task("canceled-task", "summarize", 3),
            7
        ));

        let now = Utc::now();
        let assigned_event =
            plan_assignment_event(&state, "assigned-task", &alpha, now - Duration::seconds(45))
                .expect("assigned event");
        assert_eq!(
            record_local_task_event(&mut state, assigned_event),
            TaskEventApplyOutcome::Applied
        );

        let completed_event = TaskEvent {
            event_id: Uuid::now_v7(),
            task_id: "completed-task".to_owned(),
            task_offset: Some(2),
            assignment_id: Some(Uuid::now_v7()),
            agent_id: Some("alpha".to_owned()),
            status: TaskStatus::Completed,
            attempt: 1,
            reason: None,
            emitted_at: now - Duration::seconds(5),
        };
        state
            .tasks
            .get_mut("completed-task")
            .expect("completed task")
            .attempts = 1;
        state
            .tasks
            .get_mut("completed-task")
            .expect("completed task")
            .status = TaskStatus::Completed;
        state
            .tasks
            .get_mut("completed-task")
            .expect("completed task")
            .last_event_at = completed_event.emitted_at;

        let failed_event = TaskEvent {
            event_id: Uuid::now_v7(),
            task_id: "failed-task".to_owned(),
            task_offset: Some(3),
            assignment_id: Some(Uuid::now_v7()),
            agent_id: Some("beta".to_owned()),
            status: TaskStatus::Failed,
            attempt: 2,
            reason: Some("failed".to_owned()),
            emitted_at: now - Duration::seconds(4),
        };
        state
            .tasks
            .get_mut("failed-task")
            .expect("failed task")
            .attempts = 2;
        state
            .tasks
            .get_mut("failed-task")
            .expect("failed task")
            .status = TaskStatus::Failed;
        state
            .tasks
            .get_mut("failed-task")
            .expect("failed task")
            .last_event_at = failed_event.emitted_at;

        let retry_event = TaskEvent {
            event_id: Uuid::now_v7(),
            task_id: "retry-task".to_owned(),
            task_offset: Some(4),
            assignment_id: None,
            agent_id: None,
            status: TaskStatus::RetryScheduled,
            attempt: 3,
            reason: Some("retry".to_owned()),
            emitted_at: now - Duration::seconds(3),
        };
        assert_eq!(
            record_local_task_event(&mut state, retry_event),
            TaskEventApplyOutcome::Applied
        );

        let timed_out_assignment = TaskEvent {
            event_id: Uuid::now_v7(),
            task_id: "timed-out-task".to_owned(),
            task_offset: Some(5),
            assignment_id: Some(Uuid::now_v7()),
            agent_id: Some("beta".to_owned()),
            status: TaskStatus::TimedOut,
            attempt: 2,
            reason: Some("timed out".to_owned()),
            emitted_at: now - Duration::seconds(2),
        };
        state
            .tasks
            .get_mut("timed-out-task")
            .expect("timed out task")
            .attempts = 2;
        state
            .tasks
            .get_mut("timed-out-task")
            .expect("timed out task")
            .status = TaskStatus::TimedOut;
        state
            .tasks
            .get_mut("timed-out-task")
            .expect("timed out task")
            .last_event_at = timed_out_assignment.emitted_at;

        state
            .tasks
            .get_mut("exhausted-task")
            .expect("exhausted task")
            .attempts = 3;
        state
            .tasks
            .get_mut("exhausted-task")
            .expect("exhausted task")
            .status = TaskStatus::Exhausted;
        state
            .tasks
            .get_mut("canceled-task")
            .expect("canceled task")
            .attempts = 1;
        state
            .tasks
            .get_mut("canceled-task")
            .expect("canceled task")
            .status = TaskStatus::Canceled;

        let metrics = orchestrator_metrics(&state, now);
        assert_eq!(metrics.agent_count, 2);
        assert_eq!(metrics.task_count, 8);
        assert_eq!(
            metrics.tasks,
            TaskLifecycleMetrics {
                pending: 1,
                assigned: 1,
                completed: 1,
                failed: 1,
                retry_scheduled: 1,
                timed_out: 1,
                exhausted: 1,
                canceled: 1,
                retry_count: 6,
                oldest_in_flight_age_seconds: Some(45),
            }
        );
    }
}
