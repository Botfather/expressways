# Event-Driven Orchestrator

## Purpose

The orchestrator now closes the basic local work loop:

1. consume a task work item,
2. choose an eligible agent from the live registry view,
3. publish an audited assignment event,
4. observe agent completion or failure events,
5. and safely retry or exhaust stuck work based on explicit timeout rules.

This keeps orchestration on the same broker path as every other action, rather than introducing a side channel for assignment state.

## Contracts

### `tasks`

The `tasks` topic carries `TaskWorkItem` payloads with:

- `task_id`
- `task_type`
- `requirements`
- `payload`
- `retry_policy`
- `submitted_at`

`requirements` reuse the current exact-match scheduling filters:

- `skill`
- `topic`
- `principal`

`retry_policy` keeps the first delivery model explicit:

- `max_attempts`
- `timeout_seconds`
- `retry_delay_seconds`

### `task_events`

The `task_events` topic carries `TaskEvent` payloads. The same topic is used for:

- orchestrator-published `assigned` events,
- agent-published `completed` and `failed` events,
- orchestrator-published `timed_out`, `retry_scheduled`, and `exhausted` events.

Every event includes:

- `event_id`
- `task_id`
- `task_offset`
- `assignment_id`
- `agent_id`
- `status`
- `attempt`
- `reason`
- `emitted_at`

## Components

- `expressways-orchestrator supervise`
  Bootstraps agent state from `list_agents`, tails `open_agent_watch_stream`, polls `tasks` and `task_events`, and persists the combined local view to disk.
- `expressways-orchestrator assign`
  Keeps a manual escape hatch for publishing an audited `assigned` event from the current local registry view.

## State Model

The orchestrator state file stores:

- `source_cursor`
- `task_offset`
- `task_event_offset`
- `observed_at`
- `agents`
- `tasks`
- `assignments`
- `pending_task_event_acks`

The `agents` map is refreshed from the broker stream. The `tasks` map tracks the current work item, lifecycle status, retry timing, and active assignment lease for each `task_id`. The `assignments` map keeps per-agent assignment counts so scheduling remains deterministic and explainable.

`pending_task_event_acks` prevents the supervisor from double-applying task events that it has already published successfully through the broker but has not yet re-consumed from `task_events`.

## Assignment Strategy

The current strategy remains `least_recently_assigned`:

- match active agents by optional `skill`, `topic`, and `principal`,
- ignore expired agent cards even if they are still present in local state,
- prefer agents that have never been assigned,
- otherwise prefer the agent with the oldest `last_assigned_at`,
- and use `agent_id` as the final tie-breaker.

This keeps the first task loop deterministic and easy to test.

## Failure Model

- If the supervisor loses its registry stream, it reconnects using the last observed cursor.
- If the broker reports `watch_cursor_expired`, the supervisor rebuilds the agent snapshot before reconnecting.
- If a task remains `assigned` past its lease timeout, the supervisor publishes `timed_out` and then either `retry_scheduled` or `exhausted`.
- If an agent publishes `failed`, the supervisor applies the same retry-budget rules before reassignment.
- If an agent publishes a stale outcome for an older `assignment_id`, the event is ignored so a newer assignment is not closed accidentally.
- If an audited publish fails, the local task state is not advanced for that lifecycle transition.

## Audit and Compliance

- The supervisor does not bypass broker controls.
- Task assignments and lifecycle transitions are published back through the broker, so they inherit capability checks, policy, structured logging, audit events, and topic-level compliance metadata.
- The default `tasks` and `task_events` topics both use `operational` retention and `internal` classification.

## Next Steps

- Multiple scheduling strategies such as sticky routing or weighted priority.
- Supervisor metrics and health checks.
- Topic partitioning or sharding once measurement justifies it.
- Richer task result payload conventions for higher-level orchestration features.
