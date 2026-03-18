# Event-Driven Orchestrator

## Purpose

The first orchestrator layer turns the discovery registry from a passive lookup table into an active control loop. It maintains a local, event-driven view of agent availability and uses that view to make simple assignment decisions without polling the broker for every choice.

## Components

- `expressways-orchestrator supervise`
  Bootstraps from `list_agents`, tails `open_agent_watch_stream`, and persists the current view to a local state file.
- `expressways-orchestrator assign`
  Loads the latest local state, selects an eligible agent, and publishes the assignment decision to an audited broker topic.

## State Model

The orchestrator state file stores:

- `source_cursor`
- `observed_at`
- `agents`
- `assignments`

The `agents` map is refreshed from the broker stream. The `assignments` map tracks `assignment_count` and `last_assigned_at` so the first scheduling strategy can be deterministic and explainable.

## Assignment Strategy

The first strategy is `least_recently_assigned`:

- Match active agents by optional `skill`, `topic`, and `principal`.
- Ignore expired agent cards even if they are still present in local state.
- Prefer agents that have never been assigned.
- Otherwise prefer the agent with the oldest `last_assigned_at`.
- Use `agent_id` as the final tie-breaker for determinism.

This is intentionally small. It is predictable, easy to test, and easy to reason about in audits.

## Audit and Compliance

- The supervisor itself does not bypass broker controls.
- Assignment decisions are published through the broker, so they inherit capability checks, policy, structured logging, audit events, and topic-level compliance metadata.
- The default decision topic is `orchestrator.assignments` with `operational` retention and `internal` classification.

## Failure Model

- If the supervisor loses its stream, it reconnects using the last observed cursor.
- If the broker reports `watch_cursor_expired`, the supervisor rebuilds state from a fresh snapshot before reconnecting.
- If the local state file is missing during `assign`, the command can bootstrap a fresh snapshot before making a decision.
- If the audited publish fails, the local assignment counters are not advanced.

## Next Steps

- Multiple scheduling strategies such as sticky routing or weighted priority.
- Topic-driven work assignment instead of operator-triggered `assign`.
- Supervisor metrics and health checks.
- Explicit task lifecycle events paired with assignment decisions.
