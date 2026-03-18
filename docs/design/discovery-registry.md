# Discovery Registry Design

## Purpose

The discovery registry gives local agents a durable, audited place to publish an agent card and lets operators or other trusted principals query those cards by exact metadata. It is intentionally small and predictable: this is not a semantic search engine and it is not an automatic connection broker.

## API Surface

The control plane adds three admin operations:

- `register_agent`
- `heartbeat_agent`
- `list_agents`
- `watch_agents`
- `open_agent_watch_stream`
- `cleanup_stale_agents`
- `remove_agent`

All three operations require a signed capability token and a matching policy rule for `registry:agents*`.

## Data Model

Each registry entry stores:

- `agent_id`
- `principal`
- `display_name`
- `version`
- `summary`
- `skills`
- `subscriptions`
- `publications`
- `schemas`
- `endpoint`
- `classification`
- `retention_class`
- `ttl_seconds`
- `updated_at`
- `last_seen_at`
- `expires_at`

## Ownership Rules

- The owner principal is derived from the verified token, not from client input.
- A principal cannot overwrite another principal's `agent_id`.
- A principal can refresh only its own card unless a developer principal is performing cleanup or recovery.
- A developer principal can remove another principal's entry for cleanup.
- All mutations are audited as admin operations.

## Backend Model

The first backend is `file`, which persists registry state to a local JSON document. The backend seam exists so later work can add a stronger metadata store without changing the control-plane contract.

Registry change subscriptions use an in-memory bounded event journal. That keeps the first watch implementations simple and fast, but it also means watch cursors are runtime-local rather than durable across broker restarts.

## Query Model

The first query model supports exact filters for:

- `skill`
- `topic`
- `principal`

`topic` matches either `subscriptions` or `publications`.

Stale cards are hidden by default. Operators can include them explicitly when diagnosing liveness problems.

`list_agents` returns a cursor alongside the current snapshot. Clients should use that cursor when starting `watch_agents` so they do not miss mutations that happen after the initial snapshot.

## Freshness Model

- Every card has a TTL in seconds.
- If a registration does not provide a TTL, the broker applies the configured registry default.
- `heartbeat_agent` refreshes `last_seen_at`, `updated_at`, and `expires_at`.
- `cleanup_stale_agents` removes expired cards from the backing store.
- Expired cards remain auditable in the registry file until cleanup runs, but they are not returned in normal lookups.

## Watch Model

- `watch_agents` uses a long-poll request with a cursor, query filters, maximum batch size, and wait timeout.
- `open_agent_watch_stream` upgrades a connection into a dedicated multi-frame JSON-line stream with the same cursor, query, and batch semantics.
- Both watch modes use the same bounded event journal and the same cursor validation rules.
- If matching events already exist after the provided cursor, they are returned immediately.
- If no matching events exist yet, the broker waits until a matching mutation arrives or the timeout expires.
- The streamed watch transport emits an `agent_watch_opened` frame once, `registry_events` frames for matching mutations, and `keep_alive` frames when the wait interval expires without a matching change.
- The event stream includes `registered`, `heartbeated`, `removed`, and `cleaned_up` mutations.
- Event history is bounded by configuration. If a watcher falls too far behind, the broker returns a cursor-expired error and the client must resync with `list_agents`.

## Failure Model

- If the registry backend cannot be read or written, the request fails closed.
- Publish and consume paths remain available because the discovery registry is not on the data path.
- Operators can still use direct endpoint configuration and topic routing while the registry is unavailable.
