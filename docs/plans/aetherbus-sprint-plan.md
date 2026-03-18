# AetherBus Sprint Plan

This plan assumes the scoped Phase 1 architecture, not the full original vision.

## Definition of Done for Every Sprint

- Every externally reachable action passes through authn, authz, structured logging, and audit emission.
- Every persistent artifact has a retention owner and a compliance classification.
- Every feature includes at least one automated test.
- Every API change updates the relevant design docs.

## Sprint 0: Scope Lock and Controls

### Goal

Freeze the buildable Phase 1 and remove ambiguity before implementation grows.

### Deliverables

- Phase 1 architecture doc.
- Security and compliance baseline.
- ADR capturing deferred features.
- Config model and operating assumptions.
- Initial backlog split by component.

### Exit Criteria

- Non-goals are explicit.
- Control requirements are mandatory, not optional.
- The team can explain what is intentionally deferred.

## Sprint 1: Broker Skeleton

### Goal

Create a runnable broker daemon with a control-plane listener and mandatory guardrails.

### Deliverables

- Rust workspace and crate boundaries.
- Config loading.
- Structured logging bootstrap.
- Audit log sink with tamper-evident chaining.
- Policy engine with allow/deny decisions.
- Control-plane request/response protocol.

### Exit Criteria

- The daemon starts, serves a health check, and records startup and request events.

## Sprint 2: Durable Storage

### Goal

Add append-only segmented topic storage with minimal read and write flows.

### Deliverables

- Topic catalog.
- Segment writer and segment reader.
- Topic-level compliance defaults.
- Retention class metadata.
- Publish path and consume path with policy enforcement.

### Exit Criteria

- A client can append and read messages while all actions are audited.

## Sprint 3: Identity, Capabilities, and Quotas

### Goal

Make principal handling real instead of placeholder.

### Deliverables

- Principal model with local agent identities.
- Capability issuance format.
- Quota and backpressure policy.
- Denial logging and audit coverage.

### Exit Criteria

- Unauthorized principals are denied consistently and traceably.

## Sprint 4: Observability and Operations

### Goal

Make the broker operable on a real workstation.

### Deliverables

- Metrics export.
- Log schema stabilization.
- Disk pressure handling.
- Audit retention and archival tooling.
- Failure drills and recovery playbooks.

### Exit Criteria

- Operators can explain system state from logs, metrics, and audit output alone.

## Sprint 5: Performance Pathfinding

### Goal

Measure, then optimize.

### Deliverables

- Benchmark harness.
- UDS throughput baseline.
- Storage throughput baseline.
- Cost model for Linux-specific fast paths.
- Decision memo for or against `io_uring`.

### Exit Criteria

- Optimizations are prioritized by measured bottlenecks, not architectural taste.

## Sprint 6: Advanced Features by Evidence

### Goal

Add only the features that survive Phase 1 validation.

### Candidate Work

- Linux-specific `io_uring` backend.
- Shared-memory fast path for trusted local agents.
- Pluggable discovery registry.
- Experimental priority scheduling separated from strict log topics.

### Exit Criteria

- Every advanced feature has a threat model, fallback path, and benchmark justification.

