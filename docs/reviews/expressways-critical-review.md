# Expressways Critical Review

## Executive Summary

The original Expressways concept is promising as a long-term vision, but it is not a credible Phase 1 implementation plan. It combines a local desktop broker, a distributed metadata plane, zero-copy IPC, semantic discovery, priority scheduling, cooperative rebalancing, security identity, and future in-broker transforms into one initial bet. That breadth creates too many failure modes to justify immediate full-scope implementation.

## Confidence Scores

- Original concept as written in `docs/main.md`: `4.6 / 10`
- Narrowed Phase 1 defined in this repository: `9.1 / 10`

The repository proceeds only with the narrowed Phase 1.

## Highest-Risk Findings

### 1. Scope collapse is the primary risk

The concept combines broker, coordination plane, discovery registry, schema registry, security plane, and future stream processing into a single initial product. That is too much surface area for a first release and hides critical sequencing decisions.

### 2. The transport story contradicts the desktop goal

The design centers Linux-specific `io_uring`, yet the stated product target is local desktop orchestration. A first release needs a cross-platform transport and I/O baseline before it can adopt Linux-specific fast paths.

### 3. Shared-memory zero-copy and strong isolation are in tension

Zero-copy shared memory is attractive for performance, but it complicates principal isolation, memory lifecycle, revocation, auditability, and safety when multiple local agents do not fully trust each other.

### 4. Raft is premature in a local-first broker

Embedded Raft is valuable only after a single-node broker demonstrates product value. In a first release it adds distributed-systems complexity without solving the main local orchestration problem.

### 5. Priority buckets weaken ordering semantics unless the contract changes

If high-priority messages can leapfrog earlier entries, then the system is no longer a plain ordered log from the consumer's point of view. The protocol and replay model must define this explicitly, or it becomes a source of subtle correctness bugs.

### 6. Compliance and audit requirements are implied, not designed

The concept discusses security, but it does not define a concrete audit taxonomy, retention policy model, tamper-evidence guarantees, principal lifecycle, key rotation, or incident response hooks. Those controls cannot be bolted on later.

### 7. The evidence base is uneven

The vision references real architectural ideas, but the benchmark tables and source mix are not strong enough to justify the proposed defaults. A production plan needs measured workloads, platform matrices, and failure-budget targets rather than vendor-comparison narratives.

## What Changes Make the Project Viable

### Keep for Phase 1

- Rust implementation.
- Append-only segmented log with binary records and offset indexes.
- Cross-platform local TCP control plane, with optional Unix sockets on Unix systems.
- Structured logging and audit events on every operation.
- Signed capability tokens plus policy-based access control.
- Compliance metadata and retention classes baked into the model.

### Defer Until Later

- `io_uring`
- shared-memory zero-copy IPC
- embedded Raft
- semantic registry and A2A integration
- in-broker WASM
- tiered storage
- priority buckets inside the same topic log

## Go/No-Go Decision

### Go for:

- a single-node broker skeleton,
- a durable append-only storage layer,
- a control plane with policy enforcement,
- audit and compliance guarantees from day one.

### No-go for:

- the full platform vision as a first implementation.
