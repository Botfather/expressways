# ADR 0001: Phase 1 Scope Reduction

## Status

Accepted

## Context

The original AetherBus concept includes advanced transport, clustering, discovery, and stream-processing capabilities. Building all of that in the first iteration would create too many simultaneous risks and make the system hard to reason about or validate.

## Decision

Phase 1 will be limited to a single-node broker with:

- append-only segmented storage,
- a cross-platform local TCP control plane with Unix sockets as an optional Unix-only transport,
- signed capability-based identity with a local principal and issuer registry,
- policy-based access control,
- quota-aware publish and consume handling,
- structured logging,
- tamper-evident audit logging,
- compliance metadata on topics and messages.

Phase 1 explicitly defers:

- `io_uring`,
- shared-memory zero-copy IPC,
- embedded Raft,
- semantic registry,
- in-broker WASM,
- tiered storage,
- in-log priority reordering.

## Consequences

- The system becomes buildable on a workstation immediately.
- The codebase stays honest about what is implemented.
- Later optimizations can be added behind clear interfaces after measurement.
