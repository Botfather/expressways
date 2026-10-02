# ADR 0002: Discovery Registry as the First Sprint 6 Feature

## Status

Accepted

## Context

Sprint 5 established that Expressways is not currently bottlenecked by the local control-plane transport in a way that justifies `io_uring` or shared-memory complexity. The measured pain point is still storage append throughput, and the prior feasibility review also called out semantic discovery, shared memory, and in-log priority as higher-risk areas for early expansion.

Sprint 6 therefore needs an advanced feature that:

- adds user-visible value without destabilizing the publish and consume hot path,
- preserves the mandatory auth, audit, logging, and compliance controls,
- has a safe fallback if the new subsystem is unavailable,
- does not require Linux-only assumptions.

## Decision

Expressways will start Sprint 6 with a file-backed local discovery registry behind a backend seam.

The first registry slice supports:

- signed-capability and policy-gated register, list, and remove operations,
- exact-match filtering by `skill`, `topic`, and `principal`,
- ownership derived from the authenticated principal,
- compliance metadata on every persisted discovery card,
- local JSON persistence with fail-closed behavior.

The first registry slice explicitly does not support:

- semantic or vector-backed search,
- automatic peer connection setup,
- remote service discovery,
- implicit ownership transfer,
- hot-path publish or consume routing changes.

## Threat Model

- A caller must not be able to spoof the owner of a discovery card.
- A principal must not be able to overwrite another principal's registration.
- Registry failures must not silently downgrade authorization or audit coverage.
- Discovery metadata should not bypass compliance labeling just because it is control-plane data.

## Fallback Path

If the registry is unavailable, operators can still route work through known topics and direct endpoint configuration. The broker continues to function for publish, consume, auth, metrics, and audit operations even if discovery registration or lookup is temporarily unavailable.

## Benchmark Justification

The registry is not introduced on the publish or consume hot path, so it does not need a transport or kernel-level optimization case to ship. The Sprint 5 decision memo already deferred `io_uring` because measured storage append costs dominate current optimization value. Starting with discovery lets Expressways add orchestration value without pretending the storage bottleneck has been solved.

## Consequences

- Expressways gains a concrete orchestration primitive without violating the Phase 1 guardrails.
- The implementation stays cross-platform and operationally debuggable.
- Higher-risk Sprint 6 candidates remain deferred until they have both stronger threat models and stronger benchmark evidence.
