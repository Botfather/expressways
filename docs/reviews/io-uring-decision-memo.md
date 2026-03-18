# io_uring Decision Memo

## Status

Deferred

## Decision

Do not start an `io_uring` implementation in Phase 1 yet.

## Why

The current Sprint 5 baseline does not show a transport bottleneck strong enough to justify Linux-specific complexity ahead of simpler improvements.

## Evidence

From [var/benchmarks/latest.json](/Users/tusharmohan/Documents/@labs/expressways/var/benchmarks/latest.json) on March 18, 2026:

- TCP publish baseline: about `567 ops/sec`, average latency about `1.76 ms`.
- Unix socket publish baseline: about `458 ops/sec`, average latency about `2.18 ms`.
- Storage append baseline: about `336 ops/sec`.
- Storage read baseline: about `85,394 ops/sec`.

## Interpretation

- On this workstation, Unix sockets are not outperforming TCP for the current broker path.
- The strongest bottleneck signal is storage append throughput, not local transport overhead.
- Because Expressways is intentionally cross-platform in Phase 1, a Linux-only optimization would add complexity before the shared baseline is saturated.

## Cost Model

Implementing `io_uring` now would require:

- a Linux-only transport and I/O path,
- fallback and feature-detection logic,
- duplicated test and operational surface area,
- a fresh threat and failure model for a kernel-specific backend,
- new benchmarks on Linux to prove the extra path beats the portable one.

That cost is not justified while the portable storage append path is still the more obvious limit.

## Conditions To Reopen

Revisit `io_uring` only if all of the following are true:

1. Linux is a meaningful deployment target for the next phase.
2. Storage append throughput has been improved or shown not to be the primary bottleneck.
3. A Linux rerun of the benchmark suite shows transport overhead dominating end-to-end request time.
4. The team is willing to own a fallback path and additional recovery testing.

## Recommended Next Optimizations

1. Batch append and batched consume paths.
2. Storage write-path reductions such as fewer flushes or tighter indexing work.
3. Concurrent-client benchmark runs.
4. Linux benchmark reruns before any kernel-specific implementation begins.
