# Expressways Performance Pathfinding

## Purpose

Sprint 5 exists to create evidence before optimization work begins. The benchmark harness and this document define the baseline workflow the team should use before proposing transport or storage fast paths.

## Benchmark Harness

The workspace now includes [crates/expressways-bench](/Users/tusharmohan/Documents/@labs/expressways/crates/expressways-bench), a small CLI for repeatable local measurement.

### Supported Commands

- `cargo run -p expressways-bench -- broker ...`
- `cargo run -p expressways-bench -- storage ...`
- `cargo run -p expressways-bench -- suite ...`

## Current Methodology

### Broker Transport Baseline

- Uses a signed developer token issued from the local benchmark key.
- Can reuse an already-running broker or spawn the broker from the sample config.
- Sends warmup publish requests before timed iterations begin.
- Measures publish round-trip latency and throughput over TCP.
- Measures Unix socket round-trip latency and throughput on Unix hosts.

### Storage Baseline

- Uses the storage crate directly without auth or audit overhead.
- Measures append throughput for a fixed message count and payload size.
- Measures read throughput with batched offset reads after the append run completes.
- Records resulting segment counts and total on-disk bytes.

## Current Output

The latest suite output is stored in [var/benchmarks/latest.json](/Users/tusharmohan/Documents/@labs/expressways/var/benchmarks/latest.json). The benchmark harness writes JSON so results can feed dashboards, memo generation, or regression comparisons later.

## How To Run

### Full Local Suite

```bash
cargo run -p expressways-bench -- suite --spawn-server --broker-iterations 100 --warmup-iterations 20 --payload-bytes 512 --message-count 2000 --read-batch 250 --output ./var/benchmarks/latest.json
```

### TCP Only

```bash
cargo run -p expressways-bench -- broker --spawn-server --transport tcp --iterations 100 --warmup-iterations 20 --payload-bytes 512
```

### Unix Only

```bash
cargo run -p expressways-bench -- broker --spawn-server --transport unix --iterations 100 --warmup-iterations 20 --payload-bytes 512
```

### Storage Only

```bash
cargo run -p expressways-bench -- storage --message-count 2000 --payload-bytes 512 --read-batch 250
```

## Interpretation Guidance

- Compare TCP and Unix only on the same machine and with the same payload size.
- Treat storage append throughput as the more meaningful bottleneck signal than read throughput for the current architecture.
- Do not justify Linux-specific work from a single workstation run alone.
- Re-run the suite after material storage, framing, or transport changes and compare against the last saved JSON report.

## Next Measurements

- Larger payload sweeps such as `1 KiB`, `4 KiB`, and `16 KiB`.
- Concurrent client runs instead of a single client connection.
- Separate benchmarks for `health`, `publish`, and `consume`.
- Linux host reruns before revisiting `io_uring`.
