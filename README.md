# Expressways

Expressways is a Rust workspace for building a desktop-first agent coordination bus the honest way: start with a small, reliable broker that always enforces structured logging, auditability, compliance metadata, signed capability-based access control, and then earn the right to add more ambitious transport and performance features later.

The original concept in [docs/main.md](/Users/tusharmohan/Documents/@labs/expressways/docs/main.md) is useful as a vision, but it overreaches for a first implementation. This repository is scoped around a high-confidence Phase 1:

- Single-node broker.
- Cross-platform local TCP transport by default, with Unix sockets available on Unix hosts.
- Append-only binary segmented storage with sidecar indexes.
- Control-plane-first design with signed capability tokens, multi-issuer principal verification, policy checks, and audit events on every operation.
- Per-principal quota profiles with explicit backpressure behavior on publish and consume paths.
- Admin control-plane commands for auth-state inspection and token, principal, or issuer-key revocation.
- Runtime metrics export for broker, storage, and audit state.
- Retention limits, disk-pressure reclamation, and segment recovery for local workstation safety.
- Audit verification and export tooling for operators.
- File-backed local discovery registry with exact-match filtering, TTL-based freshness, owner heartbeats, stale-entry cleanup, long-poll watch subscriptions, and a dedicated multi-frame watch stream.
- Explicit compliance metadata and retention classes from day one.

## Workspace Layout

- `crates/expressways-protocol`: shared request, response, and domain types.
- `crates/expressways-auth`: signed capability issuance and verification.
- `crates/expressways-policy`: server-side access-control policy engine.
- `crates/expressways-audit`: tamper-evident audit logging primitives.
- `crates/expressways-storage`: segmented append-only storage abstraction.
- `crates/expressways-client`: client SDK for control-plane requests.
- `crates/expressways-bench`: benchmark harness for transport and storage baselines.
- `crates/expressways-server`: broker daemon and request handling.
- `docs/reviews`: feasibility review and confidence assessment.
- `docs/plans`: sprint plans and execution sequencing.
- `docs/design`: system design, security baseline, and scope decisions.

## Quick Start

1. Review the scoped plan in [docs/plans/expressways-sprint-plan.md](/Users/tusharmohan/Documents/@labs/expressways/docs/plans/expressways-sprint-plan.md).
2. Review the architecture in [docs/design/phase-1-system-design.md](/Users/tusharmohan/Documents/@labs/expressways/docs/design/phase-1-system-design.md).
3. Generate a development keypair:

```bash
cargo run -p expressways-client --bin expresswaysctl -- generate-keypair --key-id dev --private-key ./var/auth/issuer.private --public-key ./var/auth/issuer.public
```

4. Issue a capability token for a local developer principal:

```bash
cargo run -p expressways-client --bin expresswaysctl -- issue-token --key-id dev --private-key ./var/auth/issuer.private --principal local:developer --audience expressways --scope system:broker:health --scope 'system:broker:admin' --scope 'topic:*:admin,publish,consume' --scope 'registry:agents*:admin' --output ./var/auth/developer.token
```

5. Start the daemon with:

```bash
cargo run -p expressways-server -- --config configs/expressways.example.toml
```

6. In another shell, verify health:

```bash
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 health --token-file ./var/auth/developer.token
```

7. Create a topic, publish, and consume:

```bash
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 create-topic --token-file ./var/auth/developer.token --topic tasks
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 publish --token-file ./var/auth/developer.token --topic tasks --payload "hello from scaffold"
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 consume --token-file ./var/auth/developer.token --topic tasks --offset 0 --limit 10
```

8. Inspect auth state or revoke a token through the broker admin API:

```bash
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 auth-state --token-file ./var/auth/developer.token
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 metrics --token-file ./var/auth/developer.token
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 revoke-token --token-file ./var/auth/developer.token --token-id <token-id>
```

9. Register an agent card, heartbeat it, and query the discovery registry:

```bash
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 register-agent --token-file ./var/auth/developer.token --agent-id summarizer --display-name "Summarizer" --version 1.0.0 --summary "Local document summarizer" --skill summarize --skill pdf --subscribe topic:tasks --publish-topic topic:results --endpoint-address 127.0.0.1:8811 --ttl-seconds 300
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 heartbeat-agent --token-file ./var/auth/developer.token --agent-id summarizer
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 list-agents --token-file ./var/auth/developer.token --skill summarize
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 cleanup-stale-agents --token-file ./var/auth/developer.token
```

10. Watch registry changes with long-poll or with a dedicated stream:

```bash
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 watch-agents --token-file ./var/auth/developer.token --wait-timeout-ms 30000 --follow
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 watch-agents-stream --token-file ./var/auth/developer.token --wait-timeout-ms 30000
```

11. Verify or export the audit trail locally:

```bash
cargo run -p expressways-client --bin expresswaysctl -- verify-audit --path ./var/audit/audit.jsonl
cargo run -p expressways-client --bin expresswaysctl -- export-audit --path ./var/audit/audit.jsonl --output ./var/audit/export.json
```

12. Capture a benchmark suite with TCP, Unix sockets on Unix hosts, and direct storage throughput:

```bash
cargo run -p expressways-bench -- suite --spawn-server --broker-iterations 100 --warmup-iterations 20 --payload-bytes 512 --message-count 2000 --read-batch 250 --output ./var/benchmarks/latest.json
```

## Guardrails

Every new externally reachable operation must:

1. authenticate a principal,
2. verify a signed capability token and authorize the action against policy,
3. emit an audit event,
4. emit structured operational logs,
5. carry compliance metadata or inherit it from the topic,
6. enforce principal-aware quota and backpressure policy when the path is rate- or size-sensitive,
7. expose enough metrics or verification data for operators to explain the system after the fact.

If any of those are missing, the change is incomplete.
