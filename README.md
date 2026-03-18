# AetherBus

AetherBus is a Rust workspace for building a desktop-first agent coordination bus the honest way: start with a small, reliable broker that always enforces structured logging, auditability, compliance metadata, signed capability-based access control, and then earn the right to add more ambitious transport and performance features later.

The original concept in [docs/main.md](/Users/tusharmohan/Documents/@labs/expressways/docs/main.md) is useful as a vision, but it overreaches for a first implementation. This repository is scoped around a high-confidence Phase 1:

- Single-node broker.
- Cross-platform local TCP transport by default, with Unix sockets available on Unix hosts.
- Append-only binary segmented storage with sidecar indexes.
- Control-plane-first design with signed capability tokens, multi-issuer principal verification, policy checks, and audit events on every operation.
- Per-principal quota profiles with explicit backpressure behavior on publish and consume paths.
- Admin control-plane commands for auth-state inspection and token, principal, or issuer-key revocation.
- Explicit compliance metadata and retention classes from day one.

## Workspace Layout

- `crates/aetherbus-protocol`: shared request, response, and domain types.
- `crates/aetherbus-auth`: signed capability issuance and verification.
- `crates/aetherbus-policy`: server-side access-control policy engine.
- `crates/aetherbus-audit`: tamper-evident audit logging primitives.
- `crates/aetherbus-storage`: segmented append-only storage abstraction.
- `crates/aetherbus-client`: client SDK for control-plane requests.
- `crates/aetherbus-server`: broker daemon and request handling.
- `docs/reviews`: feasibility review and confidence assessment.
- `docs/plans`: sprint plans and execution sequencing.
- `docs/design`: system design, security baseline, and scope decisions.

## Quick Start

1. Review the scoped plan in [docs/plans/aetherbus-sprint-plan.md](/Users/tusharmohan/Documents/@labs/expressways/docs/plans/aetherbus-sprint-plan.md).
2. Review the architecture in [docs/design/phase-1-system-design.md](/Users/tusharmohan/Documents/@labs/expressways/docs/design/phase-1-system-design.md).
3. Generate a development keypair:

```bash
cargo run -p aetherbus-client --bin aetherbusctl -- generate-keypair --key-id dev --private-key ./var/auth/issuer.private --public-key ./var/auth/issuer.public
```

4. Issue a capability token for a local developer principal:

```bash
cargo run -p aetherbus-client --bin aetherbusctl -- issue-token --key-id dev --private-key ./var/auth/issuer.private --principal local:developer --audience aetherbus --scope system:broker:health --scope 'system:broker:admin' --scope 'topic:*:admin,publish,consume' --output ./var/auth/developer.token
```

5. Start the daemon with:

```bash
cargo run -p aetherbus-server -- --config configs/aetherbus.example.toml
```

6. In another shell, verify health:

```bash
cargo run -p aetherbus-client --bin aetherbusctl -- --transport tcp --address 127.0.0.1:7766 health --token-file ./var/auth/developer.token
```

7. Create a topic, publish, and consume:

```bash
cargo run -p aetherbus-client --bin aetherbusctl -- --transport tcp --address 127.0.0.1:7766 create-topic --token-file ./var/auth/developer.token --topic tasks
cargo run -p aetherbus-client --bin aetherbusctl -- --transport tcp --address 127.0.0.1:7766 publish --token-file ./var/auth/developer.token --topic tasks --payload "hello from scaffold"
cargo run -p aetherbus-client --bin aetherbusctl -- --transport tcp --address 127.0.0.1:7766 consume --token-file ./var/auth/developer.token --topic tasks --offset 0 --limit 10
```

8. Inspect auth state or revoke a token through the broker admin API:

```bash
cargo run -p aetherbus-client --bin aetherbusctl -- --transport tcp --address 127.0.0.1:7766 auth-state --token-file ./var/auth/developer.token
cargo run -p aetherbus-client --bin aetherbusctl -- --transport tcp --address 127.0.0.1:7766 revoke-token --token-file ./var/auth/developer.token --token-id <token-id>
```

## Guardrails

Every new externally reachable operation must:

1. authenticate a principal,
2. verify a signed capability token and authorize the action against policy,
3. emit an audit event,
4. emit structured operational logs,
5. carry compliance metadata or inherit it from the topic,
6. enforce principal-aware quota and backpressure policy when the path is rate- or size-sensitive.

If any of those are missing, the change is incomplete.
