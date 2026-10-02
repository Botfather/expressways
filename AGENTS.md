# AGENTS.md

This file is for LLMs, coding agents, and automation tooling working inside this repository.

It is a practical companion to `README.md`, not a replacement for it.

## Start Here

If you are new to the codebase, read these in order:

1. `README.md`
2. `docs/design/phase-1-system-design.md`
3. `docs/design/security-compliance-baseline.md`
4. `docs/adr/0001-phase-1-scope.md`

Then inspect the crates you are about to modify.

## What Expressways Is

Expressways is a desktop-first, local-first coordination bus for multi-agent systems.

Current Phase 1 priorities:

- single-node broker
- signed capability-based auth
- server-side policy enforcement
- quota-aware publish and consume paths
- append-only segmented storage
- tamper-evident audit logging
- agent discovery registry with watch APIs
- degraded-mode serving and resilience controls
- installable adopter packages for hardening probes and self-heal behavior

## Repo Map

- `crates/expressways-server`
  The broker runtime, request handling, registry, resilience, adopters, and streaming.
- `crates/expressways-client`
  Client SDK and `expresswaysctl`.
- `crates/expressways-protocol`
  Shared domain types, requests, responses, metrics views, and stream frames.
- `crates/expressways-auth`
  Capability issuance/verification, principal checks, issuer state, revocation handling.
- `crates/expressways-policy`
  Server-side authorization policy engine.
- `crates/expressways-storage`
  Append-only segmented topic storage with retention/disk-pressure controls.
- `crates/expressways-audit`
  Hash-chained audit sink, verification, and export tooling.
- `crates/expressways-orchestrator`
  Event-driven orchestrator built on top of the broker.
- `crates/expressways-bench`
  Benchmark harness.
- `crates/expressways-adopter-*`
  Installable hardening packages.

## Local State and Tokens

Do not save secrets, generated tokens, or transient agent state into tracked files.

Use these locations instead:

- `./var/auth`
  Generated private keys, public keys, and issued tokens.
- `./var/agent`
  Local agent notes, scratch context, plans, summaries, or generated helper artifacts.
- `./tmp`
  Ephemeral sockets and short-lived temporary outputs.

These paths are already ignored by Git via `.gitignore`.

Recommended conventions:

- `./var/auth/issuer.private`
- `./var/auth/issuer.public`
- `./var/auth/developer.token`
- `./var/agent/context.md`
- `./var/agent/notes.md`
- `./var/agent/output.json`

Never store real secrets in:

- `AGENTS.md`
- `README.md`
- checked-in config files
- source files
- tests

## Bootstrap Commands

Generate a local issuer keypair:

```bash
cargo run -p expressways-client --bin expresswaysctl -- generate-keypair --key-id dev --private-key ./var/auth/issuer.private --public-key ./var/auth/issuer.public
```

Issue a local developer token:

```bash
cargo run -p expressways-client --bin expresswaysctl -- issue-token --key-id dev --private-key ./var/auth/issuer.private --principal local:developer --audience expressways --scope system:broker:health --scope 'system:broker:admin' --scope 'topic:*:admin,publish,consume' --scope 'registry:agents*:admin' --output ./var/auth/developer.token
```

If you plan to upload broker-managed artifacts or use `submit-task --payload-file`, include artifact scopes too:

```bash
cargo run -p expressways-client --bin expresswaysctl -- issue-token --key-id dev --private-key ./var/auth/issuer.private --principal local:developer --audience expressways --scope system:broker:health --scope 'system:broker:admin' --scope 'topic:*:admin,publish,consume' --scope 'artifact:*:publish,consume,admin' --scope 'registry:agents*:admin' --output ./var/auth/developer.token
```

Start the broker:

```bash
cargo run -p expressways-server -- --config configs/expressways.example.toml
```

Health check:

```bash
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 health --token-file ./var/auth/developer.token
```

Fetch metrics:

```bash
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 metrics --token-file ./var/auth/developer.token
```

Fetch adopter status:

```bash
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 adopters --token-file ./var/auth/developer.token
```

## Build and Test

Workspace check:

```bash
cargo check
```

Server tests:

```bash
cargo test -p expressways-server
```

Format:

```bash
cargo fmt
```

Benchmarks:

```bash
make benchmark
```

## Adopters

Adopters are the extensibility model for hardening logic.

Important constraints:

- adopters are separate crates
- adopters are installed into the server binary through Cargo features
- adopters are enabled at runtime through `configs/expressways.example.toml`
- Expressways does not support arbitrary runtime plugin loading

If you build the server with a subset of adopter features, the config allowlist must match that subset.

Example:

```bash
cargo run -p expressways-server --no-default-features --features adopter-audit-integrity,adopter-storage-guard -- --config configs/expressways.example.toml
```

Then update:

- `adopters.enabled` in config

so it only names packages compiled into that binary.

## Guardrails for Code Changes

When changing the system, do not weaken these invariants:

1. every externally reachable operation must authenticate a principal
2. capability verification must happen before action execution
3. policy must remain server-side and default deny
4. publish and consume paths must keep explicit quota behavior
5. allowed and denied actions must remain auditable
6. logs must remain structured JSON
7. compliance metadata must remain explicit
8. degraded mode must report truthfully instead of silently pretending to be healthy
9. adopters must remain allowlisted and build-installed, not dynamically loaded

## When Editing

If you touch any of these areas, update the related surfaces too:

- protocol changes:
  update `expressways-protocol`, server handlers, client CLI, and tests
- auth/policy changes:
  update tests for both allow and deny paths
- storage or audit changes:
  update resilience behavior and metrics where relevant
- adopter changes:
  update config examples, docs, metrics visibility, and tests
- operator-facing behavior:
  update `README.md`

## Preferred Agent Workflow

1. Read the relevant crate and tests before editing.
2. Make the smallest coherent change that preserves the system contract.
3. Add or update tests with the code change.
4. Run `cargo fmt`.
5. Run at least the narrowest relevant test target.
6. If the behavior is operator-facing, update docs.

## Good Places to Save Agent Context

If an agent needs scratch files while building:

- `./var/agent/context.md`
  human-readable summary of the current task
- `./var/agent/plan.md`
  work plan or TODOs
- `./var/agent/findings.md`
  review notes or bug findings
- `./var/agent/commands.log`
  useful commands run during investigation
- `./var/agent/*.json`
  structured outputs, snapshots, or generated metadata

These are local-only working files. They should stay out of Git.

## Good First Inspection Targets

For broker behavior:

- `crates/expressways-server/src/main.rs`
- `crates/expressways-server/src/config.rs`
- `crates/expressways-server/src/metrics.rs`
- `crates/expressways-server/src/adopters.rs`

For wire contract changes:

- `crates/expressways-protocol/src/lib.rs`
- `crates/expressways-client/src/bin/expresswaysctl.rs`

For storage or audit changes:

- `crates/expressways-storage/src/lib.rs`
- `crates/expressways-audit/src/lib.rs`

## If You Are Unsure

Bias toward:

- simpler architecture
- safer defaults
- explicit operator visibility
- tests over assumptions
- relative repo links in docs
- local ignored files for tokens and scratch context

Do not bias toward:

- hidden magic
- dynamic plugin loading
- undocumented behavior
- storing secrets in tracked files
- architecture claims that exceed the implementation
