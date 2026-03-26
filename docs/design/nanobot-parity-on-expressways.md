# Nanobot Parity on Expressways

## Purpose

This document describes how `crates/expressways-nanobot-system` models the architecture of [HKUDS/nanobot](https://github.com/HKUDS/nanobot) on top of Expressways, what is feature-parity compatible today, and what advantage this deployment model provides.

The design stays within Expressways Phase 1 constraints:

- authenticated and policy-governed broker operations,
- quota-aware publish and consume paths,
- audit-friendly coordination through broker topics,
- local-first, single-node operation.

## Nanobot Architecture Mapping

| Nanobot Component | Expressways Implementation |
| --- | --- |
| Inbound/outbound queue bus | Expressways topics (`<prefix>.inbound`, `<prefix>.outbound`, `<prefix>.outbound.stream`) |
| Session manager with persisted history | `SessionStore` JSONL files under `./var/agent/nanobot-runtime/sessions` |
| Agent loop with provider + tool calls | `run-runtime` + `RuleBasedProvider` + `ToolRegistry` |
| Tool registry (`filesystem`, `shell`, `spawn`, etc.) | Built-ins: `read_file`, `exec`, `echo`, `spawn_subagent` |
| Subagent spawning | `spawn_subagent` tool republishes inbound work for asynchronous handling |
| Long-term memory layer | `MemoryStore` JSONL notes + bounded summary injection |
| Cron service | `run-cron` command publishes scheduled inbound envelopes |
| Heartbeat/liveness | Registry registration + periodic `heartbeat_agent` loop |
| Channel abstraction | `ingest` and `tail-outbound` commands as channel adapter seams |
| Security controls around tools and IO | Workspace-root path allowlists + exec program allowlists |
| Multi-instance runtime separation | Explicit `instance_id` + topic-prefix-scoped bootstrapping |

## Parity Scope

The crate targets architectural parity for local agent-runtime behavior, not model-quality parity with any specific upstream model provider.

Implemented parity surface:

- bus-driven inbound/outbound orchestration,
- persistent session continuity,
- iterative tool loop with bounded steps,
- background subagent spawning path,
- memory summarization feed into response generation,
- cron-triggered message production,
- heartbeat and registry presence,
- native OpenAI and Anthropic provider modes with tool-call parsing,
- optional provider text streaming (OpenAI and Anthropic) to `<prefix>.outbound.stream` (chunk + done markers),
- transient provider retry/backoff with runtime-event visibility (`provider_error` and `provider_retry_recovered` carrying attempts/elapsed),
- provider circuit-breaker controls with cooldown and runtime events (`provider_circuit_open`, `provider_circuit_blocked`, `provider_circuit_closed`),
- optional cross-provider failover with runtime events (`provider_failover_attempt`, `provider_failover_succeeded`, `provider_failover_failed`),
- bootstrap command to provision a complete topic + policy baseline.

Deliberate differences:

- provider is rule-based by default (deterministic local behavior); external LLM behavior is optional through native OpenAI or Anthropic providers.
- channel plugins are represented as adapter seams and CLI wiring, not dynamic runtime plugin loading.
- all coordination remains broker-audited through Expressways publish/consume operations.

## Mechanism to Create a System

Use `create-system` to provision the bus and emit operator snippets:

```bash
cargo run -p expressways-nanobot-system -- --transport tcp --address 127.0.0.1:7766 create-system --token-file ./var/auth/developer.token --topic-prefix nanobot --output-dir ./var/agent/nanobot-system
```

This command:

1. ensures `<prefix>.inbound`, `<prefix>.outbound`, `<prefix>.outbound.stream`, and `<prefix>.runtime.events` topics exist,
2. writes `nanobot-system.toml` with topic wiring,
3. writes `nanobot-auth-policy-snippets.toml` with principal/quota/policy stanzas.

Then run the runtime:

```bash
cargo run -p expressways-nanobot-system -- --transport tcp --address 127.0.0.1:7766 run-runtime --token-file ./var/auth/developer.token --agent-id nanobot-runtime --state-dir ./var/agent/nanobot-runtime --ensure-topics true
```

Use OpenAI as external model provider:

```bash
cargo run -p expressways-nanobot-system -- --transport tcp --address 127.0.0.1:7766 run-runtime --token-file ./var/auth/developer.token --agent-id nanobot-runtime --state-dir ./var/agent/nanobot-runtime --provider openai --provider-model gpt-4o-mini --provider-api-key sk-1234
```

Enable streaming chunk emission:

```bash
cargo run -p expressways-nanobot-system -- --transport tcp --address 127.0.0.1:7766 run-runtime --token-file ./var/auth/developer.token --agent-id nanobot-runtime --state-dir ./var/agent/nanobot-runtime --provider openai --provider-model gpt-4o-mini --provider-api-key sk-1234 --provider-streaming true
cargo run -p expressways-nanobot-system -- --transport tcp --address 127.0.0.1:7766 run-runtime --token-file ./var/auth/developer.token --agent-id nanobot-runtime --state-dir ./var/agent/nanobot-runtime --provider anthropic --provider-model claude-3-5-sonnet-latest --provider-api-key sk-ant-1234 --provider-streaming true
```

Use Anthropic as external model provider:

```bash
cargo run -p expressways-nanobot-system -- --transport tcp --address 127.0.0.1:7766 run-runtime --token-file ./var/auth/developer.token --agent-id nanobot-runtime --state-dir ./var/agent/nanobot-runtime --provider anthropic --provider-model claude-3-5-sonnet-latest --provider-api-key sk-ant-1234
```

These integrations call provider APIs directly (no external model gateway required).

Ingress test message:

```bash
cargo run -p expressways-nanobot-system -- --transport tcp --address 127.0.0.1:7766 ingest --token-file ./var/auth/developer.token --session-id chat-1 --channel local --account-id acct-local --sender-id user-1 --text "hello from nanobot parity runtime"
```

Tail responses:

```bash
cargo run -p expressways-nanobot-system -- --transport tcp --address 127.0.0.1:7766 tail-outbound --token-file ./var/auth/developer.token --follow
cargo run -p expressways-client --bin expresswaysctl -- --transport tcp --address 127.0.0.1:7766 consume --token-file ./var/auth/developer.token --topic nanobot.outbound.stream --offset 0 --limit 200
```

Summarize provider reliability events:

```bash
cargo run -p expressways-nanobot-system -- --transport tcp --address 127.0.0.1:7766 summarize-provider-events --token-file ./var/auth/developer.token --runtime-events-topic nanobot.runtime.events --offset 0 --limit 200
```

Run a one-command streaming smoke check:

```bash
OPENAI_API_KEY=... ./scripts/nanobot-streaming-smoke.sh
ANTHROPIC_API_KEY=... PROVIDER=anthropic ./scripts/nanobot-streaming-smoke.sh
```

## Advantage Evaluation

Primary advantages:

1. Security and governance are stronger than ad-hoc local buses because identity, policy, quota, and audit are enforced by the broker before each operation.
2. Operations are clearer because runtime behavior is visible in broker metrics, topic state, registry heartbeats, and audit logs.
3. Multi-runtime coexistence is easier because OpenClaw/ZeroClaw/Nanobot-style adapters can share one governed coordination substrate.
4. Failure handling is safer because Expressways degraded-mode and retention controls reduce silent data loss patterns in local prototypes.

Tradeoffs:

1. Additional setup is required (token issuance, policy snippets, topic provisioning).
2. The default parity runtime is deterministic/provider-agnostic; production LLM providers still need explicit adapter wiring.
3. Runtime plugin loading remains intentionally restricted in Expressways, so extension happens through controlled adapter seams and compiled components.
