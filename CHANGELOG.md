# Changelog

All notable user-visible changes will be documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases use semantic versioning where the public compatibility contract permits it.

## [Unreleased]

### Added

- Open-source project governance, contribution, security, operational, and protocol documentation.
- A supported, versioned two-way chat bridge with durable at-least-once reply delivery, acknowledgements, idempotency keys, and raw artifact upload.
- Hard agent pins and serialized sticky affinity routing for ordered conversation processing.
- Versioned ingress, handoff, and reply schemas with forward-compatible additive fields.
- An authenticated loopback HTTP gateway for broker health, topic I/O, tasks, agent discovery, and integrity-checked artifact transfer.
- Resumable, authenticated topic event streaming over Server-Sent Events with bounded stream capacity and cursor recovery.
- A bounded `watch_topic` broker operation so idle event streams avoid repeated network requests and audit records.
- HTTP agent registration, heartbeat, removal, discovery, and resumable registry event streaming for non-Rust harnesses.
- A versioned OpenAPI 3.1 contract and isolated live HTTP conformance suite with restart durability checks.
- Cross-platform backbone release bundles with Windows coverage, all core runtime binaries, and native stack lifecycle helpers.
- A shared adapter SDK for monotonic durable cursors, deterministic replay identities, and verified raw artifact transfer.
- A live adapter conformance suite covering authentication, replay, affinity ordering, raw media, backpressure, destination retry, and restart recovery.
- A corrected heavy-media configuration contract with 64 MiB publish quotas and bounded frame headroom for artifact envelopes.
- Nanobot runtime model providers `--provider gemini` (Google Gemini `generateContent` API, key from `--provider-api-key` or `GEMINI_API_KEY`) and `--provider openai-compatible` (local or self-hosted OpenAI-compatible servers such as Ollama, LM Studio and vLLM; requires `--provider-base-url` and `--provider-model`, optional key from `--provider-api-key` or `OPENAI_COMPATIBLE_API_KEY`), both with streaming support.
- A `probe-provider` nanobot command that sends one test prompt with the `--provider*` flags and exits with a distinct code per failure kind (authentication failed, host unreachable, model not found, rate limited, timeout, and others).
- Classified provider failures: `provider_error` logs and runtime events now carry an `error_kind` field, and failover events carry `from_error_kind`/`to_error_kind`.

### Security

- Repository hygiene checks for accidentally tracked secrets, local paths, and generated state.
- Patched Rust TLS, certificate-validation, QUIC, and XML dependencies and removed vulnerable gateway dependencies.

### Changed

- The nanobot runtime now rejects a `--provider-base-url` or `--fallback-provider-base-url` that is not an `http://` or `https://` URL with a host.
- Storage serialization now uses bincode 2's bounded legacy configuration to preserve the existing alpha segment representation while preventing unbounded decoder input.

## [0.1.0] - Unreleased

Initial alpha release. No stable compatibility guarantee has been made yet.
