# Changelog

All notable user-visible changes will be documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases use semantic versioning where the public compatibility contract permits it.

## [Unreleased]

### Added

- Open-source project governance, contribution, security, operational, and protocol documentation.
- A supported, versioned two-way chat bridge with durable at-least-once reply delivery, acknowledgements, idempotency keys, and raw artifact upload.
- Hard agent pins and serialized sticky affinity routing for ordered conversation processing.
- Versioned ingress, handoff, and reply schemas with forward-compatible additive fields.
- An authenticated loopback HTTP gateway for broker health, topic I/O, tasks, agent discovery, and integrity-checked artifact transfer.

### Security

- Repository hygiene checks for accidentally tracked secrets, local paths, and generated state.
- Patched Rust TLS, certificate-validation, QUIC, and XML dependencies and removed vulnerable gateway dependencies.

### Changed

- Storage serialization now uses bincode 2's bounded legacy configuration to preserve the existing alpha segment representation while preventing unbounded decoder input.

## [0.1.0] - Unreleased

Initial alpha release. No stable compatibility guarantee has been made yet.
