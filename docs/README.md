# Expressways Documentation

Expressways is alpha software. Documents under `design` and `adr` describe the implemented or explicitly accepted system; documents under `archive` and `reviews` provide historical context and are not product commitments.

## Start Here

- [Repository overview and quick start](../README.md)
- [Installation](operations/installation.md)
- [Operator runbook](operations/runbook.md)
- [Protocol reference](protocol.md)
- [Security policy](../SECURITY.md)
- [Contributing](../CONTRIBUTING.md)

## Architecture and Contracts

- [Local agent backbone](design/local-agent-backbone.md)
- [Phase 1 system design](design/phase-1-system-design.md)
- [Security, compliance, and audit baseline](design/security-compliance-baseline.md)
- [Discovery registry](design/discovery-registry.md)
- [Event-driven orchestrator](design/event-driven-orchestrator.md)
- [OpenClaw and ZeroClaw interop](design/openclaw-zeroclaw-interop.md)
- [Nanobot parity](design/nanobot-parity-on-expressways.md)
- [Performance methodology](design/performance-pathfinding.md)

## Decisions and Historical Context

- [Phase 1 scope](adr/0001-phase-1-scope.md)
- [Registry-first advanced feature](adr/0002-discovery-registry-first-advanced-feature.md)
- [Original vision archive](archive/original-vision.md)
- [Critical review](reviews/expressways-critical-review.md)
- [`io_uring` decision memo](reviews/io-uring-decision-memo.md)

When documentation and code disagree, treat the code and tests as current behavior and open an issue to correct the documentation.
