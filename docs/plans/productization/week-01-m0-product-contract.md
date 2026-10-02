# Week 1 (M0) Product Contract

Date: March 26, 2026  
Status: Accepted for execution

## SKU Statement

Expressways ships first as **Expressways Desktop**: a local-first, single-node broker product with a local console, guided bootstrap, and service lifecycle controls for workstation and single-host operator use.

## Target Users

1. Agent platform engineers running multi-agent coordination locally.
2. AI application developers who need auditable publish/consume and discovery without cloud infrastructure.
3. Security-conscious operators validating identity, policy, and audit controls before team rollout.

## Top 5 Workflows

1. Install broker + console, run guided bootstrap, and pass first-run verification in under 15 minutes.
2. Create topics, publish messages, consume messages, and inspect quotas/denials from console or CLI.
3. Register agents, stream/watch registry changes, and maintain discovery freshness with heartbeats and cleanup.
4. Perform day-1/day-2 operations: inspect health, metrics, adopters, config diffs, rollback, and safe restart.
5. Export incident evidence (logs/metrics/config metadata/audit excerpts) for diagnosis and support handoff.

## Non-Goals (Scope Lock)

1. Multi-node clustering or distributed coordination.
2. Dynamic runtime plugin loading.
3. Hosted cloud control plane and billing.
4. General-purpose event streaming platform positioning.
5. Priority optimization over correctness, auditability, and policy guarantees.

## Pilot Success Metrics

| Metric | Target | Measurement Method | Owner Lane |
| --- | --- | --- | --- |
| Clean-machine time to healthy broker + console | <= 15 minutes (p90) | Timed rehearsal on new machine images | C |
| First-run bootstrap success without manual token edits | >= 90% | Pilot session completion logs | B |
| Token/principal mismatch incidents on default path | 0 | Bootstrap diagnostics + support tickets | B |
| Day-1/day-2 tasks completed without direct TOML edits | >= 90% of checklist tasks | Pilot operator checklist outcomes | A |
| Critical incident triage with available artifacts | <= 30 minutes to root-cause hypothesis | Support rehearsal drill logs | E |

## Launch-Blocking Risks and Owners

| Risk | Blocking Condition | Mitigation | Owner Lane |
| --- | --- | --- | --- |
| Installer/bootstrap drift from docs | Operator cannot reach healthy state quickly | Enforce scripted bootstrap and first-run verification | C |
| Capability scope vs policy confusion | Operators repeatedly hit avoidable deny paths | Add diagnostics panel + guided token generation | A/B |
| Weak supportability signals | Incidents require engineering source-level intervention | Ship support bundle schema and export flow before pilot expansion | E |
| Security regressions during UX hardening | Auth/policy/audit invariants bypassed | Require D-lane sign-off for auth/token/secret path changes | D |

## Week 1 Exit Decision

- SKU and non-goal statement is explicit.
- Pilot metrics are measurable with owners.
- Launch-blocking risks have mitigation owners.

