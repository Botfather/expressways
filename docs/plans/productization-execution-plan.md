# Expressways Productization Execution Plan

Date: March 26, 2026  
Status: Proposed execution baseline  
Planning horizon: 16 weeks to GA candidate

## Objective

Package Expressways as a product that can be installed, operated, upgraded, and supported without deep internal repository context.

This plan targets a **local-first single-node product** first, then expands to a hardened self-hosted team deployment path.

## Product Definition (Scope Lock)

Primary SKU in this plan:

- **Expressways Desktop**: local broker, local console, guided bootstrap, service lifecycle management, and optional Nanobot runtime integration.

Deferred from this plan:

- multi-node clustering and distributed coordination,
- dynamic runtime plugin loading,
- advanced hosted control plane,
- managed-cloud billing/control plane.

## Team and Ownership Lanes

Execution assumptions:

- 1 Product Lead (scope, UX priorities, acceptance sign-off),
- 1 Tech Lead (architecture and sequencing),
- 2 Core Rust Engineers (broker/runtime/auth/policy),
- 1 App Engineer (console UX/Tauri packaging),
- 1 Platform Engineer (release, signing, CI, installers),
- shared Security/Compliance reviewer.

Ownership lanes:

- **Lane A: Product and UX** (Product Lead + App Engineer)
- **Lane B: Core Runtime and APIs** (Tech Lead + Core Engineers)
- **Lane C: Packaging and Release** (Platform Engineer)
- **Lane D: Security and Compliance** (Tech Lead + Security reviewer)
- **Lane E: Ops, Reliability, Supportability** (Core + Platform)

## Milestones

## M0 - Product Contract and Pilot Criteria (Week 1)

Owner lanes: A, B, D

Deliverables:

- product contract doc (target user, top 5 workflows, non-goals),
- pilot acceptance checklist,
- support policy draft (what is covered vs not covered),
- release channel model (alpha/beta/stable).

Acceptance criteria:

- one agreed SKU statement and non-goal list,
- measurable pilot success metrics defined,
- launch blocking risks documented with owners.

## M1 - Install and Bootstrap in < 15 Minutes (Weeks 2-4)

Owner lanes: B, C

Deliverables:

- signed installer outputs for macOS and Linux package path (Windows optional in this phase),
- one-command bootstrap (`make` + console flow) that creates keys, token, and local data dirs,
- service lifecycle scripts integrated and surfaced in console,
- first-run verification flow (health + metrics + basic topic publish/consume).

Acceptance criteria:

- clean-machine install runbook succeeds in under 15 minutes,
- no manual token-principal mismatch in default path,
- rollback path for failed upgrade documented and tested.

## M2 - Operator Experience and Safe Configuration (Weeks 5-8)

Owner lanes: A, B

Deliverables:

- schema-driven config editor for core broker sections,
- guarded advanced control workflow for non-form commands,
- config change audit trail with actor, timestamp, component, and diff metadata,
- backup/rollback/restart flow fully documented and rehearsed.

Acceptance criteria:

- 90% of day-1/day-2 operations possible without direct CLI editing,
- invalid config cannot be applied silently,
- rollback success rate >= 99% in automated config-console tests.

## M3 - Reliability and Supportability (Weeks 9-12)

Owner lanes: E, B, C

Deliverables:

- backup/restore utility and disaster-recovery runbook,
- migration/versioning framework for config and persisted state,
- support bundle export (logs, metrics snapshot, config metadata, audit head/tail),
- rehearsal suite for degraded mode, storage pressure, and auth/policy denial paths.

Acceptance criteria:

- three full live rehearsals pass with documented outcomes,
- restore from backup to healthy state verified on a clean environment,
- support bundle sufficient to diagnose top 10 expected incidents.

## M4 - Security, Compliance, and GA Candidate (Weeks 13-16)

Owner lanes: D, C, A

Deliverables:

- key rotation workflow and operator guide,
- release signing, checksum publication, and SBOM generation,
- dependency and license audit gates in CI,
- final onboarding docs, troubleshooting matrix, and SLA/SLO draft.

Acceptance criteria:

- release artifacts are signed and reproducible from CI,
- no high-severity unresolved findings in security review,
- pilot operators can complete onboarding without engineering assistance.

## Cross-Milestone Workstreams

These run through all milestones:

- documentation freshness checks tied to behavior changes,
- automated test coverage expansion for auth/policy/storage/audit invariants,
- UX instrumentation for setup friction and operational failure points,
- weekly risk review with explicit de-scope decisions when needed.

## Pilot and GA Gates

Pilot entry gate:

- M0 + M1 acceptance criteria complete,
- installer and bootstrap flow validated by at least 2 internal operators,
- critical-path runbook reviewed and approved.

GA candidate gate:

- M2 + M3 + M4 acceptance criteria complete,
- no open P0/P1 defects,
- release checklist and rollback checklist validated in rehearsal.

## Risk Register (Initial)

Risk 1: Scope creep from enterprise asks before desktop path stabilizes  
Mitigation: strict non-goal governance and milestone gates.

Risk 2: Security posture gaps caused by fast UX iteration  
Mitigation: D-lane sign-off required for auth/token/secret handling changes.

Risk 3: Installer/platform matrix complexity delays timeline  
Mitigation: prioritize macOS + Linux first; stage Windows as controlled follow-on.

Risk 4: Operator confusion between capability scopes and policy rules  
Mitigation: guided token generation + principal registry validation in bootstrap flow.

Risk 5: Support burden from low-observability incidents  
Mitigation: support bundle and rehearsal requirements before GA gate.

## Immediate Backlog (Next 10 Working Days)

1. Add bootstrap guardrails so generated tokens map to registered principals by default.
2. Add token-principal-policy diagnostics panel in console.
3. Define installer artifact matrix and CI release workflow skeleton.
4. Draft support bundle schema and initial export command.
5. Convert config-console core sections from raw TOML-only to mixed form + raw mode.
6. Create pilot acceptance checklist file and assign initial owners.
