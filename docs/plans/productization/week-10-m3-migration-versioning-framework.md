# Week 10 (M3): Migration and Versioning Framework

Date: March 26, 2026  
Owner lanes: E, B

## Objective

Add an explicit migration/versioning framework for runtime config and persisted state so upgrades stay predictable, auditable, and fail-safe.

## Delivered Changes

1. Config schema versioning (`[schema].version`) with compatibility handling:
   - Current supported config schema version is `1`.
   - Legacy configs without a schema section are treated as schema version `0` and loaded in compatibility mode.
   - Newer/unknown config schema versions fail fast during startup with a clear error.

2. Persisted-state schema migration hooks:
   - Registry state (`agents.json`) now treats missing `schema_version` as legacy (`0`), migrates to `1`, and rewrites the file.
   - Storage topic state (`state.json`) now includes `schema_version`; legacy state is migrated to `1` and rewritten on read.
   - Auth revocation state (`revocations.json`) now includes `schema_version`; legacy files are migrated to `1` and rewritten on load.
   - Orchestrator state (`./var/orchestrator/state.json`) now persists with `schema_version`; legacy files are migrated to `1` and rewritten on load.

3. Forward-compatibility guardrails:
   - Each schema-aware loader rejects unknown future schema versions instead of silently accepting incompatible formats.
   - Errors include both found and supported versions for operator diagnostics.

## Validation Evidence

Executed:

```bash
cargo fmt
cargo test -p expressways-server -p expressways-storage -p expressways-auth -p expressways-orchestrator
```

Coverage includes:

- legacy-to-current migration tests,
- unsupported-schema rejection tests,
- persistence rewrite checks after migration.

## Operator Notes

- Existing deployments continue working with legacy files.
- Legacy state files are upgraded in-place the first time they are loaded.
- Config schema metadata should be kept in `configs/expressways.example.toml` and downstream derived configs to simplify upgrade diagnostics.
