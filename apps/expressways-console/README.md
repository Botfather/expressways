# Expressways Console

Tauri-based monitoring console for the Expressways broker.

## Stack

- React + TypeScript
- Tailwind CSS
- Zustand
- Tauri (Rust backend)

## What It Monitors

- Broker health
- Broker metrics and resilience mode
- Adopter status
- Auth state and revocations
- Discovery registry agent list

## Run

From this directory:

```bash
pnpm install
pnpm dev:tauri
```

The console reads connection settings and a capability token from the UI and calls Expressways directly through Tauri commands.

The `Config Console` view supports:

- grouped TOML configuration editing,
- mixed form + raw editing for core broker sections,
- schema-driven field validation hints with server-side apply checks for core broker fields,
- nested table-array editors in form mode for `auth.issuers`, `auth.principals`, `policy.rules`, and `quotas.profiles`,
- diff preview before apply,
- automatic local backup snapshots,
- rollback from backup history,
- restart recommendations with one-click orchestration for supported services,
- append-only config audit trail capture and inspection (`var/agent/config-audit/entries.jsonl`).

Configuration files and rollback snapshots are limited to 1 MiB, read without following symlinks, and replaced through owner-only durable temporary files. The console audit log accepts records up to 64 KiB and reports an error when its 64 MiB capacity is reached; configuration changes are rolled back if their audit append fails. Export and rotate the log before retrying.

It also includes operator controls for M1 workflows:

- `Service Lifecycle` panel for `start|stop|restart|status`,
- `Operator Workflow` panel for `bootstrap_local`, `generate_admin_token`, `verify_first_run`, and `export_support_bundle`,
- guided first-run flow with action history and command output capture.

The `Advanced Control` view supports:

- command-template bootstrapping for raw `ControlCommand` JSON payloads,
- guard acknowledgment + reason requirements for mutating command types,
- optional `attachmentBase64` request bytes for attachment-aware commands,
- response inspection with attachment preview and execution history.

The `Overview` view includes a `Token-Principal-Policy Diagnostics` panel that verifies token format/claims, principal registration and status, key allowlist alignment, scope coverage, and policy-rule coverage for baseline bootstrap operations.

Service orchestration helper used by the restart action:

```bash
scripts/expressways-service.sh <start|stop|restart|status> <expressways-server|nanobot-runtime>
```

## Build

```bash
pnpm test
pnpm build
pnpm build:tauri
```

## Dependency Policy

- Use `pnpm` as the package manager for this app.
- Commit `pnpm-lock.yaml` for deterministic installs in local and CI environments.
- Do not commit `.pnpm-store`; it is local cache state.
