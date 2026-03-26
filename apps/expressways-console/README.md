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
- diff preview before apply,
- automatic local backup snapshots,
- rollback from backup history,
- restart recommendations with one-click orchestration for supported services.

Service orchestration helper used by the restart action:

```bash
scripts/expressways-service.sh <start|stop|restart|status> <expressways-server|nanobot-runtime>
```

## Build

```bash
pnpm build
pnpm build:tauri
```

## Dependency Policy

- Use `pnpm` as the package manager for this app.
- Commit `pnpm-lock.yaml` for deterministic installs in local and CI environments.
- Do not commit `.pnpm-store`; it is local cache state.
