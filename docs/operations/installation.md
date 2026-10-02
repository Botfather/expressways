# Installation

Status: alpha; source builds and checksum-verified release bundles are supported

## Requirements

- Rust `1.93.0` with `rustfmt` and `clippy`
- Git
- Node.js 22 and pnpm 10 only when building the console
- A Unix-like shell for the bundled operational scripts

## Build from Source

```bash
git clone https://github.com/Botfather/expressways.git
cd expressways
cargo build --workspace --all-features
cargo test --workspace --all-features
```

Generate local development credentials and start the broker:

```bash
make bootstrap-local
make run-expressways
```

In another terminal:

```bash
make verify-first-run
```

Generated keys, tokens, state, and logs are written beneath `./var`; temporary sockets use `./tmp`. Both locations are ignored by Git.

## Console

```bash
cd apps/expressways-console
pnpm install --frozen-lockfile
pnpm dev:tauri
```

See the [console README](../../apps/expressways-console/README.md) for its current capabilities.

## Release Bundles

When project releases are published, select the macOS arm64, Linux x86-64, or Windows x86-64 bundle and verify its SHA-256 checksum and detached signature metadata before extracting it. Release bundles contain the broker, `expresswaysctl`, HTTP gateway, orchestrator, Nanobot runtime, chat bridge, example configuration, and lifecycle helpers. They do not contain credentials.

On macOS and Linux, manage individual components or the standard local stack with:

```bash
scripts/expressways-service.sh start expressways-server
scripts/expressways-service.sh start-all
scripts/expressways-service.sh status-all
scripts/expressways-service.sh stop-all
```

On Windows PowerShell, use the equivalent helper:

```powershell
.\scripts\expressways-service.ps1 start expressways-server
.\scripts\expressways-service.ps1 start-all
.\scripts\expressways-service.ps1 status-all
.\scripts\expressways-service.ps1 stop-all
```

The broker can start without a capability token. The gateway can then start and will authenticate each request with the caller's token. The orchestrator and Nanobot runtime require `var/auth/developer.token`; generate it with `expresswaysctl` before starting the whole stack. Override `CONFIG_PATH`, `BROKER_ADDRESS`, `HTTP_LISTEN`, or `TOKEN_FILE` in the environment when using non-default paths or ports. Logs and PID records are kept beneath `var/agent/service-control`.

The example configuration binds to loopback and is for local evaluation. Review identity, policy, quotas, storage limits, audit paths, listener exposure, file ownership, backups, and secret distribution before adapting it to another environment.

## Uninstall

Source builds do not install system-wide files automatically. Stop running processes, remove any service definition you created, and then remove the checkout and explicitly selected runtime directories. Back up audit and regulated data first. Never delete `./var` blindly when it contains data subject to retention requirements.
