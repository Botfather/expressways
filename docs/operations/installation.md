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

After credentials are provisioned, install automatic per-user startup without
administrator privileges:

```bash
scripts/install-user-service.sh install   # macOS LaunchAgent or Linux systemd --user
scripts/install-user-service.sh status
```

On Windows PowerShell:

```powershell
.\scripts\install-user-service.ps1 install
.\scripts\install-user-service.ps1 status
```

The registration points at this exact extracted bundle. Moving it manually
leaves the native startup definition pointing at the old path.
Uninstalling the startup definition stops the stack but deliberately preserves
configuration, audit records, credentials, and runtime data.

## Transactional Bundle Upgrades

Extract the new bundle beside the installed bundle, then verify and apply it:

```bash
scripts/upgrade-bundle.sh verify /path/to/extracted-new-bundle
scripts/upgrade-bundle.sh apply /path/to/extracted-new-bundle
```

```powershell
.\scripts\upgrade-bundle.ps1 verify C:\path\to\extracted-new-bundle
.\scripts\upgrade-bundle.ps1 apply C:\path\to\extracted-new-bundle
```

The transaction verifies every file listed by the incoming bundle checksum
manifest before stopping the stack. It preserves `var/` and the active
`configs/expressways.example.toml`, snapshots the previous managed binaries and
scripts, starts the new stack, and requires an authenticated broker health
check. Failed startup or health validation automatically restores and verifies
the previous payload. The new example config is saved as
`configs/expressways.example.toml.dist` for manual review rather than replacing
operator policy.

The example configuration binds to loopback and is for local evaluation. Review identity, policy, quotas, storage limits, audit paths, listener exposure, file ownership, backups, and secret distribution before adapting it to another environment.

## Uninstall

Remove the per-user startup definition with `scripts/install-user-service.sh
uninstall` or `.\scripts\install-user-service.ps1 uninstall`, then remove the
bundle and explicitly selected runtime directories. Back up audit and regulated
data first. Never delete `./var` blindly when it contains data subject to
retention requirements.
