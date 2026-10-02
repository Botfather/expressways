# Installation

Status: alpha; source builds are the supported development path

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

When project releases are published, verify the bundle SHA-256 checksum and detached signature metadata before extracting it. Release bundles contain the broker, `expresswaysctl`, the example configuration, and the service helper. They do not contain production credentials.

The example configuration binds to loopback and is for local evaluation. Review identity, policy, quotas, storage limits, audit paths, listener exposure, file ownership, backups, and secret distribution before adapting it to another environment.

## Uninstall

Source builds do not install system-wide files automatically. Stop running processes, remove any service definition you created, and then remove the checkout and explicitly selected runtime directories. Back up audit and regulated data first. Never delete `./var` blindly when it contains data subject to retention requirements.
