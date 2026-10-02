# Contributing to Expressways

Thank you for helping improve Expressways. The project is currently alpha software, so changes should favor explicit contracts, safe defaults, and evidence over speculative scope.

## Before You Start

- Read [README.md](README.md), [docs/design/phase-1-system-design.md](docs/design/phase-1-system-design.md), and [docs/design/security-compliance-baseline.md](docs/design/security-compliance-baseline.md).
- Search existing issues before opening a new one.
- For a large feature or protocol change, open an issue first so its scope and compatibility impact can be discussed.
- Report security vulnerabilities privately as described in [SECURITY.md](SECURITY.md), not in a public issue.

## Development Setup

Requirements:

- Rust `1.93.0` with `rustfmt` and `clippy`
- Node.js 22 and pnpm 10 for the console
- npm for the optional gateway

Run the core checks from the repository root:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

For console changes:

```bash
cd apps/expressways-console
pnpm install --frozen-lockfile
pnpm lint
pnpm test
pnpm build
```

Run `bash scripts/check-repository-hygiene.sh` before submitting a change.

## Change Expectations

- Keep externally reachable operations authenticated, capability-checked, policy-controlled, quota-aware where relevant, and auditable.
- Add tests for success and denial/failure paths.
- Update protocol, server, client, tests, and documentation together when the wire contract changes.
- Preserve structured JSON logging and truthful degraded-mode reporting.
- Never commit tokens, private keys, provider credentials, local state, or generated support bundles.
- Avoid unrelated formatting or refactoring in the same pull request.

## Pull Requests

Describe the problem, the chosen approach, security and compatibility effects, and the commands used to validate the change. Maintainers may ask for an ADR when a change alters a durable architectural decision.

By contributing, you agree that your contribution is licensed under the repository's [MIT License](LICENSE).
