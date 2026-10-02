# Security Policy

## Supported Versions

Expressways is alpha software. Security fixes are applied to the latest commit on the default branch and, when releases exist, to the latest published release when practical. Older snapshots are not supported unless a release note explicitly says otherwise.

## Reporting a Vulnerability

Do not open a public issue for a suspected vulnerability.

Use GitHub's private vulnerability reporting feature for this repository. Include:

- the affected component and revision,
- reproduction steps or a minimal proof of concept,
- the expected and observed security boundary,
- potential impact,
- and any suggested mitigation.

Do not include live credentials or data belonging to other people. If private vulnerability reporting is unavailable, contact a repository maintainer through their public GitHub profile and ask for a private reporting channel without disclosing vulnerability details.

Maintainers will acknowledge reports on a best-effort basis, investigate impact, coordinate a fix and disclosure, and credit reporters who request attribution. There is currently no guaranteed response-time SLA.

## Security Scope

The security model and mandatory controls are documented in [docs/design/security-compliance-baseline.md](docs/design/security-compliance-baseline.md). The sample configuration is intended for local development and is not a production deployment template. Provider keys, issuer private keys, capability tokens, audit exports, and support bundles must remain outside version control.
