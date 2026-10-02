# Week 2 Installer Artifact Matrix and CI Skeleton

Date: March 26, 2026  
Status: M1 release skeleton hardened through Week 4

## Scope

This artifact defines the minimum release outputs for M1 and maps them to a CI workflow skeleton.

## Artifact Matrix (M1 Baseline)

| Platform | Runner | Target | Output Artifact | Notes |
| --- | --- | --- | --- | --- |
| macOS (Apple Silicon) | `macos-14` | `aarch64-apple-darwin` | `expressways-desktop-macos-aarch64.tar.gz` | Primary desktop path in this phase |
| Linux (x86_64) | `ubuntu-22.04` | `x86_64-unknown-linux-gnu` | `expressways-desktop-linux-x86_64.tar.gz` | Primary self-hosted single-node path |

Each artifact bundle should contain:

1. `expressways-server` binary.
2. `expresswaysctl` binary.
3. `configs/expressways.example.toml`.
4. `scripts/expressways-service.sh`.
5. release notes and checksum file.

## CI Skeleton Mapping

Workflow file: `.github/workflows/release-skeleton.yml`

Current skeleton behavior:

1. Builds matrix artifacts for macOS + Linux.
2. Stages binaries and runtime config/script assets.
3. Archives and uploads per-platform bundles.
4. Generates per-platform checksum metadata.

Week 4 hardening added:

1. Bundle-internal release notes and checksum file (`release-notes.md`, `checksums.txt`).
2. Rollback artifact compatibility check job:
   - validates required bundle paths and executable service script.
3. Release manifest schema constraints validation:
   - validates `channel`, `version`, `checksums`, and `notes` fields.
4. Optional unsigned Tauri desktop bundle job for console packaging (workflow-dispatch controlled).

Deferred to M4 hardening:

1. Artifact signing.
2. SBOM generation and publication.
3. Reproducibility attestation and provenance policy.

## Follow-Up Tasks

1. Integrate signing secrets and notarization/notary steps for non-skeleton release channels.
2. Add release-manifest signature/provenance attestation for M4 GA hardening.
