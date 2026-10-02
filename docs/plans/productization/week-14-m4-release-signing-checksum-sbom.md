# Week 14 (M4): Release Signing, Checksum Publication, and SBOM Generation

Date: March 28, 2026  
Owner lanes: D, C

## Objective

Close the second M4 deliverable by hardening release outputs with deterministic checksum publication, SBOM generation, and detached signature support in CI.

## Delivered Changes

1. Extended release workflow for security/compliance release metadata:
   - file: `.github/workflows/release-skeleton.yml`
   - added `signing_mode` workflow input (`auto|required|disabled`)
   - added aggregate checksum publication (`dist/release-checksums.txt`)
   - added CycloneDX SBOM generation (`dist/release-sbom.cdx.json`)
   - added detached signature flow for release outputs when `EXPRESSWAYS_RELEASE_SIGNING_PRIVATE_KEY_PEM` is configured
   - added signature metadata attachment into `release-manifest.json`
   - removes private signing key from workspace before artifact upload

2. Added release SBOM generator:
   - `scripts/generate-release-sbom.sh`
   - emits CycloneDX JSON from `cargo metadata` with release version/channel metadata.

3. Added detached-signature utility:
   - `scripts/sign-release-artifacts.sh`
   - signs files with OpenSSL SHA-256 detached signatures (`*.sig`)
   - verifies signatures and emits `release-signatures.json`.

4. Extended release-manifest schema constraints:
   - `.github/release-manifest.schema.json`
   - supports optional `sbom` and `signatures` blocks while preserving backward compatibility.

5. Extended manifest validator coverage:
   - `scripts/validate-release-manifest.sh`
   - validates optional `sbom`/`signatures` blocks and checksum consistency rules.

6. Updated operator docs:
   - `README.md` release guardrails now describe signed release flow, checksum publication, and SBOM outputs.

## Validation Evidence

Executed locally:

```bash
bash -n scripts/generate-release-sbom.sh
bash -n scripts/sign-release-artifacts.sh
bash -n scripts/validate-release-manifest.sh
bash scripts/generate-release-sbom.sh --output /tmp/expressways-release-sbom-test.cdx.json --release-version v0.1.0-beta.1 --release-channel beta
bash scripts/sign-release-artifacts.sh --private-key /tmp/<key>.pem --manifest-output /tmp/<dir>/release-signatures.json /tmp/<dir>/expressways-desktop-linux-x86_64.tar.gz /tmp/<dir>/release-checksums.txt /tmp/<dir>/release-sbom.cdx.json
bash scripts/validate-release-manifest.sh /tmp/<dir>/release-manifest.json
```

Validated outputs:

- `/tmp/expressways-release-sbom-test.cdx.json`
- `/tmp/expressways-sign-test.*/*` fixture run with passing manifest validation

## M4 Progress

This week closes the second M4 deliverable:

- release signing, checksum publication, and SBOM generation.
