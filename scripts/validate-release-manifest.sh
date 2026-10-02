#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: scripts/validate-release-manifest.sh <manifest.json>"
  exit 1
fi

MANIFEST_PATH="$1"
if [[ ! -f "$MANIFEST_PATH" ]]; then
  echo "Manifest not found: $MANIFEST_PATH"
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to validate release manifest schema constraints."
  exit 1
fi

jq -e '
  type == "object" and
  (.channel | type == "string") and
  (.version | type == "string") and
  (.generated_at | type == "string") and
  (.notes | type == "string") and
  (.artifacts | type == "array" and length > 0) and
  (.checksums | type == "object" and (keys | length) > 0)
' "$MANIFEST_PATH" >/dev/null || {
  echo "Manifest missing required top-level keys or has invalid types."
  exit 1
}

jq -e '
  .channel as $channel
  | ($channel == "alpha" or $channel == "beta" or $channel == "stable")
' "$MANIFEST_PATH" >/dev/null || {
  echo "Manifest channel must be one of: alpha, beta, stable."
  exit 1
}

jq -e '
  .version
  | test("^v[0-9]+\\.[0-9]+\\.[0-9]+([-.][A-Za-z0-9]+)*$")
' "$MANIFEST_PATH" >/dev/null || {
  echo "Manifest version must match semantic release tag format (for example v0.1.0 or v0.1.0-beta.1)."
  exit 1
}

jq -e '
  .notes
  | (gsub("\\s+"; " ") | gsub("^\\s+|\\s+$"; "") | length) > 0
' "$MANIFEST_PATH" >/dev/null || {
  echo "Manifest notes must be a non-empty string."
  exit 1
}

jq -e '
  (.checksums | to_entries | length) >= (.artifacts | length)
' "$MANIFEST_PATH" >/dev/null || {
  echo "Manifest checksums map must include at least one checksum per artifact."
  exit 1
}

jq -e '
  .artifacts
  | all(
      .[];
      (type == "object") and
      (.bundle | type == "string" and test("\\.tar\\.gz$")) and
      (.sha256 | type == "string" and test("^[A-Fa-f0-9]{64}$"))
    )
' "$MANIFEST_PATH" >/dev/null || {
  echo "Each artifact must contain bundle (*.tar.gz) and sha256 (64 hex chars)."
  exit 1
}

jq -e '
  . as $manifest
  | ($manifest.artifacts)
  | all(
      .[];
      (
        ($manifest.checksums[.bundle] // "") == .sha256
      )
    )
' "$MANIFEST_PATH" >/dev/null || {
  echo "Checksums map must include each artifact bundle with matching sha256."
  exit 1
}

jq -e '
  .checksums
  | to_entries
  | all(.[]; (.value | type == "string" and test("^[A-Fa-f0-9]{64}$")))
' "$MANIFEST_PATH" >/dev/null || {
  echo "Checksums map entries must be 64-character hexadecimal strings."
  exit 1
}

jq -e '
  if has("sbom") then
    (.sbom | type == "object")
    and (.sbom.format == "cyclonedx-json")
    and (.sbom.path | type == "string" and test("\\.cdx\\.json$"))
    and (.sbom.sha256 | type == "string" and test("^[A-Fa-f0-9]{64}$"))
    and ((.checksums[.sbom.path] // "") == .sbom.sha256)
  else
    true
  end
' "$MANIFEST_PATH" >/dev/null || {
  echo "Optional sbom block must be valid and its checksum must match checksums map entry."
  exit 1
}

jq -e '
  if has("signatures") then
    (.signatures | type == "object")
    and (.signatures.algorithm | type == "string" and length > 0)
    and (.signatures.public_key | type == "string" and length > 0)
    and (.signatures.public_key_sha256 | type == "string" and test("^[A-Fa-f0-9]{64}$"))
    and (.signatures.artifacts | type == "array" and length > 0)
    and (
      . as $manifest
      | ($manifest.signatures.artifacts | all(
          .[];
          (.path | type == "string" and length > 0)
          and (.sha256 | type == "string" and test("^[A-Fa-f0-9]{64}$"))
          and (.signature | type == "string" and test("\\.sig$"))
          and (.signature_sha256 | type == "string" and test("^[A-Fa-f0-9]{64}$"))
          and (($manifest.checksums[.path] // $manifest.checksums[(.path | split("/") | .[-1])] // "") == .sha256)
        ))
    )
  else
    true
  end
' "$MANIFEST_PATH" >/dev/null || {
  echo "Optional signatures block must be valid and reference checksummed artifacts."
  exit 1
}

echo "Manifest schema checks passed: $MANIFEST_PATH"
