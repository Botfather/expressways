#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/generate-release-sbom.sh --output <path> [--release-version <version>] [--release-channel <channel>]

Generates a CycloneDX JSON SBOM from Cargo metadata.
USAGE
}

OUTPUT_PATH=""
RELEASE_VERSION="v0.0.0-manual"
RELEASE_CHANNEL="unknown"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output)
      OUTPUT_PATH="$2"
      shift 2
      ;;
    --release-version)
      RELEASE_VERSION="$2"
      shift 2
      ;;
    --release-channel)
      RELEASE_CHANNEL="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$OUTPUT_PATH" ]]; then
  echo "--output is required."
  usage
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to generate SBOM JSON."
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required to generate SBOM metadata."
  exit 1
fi

if command -v uuidgen >/dev/null 2>&1; then
  SERIAL_UUID="$(uuidgen | tr '[:upper:]' '[:lower:]')"
else
  raw_uuid="$(openssl rand -hex 16)"
  SERIAL_UUID="${raw_uuid:0:8}-${raw_uuid:8:4}-${raw_uuid:12:4}-${raw_uuid:16:4}-${raw_uuid:20:12}"
fi

GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TMP_METADATA="$(mktemp "${TMPDIR:-/tmp}/expressways-cargo-metadata.XXXXXX.json")"
trap 'rm -f "$TMP_METADATA"' EXIT

cargo metadata --format-version 1 --locked >"$TMP_METADATA"

if [[ -n "$(dirname "$OUTPUT_PATH")" && "$(dirname "$OUTPUT_PATH")" != "." ]]; then
  mkdir -p "$(dirname "$OUTPUT_PATH")"
fi

jq -n \
  --arg serial "urn:uuid:${SERIAL_UUID}" \
  --arg generated_at "$GENERATED_AT" \
  --arg release_version "$RELEASE_VERSION" \
  --arg release_channel "$RELEASE_CHANNEL" \
  --slurpfile metadata "$TMP_METADATA" '
  {
    bomFormat: "CycloneDX",
    specVersion: "1.5",
    serialNumber: $serial,
    version: 1,
    metadata: {
      timestamp: $generated_at,
      tools: [
        {
          vendor: "expressways",
          name: "generate-release-sbom.sh",
          version: "1"
        }
      ],
      component: {
        type: "application",
        name: "expressways",
        version: $release_version,
        properties: [
          {
            name: "expressways:release_channel",
            value: $release_channel
          }
        ]
      }
    },
    components: (
      $metadata[0].packages
      | sort_by(.name, .version)
      | map(
          {
            type: (if (.source // "" | startswith("path+")) or (.source == null) then "application" else "library" end),
            name: .name,
            version: .version,
            purl: ("pkg:cargo/\(.name)@\(.version)"),
            licenses: (if .license == null then [] else [{expression: .license}] end),
            externalReferences: (
              [
                (if .repository then {type: "vcs", url: .repository} else empty end),
                (if .homepage then {type: "website", url: .homepage} else empty end),
                (if .documentation then {type: "documentation", url: .documentation} else empty end)
              ]
            )
          }
          | if (.licenses | length) == 0 then del(.licenses) else . end
          | if (.externalReferences | length) == 0 then del(.externalReferences) else . end
        )
    )
  }
' >"$OUTPUT_PATH"

echo "Generated SBOM: $OUTPUT_PATH"
