#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/sign-release-artifacts.sh --private-key <path> --manifest-output <path> [--public-key <path>] [--signature-ext <ext>] <artifact>...

Creates detached signatures for each artifact and emits a signature metadata manifest JSON.
USAGE
}

PRIVATE_KEY_PATH=""
PUBLIC_KEY_PATH=""
MANIFEST_OUTPUT=""
SIGNATURE_EXT="sig"
ARTIFACTS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --private-key)
      PRIVATE_KEY_PATH="$2"
      shift 2
      ;;
    --public-key)
      PUBLIC_KEY_PATH="$2"
      shift 2
      ;;
    --manifest-output)
      MANIFEST_OUTPUT="$2"
      shift 2
      ;;
    --signature-ext)
      SIGNATURE_EXT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      while [[ $# -gt 0 ]]; do
        ARTIFACTS+=("$1")
        shift
      done
      ;;
    -*)
      echo "Unknown argument: $1"
      usage
      exit 1
      ;;
    *)
      ARTIFACTS+=("$1")
      shift
      ;;
  esac
done

if [[ -z "$PRIVATE_KEY_PATH" || -z "$MANIFEST_OUTPUT" ]]; then
  echo "--private-key and --manifest-output are required."
  usage
  exit 1
fi

if [[ "${#ARTIFACTS[@]}" -eq 0 ]]; then
  echo "At least one artifact is required."
  usage
  exit 1
fi

if [[ ! -f "$PRIVATE_KEY_PATH" ]]; then
  echo "Private key not found: $PRIVATE_KEY_PATH"
  exit 1
fi

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required for signing."
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for signature manifest output."
  exit 1
fi

hash_file() {
  local file_path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file_path" | awk '{print $1}'
  else
    shasum -a 256 "$file_path" | awk '{print $1}'
  fi
}

if [[ -z "$PUBLIC_KEY_PATH" ]]; then
  PUBLIC_KEY_PATH="$(dirname "$MANIFEST_OUTPUT")/release-signing-public.pem"
fi

mkdir -p "$(dirname "$MANIFEST_OUTPUT")"
mkdir -p "$(dirname "$PUBLIC_KEY_PATH")"

openssl pkey -in "$PRIVATE_KEY_PATH" -pubout -out "$PUBLIC_KEY_PATH" >/dev/null 2>&1

PUBLIC_KEY_SHA256="$(
  openssl pkey -pubin -in "$PUBLIC_KEY_PATH" -outform DER \
    | openssl dgst -sha256 -r \
    | awk '{print $1}'
)"

GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
ARTIFACT_ENTRIES='[]'

for artifact in "${ARTIFACTS[@]}"; do
  if [[ ! -f "$artifact" ]]; then
    echo "Artifact not found: $artifact"
    exit 1
  fi
  signature_path="${artifact}.${SIGNATURE_EXT}"

  openssl dgst -sha256 -sign "$PRIVATE_KEY_PATH" -out "$signature_path" "$artifact"
  openssl dgst -sha256 -verify "$PUBLIC_KEY_PATH" -signature "$signature_path" "$artifact" >/dev/null

  artifact_sha256="$(hash_file "$artifact")"
  signature_sha256="$(hash_file "$signature_path")"

  ARTIFACT_ENTRIES="$(
    jq \
      --arg path "$artifact" \
      --arg sha "$artifact_sha256" \
      --arg signature "$signature_path" \
      --arg signature_sha "$signature_sha256" \
      '. + [{
        "path": $path,
        "sha256": $sha,
        "signature": $signature,
        "signature_sha256": $signature_sha
      }]' <<<"$ARTIFACT_ENTRIES"
  )"
done

jq -n \
  --arg generated_at "$GENERATED_AT" \
  --arg algorithm "openssl-dgst-sha256" \
  --arg public_key "$PUBLIC_KEY_PATH" \
  --arg public_key_sha256 "$PUBLIC_KEY_SHA256" \
  --argjson artifacts "$ARTIFACT_ENTRIES" \
  '{
    generated_at: $generated_at,
    algorithm: $algorithm,
    public_key: $public_key,
    public_key_sha256: $public_key_sha256,
    artifacts: $artifacts
  }' >"$MANIFEST_OUTPUT"

echo "Generated signatures manifest: $MANIFEST_OUTPUT"
