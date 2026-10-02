#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
ACTION="${1:-}"
SOURCE_DIR="${2:-}"
TOKEN_FILE="${TOKEN_FILE:-$ROOT_DIR/var/auth/developer.token}"
BROKER_ADDRESS="${BROKER_ADDRESS:-127.0.0.1:7766}"
HEALTH_ATTEMPTS="${HEALTH_ATTEMPTS:-40}"
HEALTH_DELAY_SECONDS="${HEALTH_DELAY_SECONDS:-0.25}"
UPGRADE_ROOT="$ROOT_DIR/var/agent/upgrades"
LOCK_DIR="$ROOT_DIR/var/agent/upgrade.lock"

usage() {
  echo "Usage: scripts/upgrade-bundle.sh verify <extracted-bundle-directory>" >&2
  echo "       scripts/upgrade-bundle.sh apply <extracted-bundle-directory>" >&2
}

validate_source() {
  [[ -d "$SOURCE_DIR" ]] || { echo "Upgrade bundle directory not found: $SOURCE_DIR" >&2; return 1; }
  SOURCE_DIR="$(cd "$SOURCE_DIR" && pwd -P)"
  [[ "$SOURCE_DIR" != "$ROOT_DIR" ]] || { echo "Upgrade source must differ from the installed bundle." >&2; return 1; }
  local required
  for required in \
    bin/expressways-server \
    bin/expresswaysctl \
    bin/expressways-http-gateway \
    bin/expressways-orchestrator \
    bin/expressways-nanobot-system \
    bin/expressways-interop-bridge \
    scripts/expressways-service.sh \
    scripts/install-user-service.sh \
    scripts/upgrade-bundle.sh \
    configs/expressways.example.toml \
    checksums.txt; do
    [[ -f "$SOURCE_DIR/$required" ]] || {
      echo "Upgrade bundle is missing required path: $required" >&2
      return 1
    }
  done

  while IFS= read -r checksum_line; do
    [[ -n "$checksum_line" ]] || continue
    local expected relative actual
    if [[ "$checksum_line" =~ ^([0-9a-fA-F]{64})\ \ (.+)$ ]]; then
      expected="${BASH_REMATCH[1]}"
      relative="${BASH_REMATCH[2]}"
    else
      echo "Invalid checksum entry: $checksum_line" >&2
      return 1
    fi
    [[ "$relative" != /* && "$relative" != *\\* && "/$relative/" != *"/../"* ]] || {
      echo "Unsafe checksum path: $relative" >&2
      return 1
    }
    [[ -f "$SOURCE_DIR/$relative" ]] || { echo "Checksummed file is missing: $relative" >&2; return 1; }
    if command -v sha256sum >/dev/null 2>&1; then
      actual="$(sha256sum "$SOURCE_DIR/$relative" | awk '{print $1}')"
    else
      actual="$(shasum -a 256 "$SOURCE_DIR/$relative" | awk '{print $1}')"
    fi
    [[ "$actual" == "$expected" ]] || { echo "Checksum mismatch: $relative" >&2; return 1; }
  done < "$SOURCE_DIR/checksums.txt"
}

health_check() {
  [[ -f "$TOKEN_FILE" ]] || { echo "Health token is missing: $TOKEN_FILE" >&2; return 1; }
  local attempt
  for attempt in $(seq 1 "$HEALTH_ATTEMPTS"); do
    if "$ROOT_DIR/bin/expresswaysctl" --transport tcp --address "$BROKER_ADDRESS" \
      health --token-file "$TOKEN_FILE" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$HEALTH_DELAY_SECONDS"
  done
  return 1
}

restore_previous() {
  local backup="$1"
  "$ROOT_DIR/scripts/expressways-service.sh" stop-all >/dev/null 2>&1 || true
  mkdir -p "$backup/failed"
  [[ ! -e "$ROOT_DIR/bin" ]] || mv "$ROOT_DIR/bin" "$backup/failed/bin"
  [[ ! -e "$ROOT_DIR/scripts" ]] || mv "$ROOT_DIR/scripts" "$backup/failed/scripts"
  mv "$backup/previous/bin" "$ROOT_DIR/bin"
  mv "$backup/previous/scripts" "$ROOT_DIR/scripts"
  "$ROOT_DIR/scripts/expressways-service.sh" start-all
}

apply_upgrade() {
  validate_source
  [[ -f "$TOKEN_FILE" ]] || { echo "Refusing upgrade without health token: $TOKEN_FILE" >&2; return 1; }
  mkdir -p "$UPGRADE_ROOT"
  if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "Another upgrade transaction is active: $LOCK_DIR" >&2
    return 1
  fi
  trap 'rmdir "$LOCK_DIR" >/dev/null 2>&1 || true' EXIT INT TERM

  local transaction staging backup
  transaction="$(date -u +%Y%m%dT%H%M%SZ)-$$"
  backup="$UPGRADE_ROOT/$transaction"
  staging="$backup/staging"
  mkdir -p "$staging" "$backup/previous"
  cp -R "$SOURCE_DIR/bin" "$staging/bin"
  cp -R "$SOURCE_DIR/scripts" "$staging/scripts"
  cp "$SOURCE_DIR/checksums.txt" "$staging/checksums.txt"
  cp "$SOURCE_DIR/configs/expressways.example.toml" "$staging/expressways.example.toml"
  [[ ! -f "$SOURCE_DIR/release-notes.md" ]] || cp "$SOURCE_DIR/release-notes.md" "$staging/release-notes.md"

  "$ROOT_DIR/scripts/expressways-service.sh" stop-all
  mv "$ROOT_DIR/bin" "$backup/previous/bin"
  mv "$ROOT_DIR/scripts" "$backup/previous/scripts"
  mv "$staging/bin" "$ROOT_DIR/bin"
  mv "$staging/scripts" "$ROOT_DIR/scripts"
  chmod +x "$ROOT_DIR/scripts/expressways-service.sh" \
    "$ROOT_DIR/scripts/install-user-service.sh" "$ROOT_DIR/scripts/upgrade-bundle.sh"

  if ! "$ROOT_DIR/scripts/expressways-service.sh" start-all || ! health_check; then
    echo "Upgrade validation failed; restoring previous release." >&2
    restore_previous "$backup"
    health_check || { echo "Rollback restored files but broker health is still failing." >&2; return 1; }
    printf 'rolled_back\n' > "$backup/status"
    return 1
  fi

  cp "$staging/checksums.txt" "$ROOT_DIR/checksums.txt"
  cp "$staging/expressways.example.toml" "$ROOT_DIR/configs/expressways.example.toml.dist"
  [[ ! -f "$staging/release-notes.md" ]] || cp "$staging/release-notes.md" "$ROOT_DIR/release-notes.md"
  printf 'committed\n' > "$backup/status"
  printf '%s\n' "$transaction" > "$UPGRADE_ROOT/current"
  echo "Upgrade committed. Previous managed payload: $backup/previous"
}

case "$ACTION" in
  verify) validate_source; echo "Upgrade bundle verification passed: $SOURCE_DIR" ;;
  apply) apply_upgrade ;;
  *) usage; exit 1 ;;
esac
