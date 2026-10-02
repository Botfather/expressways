#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/expressways-bundle-upgrade.XXXXXX")"
trap 'rm -rf "$TEST_ROOT"' EXIT INT TERM

write_executable() {
  local path="$1"
  shift
  mkdir -p "$(dirname "$path")"
  printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' "$@" > "$path"
  chmod +x "$path"
}

make_bundle() {
  local root="$1"
  local version="$2"
  local healthy="$3"
  mkdir -p "$root/bin" "$root/scripts" "$root/configs" "$root/var/auth"
  printf '%s\n' "$version" > "$root/bin/version"
  local binary
  for binary in expressways-server expressways-http-gateway expressways-orchestrator \
    expressways-nanobot-system expressways-interop-bridge; do
    write_executable "$root/bin/$binary" 'exit 0'
  done
  write_executable "$root/bin/expresswaysctl" "[[ '$healthy' == 'true' ]]"
  write_executable "$root/scripts/expressways-service.sh" \
    'printf "%s\\n" "$*" >> "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/var/service-actions.log"' \
    'exit 0'
  cp "$REPO_ROOT/scripts/install-user-service.sh" "$root/scripts/"
  cp "$REPO_ROOT/scripts/upgrade-bundle.sh" "$root/scripts/"
  chmod +x "$root/scripts/install-user-service.sh" "$root/scripts/upgrade-bundle.sh"
  printf 'version = "%s"\n' "$version" > "$root/configs/expressways.example.toml"
  printf 'notes for %s\n' "$version" > "$root/release-notes.md"
  (
    cd "$root"
    find bin configs scripts -type f -print0 | sort -z | xargs -0 shasum -a 256 > checksums.txt
  )
}

CURRENT="$TEST_ROOT/current"
GOOD="$TEST_ROOT/good"
BAD="$TEST_ROOT/bad"
make_bundle "$CURRENT" old true
make_bundle "$GOOD" new true
make_bundle "$BAD" broken false
printf 'token\n' > "$CURRENT/var/auth/developer.token"
printf 'operator-owned = true\n' > "$CURRENT/configs/expressways.example.toml"

TOKEN_FILE="$CURRENT/var/auth/developer.token" \
  "$CURRENT/scripts/upgrade-bundle.sh" verify "$GOOD" >/dev/null

TOKEN_FILE="$CURRENT/var/auth/developer.token" HEALTH_ATTEMPTS=1 \
  "$CURRENT/scripts/upgrade-bundle.sh" apply "$GOOD" >/dev/null
grep -qx 'new' "$CURRENT/bin/version"
grep -qx 'operator-owned = true' "$CURRENT/configs/expressways.example.toml"
grep -qx 'version = "new"' "$CURRENT/configs/expressways.example.toml.dist"
grep -qx 'committed' "$CURRENT/var/agent/upgrades/$(cat "$CURRENT/var/agent/upgrades/current")/status"

set +e
TOKEN_FILE="$CURRENT/var/auth/developer.token" HEALTH_ATTEMPTS=1 \
  "$CURRENT/scripts/upgrade-bundle.sh" apply "$BAD" >/dev/null 2>&1
upgrade_status=$?
set -e
[[ "$upgrade_status" -ne 0 ]] || { echo "unhealthy upgrade unexpectedly succeeded" >&2; exit 1; }
grep -qx 'new' "$CURRENT/bin/version"
latest_status="$(find "$CURRENT/var/agent/upgrades" -name status -type f -print | sort | tail -n 1)"
grep -qx 'rolled_back' "$latest_status"

cp -R "$GOOD" "$TEST_ROOT/tampered"
printf 'tampered\n' >> "$TEST_ROOT/tampered/bin/expressways-server"
if "$CURRENT/scripts/upgrade-bundle.sh" verify "$TEST_ROOT/tampered" >/dev/null 2>&1; then
  echo "tampered upgrade bundle unexpectedly passed verification" >&2
  exit 1
fi

printf 'Bundle upgrade transaction passed: checksum gate, state/config preservation, commit, and automatic rollback.\n'
