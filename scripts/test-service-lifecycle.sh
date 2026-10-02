#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/expressways-service-lifecycle.XXXXXX")"
STALE_PID=""

cleanup() {
  PID_DIR="$FIXTURE_ROOT/var/agent/service-control" \
    bash "$FIXTURE_ROOT/scripts/expressways-service.sh" stop expressways-server >/dev/null 2>&1 || true
  if [[ -n "$STALE_PID" ]]; then
    kill "$STALE_PID" >/dev/null 2>&1 || true
    wait "$STALE_PID" 2>/dev/null || true
  fi
  rm -rf "$FIXTURE_ROOT"
}
trap cleanup EXIT

mkdir -p "$FIXTURE_ROOT/bin" "$FIXTURE_ROOT/scripts" "$FIXTURE_ROOT/configs"
cp "$ROOT_DIR/scripts/expressways-service.sh" "$FIXTURE_ROOT/scripts/"
cp "$ROOT_DIR/scripts/fixtures/long-running-service.sh" "$FIXTURE_ROOT/bin/expressways-server"
chmod +x "$FIXTURE_ROOT/scripts/expressways-service.sh" "$FIXTURE_ROOT/bin/expressways-server"
touch "$FIXTURE_ROOT/configs/expressways.example.toml"

PID_DIR="$FIXTURE_ROOT/var/agent/service-control" \
  bash "$FIXTURE_ROOT/scripts/expressways-service.sh" start expressways-server
PID_DIR="$FIXTURE_ROOT/var/agent/service-control" \
  bash "$FIXTURE_ROOT/scripts/expressways-service.sh" status expressways-server
PID_DIR="$FIXTURE_ROOT/var/agent/service-control" \
  bash "$FIXTURE_ROOT/scripts/expressways-service.sh" stop expressways-server

sleep 30 &
STALE_PID=$!
mkdir -p "$FIXTURE_ROOT/var/agent/service-control"
printf '%s\n' "$STALE_PID" > "$FIXTURE_ROOT/var/agent/service-control/expressways-server.pid"

if PID_DIR="$FIXTURE_ROOT/var/agent/service-control" \
  bash "$FIXTURE_ROOT/scripts/expressways-service.sh" status expressways-server >/dev/null 2>&1; then
  echo "stale pid was incorrectly reported as the managed service"
  exit 1
fi
PID_DIR="$FIXTURE_ROOT/var/agent/service-control" \
  bash "$FIXTURE_ROOT/scripts/expressways-service.sh" stop expressways-server
kill -0 "$STALE_PID"

echo "Service lifecycle checks passed."
