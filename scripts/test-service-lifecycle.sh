#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/expressways-service-lifecycle.XXXXXX")"
STALE_PID=""
SUPERVISOR_PID=""

cleanup() {
  PID_DIR="$FIXTURE_ROOT/var/agent/service-control" \
    bash "$FIXTURE_ROOT/scripts/expressways-service.sh" stop expressways-server >/dev/null 2>&1 || true
  if [[ -n "$STALE_PID" ]]; then
    kill "$STALE_PID" >/dev/null 2>&1 || true
    wait "$STALE_PID" 2>/dev/null || true
  fi
  if [[ -n "$SUPERVISOR_PID" ]]; then
    kill "$SUPERVISOR_PID" >/dev/null 2>&1 || true
    wait "$SUPERVISOR_PID" 2>/dev/null || true
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

for binary in expressways-http-gateway expressways-orchestrator expressways-nanobot-system; do
  cp "$ROOT_DIR/scripts/fixtures/long-running-service.sh" "$FIXTURE_ROOT/bin/$binary"
  chmod +x "$FIXTURE_ROOT/bin/$binary"
done
cp "$ROOT_DIR/scripts/fixtures/health-probe.sh" "$FIXTURE_ROOT/bin/expresswaysctl"
chmod +x "$FIXTURE_ROOT/bin/expresswaysctl"
mkdir -p "$FIXTURE_ROOT/var/auth"
printf 'fixture-token\n' > "$FIXTURE_ROOT/var/auth/developer.token"

SUPERVISOR_INTERVAL_SECONDS=0.1 \
HEALTH_FAILURE_THRESHOLD=1 \
PID_DIR="$FIXTURE_ROOT/var/agent/service-control" \
  bash "$FIXTURE_ROOT/scripts/expressways-service.sh" supervise \
  >"$FIXTURE_ROOT/supervisor.log" 2>&1 &
SUPERVISOR_PID=$!

SERVER_PID_FILE="$FIXTURE_ROOT/var/agent/service-control/expressways-server.pid"
for _ in $(seq 1 50); do
  [[ -s "$SERVER_PID_FILE" ]] && break
  sleep 0.1
done
[[ -s "$SERVER_PID_FILE" ]] || { cat "$FIXTURE_ROOT/supervisor.log"; exit 1; }
ORIGINAL_SERVER_PID="$(cat "$SERVER_PID_FILE")"
kill "$ORIGINAL_SERVER_PID"
for _ in $(seq 1 50); do
  RECOVERED_SERVER_PID="$(cat "$SERVER_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$RECOVERED_SERVER_PID" && "$RECOVERED_SERVER_PID" != "$ORIGINAL_SERVER_PID" ]] \
    && kill -0 "$RECOVERED_SERVER_PID" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
[[ "${RECOVERED_SERVER_PID:-}" != "$ORIGINAL_SERVER_PID" ]] \
  || { echo "supervisor did not recover the stopped broker"; cat "$FIXTURE_ROOT/supervisor.log"; exit 1; }

HEALTH_RECOVERY_BASE_PID="$RECOVERED_SERVER_PID"
touch "$FIXTURE_ROOT/var/health-fail"
for _ in $(seq 1 50); do
  HEALTH_RECOVERED_PID="$(cat "$SERVER_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$HEALTH_RECOVERED_PID" && "$HEALTH_RECOVERED_PID" != "$HEALTH_RECOVERY_BASE_PID" ]] \
    && kill -0 "$HEALTH_RECOVERED_PID" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
rm -f "$FIXTURE_ROOT/var/health-fail"
[[ "${HEALTH_RECOVERED_PID:-}" != "$HEALTH_RECOVERY_BASE_PID" ]] \
  || { echo "supervisor did not recover the unhealthy broker"; cat "$FIXTURE_ROOT/supervisor.log"; exit 1; }

kill "$SUPERVISOR_PID"
wait "$SUPERVISOR_PID" 2>/dev/null || true
SUPERVISOR_PID=""

echo "Service lifecycle and supervised recovery checks passed."
