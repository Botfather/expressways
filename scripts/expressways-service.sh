#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ACTION="${1:-}"
SERVICE="${2:-}"

PID_DIR="${PID_DIR:-./var/agent/service-control}"
LOG_DIR="${LOG_DIR:-$PID_DIR/logs}"
mkdir -p "$PID_DIR" "$LOG_DIR"

pid_file() {
  local service="$1"
  echo "$PID_DIR/$service.pid"
}

log_file() {
  local service="$1"
  echo "$LOG_DIR/$service.log"
}

service_command() {
  local service="$1"
  case "$service" in
    expressways-server)
      echo "cargo run -p expressways-server -- --config configs/expressways.example.toml"
      ;;
    nanobot-runtime)
      echo "cargo run -p expressways-nanobot-system -- --transport tcp --address 127.0.0.1:7766 run-runtime --token-file ./var/auth/developer.token --agent-id nanobot-runtime --state-dir ./var/agent/nanobot-runtime --ensure-topics true"
      ;;
    *)
      return 1
      ;;
  esac
}

ensure_known_service() {
  local service="$1"
  if ! service_command "$service" >/dev/null 2>&1; then
    echo "Unsupported service: $service"
    exit 1
  fi
}

is_running() {
  local service="$1"
  local pidf
  pidf="$(pid_file "$service")"
  if [[ ! -f "$pidf" ]]; then
    return 1
  fi
  local pid
  pid="$(cat "$pidf" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    return 1
  fi
  if kill -0 "$pid" >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

start_service() {
  local service="$1"
  ensure_known_service "$service"
  if is_running "$service"; then
    local pidf
    pidf="$(pid_file "$service")"
    echo "Service $service is already running (pid $(cat "$pidf"))."
    return 0
  fi

  local cmd
  cmd="$(service_command "$service")"
  local pidf
  pidf="$(pid_file "$service")"
  local logf
  logf="$(log_file "$service")"

  nohup bash -lc "$cmd" >>"$logf" 2>&1 &
  local pid=$!
  echo "$pid" >"$pidf"

  sleep 1
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    echo "Service $service failed to stay up. Check log: $logf"
    tail -n 40 "$logf" || true
    exit 1
  fi

  echo "Service $service started (pid $pid). Log: $logf"
}

stop_service() {
  local service="$1"
  ensure_known_service "$service"
  local pidf
  pidf="$(pid_file "$service")"
  if [[ ! -f "$pidf" ]]; then
    echo "Service $service is not running (missing pid file)."
    return 0
  fi

  local pid
  pid="$(cat "$pidf" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    rm -f "$pidf"
    echo "Service $service had empty pid file; cleaned up."
    return 0
  fi

  if kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    for _ in {1..20}; do
      if ! kill -0 "$pid" >/dev/null 2>&1; then
        break
      fi
      sleep 0.25
    done
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  fi

  rm -f "$pidf"
  echo "Service $service stopped."
}

status_service() {
  local service="$1"
  ensure_known_service "$service"
  local pidf
  pidf="$(pid_file "$service")"
  if is_running "$service"; then
    echo "Service $service is running (pid $(cat "$pidf"))."
    return 0
  fi
  echo "Service $service is stopped."
  return 1
}

restart_service() {
  local service="$1"
  stop_service "$service"
  start_service "$service"
}

case "$ACTION" in
  start)
    start_service "$SERVICE"
    ;;
  stop)
    stop_service "$SERVICE"
    ;;
  restart)
    restart_service "$SERVICE"
    ;;
  status)
    status_service "$SERVICE"
    ;;
  *)
    echo "Usage: scripts/expressways-service.sh <start|stop|restart|status> <expressways-server|nanobot-runtime>"
    exit 1
    ;;
esac
