#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
ACTION="${1:-}"
PLATFORM="${EXPRESSWAYS_SERVICE_PLATFORM:-}"

if [[ -z "$PLATFORM" ]]; then
  case "$(uname -s)" in
    Darwin) PLATFORM="macos" ;;
    Linux) PLATFORM="linux" ;;
    *) echo "Unsupported platform. Use install-user-service.ps1 on Windows." >&2; exit 1 ;;
  esac
fi

case "$PLATFORM" in
  macos|linux) ;;
  *) echo "EXPRESSWAYS_SERVICE_PLATFORM must be macos or linux." >&2; exit 1 ;;
esac

if [[ "$ROOT_DIR" == *$'\n'* || "$ROOT_DIR" == *$'\r'* ]]; then
  echo "Bundle path must not contain line breaks." >&2
  exit 1
fi

SERVICE_SCRIPT="$ROOT_DIR/scripts/expressways-service.sh"
[[ -x "$SERVICE_SCRIPT" ]] || {
  echo "Missing executable lifecycle helper: $SERVICE_SCRIPT" >&2
  exit 1
}

xml_escape() {
  printf '%s' "$1" | sed \
    -e 's/&/\&amp;/g' \
    -e 's/</\&lt;/g' \
    -e 's/>/\&gt;/g' \
    -e 's/"/\&quot;/g' \
    -e "s/'/\&apos;/g"
}

systemd_escape() {
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/%/%%/g'
}

render_macos() {
  local output="$1"
  local escaped_root escaped_script
  escaped_root="$(xml_escape "$ROOT_DIR")"
  escaped_script="$(xml_escape "$SERVICE_SCRIPT")"
  mkdir -p "$(dirname "$output")"
  cat >"$output" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>dev.expressways.backbone</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${escaped_script}</string>
    <string>start-all</string>
  </array>
  <key>WorkingDirectory</key><string>${escaped_root}</string>
  <key>RunAtLoad</key><true/>
  <key>ProcessType</key><string>Background</string>
  <key>StandardOutPath</key><string>${escaped_root}/var/agent/service-control/launch-agent.log</string>
  <key>StandardErrorPath</key><string>${escaped_root}/var/agent/service-control/launch-agent.err.log</string>
</dict>
</plist>
EOF
}

render_linux() {
  local output="$1"
  local escaped_root escaped_script
  escaped_root="$(systemd_escape "$ROOT_DIR")"
  escaped_script="$(systemd_escape "$SERVICE_SCRIPT")"
  mkdir -p "$(dirname "$output")"
  cat >"$output" <<EOF
[Unit]
Description=Expressways local agent backbone
After=network.target

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory="${escaped_root}"
ExecStart=/bin/bash "${escaped_script}" start-all
ExecStop=/bin/bash "${escaped_script}" stop-all
TimeoutStartSec=90
TimeoutStopSec=90

[Install]
WantedBy=default.target
EOF
}

render_definition() {
  local output="$1"
  case "$PLATFORM" in
    macos) render_macos "$output" ;;
    linux) render_linux "$output" ;;
  esac
}

default_path() {
  case "$PLATFORM" in
    macos) printf '%s/Library/LaunchAgents/dev.expressways.backbone.plist' "$HOME" ;;
    linux) printf '%s/.config/systemd/user/expressways-backbone.service' "$HOME" ;;
  esac
}

case "$ACTION" in
  render)
    OUTPUT="${2:-}"
    [[ -n "$OUTPUT" ]] || { echo "render requires an output path" >&2; exit 1; }
    render_definition "$OUTPUT"
    echo "Rendered $PLATFORM user service: $OUTPUT"
    ;;
  install)
    OUTPUT="$(default_path)"
    mkdir -p "$ROOT_DIR/var/agent/service-control/logs"
    render_definition "$OUTPUT"
    if [[ "$PLATFORM" == "macos" ]]; then
      launchctl bootout "gui/$(id -u)/dev.expressways.backbone" >/dev/null 2>&1 || true
      launchctl bootstrap "gui/$(id -u)" "$OUTPUT"
      launchctl enable "gui/$(id -u)/dev.expressways.backbone"
      launchctl kickstart -k "gui/$(id -u)/dev.expressways.backbone"
    else
      systemctl --user daemon-reload
      systemctl --user enable --now expressways-backbone.service
    fi
    echo "Installed Expressways per-user startup service from $ROOT_DIR."
    ;;
  uninstall)
    OUTPUT="$(default_path)"
    "$SERVICE_SCRIPT" stop-all || true
    if [[ "$PLATFORM" == "macos" ]]; then
      launchctl bootout "gui/$(id -u)/dev.expressways.backbone" >/dev/null 2>&1 || true
    else
      systemctl --user disable --now expressways-backbone.service >/dev/null 2>&1 || true
    fi
    rm -f "$OUTPUT"
    if [[ "$PLATFORM" == "linux" ]]; then systemctl --user daemon-reload; fi
    echo "Removed Expressways per-user startup service. Runtime data was preserved."
    ;;
  status)
    if [[ "$PLATFORM" == "macos" ]]; then
      launchctl print "gui/$(id -u)/dev.expressways.backbone"
    else
      systemctl --user status expressways-backbone.service
    fi
    ;;
  *)
    echo "Usage: scripts/install-user-service.sh <install|uninstall|status>" >&2
    echo "       scripts/install-user-service.sh render <output-path>" >&2
    exit 1
    ;;
esac
