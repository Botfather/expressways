#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/expressways-service-installer.XXXXXX")"
trap 'rm -rf "$TEST_ROOT"' EXIT INT TERM

MACOS_PLIST="$TEST_ROOT/dev.expressways.backbone.plist"
LINUX_UNIT="$TEST_ROOT/expressways-backbone.service"
WINDOWS_XML="$TEST_ROOT/expressways-backbone.xml"

EXPRESSWAYS_SERVICE_PLATFORM=macos \
  scripts/install-user-service.sh render "$MACOS_PLIST" >/dev/null
grep -Fq '<string>dev.expressways.backbone</string>' "$MACOS_PLIST"
grep -Fq '<string>supervise</string>' "$MACOS_PLIST"
grep -Fq '<key>RunAtLoad</key><true/>' "$MACOS_PLIST"
grep -Fq '<key>KeepAlive</key><true/>' "$MACOS_PLIST"
grep -Fq "<string>${REPO_ROOT}/scripts/expressways-service.sh</string>" "$MACOS_PLIST"
if command -v plutil >/dev/null 2>&1; then plutil -lint "$MACOS_PLIST" >/dev/null; fi

EXPRESSWAYS_SERVICE_PLATFORM=linux \
  scripts/install-user-service.sh render "$LINUX_UNIT" >/dev/null
grep -Fq 'Type=simple' "$LINUX_UNIT"
grep -Fq "ExecStart=/bin/bash \"${REPO_ROOT}/scripts/expressways-service.sh\" supervise" "$LINUX_UNIT"
grep -Fq "ExecStop=/bin/bash \"${REPO_ROOT}/scripts/expressways-service.sh\" stop-all" "$LINUX_UNIT"
grep -Fq 'Restart=on-failure' "$LINUX_UNIT"
grep -Fq 'WantedBy=default.target' "$LINUX_UNIT"

if command -v pwsh >/dev/null 2>&1; then
  pwsh -NoProfile -NonInteractive -File scripts/install-user-service.ps1 render "$WINDOWS_XML" >/dev/null
  grep -Fq '<LogonTrigger>' "$WINDOWS_XML"
  grep -Fq '<RunLevel>LeastPrivilege</RunLevel>' "$WINDOWS_XML"
  grep -Fq 'expressways-service.ps1&quot; supervise' "$WINDOWS_XML"
  grep -Fq '<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>' "$WINDOWS_XML"
fi

printf 'Per-user service definitions passed: macOS LaunchAgent, Linux systemd user unit, and available Windows rendering checks.\n'
