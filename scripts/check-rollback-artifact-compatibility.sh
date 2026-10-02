#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: scripts/check-rollback-artifact-compatibility.sh <artifact.tar.gz> [artifact.tar.gz ...]"
  exit 1
fi

REQUIRED_SUFFIXES=(
  "/bin/expressways-server"
  "/bin/expresswaysctl"
  "/configs/expressways.example.toml"
  "/scripts/expressways-service.sh"
  "/release-notes.md"
  "/checksums.txt"
)

check_artifact() {
  local artifact="$1"
  if [[ ! -f "$artifact" ]]; then
    echo "Artifact not found: $artifact"
    return 1
  fi

  local listing
  listing="$(tar -tzf "$artifact")"
  if [[ -z "$listing" ]]; then
    echo "Artifact is empty or unreadable: $artifact"
    return 1
  fi

  local top_level
  top_level="$(echo "$listing" | awk -F/ 'NF > 1 {print $1}' | sort -u)"
  local top_count
  top_count="$(echo "$top_level" | sed '/^$/d' | wc -l | tr -d ' ')"
  if [[ "$top_count" -ne 1 ]]; then
    echo "Artifact must contain exactly one top-level directory: $artifact"
    echo "Found top-level entries:"
    echo "$top_level"
    return 1
  fi

  local suffix
  for suffix in "${REQUIRED_SUFFIXES[@]}"; do
    if ! echo "$listing" | grep -Eq ".+${suffix}$"; then
      echo "Missing required rollback path suffix ${suffix} in $artifact"
      return 1
    fi
  done

  # Ensure the service script is executable in the archive for replay convenience.
  if ! tar -tvzf "$artifact" | awk '$NF ~ /\/scripts\/expressways-service\.sh$/ {print $1}' | grep -q 'x'; then
    echo "scripts/expressways-service.sh is not marked executable in $artifact"
    return 1
  fi

  return 0
}

for artifact in "$@"; do
  echo "Checking rollback compatibility for: $artifact"
  check_artifact "$artifact"
done

echo "Rollback compatibility checks passed for $# artifact(s)."
