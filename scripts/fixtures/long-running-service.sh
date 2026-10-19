#!/usr/bin/env bash
set -euo pipefail

trap 'exit 0' TERM INT
while true; do
  # Wait on a background sleep so TERM runs the trap at once, like the real binaries.
  sleep 1 &
  wait $!
done
