#!/usr/bin/env bash
set -euo pipefail

status=0

while IFS= read -r document; do
  while IFS= read -r destination; do
    destination="${destination#<}"
    destination="${destination%>}"
    case "$destination" in
      ""|\#*|http://*|https://*|mailto:*|codex:*|plugin://*) continue ;;
    esac

    target="${destination%%#*}"
    target="${target//%20/ }"
    if [[ "$target" = /* ]]; then
      printf 'absolute documentation link in %s: %s\n' "$document" "$destination" >&2
      status=1
      continue
    fi

    resolved="$(dirname "$document")/$target"
    if [[ ! -e "$resolved" ]]; then
      printf 'broken documentation link in %s: %s\n' "$document" "$destination" >&2
      status=1
    fi
  done < <(perl -ne 'while (/!?(?:\[[^]]*\])\(([^)]+)\)/g) { print "$1\n" }' "$document")
done < <(
  find . -type f -name '*.md' \
    -not -path './.git/*' \
    -not -path './target/*' \
    -not -path './var/*' \
    -not -path '*/node_modules/*' \
    | sort
)

if git grep -n -I -E 'docs/main\.md|docs/plans/' -- '*.md'; then
  printf 'stale removed-document references found\n' >&2
  status=1
fi

if ! bash scripts/check-openapi.sh; then
  status=1
fi

exit "$status"
