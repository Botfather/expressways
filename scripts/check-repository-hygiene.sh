#!/usr/bin/env bash
set -euo pipefail

required=(
  LICENSE
  README.md
  CONTRIBUTING.md
  SECURITY.md
  CODE_OF_CONDUCT.md
  GOVERNANCE.md
  SUPPORT.md
  CHANGELOG.md
)

for path in "${required[@]}"; do
  if [[ ! -s "$path" ]]; then
    printf 'required public repository file is missing or empty: %s\n' "$path" >&2
    exit 1
  fi
done

if git ls-files | grep -E '(^|/)(\.DS_Store|\.env([^/]*)?|node_modules|target|tmp|var)(/|$)|\.(private|token)$' >&2; then
  printf 'generated state, local environment, or secret-like files are tracked\n' >&2
  exit 1
fi

if git grep -n -I -E '(/Users/|/home/[^ /]+)' -- ':!scripts/check-repository-hygiene.sh'; then
  printf 'host-specific absolute paths are present in tracked text\n' >&2
  exit 1
fi

if git grep -n -I -E 'BEGIN (RSA |EC |OPENSSH |PRIVATE )?PRIVATE KEY|github_pat_[A-Za-z0-9_]{20,}|ghp_[A-Za-z0-9]{20,}' -- ':!scripts/check-repository-hygiene.sh'; then
  printf 'private-key material or token-like credentials are present in tracked text\n' >&2
  exit 1
fi

bash scripts/check-documentation.sh
git diff --check
