#!/usr/bin/env bash
set -euo pipefail

SPEC="docs/design/schemas/expressways-http-v1.openapi.json"

jq -e '
  (.openapi | startswith("3.1.")) and
  (.info.title | length > 0) and
  (.info.version | length > 0) and
  (.paths | length > 0) and
  (.components.securitySchemes.expresswaysCapability.type == "http") and
  ([.paths[] | to_entries[] | select(.key == "get" or .key == "post" or .key == "put" or .key == "patch" or .key == "delete") | .value.operationId] as $ids |
    ($ids | all(type == "string" and length > 0)) and
    (($ids | length) == ($ids | unique | length)))
' "$SPEC" >/dev/null

printf '%s\n' 'OpenAPI contract check passed.'

