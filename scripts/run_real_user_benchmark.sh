#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/aiostreams

MANIFEST_URL="${REAL_USER_MANIFEST_URL:-${MANIFEST_URL:-}}"
if [[ -z "$MANIFEST_URL" ]]; then
  echo "Set REAL_USER_MANIFEST_URL or MANIFEST_URL before running."
  exit 1
fi

python3 ./scripts/run_real_user_benchmark.py \
  --manifest-url "$MANIFEST_URL" \
  "$@"
