#!/usr/bin/env bash
set -euo pipefail

REPO="/home/ubuntu/aiostreams"
PHASE="${1:-phase1}"
MANIFEST_COMET="${MANIFEST_COMET:-https://comet.mrdouglas.uk/manifest.json}"
REPEATS="${REPEATS:-1}"
LIMIT="${LIMIT:-0}"

cd "$REPO"
python3 scripts/run_antibot_3lane.py \
  --phase "$PHASE" \
  --manifest-comet "$MANIFEST_COMET" \
  --repeats "$REPEATS" \
  --limit "$LIMIT"
