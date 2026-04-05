#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/ubuntu/aiostreams"
SEARCH_DB="${MAGNETICOLOCAL_SEARCH_DB:-$ROOT_DIR/data/comet-fresh/magnetico/active.search.sqlite3}"
CHECKPOINT="${BITMAGNET_LIVE_PROMOTION_CHECKPOINT:-$ROOT_DIR/data/comet-fresh/magnetico/live-promotion.checkpoint.json}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
PROMOTION_SCRIPT="$ROOT_DIR/scripts/promote_bitmagnet_live_to_compact.py"

if [[ ! -f "$SEARCH_DB" ]]; then
  echo "search db not found: $SEARCH_DB" >&2
  exit 1
fi

if [[ ! -f "$CHECKPOINT" ]]; then
  "$PYTHON_BIN" "$PROMOTION_SCRIPT" \
    --search-db "$SEARCH_DB" \
    --checkpoint "$CHECKPOINT" \
    --bootstrap-now
  exit 0
fi

"$PYTHON_BIN" "$PROMOTION_SCRIPT" \
  --search-db "$SEARCH_DB" \
  --checkpoint "$CHECKPOINT" \
  "$@"
