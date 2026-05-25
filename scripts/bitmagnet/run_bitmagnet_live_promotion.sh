#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/ubuntu/aiostreams"
export AIOSTREAMS_ROOT="$ROOT_DIR"
export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}$ROOT_DIR/scripts:$ROOT_DIR/scripts/bitmagnet:$ROOT_DIR/bitmagnet-media/classifier"
SEARCH_DB="${MAGNETICOLOCAL_SEARCH_DB:-$ROOT_DIR/data/comet-fresh/magnetico/active.search.sqlite3}"
CHECKPOINT="${BITMAGNET_LIVE_PROMOTION_CHECKPOINT:-$ROOT_DIR/data/comet-fresh/magnetico/live-promotion.checkpoint.json}"
LOCK_FILE="${BITMAGNET_PROMOTION_LOCK:-$ROOT_DIR/data/comet-fresh/magnetico/promotion.lock}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
PROMOTION_SCRIPT="$ROOT_DIR/scripts/bitmagnet/promote_bitmagnet_live_to_compact.py"

if [[ ! -f "$SEARCH_DB" ]]; then
echo "search db not found: $SEARCH_DB" >&2
exit 1
fi

if [[ ! -f "$CHECKPOINT" ]]; then
flock -n "$LOCK_FILE" "$PYTHON_BIN" "$PROMOTION_SCRIPT" \
--search-db "$SEARCH_DB" \
--checkpoint "$CHECKPOINT" \
--batch-size 2000 \
--bootstrap-now
exit 0
fi

flock -n "$LOCK_FILE" "$PYTHON_BIN" "$PROMOTION_SCRIPT" \
--search-db "$SEARCH_DB" \
--checkpoint "$CHECKPOINT" \
--batch-size 2000 \
"$@"
