#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/ubuntu/aiostreams"
PYTHON_BIN="${PYTHON_BIN:-python3}"
SEARCH_DB="${MAGNETICOLOCAL_SEARCH_DB:-$ROOT_DIR/data/comet-fresh/magnetico/active.search.sqlite3}"
STATE_PATH="${BITMAGNET_WIDE_DRAIN_STATE:-$ROOT_DIR/data/comet-fresh/magnetico/wide-drain-offline.state.json}"
SEED_CHECKPOINT="${BITMAGNET_WIDE_DRAIN_SEED_CHECKPOINT:-$ROOT_DIR/data/comet-fresh/magnetico/wide-drain.checkpoint.json}"
WORKDIR="${BITMAGNET_WIDE_DRAIN_WORKDIR:-$ROOT_DIR/data/comet-fresh/magnetico/wide-drain-offline}"
LOG_FILE="${BITMAGNET_WIDE_DRAIN_LOG:-$ROOT_DIR/logs/bitmagnet-wide-drain.log}"
LOCK_FILE="${BITMAGNET_WIDE_DRAIN_LOCK_FILE:-$ROOT_DIR/.bitmagnet-wide-drain.lock}"
OFFLINE_SCRIPT="$ROOT_DIR/scripts/drain_bitmagnet_wide_offline.py"
SLICE_MINUTES="${BITMAGNET_WIDE_DRAIN_SLICE_MINUTES:-30}"
CLASSIFIER_WORKERS="${BITMAGNET_WIDE_DRAIN_CLASSIFIER_WORKERS:-2}"
FTS_BATCH_SIZE="${BITMAGNET_WIDE_DRAIN_FTS_BATCH_SIZE:-1000}"
ENSURE_SOURCE_INDEXES="${BITMAGNET_WIDE_DRAIN_ENSURE_SOURCE_INDEXES:-1}"
KEEP_SPOOL_FILES="${BITMAGNET_WIDE_DRAIN_KEEP_SPOOL_FILES:-0}"

mkdir -p "$(dirname "$LOG_FILE")" "$(dirname "$STATE_PATH")" "$WORKDIR"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "wide drain already running: $LOCK_FILE" >&2
  exit 1
fi

{
  echo "[$(date -u +%FT%TZ)] starting BitMagnet offline wide drain slice_minutes=$SLICE_MINUTES classifier_workers=$CLASSIFIER_WORKERS fts_batch_size=$FTS_BATCH_SIZE state=$STATE_PATH workdir=$WORKDIR seed=$SEED_CHECKPOINT"
  exec "$PYTHON_BIN" "$OFFLINE_SCRIPT" \
    --search-db "$SEARCH_DB" \
    --state "$STATE_PATH" \
    --workdir "$WORKDIR" \
    --seed-checkpoint "$SEED_CHECKPOINT" \
    --slice-minutes "$SLICE_MINUTES" \
    --classifier-workers "$CLASSIFIER_WORKERS" \
    --fts-batch-size "$FTS_BATCH_SIZE" \
    --progress-every-slices 1 \
    $([ "$ENSURE_SOURCE_INDEXES" = "1" ] && printf '%s' '--ensure-source-indexes') \
    $([ "$KEEP_SPOOL_FILES" = "1" ] && printf '%s' '--keep-spool-files') \
    "$@"
} >>"$LOG_FILE" 2>&1
