#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/ubuntu/aiostreams"
LOCK_FILE="${BITMAGNET_PRUNE_LOCK_FILE:-$ROOT_DIR/.bitmagnet-prune.lock}"
SEARCH_DB="${MAGNETICOLOCAL_SEARCH_DB:-$ROOT_DIR/data/comet-fresh/magnetico/active.search.sqlite3}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
PRUNE_SCRIPT="$ROOT_DIR/scripts/prune_bitmagnet_workspace.py"
RETENTION_DAYS="${BITMAGNET_PRUNE_RETENTION_DAYS:-3}"
DOCKER_CONTAINER="${BITMAGNET_PRUNE_DOCKER_CONTAINER:-bitmagnet-postgres}"
BATCH_SIZE="${BITMAGNET_PRUNE_BATCH_SIZE:-1000}"
QUEUE_BATCH_SIZE="${BITMAGNET_PRUNE_QUEUE_BATCH_SIZE:-5000}"
ORPHAN_BATCH_SIZE="${BITMAGNET_PRUNE_ORPHAN_BATCH_SIZE:-1000}"

mkdir -p "$(dirname "$LOCK_FILE")"

exec flock -n "$LOCK_FILE" "$PYTHON_BIN" "$PRUNE_SCRIPT" \
  --search-db "$SEARCH_DB" \
  --docker-container "$DOCKER_CONTAINER" \
  --retention-days "$RETENTION_DAYS" \
  --batch-size "$BATCH_SIZE" \
  --queue-batch-size "$QUEUE_BATCH_SIZE" \
  --orphan-batch-size "$ORPHAN_BATCH_SIZE" \
  "$@"
