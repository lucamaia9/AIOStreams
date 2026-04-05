#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/ubuntu/aiostreams"
PYTHON_BIN="${LEAN_ID_BACKFILL_PYTHON_BIN:-$ROOT_DIR/cometouglas/.venv/bin/python}"
SCRIPT_PATH="$ROOT_DIR/scripts/backfill_lean_ids.py"
LIVE_SEARCH_DB="$ROOT_DIR/data/comet-fresh/magnetico/active.search.sqlite3"
SEARCH_DB="${LEAN_ID_BACKFILL_SEARCH_DB:-$LIVE_SEARCH_DB}"
STATE_PATH="${LEAN_ID_BACKFILL_STATE:-$ROOT_DIR/data/comet-fresh/magnetico/lean-id-backfill.state.json}"
ANIME_CACHE_DIR="${LEAN_ID_BACKFILL_ANIME_CACHE_DIR:-$ROOT_DIR/data/comet-fresh/magnetico/anime-backfill-cache}"
LOG_FILE="${LEAN_ID_BACKFILL_LOG:-$ROOT_DIR/logs/lean-id-backfill.log}"
LOCK_FILE="${LEAN_ID_BACKFILL_LOCK_FILE:-$ROOT_DIR/.lean-id-backfill.lock}"
GROUP_BATCH_SIZE="${LEAN_ID_BACKFILL_GROUP_BATCH_SIZE:-40}"
NETWORK_CONCURRENCY="${LEAN_ID_BACKFILL_NETWORK_CONCURRENCY:-4}"
HTTP_TIMEOUT="${LEAN_ID_BACKFILL_HTTP_TIMEOUT:-30}"
ANIME_CACHE_TTL="${LEAN_ID_BACKFILL_ANIME_CACHE_TTL:-604800}"
ANALYZE_EVERY_BATCHES="${LEAN_ID_BACKFILL_ANALYZE_EVERY_BATCHES:-10}"

if [[ "$SEARCH_DB" == "$LIVE_SEARCH_DB" ]]; then
  cat >&2 <<EOF
run_lean_id_backfill.sh requires LEAN_ID_BACKFILL_SEARCH_DB to point at an offline stage DB.
Direct --apply writes to $LIVE_SEARCH_DB are blocked on this host.

Supported workflow:
  1. Back up $LIVE_SEARCH_DB and copy that backup to an offline stage DB.
  2. Run: LEAN_ID_BACKFILL_SEARCH_DB=/path/to/stage.sqlite3 ./scripts/run_lean_id_backfill.sh
  3. Deploy: python3 ./scripts/rebuild_live_sqlite.py --search-db $LIVE_SEARCH_DB --historical-seed-db /path/to/stage.sqlite3 --cutover
EOF
  exit 2
fi

if [[ ! -f "$SEARCH_DB" ]]; then
  echo "stage search db not found: $SEARCH_DB" >&2
  exit 1
fi

mkdir -p "$(dirname "$LOG_FILE")" "$(dirname "$STATE_PATH")" "$ANIME_CACHE_DIR"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "lean id backfill already running: $LOCK_FILE" >&2
  exit 1
fi

{
  echo "[$(date -u +%FT%TZ)] starting lean id backfill state=$STATE_PATH batch=$GROUP_BATCH_SIZE concurrency=$NETWORK_CONCURRENCY"
  exec "$PYTHON_BIN" "$SCRIPT_PATH" \
    --search-db "$SEARCH_DB" \
    --state-path "$STATE_PATH" \
    --anime-cache-dir "$ANIME_CACHE_DIR" \
    --group-batch-size "$GROUP_BATCH_SIZE" \
    --network-concurrency "$NETWORK_CONCURRENCY" \
    --http-timeout "$HTTP_TIMEOUT" \
    --anime-cache-ttl "$ANIME_CACHE_TTL" \
    --analyze-every-batches "$ANALYZE_EVERY_BATCHES" \
    --apply \
    "$@"
} >>"$LOG_FILE" 2>&1
