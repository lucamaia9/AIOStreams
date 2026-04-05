#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/ubuntu/aiostreams"
STATE_PATH="${BITMAGNET_WIDE_DRAIN_STATE:-$ROOT_DIR/data/comet-fresh/magnetico/wide-drain-offline.state.json}"
WORKDIR="${BITMAGNET_WIDE_DRAIN_WORKDIR:-$ROOT_DIR/data/comet-fresh/magnetico/wide-drain-offline}"
LOG_FILE="${BITMAGNET_WIDE_DRAIN_LOG:-$ROOT_DIR/logs/bitmagnet-wide-drain.log}"
INTERVAL="${BITMAGNET_WIDE_DRAIN_WATCH_INTERVAL:-5}"

while true; do
  clear
  echo "== BitMagnet Wide Drain =="
  echo "time: $(date -u +%FT%TZ)"
  echo

  if [[ -f "$STATE_PATH" ]]; then
    python3 - "$STATE_PATH" <<'PY'
import json, sys, pathlib, datetime
path = pathlib.Path(sys.argv[1])
payload = json.loads(path.read_text())
export = float(payload.get("exportSeconds", 0.0) or 0.0)
classify = float(payload.get("classifySeconds", 0.0) or 0.0)
enrich = float(payload.get("enrichSeconds", 0.0) or 0.0)
merge = float(payload.get("mergeSeconds", 0.0) or 0.0)
elapsed = export + classify + enrich + merge
scanned = int(payload.get("scannedRows", 0) or 0)
rate = scanned / elapsed if elapsed > 0 else 0.0
print("state:", path)
for key in [
    "schemaVersion", "cursorType", "lastCreatedAt", "lastInfoHash",
    "slicesCompleted", "exportedRows", "scannedRows", "eligibleRows", "insertedRows", "updatedRows", "noopRows", "skippedRows", "updatedAt"
]:
    if key in payload:
        print(f"{key}: {payload[key]}")
print(f"exportSeconds: {export:.3f}")
print(f"classifySeconds: {classify:.3f}")
print(f"enrichSeconds: {enrich:.3f}")
print(f"mergeSeconds: {merge:.3f}")
print(f"rowsPerSecond: {rate:.2f}")
PY
  else
    echo "state: missing ($STATE_PATH)"
  fi

  echo
  echo "== Workdir =="
  echo "$WORKDIR"
  find "$WORKDIR/reports" -maxdepth 1 -type f 2>/dev/null | sort | tail -n 5 || true

  echo
  echo "== Source Index Progress =="
  sudo docker exec -i bitmagnet-postgres psql -U postgres -d bitmagnet -qAt -c \
    "SELECT index_relid::regclass::text, phase, lockers_total, lockers_done, blocks_total, blocks_done, tuples_total, tuples_done FROM pg_stat_progress_create_index;" \
    2>/dev/null || true

  echo
  echo "== Source Index Status =="
  sudo docker exec -i bitmagnet-postgres psql -U postgres -d bitmagnet -qAt -c \
    "SELECT c.relname, i.indisvalid, i.indisready, i.indislive FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname IN ('torrents_created_at_info_hash_idx','torrent_contents_info_hash_idx') ORDER BY c.relname;" \
    2>/dev/null || true

  echo
  echo "== Drain Processes =="
  pgrep -af 'run_bitmagnet_wide_drain.sh|drain_bitmagnet_wide_offline.py|promote_bitmagnet_live_to_compact.py --drain-all' || true

  echo
  echo "== Log Tail =="
  if [[ -f "$LOG_FILE" ]]; then
    tail -n 20 "$LOG_FILE"
  else
    echo "log missing ($LOG_FILE)"
  fi

  sleep "$INTERVAL"
done
