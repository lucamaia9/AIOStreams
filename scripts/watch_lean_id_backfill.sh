#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/ubuntu/aiostreams"
STATE_PATH="${LEAN_ID_BACKFILL_STATE:-$ROOT_DIR/data/comet-fresh/magnetico/lean-id-backfill.state.json}"
LOG_FILE="${LEAN_ID_BACKFILL_LOG:-$ROOT_DIR/logs/lean-id-backfill.log}"
INTERVAL="${LEAN_ID_BACKFILL_WATCH_INTERVAL:-5}"

while true; do
  clear
  echo "== Lean ID Backfill =="
  echo "time: $(date -u +%FT%TZ)"
  echo

  if [[ -f "$STATE_PATH" ]]; then
    python3 - "$STATE_PATH" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
payload = json.loads(path.read_text())
print("state:", path)
if "phase" in payload:
    for key in ("phase", "phaseUnit", "cursor", "processedGroups", "updatedAt", "initializingTotals"):
        if key in payload:
            print(f"{key}: {payload[key]}")
    totals = payload.get("phaseTotals") or {}
    if totals:
        print("-- phase totals")
        for key in ("totalGroups", "remainingGroups", "progressPercent"):
            if key in totals:
                print(f"{key}: {totals[key]}")
    rates = payload.get("rates") or {}
    if rates:
        print("-- rates")
        for key in ("elapsedSeconds", "groupsPerSecond", "etaSeconds"):
            if key in rates:
                print(f"{key}: {rates[key]}")
    stats = payload.get("stats") or {}
    if stats:
        print("-- batch stats")
        for key in sorted(stats):
            print(f"{key}: {stats[key]}")
else:
    coverage = payload.get("coverage") or {}
    phases = payload.get("phases") or {}
    print("-- coverage")
    for key in sorted(coverage):
        print(f"{key}: {coverage[key]}")
    print("-- phases")
    for phase in sorted(phases):
        print(f"{phase}: {phases[phase]}")
PY
  else
    echo "state: missing ($STATE_PATH)"
  fi

  echo
  echo "== Processes =="
  pgrep -af 'backfill_lean_ids.py|run_lean_id_backfill.sh' || true

  echo
  echo "== Log Tail =="
  if [[ -f "$LOG_FILE" ]]; then
    tail -n 20 "$LOG_FILE"
  else
    echo "log missing ($LOG_FILE)"
  fi

  sleep "$INTERVAL"
done
