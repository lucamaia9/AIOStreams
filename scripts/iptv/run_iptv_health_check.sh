#!/usr/bin/env bash
set -euo pipefail

# Health checker: re-checks catalog sources between pipeline runs.
# Run at offset from pipeline (e.g., 06:00 and 18:00 UTC).
#
# Usage:
#   ./run_iptv_health_check.sh                    # default catalog
#   CATALOG_PATH=/path/to/catalog.json ./run_iptv_health_check.sh

ROOT_DIR="/home/ubuntu/aiostreams"
AGGREGATOR_DIR="$ROOT_DIR/iptv-aggregator"
LOG_DIR="$ROOT_DIR/logs"
LOG_FILE="$LOG_DIR/iptv-health.log"
LOCK_FILE="$LOG_DIR/iptv-health.lock"

CATALOG_PATH="${CATALOG_PATH:-$AGGREGATOR_DIR/output/telegram_production/addon/addon_catalog.json}"
ADDON_COMPOSE_SERVICE="${IPTV_ADDON_COMPOSE_SERVICE:-iptv-addon}"

# Prevent concurrent runs
if [[ -f "$LOCK_FILE" ]]; then
  LOCK_PID=$(cat "$LOCK_FILE" 2>/dev/null)
  if [[ -n "$LOCK_PID" ]] && kill -0 "$LOCK_PID" 2>/dev/null; then
    echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Health check already running (PID $LOCK_PID). Exiting." >&2
    exit 0
  fi
  rm -f "$LOCK_FILE"
fi
echo $$ > "$LOCK_FILE"

log() {
  local level="$1"; shift
  local ts
  ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  printf '[%s] [%s] %s\n' "$ts" "$level" "$*" | tee -a "$LOG_FILE"
}

cleanup() {
  rm -f "$LOCK_FILE"
  if [[ -f "$LOG_FILE" ]] && (( $(wc -l < "$LOG_FILE") > 3000 )); then
    tail -n 2000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
  fi
}
trap cleanup EXIT INT TERM

mkdir -p "$LOG_DIR"
log "INFO" "=== Health check start ==="

# Step 1: Run health checker
log "INFO" "Checking sources in $CATALOG_PATH"
if cd "$AGGREGATOR_DIR" && node bin/iptv-health.js --catalog "$CATALOG_PATH" --parallel 20 2>&1 | tee -a "$LOG_FILE"; then
  log "OK" "Health check completed"
else
  log "FAIL" "Health check script exited with error"
  exit 1
fi

# Step 2: Restart addon to pick up updated catalog
log "INFO" "Restarting addon"
if cd "$ROOT_DIR" && docker compose restart "$ADDON_COMPOSE_SERVICE" 2>&1 | tee -a "$LOG_FILE"; then
  log "OK" "Addon restarted"
else
  log "WARN" "Addon restart failed (non-fatal)"
fi

log "OK" "=== Health check complete ==="
