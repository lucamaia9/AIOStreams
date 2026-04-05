#!/usr/bin/env bash
set -euo pipefail

# IPTV Pipeline: Telegram scraper -> Aggregator maintenance -> Addon restart -> Verify
# Usage:
#   ./run_iptv_pipeline.sh                  # full pipeline
#   SKIP_SCRAPER=1 ./run_iptv_pipeline.sh   # skip scraper, run aggregator+addon only
#   SKIP_VERIFY=1 ./run_iptv_pipeline.sh    # skip final verification

ROOT_DIR="/home/ubuntu/aiostreams"
SCRAPER_DIR="$ROOT_DIR/telegram-scraper"
AGGREGATOR_DIR="$ROOT_DIR/iptv-aggregator"
LOG_DIR="$ROOT_DIR/logs"
LOG_FILE="$LOG_DIR/iptv-pipeline.log"

SCRAPER_OUTPUT="$SCRAPER_DIR/output/sources_clean.json"
AGGREGATOR_OUTPUT_DIR="${IPTV_AGGREGATOR_OUTPUT_DIR:-$AGGREGATOR_DIR/output/telegram_production}"
ADDON_CATALOG_PATH="$AGGREGATOR_OUTPUT_DIR/addon/addon_catalog.json"
ADDON_COMPOSE_SERVICE="${IPTV_ADDON_COMPOSE_SERVICE:-iptv-addon}"
ADDON_MANIFEST_URL="${IPTV_ADDON_MANIFEST_URL:-https://iptv.mrdouglas.uk/manifest.json}"
ADDON_HEALTH_URL="${IPTV_ADDON_HEALTH_URL:-https://iptv.mrdouglas.uk/health}"

STEP_STATUS=()
FAILED=0
LOCK_FILE="$LOG_DIR/iptv-pipeline.lock"

# Prevent concurrent runs
if [[ -f "$LOCK_FILE" ]]; then
  LOCK_PID=$(cat "$LOCK_FILE" 2>/dev/null)
  if [[ -n "$LOCK_PID" ]] && kill -0 "$LOCK_PID" 2>/dev/null; then
    echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Pipeline already running (PID $LOCK_PID). Exiting." >&2
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

step_ok() {
  STEP_STATUS+=("[OK] $1")
  log "OK" "$1"
}

step_fail() {
  STEP_STATUS+=("[FAIL] $1")
  log "FAIL" "$1"
  FAILED=1
}

cleanup() {
  rm -f "$LOCK_FILE"
  if [[ -f "$LOG_FILE" ]] && (( $(wc -l < "$LOG_FILE") > 5000 )); then
    tail -n 3000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
  fi
}
trap cleanup EXIT INT TERM

mkdir -p "$LOG_DIR"
log "INFO" "=== IPTV pipeline start ==="

# ── Step 1: Telegram scraper ──────────────────────────────────────────────
if [[ "${SKIP_SCRAPER:-0}" == "1" ]]; then
  log "INFO" "Step 1: Scraper skipped (SKIP_SCRAPER=1)"
  step_ok "Scraper skipped"
else
  log "INFO" "Step 1: Running Telegram scraper"
  SCRAPER_PRE_MTIME=$(stat -c %Y "$SCRAPER_OUTPUT" 2>/dev/null || echo 0)
  SCRAPER_SUCCESS=0
  SCRAPER_ATTEMPT=0

  while [[ $SCRAPER_SUCCESS -eq 0 ]] && [[ $SCRAPER_ATTEMPT -lt 3 ]]; do
    SCRAPER_ATTEMPT=$((SCRAPER_ATTEMPT + 1))
    if [[ $SCRAPER_ATTEMPT -gt 1 ]]; then
      log "INFO" "Scraper retry $SCRAPER_ATTEMPT (waiting 5s for stale WAL lock to clear)"
      sleep 5
    fi

    SCRAPER_OUTPUT_TMP=$(docker run --rm --network host \
      -v "$SCRAPER_DIR/output:/app/output" \
      --env-file "$SCRAPER_DIR/config.env" \
      -e OUTPUT_DIR=/app/output \
      telegram-scraper 2>&1 | tee -a "$LOG_FILE")
    SCRAPER_EXIT=$?

    if [[ $SCRAPER_EXIT -eq 0 ]]; then
      SCRAPER_SUCCESS=1
    elif echo "$SCRAPER_OUTPUT_TMP" | grep -q "database is locked\|OperationalError"; then
      log "WARN" "Scraper hit SQLite lock (attempt $SCRAPER_ATTEMPT), will retry"
    else
      echo "$SCRAPER_OUTPUT_TMP" | tee -a "$LOG_FILE" > /dev/null
      break
    fi
  done

  if [[ $SCRAPER_SUCCESS -eq 1 ]]; then

    if [[ ! -f "$SCRAPER_OUTPUT" ]]; then
      step_fail "Scraper completed but output not found at $SCRAPER_OUTPUT"
    else
      SCRAPER_POST_MTIME=$(stat -c %Y "$SCRAPER_OUTPUT" 2>/dev/null || echo 0)
      SOURCE_COUNT=$(python3 -c "import json; print(len(json.load(open('$SCRAPER_OUTPUT')).get('sources',[])))" 2>/dev/null || echo "?")
      if [[ "$SCRAPER_POST_MTIME" -gt "$SCRAPER_PRE_MTIME" ]]; then
        step_ok "Scraper updated sources_clean.json ($SOURCE_COUNT sources)"
      else
        log "WARN" "Scraper ran but sources_clean.json unchanged ($SOURCE_COUNT sources, session may need refresh)"
        step_ok "Scraper ran (no new sources)"
      fi

      # Clean up stale timestamped output files (> 2 days old)
      STALE_COUNT=$(find "$SCRAPER_DIR/output" -maxdepth 1 \
        \( -name 'credentials_*.json' -o -name 'messages_*.json' \) \
        -mtime +2 -print | wc -l)
      if [[ "$STALE_COUNT" -gt 0 ]]; then
        find "$SCRAPER_DIR/output" -maxdepth 1 \
          \( -name 'credentials_*.json' -o -name 'messages_*.json' \) \
          -mtime +2 -delete
        step_ok "Cleaned $STALE_COUNT stale scraper output files"
      fi
    fi
  else
    step_fail "Scraper exited with error"
  fi
fi

# ── Step 2: Aggregator maintenance ────────────────────────────────────────
if [[ $FAILED -eq 0 ]] || [[ "${SKIP_SCRAPER:-0}" == "1" ]]; then
  log "INFO" "Step 2: Running aggregator maintenance"
  AGGREGATOR_PRE_MTIME=$(stat -c %Y "$ADDON_CATALOG_PATH" 2>/dev/null || echo 0)

  if cd "$AGGREGATOR_DIR" && node bin/iptv-aggregator.js run-optimized-telegram \
    --input "$SCRAPER_OUTPUT" \
    --output "$AGGREGATOR_OUTPUT_DIR" \
    --parallel "${IPTV_AGGREGATOR_PARALLEL:-6}" \
    --backup-target "${IPTV_AGGREGATOR_BACKUP_TARGET:-3}" \
    --evidence-ttl-hours "${IPTV_AGGREGATOR_EVIDENCE_TTL_HOURS:-48}" \
    --hls-only 2>&1 | tee -a "$LOG_FILE"; then

    if [[ ! -f "$ADDON_CATALOG_PATH" ]]; then
      step_fail "Aggregator completed but addon catalog not found at $ADDON_CATALOG_PATH"
    else
      AGGREGATOR_POST_MTIME=$(stat -c %Y "$ADDON_CATALOG_PATH" 2>/dev/null || echo 0)
      CATALOG_INFO=$(python3 -c "
import json
c = json.load(open('$ADDON_CATALOG_PATH'))
r = sum(1 for ch in c['channels'] if ch['status'] == 'ready')
u = sum(1 for ch in c['channels'] if ch['status'] == 'undercovered')
x = sum(1 for ch in c['channels'] if ch['status'] == 'unavailable')
print(f'{r} ready, {u} undercovered, {x} unavailable (generated: {c.get(\"generatedAt\", \"?\")})')
" 2>/dev/null || echo "parse error")
      step_ok "Aggregator produced catalog: $CATALOG_INFO"
    fi
  else
    step_fail "Aggregator exited with error"
  fi
else
  log "INFO" "Step 2: Skipped (scraper failed)"
  step_fail "Aggregator skipped (upstream failure)"
fi

# ── Step 3: Restart addon ─────────────────────────────────────────────────
if [[ $FAILED -eq 0 ]]; then
  log "INFO" "Step 3: Restarting addon container"
  if cd "$ROOT_DIR" && docker compose restart "$ADDON_COMPOSE_SERVICE" 2>&1 | tee -a "$LOG_FILE"; then
    step_ok "Addon container restarted"
  else
    step_fail "Addon restart failed"
  fi
else
  log "INFO" "Step 3: Skipped (upstream failure)"
fi

# ── Step 4: Verify ────────────────────────────────────────────────────────
if [[ "${SKIP_VERIFY:-0}" == "1" ]]; then
  log "INFO" "Step 4: Verification skipped (SKIP_VERIFY=1)"
  step_ok "Verification skipped"
elif [[ $FAILED -eq 0 ]]; then
  log "INFO" "Step 4: Verifying addon health"

  # Wait for addon to be ready (up to 30s)
  for i in $(seq 1 6); do
    if curl -fsS "$ADDON_HEALTH_URL" >/dev/null 2>&1; then
      break
    fi
    log "INFO" "Waiting for addon health check ($i/6)..."
    sleep 5
  done

  VERIFY_ERRORS=0

  # Health check
  if ! curl -fsS "$ADDON_HEALTH_URL" >/dev/null 2>&1; then
    step_fail "Addon health endpoint unreachable"
    VERIFY_ERRORS=1
  fi

  # Give addon time to fully reload catalog from disk after restart
  if [[ $VERIFY_ERRORS -eq 0 ]]; then
    log "INFO" "Waiting 30s for addon startup..."
    sleep 30
  fi

  # Pre-warm catalog load (triggers lazy loading on first request)
  if [[ $VERIFY_ERRORS -eq 0 ]]; then
    log "INFO" "Pre-warming catalog (triggering lazy load)..."
    CATALOG_WARM_URL="${ADDON_MANIFEST_URL%/manifest.json}catalog/tv/personal_iptv.json"
    curl -fsS "$CATALOG_WARM_URL" >/dev/null 2>&1 || true
    log "INFO" "Waiting 30s for catalog to fully load..."
    sleep 30
  fi

  # Manifest check
  if ! curl -fsS "$ADDON_MANIFEST_URL" >/dev/null 2>&1; then
    step_fail "Addon manifest endpoint unreachable"
    VERIFY_ERRORS=1
  fi

  # Catalog data freshness check
  if [[ $VERIFY_ERRORS -eq 0 ]]; then
    FRESHNESS=$(python3 -c "
import json, sys
catalog = json.load(open('$ADDON_CATALOG_PATH'))
ready = sum(1 for ch in catalog['channels'] if ch['status'] == 'ready')
if ready < 200:
    print(f'FAIL: only {ready} ready channels (threshold: 200)')
    sys.exit(1)
print(f'{ready} ready channels')
" 2>&1)
    if [[ $? -ne 0 ]]; then
      step_fail "Catalog freshness: $FRESHNESS"
    else
      step_ok "Catalog freshness: $FRESHNESS"
    fi
  fi

  if [[ $VERIFY_ERRORS -eq 0 ]]; then
    # Catalog endpoint smoke test (triggers lazy catalog load, more reliable than stream endpoint after restart)
    CATALOG_URL="${ADDON_MANIFEST_URL%/manifest.json}catalog/tv/personal_iptv.json"
    CATALOG_PASS=0
    for attempt in 1 2 3; do
      if [[ $attempt -gt 1 ]]; then
        log "INFO" "Catalog smoke test retry $attempt..."
        sleep 5
      fi
      CATALOG_CHECK=$(curl -fsS "$CATALOG_URL" 2>&1)
      CATALOG_EXIT=$?
      if [[ $CATALOG_EXIT -eq 0 ]] && [[ -n "$CATALOG_CHECK" ]]; then
        CATALOG_VALID=$(python3 -c "
import json, sys
try:
    d = json.loads(sys.stdin.read())
    metas = d.get('metas', [])
    if len(metas) >= 50:
        print(f'OK:{len(metas)}')
    else:
        print(f'TOO_FEW:{len(metas)}')
except Exception as e:
    print(f'ERROR: {e}')
" <<< "$CATALOG_CHECK" 2>&1)
        if [[ "${CATALOG_VALID:0:2}" == "OK" ]]; then
          CATALOG_PASS=1
          break
        fi
      fi
    done
    if [[ $CATALOG_PASS -eq 1 ]]; then
      step_ok "Catalog endpoint smoke test: ${CATALOG_VALID#OK:}"
    else
      step_fail "Catalog endpoint smoke test: $CATALOG_VALID"
      VERIFY_ERRORS=1
    fi
  fi

  if [[ $VERIFY_ERRORS -eq 0 ]]; then
    step_ok "Addon verification passed"
  fi
else
  log "INFO" "Step 4: Skipped (upstream failure)"
fi

# ── Summary ───────────────────────────────────────────────────────────────
log "INFO" "=== Pipeline summary ==="
for s in "${STEP_STATUS[@]}"; do
  log "INFO" "  $s"
done

if [[ $FAILED -eq 1 ]]; then
  log "FAIL" "=== Pipeline FAILED ==="
  exit 1
else
  log "OK" "=== Pipeline completed successfully ==="
  exit 0
fi
