#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

RUN_ID="rebalance_$(date -u +%Y%m%d_%H%M%S)"
OUT_DIR="test_runs/${RUN_ID}"
mkdir -p "$OUT_DIR"

LOG_FILE="${OUT_DIR}/runner.log"

{
  echo "run_id=${RUN_ID}"
  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "cwd=${ROOT_DIR}"
} > "${OUT_DIR}/run_meta.txt"

python3 ./scripts/rebalance_from_live_usage.py \
  --jackett-log-dir jackett/data/Jackett \
  --prowlarr-log-dir prowlarr/config/logs \
  --window-days 3 \
  --top-n 15 \
  --exploration-ratio 0.10 \
  --min-samples 20 \
  --max-membership-change 0.20 \
  --env-file config/comet-fresh.env \
  --compose-file compose.yaml \
  --service-name comet \
  --out-dir "${OUT_DIR}" \
  --apply >"${LOG_FILE}" 2>&1

echo "ended_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "${OUT_DIR}/run_meta.txt"

# Keep only last 30 days of rebalance artifacts.
find test_runs -maxdepth 1 -type d -name 'rebalance_*' -mtime +30 -print -exec rm -rf {} +

echo "out_dir=${OUT_DIR}"
echo "log_file=${LOG_FILE}"
