#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/aiostreams

TIMEOUT_MINUTES="${TIMEOUT_MINUTES:-30}"
POLL_SECONDS="${POLL_SECONDS:-10}"
STOP_ON_EXIT="${STOP_ON_EXIT:-true}"

run_id="dmm_incremental_$(date -u +%Y%m%d_%H%M%S)"
out_dir="/home/ubuntu/aiostreams/test_runs/${run_id}"
mkdir -p "$out_dir"

docker compose --profile builder up -d postgres comet-builder

cleanup() {
  if [[ "${STOP_ON_EXIT}" == "true" ]]; then
    docker compose --profile builder stop comet-builder >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

deadline=$(( $(date +%s) + TIMEOUT_MINUTES * 60 ))
found_line=""

while (( $(date +%s) < deadline )); do
  logs="$(docker logs --since 5m --tail 300 comet-builder 2>&1 || true)"
  printf '%s\n' "$logs" > "$out_dir/current_builder.log"

  found_line="$(printf '%s\n' "$logs" | grep -E 'Found [0-9]+ total files\. [0-9]+ are new\.' | tail -n 1 || true)"
  if [[ -n "$found_line" ]]; then
    break
  fi

  sleep "$POLL_SECONDS"
done

docker logs --tail 400 comet-builder > "$out_dir/comet-builder.log" 2>&1 || true
docker compose --profile builder ps postgres comet-builder > "$out_dir/compose-ps.txt"

echo "run_id=$run_id"
echo "found_line=${found_line:-missing}" | tee "$out_dir/summary.txt"

if [[ -z "$found_line" ]]; then
  echo "Did not observe DMM incremental file-count log within timeout." >&2
  exit 2
fi
