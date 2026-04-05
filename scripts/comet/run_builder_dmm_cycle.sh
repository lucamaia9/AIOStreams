#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/aiostreams

TIMEOUT_MINUTES="${TIMEOUT_MINUTES:-240}"
POLL_SECONDS="${POLL_SECONDS:-15}"
STOP_ON_EXIT="${STOP_ON_EXIT:-true}"

run_id="dmm_cycle_$(date -u +%Y%m%d_%H%M%S)"
out_dir="/home/ubuntu/aiostreams/test_runs/${run_id}"
mkdir -p "$out_dir"

before_entries="$(docker exec postgres psql -U comet -d comet -t -A -c "select count(*) from dmm_entries;" | tr -d '[:space:]')"
before_files="$(docker exec postgres psql -U comet -d comet -t -A -c "select count(*) from dmm_ingested_files;" | tr -d '[:space:]')"

echo "run_id=$run_id"
echo "out_dir=$out_dir"
echo "before_dmm_entries=$before_entries"
echo "before_dmm_ingested_files=$before_files"

docker compose --profile builder up -d postgres comet-builder

cleanup() {
  if [[ "${STOP_ON_EXIT}" == "true" ]]; then
    docker compose --profile builder stop comet-builder >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

deadline=$(( $(date +%s) + TIMEOUT_MINUTES * 60 ))
completed="false"

while (( $(date +%s) < deadline )); do
  logs="$(docker logs --since 2m --tail 200 comet-builder 2>&1 || true)"
  printf '%s\n' "$logs" > "$out_dir/current_builder.log"

  if grep -q "Ingestion completed." "$out_dir/current_builder.log"; then
    completed="true"
    break
  fi

  sleep "$POLL_SECONDS"
done

after_entries="$(docker exec postgres psql -U comet -d comet -t -A -c "select count(*) from dmm_entries;" | tr -d '[:space:]')"
after_files="$(docker exec postgres psql -U comet -d comet -t -A -c "select count(*) from dmm_ingested_files;" | tr -d '[:space:]')"

docker logs --tail 400 comet-builder > "$out_dir/comet-builder.log" 2>&1 || true
docker compose --profile builder ps postgres comet-builder > "$out_dir/compose-ps.txt"

cat > "$out_dir/summary.txt" <<EOF
run_id=$run_id
completed=$completed
timeout_minutes=$TIMEOUT_MINUTES
poll_seconds=$POLL_SECONDS
before_dmm_entries=$before_entries
after_dmm_entries=$after_entries
before_dmm_ingested_files=$before_files
after_dmm_ingested_files=$after_files
delta_dmm_entries=$((after_entries - before_entries))
delta_dmm_ingested_files=$((after_files - before_files))
EOF

cat "$out_dir/summary.txt"

if [[ "$completed" != "true" ]]; then
  echo "DMM cycle did not complete within timeout." >&2
  exit 2
fi
