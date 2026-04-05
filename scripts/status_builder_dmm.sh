#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/aiostreams

echo "Builder service status:"
docker compose --profile builder ps postgres comet-builder

echo
echo "DMM table counts:"
docker exec postgres psql -U comet -d comet -c \
  "select count(*) as dmm_entries from dmm_entries; select count(*) as dmm_ingested_files from dmm_ingested_files;"

echo
echo "Recent builder logs:"
docker logs --since 10m --tail 120 comet-builder 2>&1 || true
