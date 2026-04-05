#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/aiostreams

echo "Starting postgres + comet-builder (builder profile)..."
docker compose --profile builder up -d postgres comet-builder

echo
echo "Builder status:"
docker compose --profile builder ps postgres comet-builder

echo
echo "Recent builder logs:"
docker logs --since 2m --tail 80 comet-builder 2>&1 || true
