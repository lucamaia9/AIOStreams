#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/aiostreams

echo "Stopping comet-builder..."
docker compose --profile builder stop comet-builder

echo
echo "Builder status:"
docker compose --profile builder ps postgres comet-builder
