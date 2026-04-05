#!/usr/bin/env bash
set -euo pipefail

docker exec comet uv run python /app/scripts/query_magnetico_search_index.py "$@"
