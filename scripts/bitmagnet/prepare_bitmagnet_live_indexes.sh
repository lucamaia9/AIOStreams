#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/ubuntu/aiostreams"
CONTAINER="${1:-bitmagnet-postgres}"

cd "$ROOT_DIR"
sudo docker exec -i "$CONTAINER" psql -U postgres -d bitmagnet <<'SQL'
CREATE INDEX CONCURRENTLY IF NOT EXISTS torrent_hints_updated_at_info_hash_idx
ON torrent_hints (updated_at, info_hash);
SQL
