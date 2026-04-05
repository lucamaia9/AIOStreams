#!/bin/bash
# Mark torrent_contents as exported when they have canonical matches
# This allows the prune script to clean them up after 24h

set -euo pipefail
cd /home/ubuntu/aiostreams

LOG_FILE="/home/ubuntu/aiostreams/logs/mark_exported-$(date +%Y%m%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $*" >> "$LOG_FILE"
}

log "=== Marking exported content ==="

# Mark content with canonical matches (tmdb/imdb) as exported
sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet >> "$LOG_FILE" 2>&1 <<EOSQL
-- Mark matched content as exported
UPDATE torrent_contents
SET exported = true, exported_at = NOW()
WHERE content_source IS NOT NULL
AND content_id IS NOT NULL
AND exported = false;

-- Mark rejected content for deletion (set exported so prune cleans it)
UPDATE torrent_contents
SET exported = true, exported_at = NOW()
WHERE content_type IS NULL
AND exported = false;
EOSQL

log "=== Done ==="
