#!/bin/bash
# One-time script to delete all unmatched content (no TMDB ID)
# Run ONCE to clean up existing junk, then use daily cleanup for maintenance

set -euo pipefail
cd /home/ubuntu/aiostreams

LOG_FILE="/home/ubuntu/aiostreams/logs/delete-unmatched-$(date +%Y%m%d-%H%M%S).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE"
}

log "=== STARTING: Delete Unmatched Content ==="

# Safety check: ensure BitMagnet is running
if ! sudo docker compose ps bitmagnet-postgres | grep -q "Up"; then
    log "ERROR: bitmagnet-postgres is not running"
    exit 1
fi

# Count before
log "=== BEFORE DELETION ==="
sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet >> "$LOG_FILE" 2>&1 <<EOSQL
SELECT 'Total torrents' as metric, COUNT(*)::text as value FROM torrents
UNION ALL SELECT 'TMDB matched', COUNT(*)::text FROM torrent_contents WHERE content_source = 'tmdb'
UNION ALL SELECT 'Unmatched (to delete)', COUNT(*)::text FROM torrent_contents WHERE content_source IS NULL
UNION ALL SELECT 'Database size', pg_size_pretty(pg_database_size('bitmagnet'));
EOSQL

log ""
log "=== STEP 1: Delete torrent_files for unmatched content ==="
sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet >> "$LOG_FILE" 2>&1 <<EOSQL
-- Delete files for torrents without TMDB match
DELETE FROM torrent_files
WHERE info_hash IN (
    SELECT t.info_hash
    FROM torrents t
    WHERE NOT EXISTS (
        SELECT 1 FROM torrent_contents tc
        WHERE tc.info_hash = t.info_hash
        AND tc.content_source = 'tmdb'
    )
);
EOSQL

log "=== STEP 2: Delete torrent_sources for unmatched content ==="
sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet >> "$LOG_FILE" 2>&1 <<EOSQL
DELETE FROM torrents_torrent_sources
WHERE info_hash IN (
    SELECT t.info_hash
    FROM torrents t
    WHERE NOT EXISTS (
        SELECT 1 FROM torrent_contents tc
        WHERE tc.info_hash = t.info_hash
        AND tc.content_source = 'tmdb'
    )
);
EOSQL

log "=== STEP 3: Delete torrent_contents without TMDB match ==="
sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet >> "$LOG_FILE" 2>&1 <<EOSQL
DELETE FROM torrent_contents
WHERE content_source IS NULL;
EOSQL

log "=== STEP 4: Delete orphaned torrents (no content) ==="
sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet >> "$LOG_FILE" 2>&1 <<EOSQL
DELETE FROM torrents
WHERE info_hash NOT IN (SELECT info_hash FROM torrent_contents);
EOSQL

log "=== STEP 5: Delete orphaned torrent_hints ==="
sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet >> "$LOG_FILE" 2>&1 <<EOSQL
DELETE FROM torrent_hints
WHERE info_hash NOT IN (SELECT info_hash FROM torrents);
EOSQL

log "=== STEP 6: Vacuum analyze to reclaim space ==="
sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet >> "$LOG_FILE" 2>&1 <<EOSQL
VACUUM FULL ANALYZE torrents;
VACUUM FULL ANALYZE torrent_contents;
VACUUM FULL ANALYZE torrent_files;
VACUUM FULL ANALYZE torrent_hints;
EOSQL

log ""
log "=== AFTER DELETION ==="
sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet >> "$LOG_FILE" 2>&1 <<EOSQL
SELECT 'Total torrents' as metric, COUNT(*)::text as value FROM torrents
UNION ALL SELECT 'TMDB matched', COUNT(*)::text FROM torrent_contents WHERE content_source = 'tmdb'
UNION ALL SELECT 'Unmatched', COUNT(*)::text FROM torrent_contents WHERE content_source IS NULL
UNION ALL SELECT 'Database size', pg_size_pretty(pg_database_size('bitmagnet'))
UNION ALL SELECT 'Torrent files', pg_size_pretty(pg_total_relation_size('torrent_files'))
UNION ALL SELECT 'Storage saved', 'See log for details';
EOSQL

log ""
log "=== COMPLETE ==="
log "Unmatched content deleted. Database optimized."
