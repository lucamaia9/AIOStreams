#!/bin/bash
# Daily cleanup script for BitMagnet
# Runs at 3 AM via cron
# 1. Clean rejected content (passed intake but rejected by Python)
# 2. Clean unmatched content older than 7 days (no TMDB match)
# 3. Clean old queue jobs
# 4. Vacuum analyze

set -euo pipefail
cd /home/ubuntu/aiostreams

LOG_FILE="/home/ubuntu/aiostreams/logs/cleanup-$(date +%Y%m%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $*" >> "$LOG_FILE"
}

log "=== Starting BitMagnet daily cleanup ==="

# 1. Delete rejected content (content_type IS NULL - rejected by Python classifier)
log "Step 1: Deleting rejected content..."
sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet >> "$LOG_FILE" 2>&1 <<EOSQL
-- Delete rejected torrent_contents
DELETE FROM torrent_contents
WHERE content_type IS NULL
AND created_at < NOW() - INTERVAL '1 day';

-- Delete orphaned torrents (no content)
DELETE FROM torrents
WHERE info_hash NOT IN (SELECT info_hash FROM torrent_contents)
AND created_at < NOW() - INTERVAL '1 day';
EOSQL

# 2. Delete unmatched content older than 7 days (no TMDB match = useless for Comet)
log "Step 2: Deleting old unmatched content (no TMDB ID)..."
sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet >> "$LOG_FILE" 2>&1 <<EOSQL
-- Delete files for unmatched torrents older than 7 days
DELETE FROM torrent_files
WHERE info_hash IN (
    SELECT t.info_hash
    FROM torrents t
    JOIN torrent_contents tc ON t.info_hash = tc.info_hash
    WHERE tc.content_source IS NULL
    AND t.created_at < NOW() - INTERVAL '7 days'
);

-- Delete unmatched torrent_contents older than 7 days
DELETE FROM torrent_contents
WHERE content_source IS NULL
AND info_hash IN (
    SELECT info_hash FROM torrents WHERE created_at < NOW() - INTERVAL '7 days'
);

-- Delete orphaned torrents
DELETE FROM torrents
WHERE info_hash NOT IN (SELECT info_hash FROM torrent_contents)
AND created_at < NOW() - INTERVAL '7 days';
EOSQL

# 3. Clean old queue jobs
log "Step 3: Cleaning processed queue jobs..."
sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet >> "$LOG_FILE" 2>&1 <<EOSQL
DELETE FROM queue_jobs
WHERE status IN ('processed', 'failed')
AND ran_at < NOW() - INTERVAL '7 days';
EOSQL

# 4. Vacuum analyze (quick, not full)
log "Step 4: Running VACUUM ANALYZE..."
sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet >> "$LOG_FILE" 2>&1 <<EOSQL
VACUUM ANALYZE torrents;
VACUUM ANALYZE torrent_contents;
VACUUM ANALYZE torrent_files;
VACUUM ANALYZE torrent_hints;
EOSQL

# 5. Report stats
log "Step 5: Current stats..."
sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet >> "$LOG_FILE" 2>&1 <<EOSQL
SELECT 'DB Size' as metric, pg_size_pretty(pg_database_size('bitmagnet')) as value
UNION ALL
SELECT 'Torrents', COUNT(*)::text FROM torrents
UNION ALL
SELECT 'TMDB matched', COUNT(*)::text FROM torrent_contents WHERE content_source = 'tmdb'
UNION ALL
SELECT 'Unmatched (pending deletion)', COUNT(*)::text FROM torrent_contents WHERE content_source IS NULL
UNION ALL
SELECT 'Rejected today', COUNT(*)::text FROM torrent_contents 
WHERE content_type IS NULL AND created_at > NOW() - INTERVAL '1 day';
EOSQL

log "=== Cleanup complete ==="
