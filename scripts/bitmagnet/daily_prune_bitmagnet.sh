#!/bin/bash
# Daily BitMagnet Prune Script
# - Cleans processed queue jobs (7 days)
# - Prunes torrents older than 2 days (time-based retention)
# - Runs VACUUM FULL ANALYZE to reclaim space

set -euo pipefail

cd /home/ubuntu/aiostreams

LOG_FILE="/home/ubuntu/aiostreams/logs/prune-$(date +%Y%m%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') $*" >> "$LOG_FILE"
}

log "=== Starting daily prune ==="

# Check BitMagnet is running
if ! sudo docker compose ps bitmagnet-postgres | grep -q "Up"; then
  log "ERROR: bitmagnet-postgres is not running"
  exit 1
fi

# Clean old queue jobs (keep last 7 days)
log "Cleaning processed queue jobs..."
sudo docker compose exec -T bitmagnet-postgres psql -U postgres -d bitmagnet -c "
DELETE FROM queue_jobs 
WHERE status IN ('processed', 'failed') 
AND ran_at < NOW() - INTERVAL '7 days';
" >> "$LOG_FILE" 2>&1

# Prune torrents by creation time (2-day retention)
log "Pruning torrents older than 2 days..."
sudo docker compose exec -T bitmagnet-postgres psql -U postgres -d bitmagnet -c "
DELETE FROM torrents WHERE created_at < NOW() - INTERVAL '2 days';
DELETE FROM torrent_files WHERE torrent_id NOT IN (SELECT id FROM torrents);
" >> "$LOG_FILE" 2>&1

# Vacuum analyze (reclaim space)
log "Running VACUUM FULL ANALYZE..."
sudo docker compose exec -T bitmagnet-postgres psql -U postgres -d bitmagnet << 'EOSQL' >> "$LOG_FILE" 2>&1
VACUUM FULL ANALYZE torrents;
VACUUM FULL ANALYZE torrent_files;
VACUUM ANALYZE torrent_contents;
EOSQL

# Report stats
log "Storage stats:"
sudo docker compose exec -T bitmagnet-postgres psql -U postgres -d bitmagnet -c "
SELECT 'DB Size' as metric, pg_size_pretty(pg_database_size('bitmagnet')) as value
UNION ALL
SELECT 'Pending Match', COUNT(*)::text FROM torrent_contents 
  WHERE content_type IN ('movie', 'tv_show') AND content_source IS NULL
UNION ALL
SELECT 'Matched (TMDB)', COUNT(*)::text FROM torrent_contents WHERE content_source = 'tmdb'
UNION ALL
SELECT 'Torrents < 2 days', COUNT(*)::text FROM torrents
  WHERE created_at >= NOW() - INTERVAL '2 days';
" >> "$LOG_FILE" 2>&1

log "=== Prune complete ==="
