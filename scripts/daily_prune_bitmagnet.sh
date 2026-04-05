#!/bin/bash
# Daily BitMagnet Prune Script
# - Cleans processed queue jobs
# - Prunes torrents exported to SQLite > 24h ago
# - Runs VACUUM ANALYZE on large tables

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

# Prune exported torrents (> 24h ago)
log "Pruning exported torrents..."
sudo docker compose exec -T bitmagnet-postgres psql -U postgres -d bitmagnet -c "
DELETE FROM torrents 
WHERE info_hash IN (
  SELECT info_hash 
  FROM torrent_contents 
  WHERE exported = true 
  AND exported_at < NOW() - INTERVAL '24 hours'
);
" >> "$LOG_FILE" 2>&1

# Vacuum analyze
log "Running VACUUM ANALYZE..."
sudo docker compose exec -T bitmagnet-postgres psql -U postgres -d bitmagnet << 'EOSQL' >> "$LOG_FILE" 2>&1
VACUUM ANALYZE torrents;
VACUUM ANALYZE torrent_files;
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
SELECT 'Exported Pending Prune', COUNT(*)::text FROM torrent_contents 
  WHERE exported = true AND exported_at < NOW() - INTERVAL '24 hours';
" >> "$LOG_FILE" 2>&1

log "=== Prune complete ==="
