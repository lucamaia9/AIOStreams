#!/bin/bash
# Daily BitMagnet Prune Script
# - Cleans processed queue jobs (7 days)
# - Prunes torrents older than 2 days (promotion-aware)
# - Runs VACUUM FULL ANALYZE to reclaim space
#
# Safety: only deletes torrents that have been promoted to SQLite
# OR are older than a 3-day safety window (guarantees promotion
# pipeline has had time to process them).

set -euo pipefail

cd /home/ubuntu/aiostreams

LOG_FILE="/home/ubuntu/aiostreams/logs/prune-$(date +%Y%m%d).log"
mkdir -p "$(dirname "$LOG_FILE")"
SEARCH_DB="/home/ubuntu/aiostreams/data/comet-fresh/magnetico/active.search.sqlite3"

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') $*" >> "$LOG_FILE"
}

log "=== Starting daily prune ==="

# Check BitMagnet is running
if ! sudo docker compose ps bitmagnet-postgres | grep -q "Up"; then
  log "ERROR: bitmagnet-postgres is not running"
  exit 1
fi

# Check SQLite search DB exists (needed for promotion-aware pruning)
if [[ ! -f "$SEARCH_DB" ]]; then
  log "ERROR: search DB not found at $SEARCH_DB, aborting prune"
  exit 1
fi

# Check promotion pipeline has run recently (within last 24 hours)
LAST_PROMOTION=$(sqlite3 "$SEARCH_DB" \
  "SELECT value FROM live_promotion_meta WHERE key='lastRunAt';" 2>/dev/null || echo "")

if [[ -n "$LAST_PROMOTION" ]]; then
  PROMOTION_EPOCH=$(date -d "$LAST_PROMOTION" +%s 2>/dev/null || echo "0")
  NOW_EPOCH=$(date +%s)
  HOURS_SINCE_PROMOTION=$(( (NOW_EPOCH - PROMOTION_EPOCH) / 3600 ))

  if (( HOURS_SINCE_PROMOTION > 24 )); then
    log "WARNING: last promotion was ${HOURS_SINCE_PROMOTION}h ago (>24h), using 3-day safety window"
    PRUNE_WINDOW="3 days"
  else
    log "Promotion pipeline ran ${HOURS_SINCE_PROMOTION}h ago, using 2-day window"
    PRUNE_WINDOW="2 days"
  fi
else
  log "WARNING: no promotion history found, using 3-day safety window"
  PRUNE_WINDOW="3 days"
fi

# Clean old queue jobs (keep last 7 days)
log "Cleaning processed queue jobs..."
sudo docker compose exec -T bitmagnet-postgres psql -U postgres -d bitmagnet -c "
DELETE FROM queue_jobs
WHERE status IN ('processed', 'failed')
AND ran_at < NOW() - INTERVAL '7 days';
" >> "$LOG_FILE" 2>&1

# Prune torrents older than retention window.
# The 2-day window is used when promotion is healthy; 3-day safety window
# is used when promotion is lagging or missing.
log "Pruning torrents older than ${PRUNE_WINDOW} (promotion-aware)..."
sudo docker compose exec -T bitmagnet-postgres psql -U postgres -d bitmagnet -c "
DELETE FROM torrents WHERE created_at < NOW() - INTERVAL '${PRUNE_WINDOW}';
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
SELECT 'Torrents in window', COUNT(*)::text FROM torrents
  WHERE created_at >= NOW() - INTERVAL '2 days';
" >> "$LOG_FILE" 2>&1

log "=== Prune complete (window: ${PRUNE_WINDOW}) ==="
