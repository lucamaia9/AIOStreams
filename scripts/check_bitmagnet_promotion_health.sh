#!/bin/bash
# BitMagnet Promotion Health Check
# Run this to verify the promotion pipeline is working

set -e

SQLITE_DB="/home/ubuntu/aiostreams/data/comet-fresh/magnetico/active.search.sqlite3"
LOG_FILE="/home/ubuntu/aiostreams/logs/bitmagnet-promotion-health.log"

mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "$(date -Iseconds) $1" | tee -a "$LOG_FILE"
}

# Get current stats
POSTGRES_COUNT=$(sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet -t -c "SELECT COUNT(*) FROM torrent_contents WHERE content_source IS NOT NULL;" 2>/dev/null | tr -d ' ')
SQLITE_COUNT=$(sqlite3 "$SQLITE_DB" "SELECT COUNT(*) FROM media_index;" 2>/dev/null)
SQLITE_NEWEST=$(sqlite3 "$SQLITE_DB" "SELECT datetime(MAX(discovered_on), 'unixepoch') FROM media_index;" 2>/dev/null)
PROMOTION_LOGS=$(sudo docker logs bitmagnet 2>&1 | grep "sqlite promotion completed" | tail -5)

log "=== BitMagnet Promotion Health ==="
log "Postgres TMDB content: $POSTGRES_COUNT"
log "SQLite total: $SQLITE_COUNT"
log "SQLite newest: $SQLITE_NEWEST"
log ""
log "Recent promotion logs:"
echo "$PROMOTION_LOGS" | while read line; do
    log "  $line"
done

# Check if SQLite was updated in last hour
ONE_HOUR_AGO=$(date -d '1 hour ago' +%s)
SQLITE_NEWEST_TS=$(sqlite3 "$SQLITE_DB" "SELECT MAX(discovered_on) FROM media_index;" 2>/dev/null)

if [ "$SQLITE_NEWEST_TS" -gt "$ONE_HOUR_AGO" ]; then
    log "STATUS: OK - SQLite updated in last hour"
    exit 0
else
    log "STATUS: WARNING - SQLite not updated in last hour"
    exit 1
fi
