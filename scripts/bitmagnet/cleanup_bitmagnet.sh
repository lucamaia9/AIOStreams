#!/bin/bash
# BitMagnet daily cleanup
# Runs at 02:00 UTC via systemd bitmagnet-cleanup.timer
#
# Step 0: Purge torrent_files for all torrents older than 24h (biggest disk saver ~6 GB)
#         After >=2 promotion pipeline passes (every 12h), file rows serve no purpose.
# Step 1: Delete rejected content (content_type IS NULL) older than 12h
# Step 2: Delete unmatched content (content_source IS NULL) older than 1 day
# Step 3: Clean old processed/failed queue jobs
# Step 4: REINDEX bloated indexes + VACUUM FULL Comet cache (prune handles BitMagnet VACUUM FULL)

set -euo pipefail
cd /home/ubuntu/aiostreams

LOG_FILE="/home/ubuntu/aiostreams/logs/cleanup-$(date +%Y%m%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE"
}

run_psql() {
    sudo docker exec -i bitmagnet-postgres psql -U postgres -d bitmagnet \
        -v ON_ERROR_STOP=1 "$@"
}

log "=== Starting BitMagnet daily cleanup ==="

# Step 0: Purge torrent_files for torrents older than 24h.
# These have passed through at least 2 promotion pipeline runs (every 12h).
# File listings are no longer needed for search — cascade delete via FK is not
# relied upon here; we delete files directly to reclaim space faster.
log "Step 0: Purging torrent_files for torrents older than 24h..."
run_psql >> "$LOG_FILE" 2>&1 << 'EOSQL'
DELETE FROM torrent_files
WHERE info_hash IN (
    SELECT info_hash FROM torrents
    WHERE created_at < NOW() - INTERVAL '24 hours'
);
EOSQL

# Orphan torrent cleanup is intentionally skipped (same as daily-prune's
# --skip-orphan-cleanup). The NOT EXISTS anti-join on 557K+ rows is too
# slow within the 1-hour systemd timeout. Orphan rows are harmless and
# get pruned by the weekly VACUUM FULL in the daily-prune pipeline.

# Step 1: Delete rejected content (Python classifier set content_type = NULL).
# Keep a 12-hour window so a torrent that arrived just before the cleanup run
# still gets at least one Smart Hint pass.
log "Step 1: Deleting rejected content older than 12h..."
run_psql >> "$LOG_FILE" 2>&1 << 'EOSQL'
DELETE FROM torrent_contents
WHERE content_type IS NULL
AND created_at < NOW() - INTERVAL '12 hours';

DELETE FROM torrents t
WHERE NOT EXISTS (SELECT 1 FROM torrent_contents tc WHERE tc.info_hash = t.info_hash)
AND t.created_at < NOW() - INTERVAL '12 hours';
EOSQL

# Step 2: Delete unmatched content (no TMDB/IMDB source) older than 1 day.
# These will never appear in Comet results; no point keeping them.
log "Step 2: Deleting unmatched content (no content_source) older than 1 day..."
run_psql >> "$LOG_FILE" 2>&1 << 'EOSQL'
DELETE FROM torrent_contents
WHERE content_source IS NULL
AND info_hash IN (
    SELECT info_hash FROM torrents WHERE created_at < NOW() - INTERVAL '1 day'
);

-- Orphan cleanup skipped (same reason as Step 0a).
EOSQL

# Step 3: Clean old processed/failed queue jobs (keep 7-day window for debugging).
log "Step 3: Cleaning processed/failed queue jobs older than 7 days..."
run_psql >> "$LOG_FILE" 2>&1 << 'EOSQL'
DELETE FROM queue_jobs
WHERE status IN ('processed', 'failed')
AND ran_at < NOW() - INTERVAL '7 days';
EOSQL

# Step 4: REINDEX + VACUUM FULL on bloated PostgreSQL tables.
# These tables accumulate index bloat from daily DELETE heavy cleanup.
# REINDEX first (rebuilds indexes), then VACUUM FULL (reclaims free space).
# Each operation completes in 1-20s, well within the 1-hour window.
log "Step 4a: REINDEX BitMagnet bloated tables..."
run_psql >> "$LOG_FILE" 2>&1 << 'EOSQL'
REINDEX TABLE torrents_torrent_sources;
REINDEX TABLE torrent_files;
REINDEX TABLE queue_jobs;
EOSQL

log "Step 4b: VACUUM FULL Comet torrents cache (BitMagnet VACUUM FULL handled by daily-prune)..."
sudo docker exec postgres psql -U comet -d comet -v ON_ERROR_STOP=1 << 'EOSQL'
VACUUM FULL VERBOSE torrents;
EOSQL

# Step 5: Report current stats.
log "Step 5: Current stats..."
run_psql >> "$LOG_FILE" 2>&1 << 'EOSQL'
SELECT 'DB Size'                       AS metric, pg_size_pretty(pg_database_size('bitmagnet')) AS value
UNION ALL
SELECT 'Torrents',                     COUNT(*)::text FROM torrents
UNION ALL
SELECT 'torrent_files rows',           COUNT(*)::text FROM torrent_files
UNION ALL
SELECT 'TMDB matched',                 COUNT(*)::text FROM torrent_contents WHERE content_source = 'tmdb'
UNION ALL
SELECT 'Unmatched (content_source=NULL)', COUNT(*)::text FROM torrent_contents WHERE content_source IS NULL
UNION ALL
SELECT 'Rejected last 12h',            COUNT(*)::text FROM torrent_contents
    WHERE content_type IS NULL AND created_at > NOW() - INTERVAL '12 hours';
EOSQL

log "=== Cleanup complete ==="
