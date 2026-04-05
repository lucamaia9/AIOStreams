# Scripts Catalog

Organized operational scripts for the AIOStreams pipeline.

## Directory Structure

```
scripts/
├── bitmagnet/    # DHT crawler operations (51 → 46 after dedup)
├── comet/        # Stremio API operations (17 scripts)
├── iptv/         # IPTV pipeline (5 scripts)
├── testing/      # Validation & testing (5 scripts)
├── lib/          # Shared modules (4 modules)
└── misc/         # Uncategorized utilities (5 scripts)
```

## BitMagnet Scripts (`bitmagnet/`)

### Ingestion & Promotion
- `promote_bitmagnet_live_to_compact.py` - Promote PostgreSQL torrents to SQLite (live, 10-20/min)
- `promote_to_sqlite.py` - Legacy promotion bridge (stdin-based, kept for compatibility)
- `rebuild_live_sqlite.py` - Rebuild SQLite from manifest/seed (offline/bulk)

### Matching & ID Enrichment
- `match_movies_to_tmdb.py` - Match movies to TMDB IDs (canonical version)
- `match_series_to_ids.py` - Match series to TMDB/IMDB IDs (canonical version)
- `match_anime_to_ids.py` - Match anime via Kitsu ID mapping
- `match_tmdb_daily_export.py` - Daily TMDB export for offline matching

### Backfill Operations
- `backfill_lean_ids.py` - Backfill lean IDs for unmatched content
- `backfill_movies_v3.py` - Movie backfill (latest version)
- `backfill_bitmagnet_orphans.sh` - Backfill orphaned torrents
- `bulk_sync_bitmagnet_to_sqlite.py` - Bulk sync utility

### Pruning & Cleanup
- `daily_prune_bitmagnet.sh` - Daily 2-day retention prune (02:00 UTC)
- `prune_bitmagnet_workspace.py` - Sophisticated prune (configurable retention)
- `cleanup_bitmagnet.sh` - Delete rejected/unmatched content
- `mark_exported.sh` - Mark torrents as exported (for prune eligibility)
- `reset_bitmagnet_workspace.py` - Reset workspace (nuclear option)

### TMDB Index Building
- `build_tmdb_movie_index.py` - Build movie index (1.1M entries, 335MB)
- `build_tmdb_series_index.py` - Build series index (218K entries, 75MB)

### Monitoring & Diagnostics
- `check_bitmagnet_id_coverage.py` - Check TMDB coverage stats
- `check_bitmagnet_promotion_health.sh` - Monitor promotion health
- `bitmagnet_import_status.sh` - Check import queue status
- `bitmagnet_import_watch.sh` - Watch import progress

### Search & Query
- `build_compact_magnetico_search.sh` - Build lean search index
- `query_compact_magnetico_search.sh` - Query lean index
- `query_magnetico_search_index.sh` - Query full search index
- `migrate_compact_magnetico_search.py` - Migrate search index format

### Utilities
- `delete_unmatched_content.sh` - Remove unmatched torrents
- `cleanup_disk_reclaim.sh` - Reclaim disk space
- `drain_bitmagnet_wide_offline.py` - Drain wide offline queue
- `prepare_bitmagnet_live_indexes.sh` - Prepare live DB indexes
- `canonical_promotion_worker.py` - Canonical promotion worker

### Workflow Scripts
- `run_bitmagnet_import.sh` - Start import workflow
- `run_bitmagnet_prune.sh` - Manual prune trigger
- `run_bitmagnet_live_promotion.sh` - Start live promotion
- `run_backfill_24h.sh` - Backfill last 24 hours
- `run_lean_id_backfill.sh` - Run lean ID backfill
- `run_magnetico_search_index_build.sh` - Build search index
- `watch_bitmagnet_wide_drain.sh` - Monitor wide drain
- `watch_lean_id_backfill.sh` - Monitor lean backfill

## Comet Scripts (`comet/`)

### Performance & Benchmarking
- `run_real_user_benchmark.py` - Real user workload benchmark
- `run_real_user_benchmark.sh` - Benchmark shell wrapper
- `benchmark_live_reliability.py` - Live reliability tests
- `run_overnight_indexer_benchmark.sh` - Overnight benchmark suite

### Indexer Management
- `build_indexer_split.py` - Split indexer configuration
- `run_indexer_split.sh` - Run indexer split
- `run_indexer_split_benchmark.sh` - Benchmark indexer split
- `probe_indexers.py` - Probe indexer health
- `rank_indexers_from_overnight.py` - Rank indexers by performance

### Rebalancing
- `rebalance_from_live_usage.py` - Rebalance based on live data
- `run_nightly_rebalance.sh` - Nightly rebalance job

### Builder DMM (Data Management)
- `start_builder_dmm.sh` - Start DMM builder
- `stop_builder_dmm.sh` - Stop DMM builder
- `status_builder_dmm.sh` - Check DMM status
- `check_builder_dmm_incremental.sh` - Check incremental build progress
- `run_builder_dmm_cycle.sh` - Run DMM cycle

### Runtime
- `rebuild_comet_runtime_sqlite.py` - Rebuild runtime cache

## IPTV Scripts (`iptv/`)

- `run_iptv_pipeline.sh` - Run full IPTV pipeline
- `run_iptv_refresh_pipeline.sh` - Refresh IPTV channels
- `run_iptv_health_check.sh` - IPTV health check
- `run_antibot_3lane.py` - 3-lane antibot runner
- `run_antibot_3lane.sh` - Antibot shell wrapper

## Testing Scripts (`testing/`)

- `run_production_gates.py` - Production readiness checks
- `validate_intake_filter.py` - Validate Go intake filter
- `validate_unmatched_sample.py` - Validate unmatched content
- `smoke_test_movies.py` - Movie pipeline smoke test
- `smoke_test_movies_random.py` - Random smoke tests
- `test_go_python_filter_consistency.py` - Go/Python alignment
- `test_backfill_speed.py` - Backfill performance test
- `test_backfill_v2.py` - Backfill v2 tests
- `test_match_movies_to_tmdb.py` - Movie matching tests
- `investigate_1337x.py` - 1337x investigation
- `run_reliability_investigation.py` - Reliability analysis
- `run_reliability_investigation.sh` - Reliability shell wrapper

## Lib Modules (`lib/`)

- `canonical_resolver.py` - Canonical title resolution
- `classifier_runtime.py` - Classifier runtime path bootstrap
- `env_files.py` - Environment file utilities
- `radarr_parser.py` - Radarr-compatible parsing

## Misc Utilities (`misc/`)

- `batch_fuzzy_update.py` - Batch fuzzy matching
- `compare_go_vs_python_filter.py` - Go/Python filter comparison
- `generate_patterns_go.py` - Generate Go filter patterns
- `live_enrichment_worker.py` - Live enrichment worker
- `run_production_gates.py` - Production gate checks

## Usage Notes

### Running Scripts
```bash
# Python scripts
python3 scripts/bitmagnet/match_movies_to_tmdb.py --help

# Shell scripts
bash scripts/bitmagnet/daily_prune_bitmagnet.sh
```

### Cron/Systemd Integration
Scripts with systemd timers:
- `daily_prune_bitmagnet.sh` → aiostreams-prune-torrents.timer (02:00 UTC daily)

### Script Dependencies
Most scripts require:
- `AIOSTREAMS_ROOT` environment variable (auto-detected if not set)
- Access to `bitmagnet-media/classifier/` for shared modules
- Database access (PostgreSQL for BitMagnet, SQLite for search)
