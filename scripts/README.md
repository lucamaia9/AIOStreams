# Scripts Catalog

Organized operational scripts for the AIOStreams pipeline.

## Directory Structure

```
scripts/
├── bitmagnet/    # DHT crawler operations (29 active scripts)
├── comet/        # Stremio API operations (17 scripts)
├── iptv/         # IPTV pipeline (5 scripts)
├── testing/      # Validation & testing (5 scripts)
├── lib/          # Shared modules (4 modules)
├── misc/         # Uncategorized utilities (4 scripts)
└── archived/     # Retired/dead scripts (17 scripts, preserved for reference)
```

## BitMagnet Scripts (`bitmagnet/`)

### Ingestion & Promotion
- `promote_bitmagnet_live_to_compact.py` - **Authoritative**: promote PostgreSQL torrents to SQLite (live, keyset-paged)
- `promote_to_sqlite.py` - Legacy Go-side promotion bridge (stdin-based, kept for BitMagnet PROCESSOR_PROMOTION)
- `rebuild_live_sqlite.py` - Rebuild SQLite from manifest/seed (offline/bulk, atomic cutover)

### Backfill & ID Enrichment
- `backfill_lean_ids.py` - Backfill TMDB/IMDb IDs for unmatched content
- `backfill_bitmagnet_orphans.sh` - Re-enqueue orphaned torrents via BitMagnet GraphQL API

### Pruning & Cleanup
- `daily_prune_bitmagnet.sh` - **Primary**: daily 2-day retention prune (promotion-aware, see PRUNING.md)
- `prune_bitmagnet_workspace.py` - Sophisticated batch prune with promotion tracking (--apply for dry-run)
- `cleanup_bitmagnet.sh` - Delete rejected/unmatched content, clean queue jobs
- `mark_exported.sh` - Mark torrents as exported (for prune eligibility)
- `reset_bitmagnet_workspace.py` - Reset workspace (nuclear option, requires wideDrainCompleted)

### Monitoring & Diagnostics
- `check_bitmagnet_id_coverage.py` - Check TMDB/IMDb coverage SLOs
- `check_bitmagnet_promotion_health.sh` - Monitor promotion health (PostgreSQL vs SQLite counts)

### Search & Query
- `build_compact_magnetico_search.sh` - Build lean search index
- `query_compact_magnetico_search.sh` - Query lean index
- `query_magnetico_search_index.sh` - Query full search index
- `migrate_compact_magnetico_search.py` - Migrate search index format

### Wide Drain & Index Prep
- `drain_bitmagnet_wide_offline.py` - Time-sliced CSV export for wide corpus drain
- `prepare_bitmagnet_live_indexes.sh` - Create PostgreSQL indexes for promotion

### Workflow Scripts
- `run_bitmagnet_import.sh` - Start JSONL import workflow
- `run_bitmagnet_prune.sh` - Manual prune trigger (flock-wrapped)
- `run_bitmagnet_live_promotion.sh` - Start live promotion (flock-wrapped)
- `run_lean_id_backfill.sh` - Run lean ID backfill (flock-wrapped)
- `run_magnetico_search_index_build.sh` - Build search index
- `watch_bitmagnet_wide_drain.sh` - Monitor wide drain progress
- `watch_lean_id_backfill.sh` - Monitor lean backfill progress
- `run_bitmagnet_wide_drain.sh` - Run wide drain (flock-wrapped)
- `cleanup_disk_reclaim.sh` - Host-wide disk cleanup

## Comet Scripts (`comet/`)

### Performance & Benchmarking
- `run_real_user_benchmark.py` / `.sh` - Real user workload benchmark
- `benchmark_live_reliability.py` - Live reliability tests
- `run_overnight_indexer_benchmark.sh` - Overnight benchmark suite

### Indexer Management
- `build_indexer_split.py` / `.sh` - Split indexer configuration
- `run_indexer_split_benchmark.sh` - Benchmark indexer split
- `probe_indexers.py` - Probe indexer health
- `rank_indexers_from_overnight.py` - Rank indexers by performance

### Rebalancing
- `rebalance_from_live_usage.py` - Rebalance based on live data
- `run_nightly_rebalance.sh` - Nightly rebalance job

### Builder DMM
- `start_builder_dmm.sh` / `stop_builder_dmm.sh` / `status_builder_dmm.sh` - Builder lifecycle
- `check_builder_dmm_incremental.sh` - Check incremental build progress
- `run_builder_dmm_cycle.sh` - Run DMM cycle

### Runtime
- `rebuild_comet_runtime_sqlite.py` - Rebuild runtime cache

## IPTV Scripts (`iptv/`)

- `run_iptv_pipeline.sh` - Run full IPTV pipeline (systemd timer, every 12h)
- `run_iptv_refresh_pipeline.sh` - Refresh IPTV channels
- `run_iptv_health_check.sh` - IPTV health check
- `run_antibot_3lane.py` / `.sh` - 3-lane antibot runner

## Testing Scripts (`testing/`)

- `validate_intake_filter.py` - Validate Go intake filter against Python
- `test_go_python_consistency.py` - Go/Python filter consistency test
- `investigate_1337x.py` - 1337x investigation
- `run_reliability_investigation.py` / `.sh` - Reliability analysis

## Lib Modules (`lib/`)

- `canonical_resolver.py` - Canonical title resolution
- `classifier_runtime.py` - Classifier runtime path bootstrap
- `env_files.py` - Environment file utilities
- `radarr_parser.py` - Radarr-compatible parsing

## Misc Utilities (`misc/`)

- `batch_fuzzy_update.py` - Batch fuzzy matching
- `compare_go_vs_python_filter.py` - Go/Python filter comparison
- `generate_patterns_go.py` - Generate Go filter patterns from Python
- `run_production_gates.py` - Production gate checks

## Archived Scripts (`archived/`)

Retired scripts preserved for reference. Not part of the active pipeline.
See `scripts/archived/README.md` for details.

## Usage Notes

### Running Scripts
```bash
# Python scripts
python3 scripts/bitmagnet/promote_bitmagnet_live_to_compact.py --help

# Shell scripts
bash scripts/bitmagnet/daily_prune_bitmagnet.sh
```

### Pruning
See `PRUNING.md` for the authoritative pruning runbook.
The daily prune is promotion-aware: it checks if the promotion pipeline ran
within the last 24 hours and adjusts the retention window accordingly.

### Script Dependencies
Most scripts require:
- Access to `bitmagnet-media/classifier/` for shared modules
- Database access (PostgreSQL for BitMagnet, SQLite for search)
- `sudo docker` for container operations
