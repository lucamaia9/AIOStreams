# Scripts Catalog

All scripts live in `scripts/` unless otherwise noted.

## Benchmark & Testing

### `run_real_user_benchmark.sh`
Canonical real-user Comet benchmark using direct manifest URL.
```bash
export REAL_USER_MANIFEST_URL='<canonical direct Comet manifest URL>'
./scripts/run_real_user_benchmark.sh --limit 5
```
Output: `test_runs/realuser_<run_id>/`

### `run_overnight_indexer_benchmark.sh`
Full overnight benchmark with per-indexer probe data. Runs `run_protocol.sh` for each title, collects Jackett + Prowlarr probe data, emits ranked allowlists.
```bash
./scripts/run_overnight_indexer_benchmark.sh --limit-rows 5 --run-label smoke
```
Output: `test_runs/overnight_<run_id>/ranked/`

### `probe_indexers.py`
Dependency-free indexer probe utility (stdlib only). Writes JSONL records: provider, indexer, imdb_id, media_type (`movie`/`series`/`anime`), http_status, items, elapsed_ms, error, timestamp_utc.

### `build_indexer_split.py`
Builds canonical provider split artifacts from overnight probe results:
- `providers` inventory (provider key + exact Jackett/Prowlarr names + chosen owner lane)
- content lists: `movies`, `series`, `anime` (provider + chosen owner lane)
- evidence + decision report
- stability-aware owner scoring (repeat variance and timeout signals)
- runtime backup on apply (`current.<timestamp>.bak.json`)
```bash
python3 ./scripts/build_indexer_split.py --run-dir test_runs/overnight_<run_id>
python3 ./scripts/build_indexer_split.py --run-dir test_runs/overnight_<run_id> --apply
```
Output: `test_runs/overnight_<run_id>/split/`
Published runtime artifact (`--apply`): `data/comet-fresh/indexer_split/current.json`
Generated config snippet: `test_runs/overnight_<run_id>/split/content_env_snippets.txt`
Generated human-readable summary: `test_runs/<run_id>/split/split_summary.md`

### `run_indexer_split_benchmark.sh`
Canonical split benchmark runner for provider ownership and movies/series/anime assignment.
- corpus default: `test_plan/titles_30_split_v2.csv` (10 movies + 10 series + 10 anime)
- default repeats: `3`
- default per-probe timeout: `30s`
- emits raw probe artifacts and split outputs in one run
```bash
./scripts/run_indexer_split_benchmark.sh --run-label weekly
./scripts/run_indexer_split_benchmark.sh --repeats 1 --probe-timeout 10 --run-label smoke
./scripts/run_indexer_split_benchmark.sh --apply
```
Output: `test_runs/splitbench_<run_id>/`

### `run_indexer_split.sh`
Wrapper that can consume an existing run, create a fresh overnight run, or run the new 30-title split benchmark.
```bash
./scripts/run_indexer_split.sh --run-dir test_runs/overnight_<run_id>
./scripts/run_indexer_split.sh --with-overnight --apply
./scripts/run_indexer_split.sh --with-benchmark30 --apply
```

### `run_reliability_investigation.py` / `run_reliability_investigation.sh`
Phased variant execution for live-path tuning. Supports controlled restarts, score/ranking, and artifact capture.
```bash
./scripts/run_reliability_investigation.sh                              # quick phase1
PHASE=phase3 LIMIT=6 REPEATS=1 ./scripts/run_reliability_investigation.sh  # canary
PHASE=full LIMIT=30 REPEATS=2 ./scripts/run_reliability_investigation.sh   # full
```
Output: `test_runs/investigation_<run_id>/`

### `run_antibot_3lane.py` / `run_antibot_3lane.sh`
3-lane anti-bot investigation tooling. Tests proxy lane variants and byparr modes.
```bash
./scripts/run_antibot_3lane.sh
```
Output: `test_runs/antibot_<run_id>/`

### `benchmark_live_reliability.py`
Live reliability benchmarking utility.

### `run_real_user_benchmark.py`
Python companion for the real-user benchmark shell wrapper.

## Rebalance & Ranking

### `rebalance_from_live_usage.py`
Production-traffic rebalancer. Uses the Comet runtime SQLite DB for real usage signals and Jackett/Prowlarr logs for reliability penalties. Builds separate allowlists per provider. By default it now reads `config/comet-fresh.env`, resolves `DATABASE_PATH` automatically when Comet is on SQLite, and otherwise falls back to the legacy `data/comet-fresh/comet-fresh.db` path unless an explicit `REBALANCE_RUNTIME_DB_PATH` override is present in the env file.
```bash
./scripts/rebalance_from_live_usage.py --dry-run    # no mutation
./scripts/rebalance_from_live_usage.py --apply       # supervised apply
```
Output: `test_runs/rebalance_<run_id>/`

### `run_nightly_rebalance.sh`
Nightly wrapper for `rebalance_from_live_usage.py`. Can be called by systemd timer or cron. Uses `config/comet-fresh.env` as the runtime source-of-truth instead of a hardcoded root-level env path.
```bash
./scripts/run_nightly_rebalance.sh
```

### `rebuild_comet_runtime_sqlite.py`
Offline Comet runtime SQLite rebuild wrapper. Exports the live Comet PostgreSQL runtime through `python -m comet.db_cli --skip-setup`, imports it into a fresh SQLite DB, validates row counts plus `PRAGMA quick_check`, and writes a small summary artifact under `data/comet-fresh/runtime-rebuilds/`.
```bash
python3 ./scripts/rebuild_comet_runtime_sqlite.py --run-label smoke --tables anime_entries,debrid_availability,dmm_entries --target-db-path /tmp/comet-runtime-smoke.sqlite3 --work-root /tmp/comet-runtime-smoke-work
python3 ./scripts/rebuild_comet_runtime_sqlite.py
```

### `rank_indexers_from_overnight.py`
Parses overnight benchmark artifacts. Computes balanced ranking score. Emits ranked CSVs + allowlist JSON + env snippets + report.

### Systemd Timers

| Timer | Schedule | Purpose |
|-------|----------|---------|
| `iptv-pipeline.timer` | Every 12h (00:00, 12:00 UTC) | IPTV scraper → aggregator → addon restart |
| `aiostreams-rebalance.timer` | Daily 03:30 UTC | Nightly indexer rebalancing |
| `aiostreams-benchmark.timer` | Weekly Sun 02:00 UTC | Full 90-title benchmark |
| `aiostreams-indexer-split.timer` | Weekly Sun 03:00 UTC | Build/publish provider split artifact for Comet content-type filtering |
| `aiostreams-cleanup.timer` | Weekly Sat 04:00 UTC | Disk cleanup |
| `aiostreams-bitmagnet-live-promotion.timer` | Not installed on this host | Historical reference only; use inline promotion plus manual `run_bitmagnet_live_promotion.sh` catch-up runs or your own cron |
| `aiostreams-bitmagnet-prune.timer` | Disabled | Former BitMagnet prune timer; now intentionally disabled until the parity-correct wide drain completes |

```bash
systemctl --user list-timers
journalctl --user -u aiostreams-rebalance
systemctl --user start aiostreams-rebalance.service
```

Historical units `aiostreams-lean-id-backfill.*` and `aiostreams-bitmagnet-live-promotion.*` are not installed on this host.

## BitMagnet

### `run_bitmagnet_import.sh`
Imports cleaned Magnetico JSONL into BitMagnet in 100k-row chunks with resume support.
```bash
./scripts/run_bitmagnet_import.sh
```

### `bitmagnet_import_status.sh`
Check BitMagnet import progress.

### `bitmagnet_import_watch.sh`
Watch BitMagnet import progress.

## Magnetico Search

### `build_compact_magnetico_search.sh`
Builds the compact historical SQLite search DB directly from the filtered `strictVideo.manifest.json` shard set and deploys the active artifacts into `data/comet-fresh/magnetico/` by default. Set `MAGNETICO_BUILDER_DEPLOY_DIR=/path/to/mirror` only if you explicitly want a second builder mirror.
```bash
./scripts/build_compact_magnetico_search.sh /path/to/strictVideo.manifest.json
MAGNETICO_BUILDER_DEPLOY_DIR=./data/comet-builder/magnetico ./scripts/build_compact_magnetico_search.sh /path/to/strictVideo.manifest.json
```
Current measured full-run result on this host: `3.405 GiB` artifact in `~14.7 min` from `8,784,036` manifest rows.

### `rebuild_live_sqlite.py`
Staged rebuild wrapper for `data/comet-fresh/magnetico/active.search.sqlite3`.
- preferred historical authority: `strictVideo.manifest.json`
- live authority: raw BitMagnet PostgreSQL `torrents` + `torrent_files` replayed through the authoritative Python classifier
- rebuilds under `data/comet-fresh/magnetico/rebuild-runs/<timestamp>-<label>/`
- seeds the live replay from the earliest raw source cursor so a fresh rebuild does not walk empty slices from `1970`
- verifies the rebuilt artifact with SQLite integrity checks, writes a resumable `live-promotion.checkpoint.json`, and supports backup + rollback-aware cutover
- builder mirroring is opt-in; use `--builder-deploy-dir /path/to/mirror` only when you explicitly want a second deployed copy
- if the historical manifest is not present on-host, pass `--historical-manifest` explicitly or use a trusted historical-only `--historical-seed-db`
```bash
python3 ./scripts/rebuild_live_sqlite.py --historical-manifest /path/to/strictVideo.manifest.json
python3 ./scripts/rebuild_live_sqlite.py --historical-seed-db /path/to/historical.search.sqlite3 --cutover
python3 ./scripts/rebuild_live_sqlite.py --historical-seed-db /path/to/historical.search.sqlite3 --cutover --builder-deploy-dir ./data/comet-builder/magnetico
python3 ./scripts/rebuild_live_sqlite.py --allow-empty-base --live-max-slices 1 --slice-minutes 1 --run-label smoke
```

### `query_compact_magnetico_search.sh`
Queries the deployed compact SQLite search DB through Comet's read-only local search path.
```bash
./scripts/query_compact_magnetico_search.sh --title "The Matrix" --year 1999 --limit 20
./scripts/query_compact_magnetico_search.sh --title "Daredevil" --season 1 --episode 1 --limit 20
```

### `migrate_compact_magnetico_search.py`
Upgrades the active compact SQLite DB in place to the structured search projection used by the newer local-first Comet retrieval path.
```bash
python3 ./scripts/migrate_compact_magnetico_search.py
```

### `promote_bitmagnet_live_to_compact.py`
Classifies raw BitMagnet `torrents` + `torrent_files` with the authoritative Python media contract, then syncs accepted non-`reject` rows into the active compact SQLite search DB using a stable `torrents.created_at` cursor. This is the authoritative incremental/live sync path and the rebuild baseline for the live corpus. It preserves lightweight decision evidence (`confidence_tier`, `reason_codes`, `match_source`, `contract_version`) alongside promoted rows, prefers Python-authored canonical IDs, and only falls back to BitMagnet enrichment when the authoritative classifier did not emit IDs. It still supports audit mode, source-index preparation, and in-place backfill of `tmdb_id` / `imdb_id`.
```bash
python3 ./scripts/promote_bitmagnet_live_to_compact.py --batch-size 1000
python3 ./scripts/promote_bitmagnet_live_to_compact.py --audit-only --created-after 2026-03-21T21:20:50+00:00 --max-batches 1 --batch-size 5
python3 ./scripts/promote_bitmagnet_live_to_compact.py --backfill-ids --backfill-batch-size 2000
```
Default classifier concurrency now scales with host CPU (`min(8, cpu_count - 1)`) so the live sync and rebuild path can use more than the old two-worker ceiling without changing classification semantics.

### `drain_bitmagnet_wide_offline.py`
Offline high-throughput wide-drain engine. Exports time slices into local spool files, classifies them with the authoritative Python logic, merges accepted rows into SQLite, and writes durable per-slice reports and state.
```bash
python3 ./scripts/drain_bitmagnet_wide_offline.py --state ./data/comet-fresh/magnetico/wide-drain-smoke.state.json --workdir ./data/comet-fresh/magnetico/wide-drain-smoke --slice-minutes 30 --max-slices 1 --audit-only
python3 ./scripts/drain_bitmagnet_wide_offline.py --search-db ./data/comet-fresh/magnetico/active.search.sqlite3 --state ./data/comet-fresh/magnetico/wide-drain-offline.state.json --workdir ./data/comet-fresh/magnetico/wide-drain-offline --slice-minutes 30 --classifier-workers 2
```
For a clean point-in-time rebuild and safe cutover of the deployed SQLite artifact, prefer `scripts/rebuild_live_sqlite.py`.

### `run_bitmagnet_live_promotion.sh`
Wrapper for the incremental raw-torrent sync job. On first run it bootstraps the stable raw-torrent cursor, then subsequent runs classify newly-created BitMagnet torrents/files and append accepted rows into the active SQLite DB. The old user timer is not installed on this host, so use the wrapper manually or wire it into your own scheduler if you still need recurring catch-up runs beyond inline promotion.
```bash
./scripts/run_bitmagnet_live_promotion.sh
./scripts/run_bitmagnet_live_promotion.sh --max-batches 1
```

### `run_bitmagnet_wide_drain.sh`
Locking wrapper for the long-running offline wide BitMagnet -> lean SQLite drain. Uses the offline state/workdir paths, prepares the fast Postgres source indexes by default, runs the spool-based wide-drain engine, and logs to `logs/bitmagnet-wide-drain.log`.
```bash
./scripts/run_bitmagnet_wide_drain.sh
BITMAGNET_WIDE_DRAIN_SLICE_MINUTES=30 BITMAGNET_WIDE_DRAIN_CLASSIFIER_WORKERS=2 ./scripts/run_bitmagnet_wide_drain.sh
```

### `watch_bitmagnet_wide_drain.sh`
Terminal watcher for the active offline wide drain. Shows state progress, source-index status, running drain processes, recent slice reports, and the tail of `logs/bitmagnet-wide-drain.log`.
```bash
./scripts/watch_bitmagnet_wide_drain.sh
BITMAGNET_WIDE_DRAIN_WATCH_INTERVAL=10 ./scripts/watch_bitmagnet_wide_drain.sh
```

### `backfill_lean_ids.py`
Historical lean-DB enrichment worker. On this host, use it only against an offline stage copy of the historical SQLite corpus, then deploy with `scripts/rebuild_live_sqlite.py --historical-seed-db <stage-db> --cutover`. Direct `--apply` writes to the live `active.search.sqlite3` are blocked by default unless `--allow-live-search-db` is set explicitly as an unsafe override. The worker uses phased group-based resolution, SQLite staging tables, TMDB-first metadata lookup, anime-aware catalog matching, phase-specific partial indexes, a materialized propagation-source table, and a persistent resolver cache. The non-anime `resolve_series` phase is now parser-backed:
- `guessit` plus local cleanup derive canonical series identity candidates from dirty historical `series_key` values
- parsed identities are staged in `id_backfill_series_parsed`
- exact/strong successful series resolutions are persisted in `id_backfill_series_mapping` so later reruns can reuse learned aliases
- canonical non-anime cluster build uses parsed/mapped titles via `id_backfill_series_cluster`
- `id_backfill_series_resolved_group` materializes already-resolved raw groups so the cluster rebuild avoids repeated anti-lookups against the full `media_index`
- resolved backfill output now also populates the shared compact canonical layer used by both enrichment and live local search:
  - `canonical_title_family`
  - `canonical_title_variant`
- cluster resolution still keeps strict `exact` / `strong` write policy, safe sibling fanout, and raw-group TMDB fallback only for unresolved leftovers
```bash
cp ./data/comet-fresh/magnetico/active.search.sqlite3 ./data/comet-fresh/magnetico/backups/enriched-stage.backup.sqlite3
cp ./data/comet-fresh/magnetico/backups/enriched-stage.backup.sqlite3 ./data/comet-fresh/magnetico/enriched-stage.sqlite3
/home/ubuntu/aiostreams/cometouglas/.venv/bin/python ./scripts/backfill_lean_ids.py --search-db ./data/comet-fresh/magnetico/enriched-stage.sqlite3 --phase resolve_series --max-groups 50 --audit-only
/home/ubuntu/aiostreams/cometouglas/.venv/bin/python ./scripts/backfill_lean_ids.py --search-db ./data/comet-fresh/magnetico/enriched-stage.sqlite3 --phase resolve_anime --max-groups 25 --audit-only
/home/ubuntu/aiostreams/cometouglas/.venv/bin/python ./scripts/backfill_lean_ids.py --search-db ./data/comet-fresh/magnetico/enriched-stage.sqlite3 --phase resolve_movies --max-groups 50 --audit-only
/home/ubuntu/aiostreams/cometouglas/.venv/bin/python ./scripts/backfill_lean_ids.py --search-db ./data/comet-fresh/magnetico/enriched-stage.sqlite3 --apply
python3 ./scripts/rebuild_live_sqlite.py --search-db ./data/comet-fresh/magnetico/active.search.sqlite3 --historical-seed-db ./data/comet-fresh/magnetico/enriched-stage.sqlite3 --integrity-check full --cutover
```

### `match_movies_to_tmdb.py`
Stage-only exact movie recovery helper for unresolved historical `movie_key` groups. It loads the local TMDB movie index into memory, picks the best representative release name per unresolved group, runs the live-parity Radarr parsed-title exact cascade first, and then falls back to the legacy cleaned `movie_key` exact path. The representative-name path stays conservative: it skips obvious episodic/broadcast-style rows (calendar-date releases, explicit season/episode-style hints, and numeric-only parsed title noise). Non-dry-run writes to the live `data/comet-fresh/magnetico/active.search.sqlite3` are blocked by default; point `--db-path` at an offline stage DB unless you explicitly pass the unsafe override.
```bash
python3 ./scripts/match_movies_to_tmdb.py --dry-run --limit 1000
python3 ./scripts/match_movies_to_tmdb.py --db-path ./data/comet-fresh/magnetico/enriched-stage.sqlite3 --limit 50000
python3 ./scripts/match_movies_to_tmdb.py --db-path ./data/comet-fresh/magnetico/enriched-stage.sqlite3 --batch-size 10000
```

### `smoke_test_movies.py`
Deterministic top-group smoke for the stage-only movie recovery helper. It samples the highest-count unresolved `movie_key` groups, runs the same representative-name exact path as `match_movies_to_tmdb.py`, and reports match-rate plus source breakdown. Use `--json` for gate automation.
```bash
python3 ./scripts/smoke_test_movies.py --sample-size 1000
python3 ./scripts/smoke_test_movies.py --sample-size 1000 --db-path ./data/comet-fresh/magnetico/enriched-stage.sqlite3 --json
```

### `smoke_test_movies_random.py`
Deterministic seeded random smoke for the stage-only movie recovery helper. It samples unresolved `movie_key` groups from a stable seed, reports match-rate and example hits, and is suitable for the canonical-precision section of the production gate bundle.
```bash
python3 ./scripts/smoke_test_movies_random.py --sample-size 1000 --seed 20260404
python3 ./scripts/smoke_test_movies_random.py --sample-size 1000 --seed 20260404 --db-path ./data/comet-fresh/magnetico/enriched-stage.sqlite3 --json
```

### `validate_unmatched_sample.py`
Deterministic reservoir-sample audit for unmatched SQLite rows. It classifies sampled unmatched rows as likely recoverable media vs noise/rejects, supports JSON output, and is the category-audit input for the production gate bundle.
```bash
python3 ./scripts/validate_unmatched_sample.py --category all --sample 200 --seed 20260404
python3 ./scripts/validate_unmatched_sample.py --category all --sample 200 --seed 20260404 --json
```

### `test_match_movies_to_tmdb.py`
Focused regression suite for the stage-only movie exact recovery helper. Covers the real false-positive classes discovered during rollout: numeric parser artifacts, broadcast/news releases, episodic releases, and safe representative-name positive matches.
```bash
python3 ./scripts/test_match_movies_to_tmdb.py -q
```

### `run_production_gates.py`
Host-level production gate bundle for BitMagnet -> SQLite. Runs the dual-SLO coverage report, deterministic unmatched audit, movie precision smokes, and the movie + MagneticoLocal regression suites, then emits one JSON report. It exits non-zero when a hard gate fails.
```bash
python3 ./scripts/run_production_gates.py --json
python3 ./scripts/run_production_gates.py --report-path ./data/comet-fresh/magnetico/production-gates/latest.json
```

### `run_lean_id_backfill.sh`
Locking wrapper for the long-running stage-only historical lean-ID backfill. Set `LEAN_ID_BACKFILL_SEARCH_DB` to an offline stage DB; the wrapper refuses the live `active.search.sqlite3` path on this host. Use `LEAN_ID_BACKFILL_STATE` / `LEAN_ID_BACKFILL_LOG` if you want stage-specific state and log artifacts instead of the defaults.
```bash
LEAN_ID_BACKFILL_SEARCH_DB=./data/comet-fresh/magnetico/enriched-stage.sqlite3 ./scripts/run_lean_id_backfill.sh
LEAN_ID_BACKFILL_SEARCH_DB=./data/comet-fresh/magnetico/enriched-stage.sqlite3 LEAN_ID_BACKFILL_STATE=./data/comet-fresh/magnetico/enriched-stage.state.json LEAN_ID_BACKFILL_LOG=./logs/enriched-stage.log LEAN_ID_BACKFILL_GROUP_BATCH_SIZE=40 LEAN_ID_BACKFILL_NETWORK_CONCURRENCY=4 ./scripts/run_lean_id_backfill.sh
```

### `watch_lean_id_backfill.sh`
Terminal watcher for the historical lean-ID backfill. Use the same `LEAN_ID_BACKFILL_STATE` / `LEAN_ID_BACKFILL_LOG` overrides that were used for the stage run so the watcher follows the correct artifacts.
```bash
LEAN_ID_BACKFILL_STATE=./data/comet-fresh/magnetico/enriched-stage.state.json LEAN_ID_BACKFILL_LOG=./logs/enriched-stage.log ./scripts/watch_lean_id_backfill.sh
LEAN_ID_BACKFILL_STATE=./data/comet-fresh/magnetico/enriched-stage.state.json LEAN_ID_BACKFILL_LOG=./logs/enriched-stage.log LEAN_ID_BACKFILL_WATCH_INTERVAL=10 ./scripts/watch_lean_id_backfill.sh
```
The selected state JSON is the main progress source; if you do not override it, the default is `data/comet-fresh/magnetico/lean-id-backfill.state.json`. The worker now caches per-phase totals and emits `phaseUnit`, `progressPercent`, `remainingGroups`, and `etaSeconds` once the phase total has been computed. The paired log file now carries timing breadcrumbs for the `resolve_series` bootstrap so operators can distinguish SQLite seed work from parser-bound warmup.
During the parser-backed `resolve_series` helper rebuild, the state file intentionally stays at `status=rebuilding_series_clusters` until the parsed cluster tables have been rebuilt; use the log timings plus helper-table row growth in `id_backfill_series_parsed` / `id_backfill_series_cluster` as the low-level progress signal during that bootstrap window.

### `prune_bitmagnet_workspace.py`
Prunes the BitMagnet PostgreSQL workspace after rows have been incorporated into the compact SQLite DB and the wide drain has been marked complete in SQLite metadata. Dry-run by default; `--apply` executes deletes. Optional `--allow-before-wide-drain` is an emergency override only.
```bash
python3 ./scripts/prune_bitmagnet_workspace.py
python3 ./scripts/prune_bitmagnet_workspace.py --apply --allow-before-wide-drain
python3 ./scripts/prune_bitmagnet_workspace.py --apply --vacuum-full-tables queue_jobs
```

### `run_bitmagnet_prune.sh`
Wrapper for the BitMagnet workspace prune job. Uses the active SQLite DB as the authority and stays blocked until the wide drain is complete.
```bash
./scripts/run_bitmagnet_prune.sh
./scripts/run_bitmagnet_prune.sh --apply --allow-before-wide-drain
```

### `reset_bitmagnet_workspace.py`
Full post-convergence BitMagnet workspace reset. Validates `wideDrainCompleted=true`, verifies that the saved offline-drain cursor matches the latest `torrents(created_at, info_hash)` source frontier with `0` rows after it, refuses to run while the `bitmagnet` app container is up by default, then truncates the wide staging tables so the PostgreSQL workspace returns to a small ingest-only baseline.
```bash
python3 ./scripts/reset_bitmagnet_workspace.py
python3 ./scripts/reset_bitmagnet_workspace.py --apply
```
Use this after the full wide drain is complete; it is the convergence reset, not the recurring incremental prune.

### `promote_to_sqlite.py`
Legacy compatibility bridge for inline Go-triggered promotion of already-accepted rows into SQLite `media_index`. Called by the Go processor after successful Postgres persistence. It is not the authority for filtering, categorization, reject logic, or canonical matching, and should not be used as the rebuild path.
```bash
# Test with sample payload
echo '{"contents": [...]}' | python3 scripts/promote_to_sqlite.py --stdin --dry-run --verbose

# Real usage (called by Go processor)
python3 scripts/promote_to_sqlite.py --stdin < payload.json
```
Input JSON format:
```json
{
  "contents": [
    {
      "info_hash": "abc123...",
      "name": "Movie.Title.2024.1080p.mkv",
      "size": 1234567890,
      "created_at": "2024-03-30T12:00:00Z",
      "content_type": "movie",
      "content_source": "tmdb",
      "content_id": "12345",
      "episodes": {"1": {"5": {}}},
      "video_resolution": "V1080p"
    }
  ],
  "search_db": "/path/to/search.sqlite3"
}
```

### `prepare_bitmagnet_live_indexes.sh`
Legacy helper for the superseded `torrent_hints.updated_at` live-promotion cursor. The current raw-torrent sync path does not require it.
```bash
./scripts/prepare_bitmagnet_live_indexes.sh
```

### `run_magnetico_search_index_build.sh`
Builds the Magnetico-backed local search index in Comet PostgreSQL.
```bash
./scripts/run_magnetico_search_index_build.sh
```

### `query_magnetico_search_index.sh`
Queries the built Magnetico search index.
```bash
./scripts/query_magnetico_search_index.sh
```

## Builder DMM

### `run_builder_dmm_cycle.sh`
Full-cycle DMM builder validation.
```bash
TIMEOUT_MINUTES=240 ./scripts/run_builder_dmm_cycle.sh
```

### `check_builder_dmm_incremental.sh`
Incremental DMM refresh validation.
```bash
TIMEOUT_MINUTES=30 ./scripts/check_builder_dmm_incremental.sh
```

### `start_builder_dmm.sh`
Start the DMM builder process.

### `status_builder_dmm.sh`
Check DMM builder status.

### `stop_builder_dmm.sh`
Stop the DMM builder process.

## IPTV

### `run_iptv_pipeline.sh`
Full IPTV pipeline: Telegram scraper → aggregator (`run-optimized-telegram`) → addon restart → health verification. Prevents concurrent runs via lock file. Auto-rotates log to 3000 lines.
```bash
./scripts/run_iptv_pipeline.sh                  # full pipeline
SKIP_SCRAPER=1 ./scripts/run_iptv_pipeline.sh   # skip scraper, run aggregator+addon only
SKIP_VERIFY=1 ./scripts/run_iptv_pipeline.sh    # skip final verification
```
Output: `logs/iptv-pipeline.log`

### `run_iptv_refresh_pipeline.sh`
Runs IPTV maintenance export, then rebuilds/recreates the `iptv-addon` container and checks a live `/meta` response. Ensures stale runtime code is caught automatically.
```bash
./scripts/run_iptv_refresh_pipeline.sh
```

## Maintenance

### `cleanup_disk_reclaim.sh`
Disk cleanup script. Reclaims Docker images, build cache, old test artifacts.
```bash
./scripts/cleanup_disk_reclaim.sh
```
Last run: `cleanup_20260220_110605` -- reduced disk from 76% to 20%.

## Other

### `investigate_1337x.py`
Historical investigation script for 1337x indexer issues.

### `run_protocol.sh` (root, not in scripts/)
Legacy one-command test protocol targeting the older `aiostreams-custom` / `mrdouglas2.duckdns.org` flow. Kept at root because `scripts/run_overnight_indexer_benchmark.sh` references it by relative path (`./run_protocol.sh`) and the benchmark runs as a systemd timer. Do not move without updating that chain.

### `test_aiostreams_stream.py` (root, not in scripts/)
Python regression test script for Stremio stream endpoints. Kept at root because `run_protocol.sh` references it by relative path (`./test_aiostreams_stream.py`).
```bash
python3 ./test_aiostreams_stream.py --manifest-url "https://comet.douglinhas.mywire.org/manifest.json" --media-type movie --imdb-id tt0133093
```
