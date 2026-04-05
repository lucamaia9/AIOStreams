# AIOStreams / Comet Local Changelog

## 2026-04-05 -- Master Plan Cleanup: Docs, Pruning, Scripts

- **Doc tree consolidation**: Copilot reorg had renamed all root-level operational
  docs into `docs/`, breaking AGENTS.md references. Restored originals to root,
  deleted redundant `docs/` tree (all content was duplicated).
- **Pruning safety fix**: `daily_prune_bitmagnet.sh` now checks if the promotion
  pipeline ran within 24h before using the 2-day retention window. Falls back to
  3-day safety window when promotion is lagging. Prevents deleting un-promoted torrents.
- **Script cleanup**: Archived 17 retired/dead scripts to `scripts/archived/`
  (old backfill, matching, test, and legacy promotion scripts).
- **New docs**: Added `PRUNING.md` (authoritative pruning runbook), updated
  `GO_PYTHON_ALIGNMENT.md` (replaced unverified claims with actual analysis),
  updated `scripts/README.md` (reflects current active script set).
- **Unified classifier audit**: Confirmed the shim is NOT used by production paths.
  `bitmagnet_smart_hint.py`, `compact_media_search.py`, and `stream_export_bitmagnet.py`
  all import `classify_compact_row` directly from `compact_media_search`.
- **System state**: 20 services healthy, BitMagnet actively filtering (adult_code,
  adult_brand, cjk_adult, explicit_pattern rejections confirmed in logs), smart hint
  Python classifier running (batches of 32, ~50% reject rate), SQLite at 8.2M rows.

## 2026-04-04 -- Comet Runtime SQLite Rebuild Tooling + Rebalance Path Fixes

- Added host-safe runtime SQLite rebuild plumbing for Comet:
  - `cometouglas/comet/core/models.py` now honors `COMET_ENV_FILE` so host-side and maintenance CLI runs can target an explicit env file instead of relying on `.env`
  - `cometouglas/comet/db_cli.py` now supports `--skip-setup` so live exports can avoid startup cleanup and transient-table clearing
  - added `scripts/rebuild_comet_runtime_sqlite.py` to export the live Comet PostgreSQL runtime, import it into a fresh SQLite runtime DB, validate row counts plus `PRAGMA quick_check`, and write a small rebuild summary under `data/comet-fresh/runtime-rebuilds/`
- Closed the two real runtime-rebuild correctness gaps found during the first full run:
  - `cometouglas/comet/core/db_manager.py` now exports PostgreSQL tables inside a `repeatable_read` snapshot and uses deterministic ordered/keyset pagination so live hot tables like `torrents` no longer duplicate rows during export
  - the same import path now sorts parent tables before dependent tables so `anime_ids` and `background_scraper_episodes` no longer import before their referenced parent rows
- Successfully rebuilt the full offline Comet runtime SQLite candidate:
  - output path: `data/comet-fresh/runtime/comet-runtime.sqlite3`
  - rebuild summary: `data/comet-fresh/runtime-rebuilds/20260404T183000Z-runtime-sqlite/summary.json`
  - validated counts: `2,778,051` exported rows across runtime tables, `1,026,580` `torrents`, `1,315,325` `dmm_entries`
  - SQLite checks: `PRAGMA quick_check=ok`, journal mode `wal`
- Fixed the rebalance consumer to read the current runtime schema:
  - `scripts/rebalance_from_live_usage.py` now uses `updated_at` when present and falls back to legacy `timestamp`
  - dry-run against the rebuilt runtime candidate now produces non-zero indexer recommendations instead of failing on the stale column name
- Fixed the rebalance runtime path handling:
  - `scripts/rebalance_from_live_usage.py` now defaults to `config/comet-fresh.env`
  - it resolves the runtime SQLite DB from `DATABASE_PATH` when Comet is on SQLite and otherwise falls back to the legacy `data/comet-fresh/comet-fresh.db` path unless an explicit env override is present
  - `scripts/run_nightly_rebalance.sh` now points at `config/comet-fresh.env` instead of the stale root-level env path
- Dry-run minimization evidence on the current host:
  - `scripts/prune_bitmagnet_workspace.py` currently finds `0` eligible promoted hashes, so there is no safe incremental reclaim available right now
  - `scripts/reset_bitmagnet_workspace.py` currently reports `rowsAfterCursor=79950` and `bitmagnetContainerRunning=true`, so a full reset remains unsafe at the current frontier even though `wideDrainCompleted=true`

## 2026-04-04 -- BitMagnet Storage Consolidation + Mount Correction

- Captured small local provenance artifacts for the current dirty working trees before purge:
  - `bitmagnet-media` status/diff/untracked snapshot
  - `cometouglas` status/diff/untracked snapshot
  - live DB hash/integrity/state evidence
- Re-verified the current live mounts:
  - `bitmagnet` uses `bitmagnet-media/classifier`, `bitmagnet-media/runtime/config`, `bitmagnet-media/runtime/data`, and `data/comet-fresh/magnetico/active.search.sqlite3`
  - `comet` and `comet-builder` use `data/comet-fresh`
  - live containers do **not** use `bitmagnet.old`, `data/postgres-old`, or `data/comet-builder`
- Switched builder mirroring to opt-in only:
  - `scripts/build_compact_magnetico_search.sh` now deploys only to `data/comet-fresh/magnetico/` unless `MAGNETICO_BUILDER_DEPLOY_DIR` is set
  - `scripts/rebuild_live_sqlite.py` now creates a builder mirror only when `--builder-deploy-dir` (or `REBUILD_LIVE_SQLITE_BUILDER_DEPLOY_DIR`) is set
  - `scripts/run_production_gates.py`, `BITMAGNET_OPERATIONS.md`, and `SCRIPTS_CATALOG.md` now treat the builder mirror as optional instead of required
- Host retention target is now one large historical SQLite artifact on-host: `data/comet-fresh/magnetico/active.search.sqlite3`
- Purged the redundant large local copies and legacy payloads:
  - removed `staging/`, `backups/`, `rebuild-runs/`, the builder mirror, `data/postgres-old`, and the heavy `bitmagnet.old/postgres*` trees
  - removed stale sidecar files from deleted SQLite copies plus old `test_runs/`, `logs/*`, and `bitmagnet-backup-core/core-logic-backup.tar.gz`
  - resulting footprint: `/home/ubuntu/aiostreams` about `92G -> 15G`, host `/home/ubuntu` about `37%` used with `124G` free

## 2026-04-04 -- AdGuard VPN 4-Lane Consolidation + Resource Optimization

- Renamed `adguard-proxy-prowlarr` → `adguard-proxy-london` (location-based naming)
- Added `adguard-proxy-nyc` (79.127.206.162, NYC exit IP 79.127.206.186)
- Optimized resource limits for all 4 AdGuard containers: `cpus: 0.50→0.25`, `mem_limit: 256m→96m`, `pids_limit: 256→128` (actual usage ~18-19 MiB, ~0.01-0.21% CPU)
- Updated `flaresolverr-prowlarr` NO_PROXY to reference `adguard-proxy-london`
- Updated `scripts/run_antibot_3lane.py` references
- Updated `INFRASTRUCTURE.md` and `PROJECT_OVERVIEW.md` documentation
- Verified all 4 containers healthy with correct exit IPs: Helsinki (185.77.216.3), Amsterdam (84.17.46.88), London (154.47.24.154), NYC (79.127.206.186)
- Downstream services (aiostreams, prowlarr, flaresolverr-helsinki, flaresolverr-prowlarr) unaffected

## 2026-04-04 -- Series Backfill Warmup Instrumentation

- Instrumented `scripts/backfill_lean_ids.py` so the `resolve_series` warmup now emits breadcrumbs for:
  - the initial resolved-group seed build
  - series-mapping load
  - each rebuild batch, including fetch/parse/write timings
  - slow unresolved-series summary queries
- Tightened the unresolved-series fetch path by switching the resolved-group exclusion from `LEFT JOIN ... IS NULL` to `NOT EXISTS`.
- Reused a configured `GuessItApi` instance for the parser-backed `resolve_series` warmup instead of re-creating the default helper path on every candidate parse.
- Removed redundant per-row `title_noise_score()` recomputation during series cluster writes by carrying the group-key noise score forward from parsing.
- Stage evidence from `data/comet-fresh/magnetico/staging/20260404T095047Z-movie-recovery-stage.sqlite3` now shows the real `resolve_series` bottleneck clearly:
  - resolved-group seed build: about `17s` for `280,332` already-resolved series groups
  - first rebuild batch: about `50s` for `5,000` summaries
  - fetch time stayed low (`~0.7s` to `1.9s`) while parse time dominated (`~47s` to `49s`)
  - after reusing `GuessItApi`, the same rebuild path dropped to about `45s` to `46s` per `5,000` summaries, with parse time down to about `44s`
  - this confirms the current warmup is primarily Python/GuessIt parsing work, not SQLite fetch latency, and that parser reuse yields a modest but real speedup

## 2026-04-04 -- BitMagnet Production Gate Bundle

- Added deterministic production-gate plumbing for the BitMagnet -> SQLite stack.
  - `scripts/validate_unmatched_sample.py` now supports deterministic reservoir sampling plus JSON output for machine-readable category audits
  - `scripts/smoke_test_movies.py` and `scripts/smoke_test_movies_random.py` now support JSON output and deterministic sampling so movie recovery precision can be tracked reproducibly
  - added `scripts/test_match_movies_to_tmdb.py` to lock in the movie exact-recovery guardrails discovered during rollout:
    - numeric parser artifacts must not match
    - broadcast/news representative names must not match
    - episodic representative names must not match
    - clean representative movie releases must still match
  - added `scripts/run_production_gates.py` to collect the SLO report, unmatched audit, movie precision smokes, and regression suites into a single JSON report with hard-gate exit status
- Tightened the movie representative-name guard one more step to skip `stage 09`-style event releases in addition to explicit season/episode and broadcast/date patterns.
- Updated `BITMAGNET_OPERATIONS.md` and `SCRIPTS_CATALOG.md` with the production gate bundle, cutover checklist, and rollback rehearsal checklist.

## 2026-04-04 -- Stage-Only Movie Parsed-Title Exact Recovery

- Upgraded `scripts/match_movies_to_tmdb.py` from a movie-key-only exact matcher into the safer stage-only movie recovery path for the current canonical gap.
  - the script now picks a representative unresolved release name per `movie_key` group and runs the same Radarr parsed-title exact TMDB cascade already used by `bitmagnet-media/classifier/bitmagnet_smart_hint.py`
  - only if that parsed-title path misses does it fall back to the legacy cleaned `movie_key` exact lookup
  - numeric-only parsed-title artifacts are ignored, and representative-name matching now skips obvious episodic/broadcast-style rows (calendar-date releases, season/episode-style hints, and common broadcast terms) before attempting an exact movie lookup
  - non-dry-run writes to the live `data/comet-fresh/magnetico/active.search.sqlite3` are now blocked by default unless `--allow-live-search-db` is passed as an explicit unsafe override
- Updated the companion smoke scripts so they validate the real grouped-key + representative-name flow instead of the stale movie-key-only path:
  - `scripts/smoke_test_movies.py`
  - `scripts/smoke_test_movies_random.py`
- Validation:
  - `python3 scripts/match_movies_to_tmdb.py --dry-run --limit 1000` -> `26 / 1000` top grouped keys matched (`2.6%`, all from `representative_name`)
  - `python3 scripts/smoke_test_movies_random.py` -> `306 / 1000` random grouped keys matched (`30.6%`)
  - top-count audit after the numeric/broadcast guard stopped the earlier junk hits; the first safe retained sample was `Viswasam (2019)`

## 2026-04-04 -- BitMagnet Live Promotion Throughput Tuning

- Tuned `scripts/promote_bitmagnet_live_to_compact.py` for safer higher throughput without changing acceptance or matching rules.
  - raised the default classifier worker ceiling from `2` to `min(8, cpu_count - 1)` so the live/rebuild path can use available CPU more effectively
  - parallelized bounded `fetch_id_rows()` Postgres enrichment lookups across chunked read-only batches to reduce repeated serial wait time during promotion
- Updated `SCRIPTS_CATALOG.md` to document the new default live-promotion concurrency behavior.

## 2026-04-04 -- BitMagnet SLO Baseline + Live Unknown-Video Parity

- Added dual-SLO proxy reporting to `scripts/check_bitmagnet_id_coverage.py`.
  - JSON and key/value output now include:
    - `unknown_video_rows` / `unknown_video_pct_total`
    - `canonical_missing_rows` / `canonical_missing_pct_total`
    - per-SLO pass/fail status for category and canonical coverage
- Captured the current active-SQLite baseline:
  - overall coverage: `30.52%`
  - `unknown_video`: `2.01%` of total rows
  - canonical missing IDs: `69.48%` of total rows
- Added `scripts/lib/classifier_runtime.py` and migrated the main live/rebuild/reporting utilities to the authoritative `bitmagnet-media/classifier` tree.
  - updated: `scripts/promote_bitmagnet_live_to_compact.py`
  - updated: `scripts/promote_to_sqlite.py`
  - updated: `scripts/rebuild_live_sqlite.py`
  - updated: `scripts/validate_unmatched_sample.py`
  - updated: `scripts/validate_intake_filter.py`
  - updated: `scripts/test_go_python_consistency.py`
- Removed the stale `unknown_video_dropped` audit marker from `scripts/promote_bitmagnet_live_to_compact.py`; those rows were never actually dropped by the staged SQLite path.
- Updated `bitmagnet-media/classifier/bitmagnet_smart_hint.py` so `unknown_video` is no longer hard-rejected in live smart-hint classification.
  - when no `contentType` can be safely projected, BitMagnet keeps the torrent and simply skips creating a `torrent_hints` row
  - staged Python -> SQLite sync remains the authoritative path that preserves review-tier `unknown_video` rows
- Updated `bitmagnet-media/classifier/shared_bitmagnet_bridge.py` and `scripts/test_go_python_consistency.py` so compatibility helpers and audit tooling reflect the real live smart-hint contract instead of the stale unmatched-reject rule.
- Hardened `scripts/backfill_lean_ids.py` so `--apply` against the live `data/comet-fresh/magnetico/active.search.sqlite3` now fails fast by default.
  - operators must use an offline stage DB and deploy with `scripts/rebuild_live_sqlite.py --historical-seed-db <stage-db> --cutover`
  - `--allow-live-search-db` exists only as an explicit unsafe override for legacy debugging

## 2026-04-04 -- Staged BitMagnet SQLite Enrichment Cutover

- Completed the BitMagnet/Comet historical-ID enrichment rollout with a staged offline workflow instead of live in-place SQLite writes.
- Key safety finding:
  - direct bulk writes to `data/comet-fresh/magnetico/active.search.sqlite3` are unsafe in this environment
  - both full exact and full fuzzy enrichment eventually corrupted the active DB with `database disk image is malformed`
  - future enrichment must treat the active DB as deploy-only and do all bulk writes on an offline stage copy
- Created and verified the clean rollback seed:
  - `data/comet-fresh/magnetico/backups/active-pre-enrichment-20260404T043852Z.sqlite3`
- Created offline stage DB:
  - `data/comet-fresh/magnetico/staging/enrichment-stage.sqlite3`
- Applied exact plus the useful fuzzy tranche only to the stage DB:
  - stage `rows_with_ids`: `2,396,890 -> 2,506,639`
  - cut-over active DB `rows_with_ids`: `2,507,559` (`+110,669`, `+4.62%` vs the clean pre-enrichment backup)
- Fixed compatibility gaps discovered during the rebuild/enrichment path:
  - `scripts/promote_to_sqlite.py`: fixed classifier path bootstrap for both host and container layouts, and updated inline promotion to the rebuilt contentless `media_fts(search_text)` contract
  - `scripts/match_series_to_ids.py`: added SQLite busy timeout and FTS refresh for updated rows
  - `scripts/batch_fuzzy_update.py`: switched to streaming reads, separate read/write connections, WAL + busy-timeout tuning, IMDb preservation, and FTS refresh
  - `scripts/rebuild_live_sqlite.py`: fixed `reset_live_overlay()` for contentless FTS5 by using delete tokens instead of `DELETE FROM media_fts`
- Final deployment:
  - run id: `20260404T061324Z-enriched-stage-cutover`
  - `integrity_check = ok`
  - `media_index = 8,223,899`
  - `live_promotions = 72,479`
  - final artifact SHA-256: `1473d7b7fa90a4a812773f47287eaf245fd2ecc5219dfbd0f0e6e411b7b54100`
  - rollback bundle: `data/comet-fresh/magnetico/backups/20260404T061324Z-enriched-stage-cutover/`
  - builder mirror refreshed at `data/comet-builder/magnetico/active.search.sqlite3` during cutover; later storage consolidation removed that extra on-host copy and made future mirroring opt-in
- Post-cutover live coverage:
  - movies: `19.8%`
  - episodes: `50.5%`
  - multi-episodes: `60.1%`
  - season packs: `54.8%`
  - anime episodes: `77.1%`
  - anime packs: `32.6%`
  - unknown video: `12.5%`
- Runtime verification after cutover:
  - BitMagnet `/status` healthy
  - repeated `sqlite promotion completed` events continue after restart
- Deliberately not executed:
  - the destructive unmatched-clear phase remains deferred until a separate audit can prove category-level error rates are low enough to justify deletes
- Post-cutover unmatched audit:
  - `python3 scripts/validate_unmatched_sample.py --category all --sample 200 --db-path data/comet-fresh/magnetico/active.search.sqlite3`
  - sampled `1,400` unmatched rows and still found `1,089` (`77.8%`) that look recoverable
  - category signal remained far too strong to justify deletes (`episodes 97.0% valid`, `multi-episode 95.0%`, `season_pack 90.5%`, `anime_episode 82.5%`, `anime_pack 89.0%`, `movie 67.5%`)

## 2026-04-03 -- RapidFuzz Fuzzy Matching Implementation

- **Added rapidfuzz fuzzy matching for series/episode/anime enrichment**:
  - Created `bitmagnet-media/classifier/fuzzy_series_matcher.py`: Fuzzy matcher using RapidFuzz with first-letter blocking
  - Integrated into `bitmagnet-media/classifier/tmdb_series_index.py`: Added `find_with_fuzzy()` method and lazy fuzzy matcher loading
  - Updated `bitmagnet-media/classifier/bitmagnet_smart_hint.py`: Added fuzzy fallback after exact matching fails
  - Updated `bitmagnet-media/Dockerfile`: Added `py3-pip` and `rapidfuzz` package

- **Key findings**:
  - RapidFuzz `token_set_ratio` with `default_process` works well for media titles
  - Blocking by first letter reduces candidates from 218K to ~8K per bucket
  - Internal title cleaning uses `lib.radarr_parser.clean_movie_title` (same as index build)
  - Threshold of 82 balances precision/recall

- **Validation results**:
  - Smoke test (1,000 keys): 5.7% match rate, 2.0% error rate (PASSED <3%)
  - Full series test (50K keys): 8.6% match rate
  - **Unmatched item analysis** (manual validation):
    - Series: 91% are VALID media, 9% are noise/reject
    - Anime: 88% are VALID media, 12% are noise/reject
    - Movies: 71% are VALID media, 29% are uncertain/noise

- **Scripts created**:
  - `scripts/batch_fuzzy_update.py`: Batch update script for processing all unmatched rows
  - `scripts/validate_unmatched_sample.py`: Validation script for sampling unmatched items

- **Container rebuilt**: sha256:650ce2958b5db546ef5ae0b38043c06043da95f2ff44a7f578412da90f8a6c78
- **Live system updated**: New fuzzy matching is active for new crawls

## 2026-04-03 -- Episode Parsing Overwrite Fix Deployed

- **Fixed Go/Python episode parsing conflict**: Rebuilt BitMagnet container with commit `cd1efd8` which implements the fix in `action_parse_video_content.go:30-41`
- **Before fix**: Go's ~4-pattern episode parser was overwriting Python's 109-pattern Sonarr parser via `Merge()` because the check `len(a.Episodes) == 0` happened AFTER Go parsed
- **After fix**: If `torrent.Hint.Episodes` is populated by Python's smart hint provider, Go uses that directly and skips its own parser
- **Impact**: Anime absolute episode numbers, multi-episode detection, and season inference now use Python's superior Sonarr patterns
- Container rebuild: `sudo docker compose build bitmagnet && sudo docker compose up -d bitmagnet`
- New image: sha256:45b27757bf6d202aded3a9b6d3220da60ff3fa4e48d45f503d203dbb8b9fa911
- Updated `WORKFLOW.md` section 6.1 to mark issue as fixed

## 2026-04-03 -- Series Matching Execution + Movie Matching Investigation

- **Investigated backfill failure**: The backfill service was killed (SIGTERM) after running for 14+ hours on `resolve_movies` phase with 0.17 groups/sec and 280-year ETA. TMDB API calls were the bottleneck.
- **Investigated movie matching**: `match_movies_to_tmdb.py` achieves 0% match rate on random samples because the Go classifier produces dirty `movie_key` values (includes release groups like "exkinoray", "megusta", quality tags). TMDB index has clean titles but the mismatch is unrecoverable with exact matching.
- **Investigated series matching**: Similar issue with dirty `series_key` values, but many are recoverable by stripping leading articles ("the ") and release group patterns.
- **Created `resolve_series_direct.py`**: Direct TMDB series resolution with multi-variant title cleaning (article stripping, noise pattern removal). Matches "the walking dead" → TMDB:1402, "vikings" → TMDB:44217, etc.
- **Executed series matching**: Ran in 61s, matched 14,954 series keys (2.5% match rate on 605K keys), updated 92,157 rows. Coverage improvements: episode 47.5%→49.4%, season_pack 55.6%→62.5%, unknown_video 9.4%→12.2%.
- **Current coverage**: Overall 31.73%, Anime 72.09%, Movies 22.12%, Episodes 52.30%
- **Key insight**: The historical backfill was fundamentally wrong approach (API calls vs local index). Local exact matching with cleaned titles is fast but limited by Go classifier noise in keys.
- **Movie matching is blocked**: Need TMDB fuzzy API for movies (too slow historically). Series matching has reached practical ceiling.
- Scripts created: `resolve_series_direct.py`, `smoke_test_movies.py`, `smoke_test_movies_random.py`
- All DB writes done against backup verified and WAL mode enabled

## 2026-04-03 -- BitMagnet Pipeline Roadmap + Coverage Assessment

- Created `BITMAGNET_PIPELINE_ROADMAP.md` — prioritized 6-item backlog for improving the end-to-end pipeline:
  - **P1**: Check backfill service progress (is it stuck or just slow?)
  - **P2**: Build episode enrichment script (`match_episodes_to_ids.py`, target 70% episode coverage)
  - **P3**: Fix Go/Python episode parsing merge conflict (Go's 4 regex patterns overwriting Python's 109-pattern Sonarr parser)
  - **P4**: Recreate BitMagnet container for new `bitmagnet-media/` paths
  - **P5**: Re-enable pruning + live promotion timers
  - **P6**: Movie coverage improvement (deferred — 21.75% blocked by misclassified content, not matching logic)
- Current coverage: Overall 30.72%, Anime 71.61% (✅), Movies 21.75% (❌), Episodes 49.82% (🟡)
- Pipeline is fully functional end-to-end (DHT → Go filter → Python classifier → Go enrichment → SQLite promotion → Comet search) but historical ID gap (~8.17M rows without TMDB/IMDB IDs) remains the biggest bottleneck
- Updated `BITMAGNET_OPERATIONS.md` with coverage baseline table and roadmap reference

## 2026-04-03 -- BitMagnet Repo Reorganization + End-to-End Validation

- Reorganized `bitmagnet/` into a unified `bitmagnet-media/` repository matching `lucamaia9/bitmagnet-media` on GitHub:
  - Go code at root (`internal/`, `cmd/`, `migrations/`, `webui/`, `go.mod`, `main.go`, `Dockerfile`)
  - Python pipeline in `classifier/` (with `export/`, `validation/`, `analysis/`, `unified_classifier/`)
  - Deployment assets in `deploy/`, documentation in `docs/`, runtime bind-mounts in `runtime/` (gitignored)
- Updated all path references across:
  - `compose.yaml` & `compose_fixed.yaml` (build context `./bitmagnet-media`, volume mounts)
  - `deploy/docker-compose.yml` (relative paths)
  - 15+ internal `.md` docs inside `bitmagnet-media/`
  - External docs (`BITMAGNET_OPERATIONS.md`, `AGENTS.md`, `BITMAGNET_WORKFLOW_EXPLAINED.md`, `info.md`)
  - Scripts (`scripts/generate_patterns_go.py`, `scripts/compare_go_vs_python_filter.py`, `scripts/build_compact_magnetico_search.sh`)
  - Metadata JSONs (`active.import.report.json`, `active.import.checkpoint.json`, rebuild reports)
- GitHub: pushed to `main` branch, set `main` as default, deleted `smart-hint-integration` branch
- Backup: `bitmagnet.old/` retained as fallback, `bitmagnet-backup-core.zip` (21MB) archived
- **Historical note**: At this point the live `bitmagnet` container still used old bind mounts (`bitmagnet.old/classifier-tools`). That is no longer the current runtime state after the later restart/inspection.
- End-to-end validation (2026-04-03 09:10 UTC):
  - All containers healthy (bitmagnet, bitmagnet-postgres, comet, comet-builder)
  - DHT crawler active: 5,974 torrents in last hour, 885K total in Postgres
  - Intake filter rejecting ~60-70% (adult brands, small files, scene releases)
  - Queue healthy: 3 pending, 10,864 processed, 2 failed
  - Smart hint active: processing 32-torrent batches, ~50-60% hints, rest rejects
  - Classified contents: 885,249 (299K tmdb/movies, 284K tmdb/tv_shows, 244K untagged movies, 58K untagged tv_shows)
  - Inline promotion working: 1-7 inserts per batch, 0 errors
  - SQLite: 9,075,367 entries (6.1M movies, 1.8M episodes, 597K multi-episodes, 197K season packs, 187K anime episodes)
  - Comet E2E smoke test:
    - Manifest: 200 OK
    - The Matrix (tt0133093): 59 streams, 11.1s
    - Breaking Bad S01E01 (tt0903747:1:1): 17 streams, 12.0s
    - Barbie (tt1517268, DMM hit): 17 streams, 6.0s
    - The Last of Us S01E01 (tt6473344:1:1, known zero-hit): 2 streams, 33.4s
    - MagneticoLocal confirmed active in Comet logs (120 candidates for Barbie, 80 for Breaking Bad)

## 2026-04-03 -- TMDB Movie Contract Restore + SQLite Drift Contract Checks

- Restored the intended TMDB stream behavior in Comet endpoint routing:
  - `tmdb:` **movie** requests now proceed through the normal resolver/search path instead of being hard-stopped as empty results
  - `tmdb:` series requests remain explicitly unsupported for now and return empty with a clear log reason
- Added regression tests in `tests/test_stream_matching.py` for:
  - movie-vs-series TMDB support contract
  - TMDB movie external-id resolution (`tmdb -> imdb`) path
- Hardened SQLite drift detection across runtime and ops tooling:
  - `comet/services/magnetico_local.py` now emits a one-time schema drift warning when:
    - `media_index.total_size` is not integer affinity
    - required lookup indexes are missing on `movie_key`, `series_key`, `imdb_id`, `tmdb_id`, `exact_episode_key`, or `season_pack_key`
  - `scripts/rebuild_live_sqlite.py` now treats that schema contract as mandatory validation for staged artifacts before cutover
  - `scripts/promote_bitmagnet_live_to_compact.py` now warns to stderr when it detects schema drift on the active DB, pointing operators to the rebuild/cutover path
- Documentation updates:
  - `COMET_OPERATIONS.md`
  - `BITMAGNET_OPERATIONS.md`

## 2026-04-02 -- Comet Magnetico Size-Type Hardening

- Fixed stream-path crashes caused by mixed size types reaching ranking and DB upserts.
- Root issue observed in live logs:
  - `TypeError: '>' not supported between instances of 'str' and 'float'`
  - `invalid input for query argument ... ('str' object cannot be interpreted as an integer)`
- Implemented runtime coercion guardrails in Comet:
  - `comet/services/ranking.py`: safe size coercion before `maxSize` filtering
  - `comet/services/magnetico_local.py`: coerce `total_size` to numeric in scraper output
  - `comet/services/torrent_manager.py`: coerce numeric DB-bound fields (`size`, `seeders`, `file_index`, `season`, `episode`)
  - added MagneticoLocal startup warning when `media_index.total_size` schema affinity is non-integer
- Added regression tests:
  - `tests/test_size_type_coercion.py`
  - existing `tests/test_magnetico_local.py` still passing with the new path

## 2026-04-02 -- BitMagnet SQLite Recovery + Promotion Stabilization

- Recovered `data/comet-fresh/magnetico/active.search.sqlite3` from a malformed source by rebuilding a runtime-safe DB from readable ranges and required runtime tables, then cutting over.
- Confirmed repaired DB integrity with:
  - `PRAGMA quick_check;` -> `ok`
- Removed obsolete corrupted/failed DB artifacts after successful cutover verification.
- Corrected runtime wiring to keep promotions healthy:
  - ensured scripts bridge mount is present (`./scripts:/opt/bitmagnetico/scripts:ro`)
  - ensured SQLite mount is writable for promoter path (no read-only canonical DB mount)
  - force-recreated `bitmagnet` container to clear stale SQLite sidecar state mismatch
- Verified end-to-end live flow:
  - repeated `sqlite promotion completed` log lines with `errors: 0`
  - no `sqlite promotion failed`, `database disk image is malformed`, or `attempt to write a readonly database` signatures in verification windows
  - `media_index` count and `max(torrent_id)` advanced during live watch
- Documentation updates:
  - `BITMAGNET_OPERATIONS.md`
  - `bitmagnet-media/docs/current/OPERATIONS.md`

## 2026-04-02 -- BitMagnet Episode Parsing Fix

- Fixed Go classifier overwriting Python's more accurate episode parsing results.
- **Problem**: Python uses 109 Sonarr regex patterns for episode detection; Go uses ~4 simple patterns and overwrites Python's results.
- **Solution**: Modified `action_parse_video_content.go` to check if Python provided episodes before re-parsing:
  ```go
  if len(ctx.torrent.Hint.Episodes) > 0 {
      cl.Episodes = ctx.torrent.Hint.Episodes  // Trust Python's 109-pattern parser
  } else {
      parsed, err := parsers.ParseVideoContent(ctx.torrent, ctx.result)
      cl.Merge(parsed)
  }
  ```
- **Impact**: Python's episode parsing is now preserved, improving accuracy for:
  - Anime absolute episode numbers
  - Multi-episode ranges
  - International formats (Spanish, Dutch, Turkish)
  - Date-based episodes
- **Validation**: Tested on 5000 real torrents with 99.28% single-episode accuracy.
- Documentation updates:
  - Created `docs/DOCUMENTATION_INDEX.md` - Master documentation index
  - Created `classifier/unified_classifier/WORKFLOW.md` - Full pipeline documentation
  - Updated `classifier/unified_classifier/README.md` - Added validation results
  - Updated `BITMAGNET_OPERATIONS.md` - Added Unified Classifier section
  - Updated `BITMAGNET_TARGET_LOGIC.md` - Added unified_classifier components
  - Updated `AGENTS.md` - Referenced new documentation
  - Removed `BITMAGNET_WORKFLOW_EXPLAINED.md` - Consolidated into WORKFLOW.md

## 2026-04-01 -- BitMagnet Authoritative Guardrail Tightening

- Tightened the authoritative Python classifier guardrails in `classifier/compact_media_search.py`.
- Added file-path-aware rejects for obvious sports/event dumps and preview-only junk payloads before TMDB attachment.
- Expanded targeted adult coverage for observed false accepts (missing performer aliases, `WAAA`-style codes, and explicit CJK/adult phrasing) while avoiding the previous `lesson01` short-code false positive.
- Added courseware/software coverage for Excel / PowerPivot / DAX-style training dumps so they reject as software instead of drifting into movie attachments.
- Added anime batch fallback handling for fansub-style volume packs so multi-file anime releases are preserved as `anime_pack` rather than defaulting to `movie`.
- Added focused regression tests in `tests/dht_pipeline/test_classifier_guardrails.py`.

## 2026-04-01 -- BitMagnet Promotion Evidence Retention

- Preserved authoritative Python evidence through the BitMagnet -> SQLite promotion boundary instead of collapsing rows down to bare search keys only.
- `scripts/promote_bitmagnet_live_to_compact.py` now:
  - keeps Python-authored canonical IDs when available
  - records `confidence_tier`, `reason_codes`, `match_source`, and `contract_version` in both `media_index` and `live_promotions`
  - treats BitMagnet `torrent_contents` ID enrichment as fallback-only so it does not overwrite a Python decision
- `scripts/drain_bitmagnet_wide_offline.py` now mirrors the same fallback/retention semantics during offline wide-drain merges.
- Updated lean/import/migration helpers so rebuilt or migrated compact SQLite DBs can retain the same evidence fields.
- Extended smart-hint responses and audit samples with the retained evidence fields for easier operator review.
- Documentation updates:
  - `BITMAGNET_OPERATIONS.md`
  - `SCRIPTS_CATALOG.md`

## 2026-03-31 -- BitMagnet SQLite Promotion Integration

- Added inline promotion from BitMagnet to SQLite `media_index` after Postgres persistence.
- New Go promotion interface (`internal/processor/promotion.go`):
  - `SQLitePromoter` interface with `Promote()` method
  - `pythonSQLitePromoter` calls Python script with full `TorrentContent` data
  - `noopSQLitePromoter` for disabled mode
- Integrated promotion into processor workflow (`internal/processor/processor.go`):
  - Triggers after successful `persist()` to Postgres
  - Filters to only contents with TMDB/IMDB IDs
  - Logs warning on failure, leaves content in Postgres for retry
- Added promotion config (`internal/processor/config.go`):
  - `PromotionConfig` struct with `Enabled`, `ScriptPath`, `SearchDB`, `Timeout`
  - Environment variables: `PROCESSOR_PROMOTION_ENABLED`, `PROCESSOR_PROMOTION_SCRIPT_PATH`, `PROCESSOR_PROMOTION_SEARCH_DB`
- New Python script (`scripts/promote_to_sqlite.py`):
  - Receives JSON payload via stdin with `TorrentContent` data
  - Computes all `media_index` fields: `content_class`, `season`, `episode_start/end`, etc.
  - Handles deduplication via `info_hash` uniqueness check
  - Supports `--dry-run` and `--verbose` flags for testing
- Updated docker-compose.yml:
  - Added promotion environment variables
  - Added volumes for scripts and SQLite database
- Documentation updates:
  - `BITMAGNET_OPERATIONS.md`: Added "SQLite Promotion" section
  - `SCRIPTS_CATALOG.md`: Added `promote_to_sqlite.py` entry

## 2026-03-28 -- Split Benchmark v2 (30-title repeated routine + stability-aware ownership)

- Added canonical split benchmark runner: `scripts/run_indexer_split_benchmark.sh`.
  - default corpus: `test_plan/titles_30_split_v2.csv` (`10 movies + 10 series + 10 anime`)
  - default repeats: `3`
  - default per-probe timeout: `30s`
  - outputs complete probe artifacts + split output under `test_runs/splitbench_<run_id>/`
- Extended probe records in `scripts/probe_indexers.py` with split-benchmark metadata:
  - `slot`
  - `category`
  - `repeat_id`
- Upgraded split builder `scripts/build_indexer_split.py`:
  - scoring model `indexer-split-v2` with stability and timeout signal:
    - repeat-rate variance (`repeat_success_stddev`, `repeat_non_empty_stddev`)
    - `stability_score`
    - `timeout_rate`
  - richer decision outputs:
    - `split/owner_decisions.csv`
    - `split/decision_report.md` now includes failure-signal summary, low-stability providers, and explicit content exclusions/reasons
  - `--apply` now supports runtime backup by default:
    - `data/comet-fresh/indexer_split/current.<timestamp>.bak.json`
- Updated wrapper `scripts/run_indexer_split.sh`:
  - added `--with-benchmark30` flow (and benchmark tuning flags)
  - supports `splitbench_*` run directories as direct split sources
- Documentation updates:
  - `TESTING_RUNBOOK.md` split section now uses the benchmark30-first workflow
  - `SCRIPTS_CATALOG.md` documents `run_indexer_split_benchmark.sh` and v2 split scoring/report outputs
  - `COMET_OPERATIONS.md` wording corrected to reflect config-driven (artifact/env) content routing
- Reliability hardening:
  - fixed benchmark URL resolution for host-run probes (compose `jackett/prowlarr` URLs now map to routed endpoints instead of `127.0.0.1`)
  - added provider preflight + zero-indexer guard so invalid runs fail fast instead of producing garbage splits
- Added human-readable run summary log:
  - `split/split_summary.md` (owner lanes + movies/series/anime lane breakdown)
- Full benchmark run executed:
  - run: `test_runs/splitbench_20260328_125734_split30`
  - corpus: `30 titles` (`10 movies + 10 series + 10 anime`)
  - repeats: `3` (total probe slots: `90`)
  - resulting inventory: `49` providers (`jackett=23`, `prowlarr=26`)
  - resulting content lanes: `movies=44`, `series=37`, `anime=26`
- Published runtime artifact:
  - `data/comet-fresh/indexer_split/current.json`
  - backup: `data/comet-fresh/indexer_split/current.20260328_131520.bak.json`

## 2026-03-28 -- ExtraTorrent.st (`extratorrent-st`) Deep-Dive via FlareSolverr

- Implemented a custom definition: `prowlarr/config/Definitions/Custom/extratorrent_st_douglas.yml`.
- Created indexer `ExtraTorrent.st Douglas` (ID `53`) with tag `[1]` (`FlareSolverr-Clean`) and custom definition binding.
- Observed behavior:
  - Prowlarr correctly detects Cloudflare and routes through FlareSolverr for search URLs.
  - Despite FlareSolverr challenge solve, Prowlarr receives `HTTP 403 (548 bytes)` from `extratorrent` search requests and returns 0 results / 400 API failures.
  - Manual direct calls to FlareSolverr against the same URLs can return `solution.status=200` with full HTML and magnets, indicating an integration/runtime mismatch rather than a dead parser alone.
- Operational safety action:
  - Disabled indexer ID `53` (`Enable=0`) to avoid repeated cooldown/failure penalties in production search runs.
- Working alternative remains active:
  - `EXT Torrents` (ID `52`, definition `exttorrents`) on tag `[1]` continues to return results.

## 2026-03-28 -- Added Prowlarr EXT Torrents (ext.to) via FlareSolverr

- Added new Prowlarr indexer: `EXT Torrents` (ID `52`).
- Bound to built-in Cardigann definition: `exttorrents` (already includes `https://ext.to/`).
- Configured with tag `1`, so requests route through `FlareSolverr-Clean`.
- Verified live search via Prowlarr API (indexer 52):
  - `the simpsons s02e04` -> `HTTP 200`, `5.639s`, `4` results
  - `the simpsons s02` -> `HTTP 200`, `10.187s`, `100` results
  - `the wire s01e06` -> `HTTP 200`, `9.830s`, `32` results
- FlareSolverr logs confirmed Cloudflare challenge solve for `https://ext.to/browse/...`.

## 2026-03-28 -- Config-Driven Indexer Split (No Hardcoded Python Category Maps)

Implemented a config-first indexer split pipeline so movies/series/anime definitions are not hardcoded in Python.

### What changed
- Added split generation scripts:
  - `scripts/build_indexer_split.py`
  - `scripts/run_indexer_split.sh`
- Added systemd templates for weekly refresh:
  - `scripts/systemd/aiostreams-indexer-split.service`
  - `scripts/systemd/aiostreams-indexer-split.timer`
- Added Comet runtime config key:
  - `INDEXER_SPLIT_ARTIFACT_PATH` (default: `/app/data/indexer_split/current.json`)
- Added config-native fallback env lists:
  - `JACKETT_MOVIE_INDEXERS`, `JACKETT_SERIES_INDEXERS`, `JACKETT_ANIME_INDEXERS`
  - `PROWLARR_MOVIE_INDEXERS`, `PROWLARR_SERIES_INDEXERS`, `PROWLARR_ANIME_INDEXERS`
- Comet filtering now uses:
  1. split artifact (owner + content),
  2. env lists fallback,
  3. permissive fallback (`movies+series+anime`) for unknown indexers.

### Operational effect
- Owner-lane dedup is now generated and enforced at runtime without hardcoded tracker/category maps in code.
- Movies/series/anime routing is now an artifact/config concern aligned with the rest of the Comet configuration model.

## 2026-03-27 -- Workflow Optimization: RTN-first RD Probing + needs_debrid_check Fix

**Critical bug fixed**: `needs_debrid_check` was always False when DB had 1+ cached entries, preventing RD API calls for unprobed hashes. Root cause: `DEBRID_CACHE_CHECK_RATIO=0.0` made the ratio check `X < 0.0` (never True for positive numbers).

**Fixes applied** (`comet/api/endpoints/stream.py`):
1. `needs_debrid_check`: Added `unverified_ratio > threshold` and `cachedOnly < target` conditions
2. **RTN filter moved BEFORE RD API check**: Only surviving hashes sent to RD API. 20-50% fewer calls. Log: `saved N by RTN filter`
3. `cachedOnly` override: Forces foreground scrape when DB has entries but cached < target (10)
4. Smart logging: `🔄 RD check needed: ranked=N verified=M ratio=R`, `🔄 Checking availability: realdebrid (N hashes, saved M by RTN filter)`

**Workflow change**:
- Before: `Scrape → DB check → RD API ALL hashes → RTN filter → Response`
- After: `Scrape → DB check → RTN filter → RD API only surviving hashes → Response`

**Prowlarr fix** (`config/comet-fresh.env`): Removed 8 broken/duplicate indexers (BitRu had 100+ timeouts).

**Test results**:
- Friends S01E16: 10 cached (was 1)
- Mr. Robot S01E01: 9 cached (was 3)
- Breaking Bad: 19 cached, 7s
- The Matrix: 56 cached, 5s

**Fix** (`comet/api/endpoints/stream.py` line ~1368):
- Added `unverified_ratio` check: `1 - (verified/total) > threshold` replaces broken ratio
- Added `cachedOnly` override: forces RD check when `verified_cached < target` (10)
- Added logging: `🔄 RD check needed for {title}: total={N} verified={M} unverified_ratio={R}`
- Result: Friends S01E16 went from 1 → 10 cached streams. Mr. Robot went from 3 → 9.

**Also fixed**: `cachedOnly` override forces live scrape when DB has entries but `should_scrape_now=False`

**Test results**:
- Friends S01E16: 10 cached (was 1)
- Mr. Robot S01E01: 9 cached (was 3)
- Vikings S05E03: 5 cached (unchanged — cold title, RD limitation)

**Problem**: Some requests returned with fewer than 10 cached results despite `cachedOnly=true`. Root cause: when DB had cached entries for a title, `CacheStateManager` could decide `should_scrape_now=False`, causing the request to return only DB-level results without a live scrape. Cold titles (few RD-cached torrents) would return 1-5 streams instead of waiting for a fresh scrape to find more.

**Fix** (`comet/api/endpoints/stream.py`):
- Added `cachedOnly` override: when `cachedOnly=true` and DB has entries but no live scrape is scheduled, force a foreground scrape to maximize RD cache coverage
- Added smart logging: `📦 DB cache: ... cachedOnly=True, target=10` and `🔄 cachedOnly override: forcing live scrape` for visibility

**Prowlarr fix** (`config/comet-fresh.env`):
- Removed 8 broken/duplicate indexers from `PROWLARR_INDEXERS` (BitRu, RuTorrent, EZTV, Uindex, TorrentGalaxyClone, TorrentDownload, LimeTorrents, Torrent Downloads)
- BitRu had 100+ consecutive timeouts, causing ALL Prowlarr queries to fail
- Result: Prowlarr now returns 475+ results per query (was 0)

**Test results** (15 titles, comprehensive):
- 15/15 requests returned results (0 errors)
- 12/15 have ≥10 cached results
- Median: 3.6s, Avg: 7.1s
- Hot titles: 2.8-5.0s (fast return from cache)
- Cold titles: 20-21s (full scrape, low RD cache hit rate is expected)


## 2026-03-26 -- Unified Single-Wave Search + Content-Type Indexer Filtering

**Problem**: The 3-wave sequential architecture wasted time on empty waits (House S03E05: 24s total). Jackett/Prowlarr fired late (gated behind local phase). 23 indexers were duplicated across both providers. No content-type awareness — anime indexers queried for movies, movie indexers queried for anime.

**Architecture Changes** (code changes in `cometouglas/`):
- **Single-wave parallel**: All scrapers (magnetico_local, dmm, jackett, prowlarr) fire simultaneously — no wave gating
- **Content-type detection**: `detect_content_type()` in `stream.py` determines movie/series/anime from media_id
- **Indexer filtering**: `filter_indexers_by_content_type()` in `comet/core/constants.py` — anime requests query only anime indexers (nyaa, dmhy, mikan, etc.)
- **Indexer dedup**: Same indexer in both Jackett + Prowlarr → removed from Prowlarr. Final: 40 Jackett + 5 Prowlarr-unique = 45 total with zero overlap
- **Simplified search plan**: Removed `season_fallback` and `broad_title_rescue` stages — all query variants run simultaneously

**Files Modified**:
- `comet/core/constants.py` -- added `JACKETT_INDEXER_CONTENT_TYPES`, `PROWLARR_INDEXER_CONTENT_TYPES`, `filter_indexers_by_content_type()`
- `comet/scrapers/models.py` -- added `content_type: str = "auto"` to `ScrapeRequest`
- `comet/scrapers/search_plan.py` -- simplified (removed stage widening, kept for DMM backward compat)
- `comet/scrapers/jackett.py` -- replaced wave-based `_select_indexers_for_request` with `_filter_indexers` (content-type)
- `comet/scrapers/prowlarr.py` -- replaced wave-based lane selection with content-type filtering
- `comet/api/endpoints/stream.py` -- added `detect_content_type()`, replaced 3-wave orchestration with single unified scrape
- `comet/services/orchestration.py` -- added `content_type` parameter to `scrape_torrents()`

**Smoke Test Results**:
- The Matrix (movie): 55 streams, 3s
- House S03E05 (series, cached): 10 streams, 1s
- Anime (Kitsu): content_type=anime detected, 2 Jackett + 1 Prowlarr indexer filtered

**Deprecated Settings** (kept in env for backward compat, no longer used by code):
- `LIVE_CACHED_ONLY_WAVE1/2/3_*` -- wave timeouts and scraper lists
- `LIVE_PARALLEL_WAVE1_*` -- parallel fast-lane settings
- `LIVE_SLOW_WAVE_FALLBACK_TIMEOUT_S` -- slow-wave budget
- `JACKETT_FAST_INDEXERS`, `JACKETT_SLOW_INDEXERS` -- wave-based lane selection
- `PROWLARR_FAST_INDEXERS`, `PROWLARR_SLOW_INDEXERS` -- wave-based lane selection

## 2026-03-25 -- Prowlarr Parallelization + Wave Optimization

**Problem**: Prowlarr returned 0 results for series searches (e.g., Mentalist). Root cause: `PROWLARR_GROUP_SIZE=1` sent one API call per indexer per query (18 calls for 6 queries × 3 indexers). Each call through the VPN proxy took 2-5s, exceeding the 5s wave timeout.

**Changes** (`config/comet-fresh.env`):
- `PROWLARR_GROUP_SIZE=1` → `3`: Batches 3 indexers per API call (6 calls instead of 18)
- `LIVE_PARALLEL_WAVE1_TIMEOUT_S=5` → `10`: More time for Prowlarr through VPN proxy
- `LIVE_CACHED_ONLY_WAVE2_SCRAPERS=["prowlarr"]` → `["jackett"]`: Wave2 now uses Jackett (was redundant Prowlarr retry)
- `LIVE_CACHED_ONLY_WAVE3_SCRAPERS=["jackett"]` → `[]`: Removed season_fallback wave
- `LIVE_CACHED_ONLY_WAVE3_TIMEOUT_S=4` → `0`: Zero timeout for disabled wave3
- `LIVE_RD_CALLBACK_CHUNK_SIZES=[24,72,144]` → `[240]`: Probe all ranked hashes in one callback
- `LIVE_RD_PRECHECK_CHUNK_SIZES=[24,72]` → `[240]`
- `LIVE_RD_FINAL_CHUNK_SIZES=[72,144,240]` → `[240]`
- `LIVE_RD_CHECK_BUDGET_S=6` → `10`: More time per RD probe callback
- `LIVE_RD_MIN_NEW_CANDIDATES_FOR_REPROBE=12` → `50`: Avoid wasteful re-probes on small batches
- `LIVE_SCRAPE_HARD_TIMEOUT=30` → `35`, `LIVE_TOTAL_BUDGET_S=30` → `35`, `LIVE_SCRAPE_BUDGET_S=24` → `30`

**Results**: Prowlarr now returns 472+ results per search (was 0). Wave flow: local (5s) → parallel wave1 jackett+prowlarr (10s) → wave2 jackett (10s). RD probe checks all ranked hashes in one callback. Mentalist S02E04: 6 cached streams (was 2).

## 2026-03-24 -- Prowlarr Full Indexer Audit (38 indexers tested)
Tested all 38 Prowlarr indexers with 3 search queries (Matrix, Simpsons, Breaking Bad) + follow-up verification. Each tested through Prowlarr API with FlareSolverr-Clean (tag 1, direct egress) or FlareSolverr-Helsinki (tag 7, Helsinki VPN).

**38/38 indexers working** (return results for relevant content). No dead/broken indexers found.

### Fast Indexers (< 1s, general-purpose)
- The Pirate Bay (26): 0.14s, ~100 results
- NorTorrent (21): 0.11s, ~13 results
- Demonoid Clone (44): 0.14s, ~5 results
- SubsPlease (1): 0.22s, anime-only
- Bangumi Moe (5): 0.19s, anime-only
- Mikan (42): 0.21s, anime-only
- Torrent9 (28): 0.23s, ~18 results
- BigFANGroup (6): 0.23s, ~30 results
- SkTorrent.org (45): 0.23s, ~25 results
- TorrentDownload (30): 0.24s, ~49 results
- YTS (2): 0.25s, ~24 results
- LimeTorrents (16): 0.25s, ~40 results
- ilCorSaRoNeRo (12): 0.25s, ~17 results
- BitRu (7): 0.27s, ~50 results
- TorrentQQ (33): 0.28s, varies (Chinese content)
- BitSearch (8): 0.20s, ~20 results
- RuTor (25): 0.29s, ~100 results
- NoNaMe Club (20): 0.34s, ~50 results
- nekoBT (49): 0.35s, anime-only (torznab API)
- MoviesDVDR (19): 0.37s, ~2 results
- Uindex (36): 0.38s, ~100 results
- Shana Project (41): 0.42s, anime-only
- TheRARBG (47): 0.43s, ~50 results (custom definition)
- Nyaa.si (22): 0.46s, ~75 results (anime)
- kickasstorrents.ws (15): 0.17s, ~85 results (base URL kick4ss.com)

### Medium Indexers (1-3s)
- UzTracker (3): 0.76s, ~9 results
- Elitetorrent-wf (10): 0.69s, ~6 results (Spanish)
- dmhy (9): 0.85s, varies (Chinese anime)
- EZTV (50): 0.93s, ~48 results (TV-only, tag 7 Helsinki)
- TorrentGalaxyClone (32): 0.74s, ~50 results
- MegaPeer (18): 2.2s, ~40 results
- MagnetDownload (51): 1.4s, ~10 results (no tag, direct HTTP)
- Tokyo Toshokan (48): 3-9s, anime-only

### Slow Indexers (> 5s)
- 1337x (4): 6.2s, ~80 results. `requestDelay: 2` in definition + site response time.
- Torrent[CORE] (29): 19-33s, ~50 results. `requestDelay: 10.1` + Cloudflare challenge (~11s per solve).
- Torrentsome (34): 18-58s, varies results. Cloudflare per-page solve (~11s) + pagination (2-4 pages).

### Fixes Applied
- **EZTV (ID 50)**: Re-added after deletion from tag rename. Stock `eztv` definition with tag 7 (flaresolverr-helsinki). Custom `eztv_douglas` exists but fails Prowlarr API validation.
- **kickasstorrents.ws (ID 15)**: Base URL switched from kickass.ws to kick4ss.com. Reduces FlareSolverr time from ~15s (Cloudflare challenge) to ~5s (no challenge).

### Notes
- Anime-only indexers return 0 for non-anime queries. Working correctly for anime.
- MagnetDownload (ID 51) has no tags — direct HTTP, may break if Cloudflare added.
- Torrent[CORE] and Torrentsome: inherently slow due to Cloudflare + rate-limiting. Not fixable without custom definitions.
- 1337x: 6.2s minimum with `requestDelay: 2`. Custom definition could reduce to ~4s but risk of ban.

## 2026-03-24 -- EZTV Re-added to Prowlarr
- EZTV indexer was missing after tag rename from `eztv-helsinki-flaresolverr` (tag 6) to `flaresolverr-helsinki` (tag 7).
- Re-added as ID 50 with stock `eztv` definition, tag 7, base URL `https://eztv1.xyz/`.
- Custom `eztv_douglas` definition at `/config/Definitions/Custom/` fails Prowlarr validation.
- Verified: "The Simpsons" returns 199 results.

## 2026-03-24 -- kickasstorrents-ws Base URL Switched + FlareSolverr Lane Notes
- Investigated kickasstorrents-ws slowness: kickass.ws has aggressive Cloudflare JS challenges (FlareSolverr ~15s). Alternative mirrors (kick4ss.com, kickasstorrents.bz) have no challenge (~5s).
- Changed base URL in Prowlarr to `https://kick4ss.com/`.
- FlareSolverr / HTTP proxy lane summary:
  - `flaresolverr-clean` (tag `flaresolver`, ID 1): direct egress, production for all non-EZTV indexers.
  - `flaresolverr-eztv-helsinki` (tag `flaresolverr-helsinki`, ID 7): Helsinki VPN + `Http-Helsinki` HTTP proxy on same tag.

## 2026-03-24 -- IPTV HLS Upgrade: All Streams Now .m3u8 (HLS)
- Changed aggregator default output from `ts` to `hls` in `telegram-scan.js:638` and `optimized.js:1000`.
- All 2,410 stream URLs now use `.m3u8` (HLS) format. Previously 82% were `.ts`.
- Simplified addon `personal-addon.js`: removed `checkHlsSupport()` HEAD check, `resolveStreamUrl()`, and all associated caches/constants. `getStreams()` is now synchronous — no per-source HTTP request on channel selection.
- Verified: 482 ready channels, 0 undercovered, 2 unavailable. Addon tests pass.
- Proof: `telegram_production_hls_known` output (Mar 21) showed all 88 Xtream servers accept `output=hls`. The `.ts` default was just the Telegram scraper's fallback, not a server limitation.
- Note: Next aggregator run will rebuild the server index with `.m3u8` URLs automatically. No special migration needed for subsequent runs.

## 2026-03-24 -- kickasstorrents-ws Base URL Switched + FlareSolverr Lane Notes
- Investigated kickasstorrents-ws slowness: kickass.ws has aggressive Cloudflare JS challenges that FlareSolverr solves in ~15s per request (no session reuse). Alternative mirrors (`kickasstorrents.bz`, `kick4ss.com`) have no challenge and resolve in ~5s.
- Changed kickasstorrents-ws base URL in Prowlarr from `https://kickass.ws/` to `https://kick4ss.com/`.
- FlareSolverr / HTTP proxy lane summary:
  - `flaresolverr-clean` (tag `flaresolver`, ID 1): direct egress, no proxy, no VPN. Assigned to kickasstorrents-ws and other non-proxied indexers.
  - `flaresolverr-eztv-helsinki` (tag `eztv-helsinki-flaresolverr`, ID 6): FlareSolverr routed through Helsinki VPN via `HTTP_PROXY=http://adguard-proxy-helsinki:8888`. Also has matching `Http-EZTV-Helsinki` HTTP proxy (ID 6, same tag 6) on the same lane.
  - Tag assignment determines which FlareSolverr/HTTP-proxy instance a Prowlarr indexer uses — check `/api/v1/indexerproxy` for the mapping.

## 2026-03-24 -- EZTV Works Through Helsinki First-Hop + Helsinki FlareSolverr
- Added `flaresolverr-eztv-helsinki` in `compose.yaml` as a dedicated Helsinki-backed FlareSolverr lane for EZTV testing.
- Added custom Prowlarr definition `prowlarr/config/Definitions/Custom/eztv_douglas.yml` so EZTV no longer requires inline magnet links on search rows and resolves downloads from the detail page.
- Confirmed the real blocker was the first hop: direct Prowlarr-to-EZTV traffic still got `HTTP 451`.
- Final clean fix:
  - reverted the temporary whole-container `prowlarr` Helsinki proxy
  - added a per-indexer `Http` proxy row `Http-EZTV-Helsinki` on the EZTV tag
  - kept `FlareSolverr-EZTV-Helsinki` on the same EZTV tag
  - kept `eztv_douglas` as the live EZTV definition
- Validation:
  - broad `The Simpsons` on Prowlarr `indexerIds=39` returned `199` results
  - non-EZTV sanity checks still worked after cleanup:
    - `Uindex` `The Simpsons` -> `100` results
    - `RuTor` `The Simpsons S30` -> `9` results
  - `flaresolverr-eztv-helsinki` logs showed the successful EZTV search request and returned cookies/session state for the indexer
  - documented TRaSH Guides as the operational reference for Prowlarr proxy setup: `https://trash-guides.info/Prowlarr/prowlarr-setup-proxy/`

## 2026-03-24 -- Fixed Prowlarr Uindex Cardigann Parsing
- Confirmed the live Uindex site is reachable from the clean direct-egress Prowlarr container and returns valid Simpsons search HTML, but upstream Prowlarr `uindex.yml` still targets the old `table.maintable` layout.
- Added a unique custom Cardigann definition at `prowlarr/config/Definitions/Custom/uindex_douglas.yml` using the current `table.sr-table` selectors and updated the live Prowlarr `Uindex` row to `definitionFile=uindex_douglas`.
- Verified the fix after restart:
  - broad manual search `The Simpsons` on `indexerIds=36` returns real releases instead of `[]`
  - exact `tvsearch` examples like `The Simpsons S08E03`, `The Simpsons S30E14`, and `The Simpsons S37E08` now return releases through Prowlarr/Uindex again
- Restored temporary debug tracing back to normal (`logLevel=debug`, `logIndexerResponse=false`) after validation.

## 2026-03-24 -- Added Clean FlareSolverr Test Instance
- Added `flaresolverr-clean` to `compose.yaml` as a separate direct-egress FlareSolverr service for manual Prowlarr testing.
- Purpose: allow testing a fresh FlareSolverr process at `http://flaresolverr-clean:8191` without changing the normal `prowlarr -> flaresolverr` wiring first.

## 2026-03-24 -- Restored Missing Prowlarr Uindex Indexer
- Confirmed the live `prowlarr.db` had lost the `Uindex` row entirely while the scheduled backup from `2026-03-18` still contained it.
- Restored the missing `Uindex` `Indexers` row and its matching `IndexerStatus` row from the backup after taking a safety copy of the live DB at `prowlarr/config/prowlarr.db.pre_uindex_restore_20260324`.
- Verified the live Prowlarr API now lists `Uindex` again under `/api/v1/indexer`.
- This fixed the concrete runtime regression where Comet/Prowlarr could not use `Uindex` because it was missing from the active Prowlarr config, even before considering any separate result-parsing issues.

## 2026-03-24 -- Jackett Progressive Per-Indexer Merge
- Patched `cometouglas/comet/scrapers/manager.py` so scraper tasks can emit multiple partial batches instead of forcing every scraper to return one final list.
- Converted `Jackett` to stream processed torrent batches as individual indexer/query tasks complete on the live path, instead of waiting for the slowest selected indexer before returning anything to `TorrentManager`.
- This keeps the existing orchestration contract intact: `TorrentManager.scrape_torrents()` was already prepared to filter and merge each yielded batch immediately.
- Added focused regressions in:
  - `cometouglas/tests/test_scraper_manager_streaming.py`
  - `cometouglas/tests/test_jackett_process_torrent.py`

## 2026-03-24 -- Reverted Jackett Helsinki Proxy Routing
- Removed the temporary Helsinki `HTTP_PROXY` / `HTTPS_PROXY` routing from `jackett` and `flaresolverr-jackett` in `compose.yaml`.
- Reverted the Jackett lane to direct egress after multiple public Jackett indexers (`torrentqq`, `torrentsome`, `torrenttip`, and repeated `1337x`) failed with the same runtime error: `The cookies provided by FlareSolverr are not valid`.
- FlareSolverr itself was still solving the challenges under Helsinki, so the regression point was the Jackett + FlareSolverr clearance handoff under proxied egress, not simple upstream reachability.

## 2026-03-24 -- Local Jackett EZTV Override Added
- Added a persistent local Cardigann override at `jackett/data/cardigann/definitions/eztv.yml`.
- The shipped Jackett EZTV definition was still requiring inline `a.magnet` links on the search rows, but current EZTV pages expose valid episode rows first and the magnet on the detail page.
- The local override keeps the same search shape but:
  - stops discarding search rows that lack inline magnets
  - points `download` to the episode detail URL
  - extracts magnet or `.torrent` links from the detail page, matching the current EZTV HTML structure
  - adds `info_flaresolverr` so the runtime/UI reflects the Cloudflare-solved requirement more clearly
- Bound that file over `/app/Jackett/Definitions/eztv.yml` in `compose.yaml`, because Jackett ignores same-ID local Cardigann overrides when the built-in definition already exists.

## 2026-03-24 -- Jackett + FlareSolverr Routed Through Helsinki Proxy
- Updated `compose.yaml` so both `jackett` and `flaresolverr-jackett` use `HTTP_PROXY` / `HTTPS_PROXY` via `adguard-proxy-helsinki`.
- Kept `NO_PROXY` aligned so local service-to-service traffic remains direct while outbound site traffic shares the same Helsinki VPN egress.
- This was done as the smallest operational test for blocked public trackers such as `EZTV`, with cookie/session consistency in mind by making Jackett and its paired FlareSolverr exit through the same proxy lane.

## 2026-03-24 -- Removed Local Candidate-Count Early Return For Live Series
- Patched the live stream path in `comet/api/endpoints/stream.py` so standard-series requests no longer stop in the local phase just because MagneticoLocal hit the generic `LIVE_EARLY_RETURN_CACHED_THRESHOLD`.
- Local MagneticoLocal/DMM candidate count is no longer treated as sufficient evidence to suppress online exact waves.
- RD-cached fast return remains in place; only the generic candidate-volume stop was removed from the local live series phase.

## 2026-03-24 -- Jackett MagnetUri Hash Recovery
- Narrowed the Jackett live-path loss where raw results with `MagnetUri` but no explicit `InfoHash` were being dropped.
- Reused the existing magnet hash parser in `comet/services/torrent_manager.py` and added a Jackett fallback so `process_torrent()` now:
  - derives `infoHash` from `MagnetUri` when `InfoHash` is missing
  - keeps tracker sources from the magnet
  - enqueues the magnet for background metadata resolution without enabling live `.torrent` downloads
- Added a focused regression test in `cometouglas/tests/test_jackett_process_torrent.py` covering both the magnet-hash recovery path and the non-magnet no-hash drop path.

## 2026-03-23 -- Sonarr-Style Standard Series Search Staging
- Added a bounded series search planner in `cometouglas/comet/scrapers/search_plan.py` for standard episodic TV requests:
  - `episode_exact`
  - `season_fallback`
  - `broad_title_rescue`
- Wired live-wave labels to search specificity so cached-only requests now use:
  - local + `wave1` -> exact episode intent
  - `wave2` -> season fallback
  - `wave3` -> broad title rescue
- Updated provider behavior without adding a second orchestration stack:
  - `DMM` now searches exact episode markers first and only falls back to broad title search as a last rescue stage
  - `Prowlarr` now widens exact -> season -> broad in order and logs per-stage unique-result counts
  - `Jackett` now follows the same staged widening on the existing stable JSON results endpoint
- Removed a duplicated cached-only scrape block in `comet/api/endpoints/stream.py` so the live exact/season/broad flow runs once per request instead of replaying the local/wave expansion block twice.
- Added focused planner regressions in `cometouglas/tests/test_series_search_plan.py`.

## 2026-03-23 -- Cached-Only Wave Order Fix
- Adjusted cached-only live wave orchestration so the host now exhausts exact search across both fast and slow online lanes before widening intent:
  - `local` -> exact local
  - `wave1` -> exact fast
  - `wave2` -> exact slow
  - `wave3` -> season fallback
- Root cause:
  - with `LIVE_PARALLEL_WAVE1_ENABLED=True`, the prior flow skipped the slow exact wave entirely and could jump from fast exact directly to fallback
- Updated lane semantics accordingly:
  - `Prowlarr wave2` now means slow-lane exact search
  - `Jackett wave3` now prefers fast-lane fallback rather than acting as an unlabeled broad pass

## 2026-03-23 -- MagneticoLocal Series Exact-ID Episode-Aware Patch
- Fixed a live-local-search regression in `comet/services/magnetico_local.py` for enriched series requests with long catalogs.
- Root cause:
  - the series exact-ID tier fetched the first `LIMIT` slice for `imdb_id` / `tmdb_id` without an `ORDER BY`
  - long-running shows could fill the candidate budget with unrelated seasons before the requested episode was ever considered
  - broad exact-ID rows could also starve the later `series_key` / `season_pack_key` title tiers
- New behavior:
  - exact-ID series search now stages exact episode rows first, then containing multi-episode rows, then same-season packs
  - broad exact-ID rescue rows are capped and ordered, so title/key tiers still run when the enriched ID slice is noisy
  - request logs now break exact-ID candidate counts into `ep`, `contains`, `season`, and `rescue`
- Added a focused SQLite regression test under `cometouglas/tests/test_magnetico_local.py` that reproduces the long-running-series failure shape and verifies exact-episode recovery.

## 2026-03-23 -- Directory Reorganization
- Moved legacy directories to `_archive-delete/`:
  - `comet-custom/` (replaced by `cometouglas/`)
  - `aiostreams-custom/` (replaced by `ghcr.io/viren070/aiostreams` image)
  - `comet-discord-chats/` (old Discord exports from 2026-03-08)
- Moved SSL certificates to `certs/`:
  - `comodo_amsterdam.crt`, `comodo_helsinki.crt`, `comodo_london.crt`, `comodo_nyc.crt`
  - Updated `compose.yaml` paths and `INFRASTRUCTURE.md` documentation
- Moved environment files to `config/`:
  - All `*.env` files now in `config/` directory
  - Updated `compose.yaml` env_file paths
- Moved IPTV documentation to `docs/`:
  - `IPTV_ANALYSIS_INDEX.md` and `IPTV_EXECUTIVE_SUMMARY.txt` from root `/home/ubuntu/`
- Cleaned up `scripts/__pycache__/`
- Updated `run_protocol.sh` to capture logs from correct `aiostreams` service (was incorrectly referencing `aiostreams-custom`)
- Archived legacy flaresolverr config: `flaresolverr/9401d6c1-...aiostreams.env`
- All Docker services verified healthy after changes

## 2026-03-22 -- Historical Lean-ID Backfill Pipeline Added
- Added `scripts/backfill_lean_ids.py` as the new historical SQLite ID-enrichment worker for the lean Magnetico corpus.
- Verified the historical corpus was effectively unenriched from scratch:
  - `8,165,894` historical rows outside `live_promotions`
  - `0` historical rows with `imdb_id`
  - `0` historical rows with `tmdb_id`
- Added `scripts/run_lean_id_backfill.sh`, `scripts/watch_lean_id_backfill.sh`, and `~/.config/systemd/user/aiostreams-lean-id-backfill.service`.
- Hardened the long-running backfill service for persistence:
  - enabled the user service under `default.target`
  - enabled `loginctl enable-linger ubuntu` so the job survives logout/Codex session end
  - removed the systemd restart rate limit (`StartLimitIntervalSec=0`) while keeping `Restart=on-failure`
- The new pipeline is architecture-aligned:
  - live BitMagnet enrichment remains accepted-hash-only
  - historical enrichment now runs directly against `active.search.sqlite3`
  - phases are local propagation -> non-anime series -> anime -> movies
  - anime uses a dedicated catalog path built from Anime Offline Database, Fribb, and Kitsu/IMDb mappings instead of collapsing into generic series logic
- Operational note:
  - `data/comet-fresh/magnetico/lean-id-backfill.state.json` is the authoritative progress artifact for the running backfill; `logs/lean-id-backfill.log` is intentionally minimal
  - the worker now caches per-phase group totals into SQLite metadata and emits phase-level `progressPercent`, `remainingGroups`, and `etaSeconds` in the state file after each total is computed
- Smoke checks:
  - series audit resolved `Reef Doctors` to `tt2122080` / `tmdb 42208`
  - a tiny apply run updated one historical `Reef Doctors` row in place
  - anime-specific resolver smoke resolved:
    - `Attack on Titan` -> `tt2560140` / `tmdb 1429`
    - `One Punch Man` -> `tt4508902` / `tmdb 63926`
    - `Death Note` -> `tt0877057` / `tmdb 13916`

## 2026-03-23 -- Historical Lean-ID Backfill Acceleration Pass
- Reworked the `propagate_*` phases in `scripts/backfill_lean_ids.py` from Python per-group lookups into a set-based SQLite pipeline:
  - added materialized `id_backfill_propagation_source`
  - staged propagation updates with `INSERT ... SELECT`
  - preserved the same in-place merge/checkpoint architecture
- Added phase-specific partial indexes for unresolved/resolved series, anime, and movie groups in `media_index`
- Added persistent `id_backfill_resolver_cache` so `resolve_series` / `resolve_anime` / `resolve_movies` can reuse TMDB/anime results across restarts
- Kept the existing high-level architecture unchanged:
  - historical backfill still runs in place against `active.search.sqlite3`
  - live BitMagnet enrichment remains separate
  - anime remains a dedicated phase rather than collapsing into generic series logic
- Rebuilt the propagation source, validated the new query plan no longer uses the old correlated self-`EXISTS` path, and resumed the systemd-managed background job from checkpoint

## 2026-03-22 -- MagneticoLocal Lean-DB Query Optimization
- Refactored `MagneticoLocal` retrieval into stop-early tiers:
  - exact `imdb_id`
  - exact `tmdb_id`
  - exact structured keys
  - bounded `title_key` prefix fallback
  - bounded FTS fallback
  - `norm_title LIKE` only as a final rescue path
- Fixed the main SQLite planner regression by switching exact-ID retrieval to a rowid-first pattern (`torrent_id` lookup first, full row fetch second); direct exact-ID row lookups are now index-backed again instead of full-table scanning
- Reduced the live `MAGNETICOLOCAL_CANDIDATE_LIMIT` from `250` to `120` and kept fuzzy expansion conditional on whether the strong tiers already produced enough candidates
- Added per-tier timing/candidate diagnostics to the MagneticoLocal log line (`exact_id_ms`, `exact_key_ms`, `prefix_ms`, `fts_ms`, `contains_ms`)
- Validation:
  - direct SQLite exact-ID rowid lookups use `media_index_imdb_id_idx` / `media_index_tmdb_id_idx` and complete in effectively `0.0s`
  - manifest-path replay after rebuild:
    - `The Surfer` -> `11` streams in about `2.06s`
    - `Cold Storage` -> `13` streams in about `2.12s`
    - `Playdate` first hit was constrained by stale cached torrent rows, but after the background refresh it returned `4` streams in about `1.82s`

## 2026-03-22 -- Comet The Surfer 500 Fix
- Fixed a regression in the new local-search ID plumbing where IMDb-only requests passed `external_ids={"imdb_id": "...", "tmdb_id": None}` into `ScrapeRequest`
- Sanitized `TorrentManager.external_ids` so only real string IDs are forwarded to scrapers
- Verified the exact public manifest request for `tt27813235` (`The Surfer`) no longer 500s and now returns `11` streams
- Confirmed this was not a lean-DB coverage issue:
  - lean SQLite already had `13` exact `imdb_id=tt27813235` rows and `13` exact `tmdb_id=1128655` rows
  - live logs now show local cached-only fast return with `9` cached hits and final stream count `11`

## 2026-03-22 -- Comet Local Search Precision Pass
- Reworked MagneticoLocal movie retrieval to search in tiers:
  - exact `imdb_id`
  - exact `tmdb_id`
  - exact structured keys
  - bounded FTS/title fallback
- Added TMDB-aware request support in Comet:
  - `tmdb:` movie requests no longer return empty immediately
  - Comet resolves TMDB -> IMDb when available and passes both IDs into MagneticoLocal
- Added local-search diagnostics to Comet logs:
  - MagneticoLocal candidate composition by tier
  - selected hash counts before final stream shaping
  - final visible stream counts after cached-only/dedupe shaping
- Smoke-verified on the live endpoint:
  - `tmdb:1248226` (`Playdate`) now returns `7` streams under the current cached-only RD manifest
  - `tt31425731` (`Playdate`) also returns `7` streams under the same config
  - live logs show `Playdate` local search at `123` filtered candidates with `21` exact-ID hits

## 2026-03-22 -- BitMagnet Wide Drain Completion And Workspace Reset
- Verified the offline BitMagnet wide drain had reached the current `torrents(created_at, info_hash)` source frontier exactly:
  - `lastCreatedAt = 2026-03-21T22:26:48.805553+00:00`
  - `lastInfoHash = ef00aa64a78d96584816a1d0c432dfda33bebe26`
  - rows after cursor in BitMagnet Postgres: `0`
- Marked completion in lean SQLite metadata under `live_promotion_meta`:
  - `wideDrainCompleted=true`
  - `wideDrainCompletionMode="source-frontier-equality"`
  - `wideDrainFrontierCreatedAt`, `wideDrainFrontierInfoHash`, `wideDrainScannedRows`
- Added `scripts/reset_bitmagnet_workspace.py` as the reproducible full post-convergence cleanup script. It validates `wideDrainCompleted`, verifies frontier equality from the offline state file, refuses to run while `bitmagnet` is up by default, and truncates the full wide staging workspace.
- Executed the full workspace reset after the verified drain completion:
  - pre-reset wide staging tables included roughly `torrent_files ~14.9G`, `torrent_contents ~7.6G`, `torrents ~4.3G`, `torrents_torrent_sources ~4.1G`, `torrent_hints ~2.0G`
  - post-reset BitMagnet logical DB size: `824 MB`
  - post-reset `bitmagnet-media/runtime/postgres` on-disk directory after `CHECKPOINT`: `3.8G`
  - wide staging tables reduced to near-empty baselines
- Final convergence validation after reset:
  - lean SQLite `media_index = 9,076,823`
  - lean SQLite `live_promotions = 910,905`
  - `prune_bitmagnet_workspace.py` dry-run now reports `wideDrainCompleted=true`, `pruneBlocked=false`, and `eligiblePromotedInfoHashes=0`

## 2026-03-21 -- BitMagnet Lean-DB Convergence
- Replaced the old BitMagnet live promotion seam with a parity-correct raw-torrent sync path in `scripts/promote_bitmagnet_live_to_compact.py`
- The sync path now classifies raw `torrents` + `torrent_files` with the same Python contract used by the historical Magnetico build and writes into the same lean SQLite schema
- Locked the acceptance contract so every non-`reject` smart class remains eligible for the lean DB, including `unknown_video`
- Added audit mode, wide-drain mode, and stable `torrents.created_at + info_hash` checkpointing so the wide BitMagnet corpus can be drained into SQLite without relying on noisy `torrent_hints.updated_at`
- Added `classifier/lean_media_contract.py` and switched the historical importer to reuse that shared row-shaping contract
- Blocked wide-DB pruning until the drain proves incorporation in SQLite metadata and disabled `aiostreams-bitmagnet-prune.timer` pending full drain completion
- Disabled `aiostreams-bitmagnet-live-promotion.timer`, captured a wide-drain preflight snapshot plus batch-size benchmarks, selected `batch-size=500`, and launched the full resumable drain through `scripts/run_bitmagnet_wide_drain.sh`
- Updated authoritative BitMagnet docs plus the new `docs/current/LEAN_DB_CONVERGENCE.md` reconciliation note so the final architecture survives future context compaction

## 2026-03-21 -- BitMagnet Fast Wide-Drain Refactor
- Replaced the first raw-torrent wide-drain loop in `scripts/promote_bitmagnet_live_to_compact.py` with a staged 3-phase engine:
  - flat Postgres extraction (`torrents` first, `torrent_files` second)
  - bounded parallel Python classification
  - SQLite staging-table merge with chunked FTS refresh
- Removed the hot-path `json_agg(...)` file reconstruction and row-by-row SQLite/FTS update pattern that was capping the drain at roughly tens of rows per second
- Added optional Postgres source-index preparation (`torrents(created_at, info_hash)` and `torrent_contents(info_hash)`) plus progress output with phase timings and rate/ETA reporting
- Updated `scripts/run_bitmagnet_wide_drain.sh` to use the new staged engine defaults (`batch-size=1000`, `classifier-workers=2`, index prep on by default)
- Smoke validated the new path:
  - audit mode on a 200-row batch preserved `unknown_video` parity and showed the expected legacy-drop diagnostics
  - apply mode on a 200-row batch completed with `extract=3.10s`, `classify=0.34s`, `enrich=1.12s`, `merge=0.01s`
- Added `scripts/watch_bitmagnet_wide_drain.sh` to watch checkpoint progress, source-index status, running drain processes, and the live wide-drain log tail from one terminal
- During the maintenance-window relaunch, stopped `bitmagnet`, terminated stale blocking Postgres queries, rebuilt the fast source indexes cleanly, and restarted the full drain on the new engine
- Early live wide-drain progress after the relaunch reached about `227` scanned rows/sec on 1000-row chunks (`5200` scanned, `1375` eligible, `526` inserted, `694` updated)
- Updated the authoritative BitMagnet docs and scripts catalog to reflect the staged engine, the disabled old incremental timer during convergence, and the safe final prune boundary

## 2026-03-21 -- BitMagnet Offline Wide-Drain Redesign
- Added `scripts/drain_bitmagnet_wide_offline.py` as the new one-time high-throughput convergence path:
  - export time slices into local spool CSVs
  - classify locally with the same Python smart contract
  - merge accepted rows into the lean SQLite DB
  - persist durable offline state plus per-slice reports
- Repointed `scripts/run_bitmagnet_wide_drain.sh` and `scripts/watch_bitmagnet_wide_drain.sh` to the offline spool/state layout
- Kept `scripts/promote_bitmagnet_live_to_compact.py` as the incremental/live raw-torrent sync path rather than the historical backfill engine
- Smoke validated the offline path end to end with temporary state/workdirs:
  - 5-minute audit slice: `1402` scanned, `349` eligible, `143` inserts, `203` updates
  - 5-minute apply slice: same counts with `mergeSeconds=0.876`
- Fixed the offline-path enrichment lookup to chunk accepted hashes so larger slices do not hit the shell argument limit when calling `psql`
- 30-minute apply slice validated on the real DB/workload shape:
  - `8867` scanned
  - `2339` eligible
  - `exportSeconds=21.217`
  - `classifySeconds=10.322`
  - `mergeSeconds=3.536`
  - observed throughput about `236 rows/sec`
- Started the production offline drain with BitMagnet still paused; the first production slice reused rows already incorporated during smoke validation so it recorded mostly `noopRows`, but the offline state/workdir/report path is now live
- Updated BitMagnet operations/docs so the offline spool drain is now the documented wide-drain path and the direct sync path is documented as future incremental ingestion only

## 2026-03-21 -- BitMagnet 3-Day Workspace Retention
- Added `scripts/prune_bitmagnet_workspace.py` plus `scripts/run_bitmagnet_prune.sh` to enforce the new intake-only BitMagnet storage policy from SQLite promotion state
- Installed and enabled the user-level `aiostreams-bitmagnet-prune.timer` for daily 02:25 UTC workspace cleanup
- First live validation pruned old terminal `queue_jobs` while leaving promoted torrent rows untouched because no promoted hashes were yet older than the 3-day cutoff
- Updated BitMagnet and Comet operations docs to reflect the real SQLite-first architecture: BitMagnet Postgres is now treated as a short-lived workspace, not the long-term media DB

## 2026-03-21 -- BitMagnet Hint Persistence And Rich SQLite IDs
- Changed BitMagnet processor behavior so accepted smart-hint rows persist to `torrent_hints` even when downstream Go/TMDB enrichment fails
- Kept failed enrichment hashes on the retry path instead of coupling acceptance persistence to TMDB success
- Updated `scripts/promote_bitmagnet_live_to_compact.py` to carry `tmdb_id` and `imdb_id` into the compact SQLite `media_index` rows
- Added `--backfill-ids` support so already-promoted live rows can be enriched in place without rebuilding the compact DB
- Verified the normal promotion path still runs after the change (`--max-batches 1` promoted 158 rows), completed a live backfill pass (`scannedRows=21601`, `updatedTmdbIds=2894`, `updatedImdbIds=2848`), and verified SQLite IDs against BitMagnet Postgres on live data

## 2026-03-21 -- BitMagnet Final Docs And Layout Cutover
- Renamed the legacy BitMagnet code/layout paths:
  - `bitmagnet/magnetico-dump/bitmagnet-main` -> `go/`
  - `bitmagnet/magnetico-dump/tools` -> `classifier/`
  - `bitmagnet/magnetico-dump/deploy` -> `classifier/`
- Updated live compose/script paths and active import metadata to the new layout
- Finished the docs cutover:
  - merged current implementation guidance into `docs/current/LOCAL_IMPLEMENTATION.md`
  - added `docs/current/ROLLOUT_RUNBOOK.md`
  - added active implementation-support docs under `classifier/docs/`
  - moved historical local docs under `docs/historical/`
- Archived upstream BitMagnet docs under `docs/reference/upstream-bitmagnet/` and left the originals inside `go/` so the nested repo remains self-contained
- Left short compatibility stubs under `bitmagnet/magnetico-dump/docs/` during the cleanup phase

## 2026-03-21 -- BitMagnet Docs Surface Cleanup
- Added a new BitMagnet docs map under `docs/` with explicit `current`, `historical`, and `reference` roles
- Established the intended entrypoint order for BitMagnet work:
  - `BITMAGNET_OPERATIONS.md`
  - `bitmagnet-media/BITMAGNET_TARGET_LOGIC.md`
  - `bitmagnet-media/RECOVERY.md` for recovery-only work
- Added concise current-state and deployment guides so agents/operators no longer need to start from the sprawling legacy `bitmagnet/magnetico-dump/docs/` tree
- Reframed the legacy `magnetico-dump/docs` surface as transitional/historical material instead of the primary operational truth

## 2026-03-21 -- BitMagnet Smart Hint Pipeline Fixes
- Fixed critical deadlock in `processor.go` where rejected torrents would hold mutex without calling `wg.Done()`, blocking all queue jobs until timeout
- Added concurrency semaphore (4 max) to prevent TMDB API overload from 100 simultaneous goroutines
- Fixed `persist()` to actually write to `torrent_hints` table (hints were classified but discarded)
- Increased queue job timeout from 10 to 60 minutes
- Added `releaseYear` support from Python classifier through Go to database
- Verified full pipeline: Python classifier → `torrent_hints` → Go TMDB enrichment → live promotion → SQLite
- DHT crawler now produces ~700 hints/minute with proper categorization

## 2026-03-21 -- IPTV Live HLS Cutover
- Completed the live IPTV addon cutover to HLS-backed stream URLs by rebuilding a verified HLS inventory and merging it into `iptv-aggregator/output/telegram_production/addon/addon_catalog.json`
- Live addon smoke checks passed after restart: `/health`, `/manifest.json`, and sampled `/stream/tv/personal_tv_*.json` endpoints now return `.m3u8` URLs only
- Current live bundle state: 484 catalog channels, 1039 accepted stream URLs, `0` `.ts` URLs in the addon catalog
- The clean full `run-optimized-telegram` wrapper still has a follow-up issue in the addon export tail, so the production cutover used the verified HLS inventory directly rather than waiting on that wrapper path

## 2026-03-20 -- IPTV Xtream HLS Default
- Changed the IPTV aggregator Xtream default from `output=ts` to `output=hls` in `probe-utils.js`, `inventory.js`, and `telegram-scan.js`
- Explicit per-source `output` still wins, so setting `output: "ts"` preserves MPEG-TS for providers that need it
- Smoke tested against real Telegram Xtream providers before touching production: `get.php?...output=hls` returned valid M3U playlists and the isolated subset aggregator run emitted HLS-style stream URLs

## 2026-03-20 -- IPTV Artwork Export Path Fix
- Fixed the optimized IPTV export flow in `iptv-aggregator/src/aggregator-v2/optimized.js` to pass the active production `serverIndexDir` into `exportAddonBundle`
- Root cause: optimized exports were defaulting artwork lookup to `iptv-aggregator/output/server-index` instead of `iptv-aggregator/output/telegram_production/server-index`
- Symptom: addon logo review showed only 6 matched channels and 478 `readable_fallback` text tiles even though the real production server index still contained logo-bearing candidates
- Expected behavior after regeneration: addon artwork resolves against the production server index again and real channel logo PNGs are rendered back into `output/telegram_production/addon/assets/logos/`

This is a chronological changelog of significant changes. For operational details, see the specific docs:
- Comet code, live path, waves, filtering -> `COMET_OPERATIONS.md`
- IPTV aggregator + addon -> `IPTV_OPERATIONS.md`
- BitMagnet / Magnetico -> `BITMAGNET_OPERATIONS.md`
- Infrastructure (Traefik, VPN, proxy) -> `INFRASTRUCTURE.md`
- Scripts -> `SCRIPTS_CATALOG.md`
- Testing / benchmarks -> `TESTING_RUNBOOK.md`
- Architecture overview -> `PROJECT_OVERVIEW.md`
- Agent guide -> `AGENTS.md`

---

## 2026-03-20 -- BitMagnet Lean Fresh-Import Path
- Adopted the fresh curated BitMagnet production path: no legacy pre-filter DHT corpus in the main DB
- Trusted shard importer now defaults to `skip_queue=true` and uses `.trusted-import-state.json`
- Trusted import now materializes `torrent_contents` directly from import hints so the historical corpus is searchable without a second full queue pass
- Added `deploy/vps/prepare_lean_db.sh` and wired `deploy/vps/import_and_validate.sh` to apply lean DB settings, run trusted import, analyze search tables, and assert `queue_jobs=0`
- Updated BitMagnet deployment/runbook docs to preserve the TSV search index for Torznab/API title queries and to document the fresh curated rollout as the primary path

## 2026-03-20 -- Historical JSONL-to-SQLite Import Completed
- Added `bitmagnet/magnetico-dump/tools/import_compact_search_jsonl.py` to build the historical search DB directly from `strictVideo.manifest.json` and its filtered JSONL shards
- Updated `scripts/build_compact_magnetico_search.sh` to deploy the active search DB from the filtered manifest into both `data/comet-fresh/magnetico/` and `data/comet-builder/magnetico/`
- Fixed `comet/services/magnetico_local.py` to open the deployed SQLite artifact as immutable read-only, which avoids read-side SQLite write errors on the production artifact
- Fixed `scripts/query_compact_magnetico_search.sh` to query through Comet's read-only local search path
- Completed the full historical import from the filtered shard set:
  - `8,784,036` processed rows
  - `8,662,837` inserted searchable rows
  - final deployed DB size `3.405 GiB`
  - elapsed time `881.999s` (~14.7 min)
- Updated Comet/BitMagnet operations docs, scripts catalog, and BitMagnet current-state notes for the JSONL-first historical corpus path

## 2026-03-20 -- Live BitMagnet DHT + SQLite Promotion Cutover
- Enabled the host BitMagnet runtime in live mode with `http_server,queue_server,dht_crawler`
- Kept the Python `smart_hint` seam active so live DHT intake continues to use the authoritative BitMagnetico filtering/categorization logic
- Added `scripts/prepare_bitmagnet_live_indexes.sh` to create the `torrent_hints(updated_at, info_hash)` index used by the recurring live-promotion cursor
- Hardened `scripts/promote_bitmagnet_live_to_compact.py` to:
  - cursor on `torrent_hints.updated_at`
  - reuse existing `torrent_id` values on `info_hash` collisions
  - preserve episode/multi-episode/season-pack rows in the live SQLite sync path
  - support `--bootstrap-now` so live promotion starts from the current cutover point instead of replaying legacy BitMagnet state
- Added `scripts/run_bitmagnet_live_promotion.sh` plus the user-level `aiostreams-bitmagnet-live-promotion.timer` for 10-minute recurring sync into the active compact SQLite DB
- Enabled `SCRAPE_MAGNETICOLOCAL=True` and kept `SCRAPE_BITMAGNET=False` in both `comet-fresh.env` and `comet-builder.env`
- Rebuilt/restarted `bitmagnet`, `comet`, and `comet-builder`; validated:
  - BitMagnet `/status` reports `dht.status=up`
  - BitMagnet logs show `dht_crawler` started
  - both Comet containers became healthy with the SQLite local-search path enabled

## 2026-03-19 -- Optimized IPTV Aggregator Pipeline
- Created `run-optimized-telegram` command in `iptv-aggregator` (optimized.js)
- Fast path: skips revalidation for channels already at backup target (401 of 495 channels)
- Per-channel 30s timeout prevents hanging on dead servers
- Resume support via `optimized_state.json`
- Pipeline now completes in ~3 min (vs hours for the original full discovery cycle)
- 391 ready addon channels, 36 live checks on first run, 401 skipped via fast path
- Wired as default aggregator command in `scripts/run_iptv_pipeline.sh`
- Updated `IPTV_OPERATIONS.md` and `IPTV_INFRASTRUCTURE_ANALYSIS.md` to reflect current state

## 2026-03-19 -- Telegram Scraper Fix
- Upgraded Telethon 1.25.0 → 1.40.0 (fixes `UPDATE_APP_TO_LOGIN` error)
- Removed `cryptg` dependency (no aarch64 wheel, optional speedup)
- Fixed Docker OUTPUT_DIR mismatch: scraper now prefers env vars over config.env
- Fresh session created (logged in as @zimoroque, DC 4)
- Scraper confirmed working in Docker: 20,215 sources, session persisted
- Fixed scraper `docker-compose.yml`: removed broken `session.session` mount, added `OUTPUT_DIR=/app/output`

## 2026-03-19 -- IPTV Pipeline Stabilization
- Created unified pipeline script `scripts/run_iptv_pipeline.sh` (scraper → aggregator → addon restart → health verify)
- Fixed Telegram scraper: `str()` cast for SESSION_FILE (Telethon 1.25.0 Path compatibility)
- Fixed legacy `scripts/run_iptv_refresh_pipeline.sh`: removed broken session volume mount, rebuild→restart
- Created systemd units: `iptv-pipeline.service` + `iptv-pipeline.timer` (12h, Persistent=true)
- Added lock-file mechanism to prevent concurrent runs
- Pipeline uses `--evidence-ttl-hours 336` to reuse server discovery across runs
- **Known issue**: Telegram scraper Telethon 1.25.0 too old for current Telegram API (`UPDATE_APP_TO_LOGIN`). Needs upgrade to ≥1.30+ for fresh auth. Existing `sources_clean.json` (6083 sources, 315 servers) still valid.
- First aggregator run in progress (~3-4h full server discovery). Subsequent runs will be fast with evidence reuse.

## 2026-03-19 -- Live Path Restoration
- The v2.53.0 upstream merge (commit `bc86903`) accidentally deleted ~840 lines of custom live-path logic from `stream.py`
- Restored wave orchestration (wave1->wave2->wave3), chunked RD probing, cached-only fast return
- Removed dead `LIVE_DMM_MAX_RESULTS` from `models.py`
- Config changes: `LIVE_FAST_RETURN_RD_CACHED_RESULTS=10`, wave timeouts 4s, background scraper enabled
- Test results (10-title benchmark): median 2.585s, p95 16.19s, 10/10 RD cached positive
- Git branch: `restore/live-path` from `pre-restore-baseline` tag
- Plan: `cometouglas/LIVE_PATH_RESTORATION_PLAN.md`

## 2026-03-18 -- Comet v2.53.0 Merge
- Upgraded cometouglas from local version to upstream v2.53.0
- Preserved: fast/slow Prowlarr lanes, alias normalization, multilingual matching, BitMagnet series query, DMM unlimited query, Magnetico tables
- Accepted upstream: EpisodeIndexService, Rust filename parser, scraper orchestration, schema migration
- Validation: 167 streams for tt0133093 (The Matrix)

## 2026-03-18 -- RD Availability Fix
- Fixed StremThru availability mismatch in `comet/debrid/stremthru.py`
- Availability parsing now accepts `cached` + `downloaded` before file-level validation

## 2026-03-18 -- VPN Proxy DNS Fix
- Root cause of Torrentio proxy failures: DNS resolution inside `adguard-proxy-helsinki`
- Fix: restore Docker embedded DNS after tunnel, Squid configured with `dns_v4_first on`
- Enabled addon proxy: only `*.strem.fun` uses Helsinki VPN
- Details: `INFRASTRUCTURE.md`

## 2026-03-16 -- DMM Accepted Topology
- `comet-builder` no longer behind compose profile
- Public `comet`: `SCRAPE_DMM=live`, `DMM_INGEST_ENABLED=False`
- Builder: `DMM_INGEST_ENABLED=True`, shared PostgreSQL
- Removed `prowlarr-clean` from compose/runtime

## 2026-03-16 -- BitMagnet Recovery & Restart
- Verified bitmagnet-postgres is reusable: torrents ~11.16M, queue 136473 processed
- Both containers restart cleanly; Torznab returns valid RSS/XML
- Details: `BITMAGNET_OPERATIONS.md`, `bitmagnet-media/RECOVERY.md`

## 2026-03-15 -- IPTV Curation & Deployment Sync
- Export-time presentation cleanup: quality/codec markers removed, all-caps normalized, categories fixed
- Deployment sync fix: `run_iptv_refresh_pipeline.sh` rebuilds addon + checks live `/meta`
- Artwork pipeline: local 320x320 PNG assets replace IPTV.org
- Details: `IPTV_OPERATIONS.md`

## 2026-03-14 -- IPTV Addon Bugfixes
- Category dropdown bug: `&` in genre values -> safe labels like "Kids and Family"
- Artwork bug: source-truth logos preserved earlier in pipeline
- Details: `IPTV_OPERATIONS.md`

## 2026-03-13 -- IPTV Aggregator Lean Inventory
- Replaced batch inventory with per-server indexes and per-channel candidate files on disk
- API-first indexing with playlist fallback
- Channel-universe builder: accepted 506 canonical channels
- Universe-aware re-index reduced fill target from 74207 to 495
- Maintenance mode: `run-maintenance-telegram`, `rebalance-inventory`, `export-addon-bundle`
- Details: `IPTV_OPERATIONS.md`

## 2026-03-12 -- IPTV Addon Launched
- New self-hosted IPTV live channels addon: `iptv.douglinhas.mywire.org`
- Branding: TV Doug
- Personal mode consumes aggregator addon_catalog.json
- Details: `IPTV_OPERATIONS.md`

## 2026-03-12 -- AIOStreams Refresh
- AIOStreams refreshed to v2.25.3 (`ghcr.io/viren070/aiostreams:latest`)

## 2026-03-11 -- AIOMetadata Staged
- Services added to compose: `aiometadata`, `aiometadata_redis`
- Public URL: `https://aiometadata.douglinhas.mywire.org`

## 2026-03-11 -- BitMagnet Scraping Disabled
- `SCRAPE_BITMAGNET=False` on both `comet` and `comet-builder.env`
- BitMagnet service itself kept running for DHT/storage

## 2026-03-10 -- BitMagnet Import & A/B
- Import from `exports-clean-full-parallel/kept.jsonl` completed (~9.87M torrents)
- Title-first series search implemented in cometouglas
- Two live A/B rounds: no clear win for default live wave-1, kept disabled
- Magnetico search architecture started (tables, builder, query tools)
- Details: `BITMAGNET_OPERATIONS.md`

## 2026-03-09 -- Prowlarr Wave Fix & Operational Notes
- Prowlarr fast/slow lane split: `PROWLARR_FAST_INDEXERS`, `PROWLARR_SLOW_INDEXERS`
- Fixed cached-only fast-return to key off post-RTN cached set (not raw hit count)
- Removed live DMM cap (was 40)
- Corrected series cache clearing: season-pack rows must also be removed
- Private fork created: `https://github.com/lucamaia9/cometouglas`
- Indexer name normalization patch in `indexer_manager.update_prowlarr`
- Details: `COMET_OPERATIONS.md`

## 2026-03-09 -- BitMagnet Installed
- Compose profile `bitmagnet`: `bitmagnet-postgres` + `bitmagnet`
- Dedicated PostgreSQL, internal-only, DHT port 3334
- Cold start expected; Torznab 200 with empty RSS initially

## 2026-03-01 -- Resource Right-Sizing & Fixes
- CPU/RAM/PID limits for active services in compose.yaml
- Traefik log level: DEBUG -> INFO
- `prowlarr-clean` moved behind profile `ops`
- AdGuard proxy restart-loop fix: idempotent rsyslog in `vpn-proxy/start.sh`
- AdGuard zombie prevention: `workers 0` in `squid.conf`
- Boot stability hardening: bounded VPN retry, local health check, relaxed window
- Details: `INFRASTRUCTURE.md`

## 2026-03-02 -- Byparr Removed
- Removed `byparr`, `byparr-jackett`, `byparr-prowlarr` from compose
- Reason: `BYPARR_MODE=off`, not in traffic path
- Details: `INFRASTRUCTURE.md`

## 2026-02-20 -- 3-Lane Anti-Bot Hardening
- 3-lane AdGuard proxy topology: Helsinki (addons), Amsterdam (Jackett, disabled), London (Prowlarr)
- Comet anti-burst: `LIVE_QUERY_DEDUP_TTL_S`, `INDEXER_REQUEST_JITTER_MAX_MS`, `query_dedupe.py`
- Details: `INFRASTRUCTURE.md`, `COMET_OPERATIONS.md`

## 2026-02-20 -- Disk Reclaim
- `scripts/cleanup_disk_reclaim.sh`: disk from 76% to 20%
- /home/ubuntu reduced from ~52G to ~18G

## 2026-02-19 -- Reliability Investigation & Rebalance
- 20s reliability investigation framework: `scripts/run_reliability_investigation.py`
- New runtime knobs: `LIVE_TOTAL_BUDGET_S`, split budgets, early return thresholds
- Adaptive rebalance: `scripts/rebalance_from_live_usage.py`, nightly wrapper, systemd timers
- 20s latency profile applied to `comet-fresh.env`
- Details: `COMET_OPERATIONS.md`, `SCRIPTS_CATALOG.md`

## 2026-02-19 -- Comet Reliability Overhaul
- Live path controls: hard timeout, early return threshold, min provider count
- Prowlarr chunked indexer groups with partial-result merge
- Indexer circuit breaker: `INDEXER_FAIL_STREAK_THRESHOLD=3`, `INDEXER_COOLDOWN_SECONDS=900`
- Magnet/infohash-first: `.torrent` download skipped on live requests
- Details: `COMET_OPERATIONS.md`

## 2026-02-18 -- Overnight Benchmark Pipeline
- 90-title template, overnight runner, probe utility, ranking parser
- Scoring: success_rate 0.40, p95_latency 0.30, avg_results 0.20, error_penalty 0.10
- Details: `SCRIPTS_CATALOG.md`

## 2026-02-18 -- Prowlarr 1337x Fix
- FlareSolverr proxy: host -> internal Docker DNS, timeout -> 30s
- Cleared stale 1337x IndexerStatus in Prowlarr DB
- Details: `COMET_OPERATIONS.md` (historical notes)

## 2026-02-18 -- Jackett Timeout Investigation
- Root cause: FlareSolverr URL used public host IP instead of internal Docker DNS
- Fix: `FlareSolverrUrl` -> `http://flaresolverr:8191`
- Details: `COMET_OPERATIONS.md` (historical notes)

## 2026-02-17 -- AIOStreams/Comet Project Handoff
- Established compose.yaml with local Comet source build (`../comet`)
- Prowlarr switched to "new mode" (no explicit indexerIds)
- Jackettio parser hardened, Prowlarr scraper improvements
- Stream timeout UX: suppress timeout errors when normal streams exist
- Initial testing protocol via `run_protocol.sh`
- Details: `PROJECT_OVERVIEW.md`, `TESTING_RUNBOOK.md`
## 2026-03-20

- Added Comet `MagneticoLocalScraper` and compact-SQLite historical search service so Comet can read a local `active.search.sqlite3` artifact directly instead of relying on BitMagnet for historical search.
- Added host scripts for the SQLite-first path:
  - `scripts/build_compact_magnetico_search.sh`
  - `scripts/query_compact_magnetico_search.sh`
  - `scripts/promote_bitmagnet_live_to_compact.py`
- Staged `SCRAPE_MAGNETICOLOCAL` settings in `comet-fresh.env` and `comet-builder.env` with the active artifact path `/app/data/magnetico/active.search.sqlite3`.
- Updated Comet/BitMagnet operations docs and scripts catalog for the SQLite-first historical search design.

## 2026-03-21

- Updated cached-only live wave config in `comet-fresh.env` and `comet-builder.env` so `magneticolocal` runs in wave 1 alongside `prowlarr`, allowing SQLite Magnetico hashes to participate in RD cached-only probing without exposing uncached torrent results.
- Added `cometouglas/.dockerignore` to exclude the host `.venv` from Docker builds after confirming it was overwriting the image-built venv and forcing slow runtime `uv` environment rebuilds on container recreate.
- Added a repo-pinned search plan at `bitmagnet/magnetico-dump/docs/comet-bitmagnetico-search-plan.md`.
- Shifted Comet toward a local-first cached-only retrieval order:
  - wave 1: `magneticolocal` (+ `dmm` when enabled)
  - wave 2: `prowlarr`
  - wave 3: `jackett`
- Added structured compact-search migration/build support for `movie_key`, `series_key`, `exact_episode_key`, and `season_pack_key`.
- Refined the cached-only live path so RD account snapshot is now merged into the early local phase before provider expansion, instead of being attached only after scrape waves finish.
- Kept the next-pass search logic intentionally stable:
  - `MagneticoLocal` remains a broad structured retriever
  - `DMM` remains a broad local source
  - `Prowlarr` and `Jackett` stay expansion-only
- Documented that the active SQLite corpus currently relies on title/episode structured keys; `imdb_id`, `tmdb_id`, and `aliases_text` are present but not populated enough to be required runtime signals.

## 2026-03-22

- Fixed a cached-only correctness bug in Comet where `DEBRID_CACHE_TTL=0` caused `get_cached_availability()` to treat all RD cache rows as expired immediately, collapsing follow-up requests back to partial live probing even after cached RD rows had been written.
- Updated active cache policy in `comet-fresh.env` / `comet-builder.env`:
  - `TORRENT_CACHE_TTL=604800`
  - `LIVE_TORRENT_CACHE_TTL=900`
  - `DEBRID_CACHE_TTL=21600`
- Hardened debrid cache consumers to use an effective positive RD TTL fallback and added startup/request logging so cached RD reuse is visible in logs.
- Fixed series episode recall for long-running IMDb-backed shows:
  - `MagneticoLocal` now prioritizes exact-ID series rows by requested season/episode after indexed IMDb/TMDB lookup.
  - `TorrentManager` now keeps season-matched packs/bundles in the candidate set for episode requests so strict file-level RD matching can validate them later.
- Validation case: `Grey's Anatomy S01E03 (tt0413573:1:3)` went from sync-only/no playable results to `3` total streams (`2` real cached results) and now reuses `23` cached candidate torrents on follow-up requests.
- Details: `COMET_OPERATIONS.md`

## 2026-03-23

- Added a shared compact canonical title layer to the lean SQLite DB so historical enrichment and live local search can reuse the same learned title families:
  - durable families now live in `canonical_title_family`
  - durable learned variants now live in `canonical_title_variant`
  - existing historical `id_backfill_series_mapping` rows are synced into that shared layer
- Updated `scripts/backfill_lean_ids.py` so the historical worker is the only writer to the shared canonical layer:
  - exact/strong movie/series/anime resolutions now upsert canonical families plus compact variant rows
  - the large parsed/cluster tables remain work tables; the canonical layer is the long-lived storage-light artifact
- Updated `MagneticoLocal` to consult the shared canonical layer read-only between exact-ID/key matching and fuzzy fallback:
  - request titles now expand with learned canonical family titles when the variant mapping is unique and structurally compatible
  - smoke check: `All American Homecoming Sheeeit` now expands to canonical `all american homecoming` through the shared layer and returns local results without loosening resolver thresholds
- Rebuilt/restarted the background lean-ID backfill service and rebuilt the `comet` container so both sides are running on the shared-layer code.
- Raised `~/.config/systemd/user/aiostreams-lean-id-backfill.service` from `MemoryMax=2G` to `MemoryMax=4G`, reloaded/restarted the unit, and verified `resolve_series` resumed cleanly with the cursor advancing immediately after restart.
- Recorded the current matching-architecture decision in `BITMAGNET_OPERATIONS.md`:
  - keep the shared parser + canonical-family layer as the baseline
  - do not add the fuller deferred Sonarr-style scene-mapping layers during the current `resolve_series` run
  - only revisit those heavier layers later if post-run audit data shows a repeated miss class that the current design is not handling well

- Reworked the historical lean-ID `resolve_series` phase in `scripts/backfill_lean_ids.py` from raw `series_key` TMDB lookups to canonical non-anime series clusters:
  - new helper table: `id_backfill_series_cluster`
  - cluster key: cleaned title + year hint + coarse season bucket
  - one cluster-level resolution now fans out to compatible sibling raw groups
  - already-resolved sibling groups can short-circuit TMDB as `local_cluster_propagation`
  - raw-group TMDB fallback remains, but only for leftovers inside unresolved clusters
- Added cluster-aware progress metadata (`phaseUnit=cluster`) and watcher support in `scripts/watch_lean_id_backfill.sh`.
- Bumped the `resolve_series` strategy version so the background service resets that phase cleanly and rebuilds the helper table on restart.
- Bounded smoke checks:
  - `11.22.63` noisy siblings now inherit IDs from an already-resolved sibling through `local_cluster_propagation`
  - `12 Monkeys` resolves cleanly at cluster level
  - the noisy `24` sample is now guarded instead of being over-assigned
  - `101 Places to Party Before You Die` still needs a cleaner representative from the full helper rebuild, which is expected from the bounded sample
- Reworked historical non-anime `resolve_series` again into the permanent parser-backed `cluster-v3` path:
  - added `guessit` as the structured series parser for dirty historical `series_key` values
  - new helper tables:
    - `id_backfill_series_parsed`
    - `id_backfill_series_mapping`
    - `id_backfill_series_resolved_group`
  - parsed/mapped canonical titles now drive `id_backfill_series_cluster` instead of raw string cleanup alone
  - exact/strong successful series resolutions are persisted as reusable mappings so future passes can reuse learned aliases instead of repeating the same TMDB misses
  - the rebuild hot path now materializes resolved raw groups first and uses `COALESCE(NULLIF(imdb_id,''), NULLIF(tmdb_id,'')) IS NULL` for the unresolved scan, which moved the summary query back onto the `series_key`-driven plan instead of the bad multi-index `imdb_id` plan
- Parser-backed smoke checks now produce materially better canonical series identities for the previously bad samples:
  - `00035 the vampire diaries s03` -> `the vampire diaries`
  - `12 monkeys 1 lostfilm tv` -> `12 monkeys`
  - `11 22 63 complete dimension` -> `11 22 63`
  - `911 lone star s01` -> `911 lone star`

- Added `TheRARBG` back to Prowlarr on `2026-03-24` using custom definition `prowlarr/config/Definitions/Custom/therarbg_douglas.yml`, pinned to `https://rarbg.unblockninja.st/`.
- Verified the new mirror serves the same `get-posts/...:format:json/` API as the stock `therarbg` family, so the custom definition only reorders the mirror list and keeps the stock parser logic.
- Added the indexer cleanly through the live Prowlarr API as `TheRARBG` (`definitionFile=therarbg_douglas`) and confirmed live searches return results:
  - `The Simpsons` -> `50` results
  - `The Simpsons S30E04` -> `50` results
- Noted one behavior caveat in ops docs: TheRARBG's own search is broad for episodic queries, so precision still depends on downstream Prowlarr/Comet filtering rather than tight upstream `SxxExx` matching.

- Added easy built-in Prowlarr presets on `2026-03-24`:
  - `Tokyo Toshokan` as indexer `48`
  - `nekoBT` as indexer `49`
- Verified both with live Prowlarr API searches:
  - `Tokyo Toshokan` + `One Piece` -> `47` results
  - `nekoBT` + `One Piece` -> `25` results
- Attempted the other two easy built-ins from the same pass, but they failed upstream during add-time validation and were not kept:
  - `Anidex` -> `502 Bad Gateway`
  - `RuTracker.RU` -> `403 Forbidden`
- `RuTracker.org` was intentionally skipped in this pass because it is semi-private and not an easy add without credentials.

- Added a staged live SQLite rebuild path on `2026-04-01` for `data/comet-fresh/magnetico/active.search.sqlite3`:
  - new wrapper: `scripts/rebuild_live_sqlite.py`
  - preferred historical authority is `strictVideo.manifest.json`; live authority is raw BitMagnet `torrents` + `torrent_files`
  - rebuilds now stage under `data/comet-fresh/magnetico/rebuild-runs/<timestamp>-<label>/`, seed the live replay from the earliest raw source cursor, validate the rebuilt SQLite artifact with integrity checks, and write a resumable `live-promotion.checkpoint.json`
  - optional cutover now creates timestamped backups plus a generated rollback script before replacing the deployed artifact
  - when only a trusted historical seed DB is available, the wrapper strips any tracked live overlay and replays live rows from raw authoritative inputs instead of trusting old SQLite live state
