# BitMagnet Operations

This document is the authoritative runtime and operations guide for the BitMagnet area in AIOStreams.

Use it together with:

- `bitmagnet-media/BITMAGNET_TARGET_LOGIC.md` for architecture and ownership boundaries
- `bitmagnet-media/RECOVERY.md` for recovery-only notes
- `classifier/unified_classifier/WORKFLOW.md` for classifier pipeline details
- `docs/DOCUMENTATION_INDEX.md` for the complete documentation map
- `BITMAGNET_PIPELINE_ROADMAP.md` for the prioritized improvement backlog
- `BITMAGNET_PIPELINE_ROADMAP.md` for the prioritized improvement backlog

## Service Architecture

- Compose profile: `bitmagnet`
- Services: `bitmagnet-postgres` (dedicated PostgreSQL), `bitmagnet`
- Database: dedicated PostgreSQL at `bitmagnet-postgres:5432`, NOT shared with Comet
- Runtime: internal-only, manual/off-peak
- API: `http://bitmagnet:3333`
- Torznab: `http://bitmagnet:3333/torznab/api`
- Host BitTorrent/DHT port: `3334/tcp` and `3334/udp`
- Health: `/status` returns `status=up` when healthy; may return `503` during cold start
- Current host runtime target:
  - `http_server,queue_server,dht_crawler` enabled
  - `smart_hint` enabled
  - the old 10-minute `aiostreams-bitmagnet-live-promotion.timer` is not installed on this host; rely on inline promotion and manual `scripts/run_bitmagnet_live_promotion.sh` catch-up runs if needed
  - live incorporation is now handled by the staged raw-torrent sync engine in `scripts/promote_bitmagnet_live_to_compact.py`
  - wide-DB pruning is currently blocked until the parity-correct wide drain completes and marks `wideDrainCompleted=true` in SQLite metadata

### End-to-end validation snapshot (2026-04-03 09:10 UTC)

- All containers healthy: bitmagnet (Up 15h), bitmagnet-postgres (Up 13d, healthy), comet (Up 14h, healthy), comet-builder (Up 10d, healthy)
- BitMagnet `/status`: `{"status":"up","details":{"dht":"up","postgres":"up","tmdb":"up"}}`
- DHT crawler: 5,974 torrents in last hour, 885,493 total in Postgres
- Intake filter: actively rejecting adult brands, small files, scene releases (~60-70% rejection rate)
- Queue: 3 pending, 10,864 processed, 2 failed (healthy, no backlog)
- Smart hint: processing 32-torrent batches, ~50-60% hints, rest rejects
- Classified contents: 885,249 (299K tmdb/movie, 284K tmdb/tv_show, 244K movie, 58K tv_show)
- Inline promotion: 1-7 inserts per batch, 0 errors
- SQLite: 9,075,367 entries (6.1M movies, 1.8M episodes, 597K multi-episodes, 197K season packs, 187K anime episodes)
- Comet E2E smoke test (cachedOnly=true, Real-Debrid):
  - The Matrix (tt0133093): 59 streams, 11.1s
  - Breaking Bad S01E01 (tt0903747:1:1): 17 streams, 12.0s
  - Barbie (tt1517268, DMM hit): 17 streams, 6.0s
  - The Last of Us S01E01 (tt6473344:1:1, known zero-hit): 2 streams, 33.4s
  - MagneticoLocal confirmed active in Comet search logs

### Compose path migration note (2026-04-03)

- BitMagnet source reorganized from `bitmagnet/` to `bitmagnet-media/`
- `compose.yaml` updated: build context `./bitmagnet-media`, volume mounts use `./bitmagnet-media/classifier`, `./bitmagnet-media/runtime/config`, `./bitmagnet-media/runtime/data`
- **Current runtime**: The live `bitmagnet` container now mounts `bitmagnet-media/classifier`, `bitmagnet-media/runtime/config`, and `bitmagnet-media/runtime/data`.
- `bitmagnet.old/` is historical fallback material only; it is not part of the live runtime mount set anymore.

### Recovery status (2026-04-02)

- Active compact DB path: `data/comet-fresh/magnetico/active.search.sqlite3` (~5.6 GB)
- Integrity check: `sqlite3 ... "PRAGMA quick_check;"` returns `ok`
- Corruption source that was removed from runtime:
  - prior malformed source had unreadable tail beginning at `torrent_id >= 9070128`
  - repaired runtime DB was rebuilt from readable ranges + runtime tables, then cut over
- Corrupted/obsolete DB artifacts were deleted after cutover verification
- Promotion failure signatures previously seen (`readonly`, `database disk image is malformed`) are not present after compose + DB repair cutover

### Promotion verification snapshot (2026-04-02)

- Container health:
  - `sudo docker compose -f /home/ubuntu/aiostreams/compose.yaml ps bitmagnet` -> `Up`
  - `/status` endpoint remains healthy during promotion activity
- Live promotion behavior:
  - repeated `sqlite promotion completed` events with `errors: 0`
  - no `sqlite promotion failed`, no `malformed`, no `readonly` signatures in the verification window
- Runtime write evidence:
  - `media_index` count and `max(torrent_id)` continue increasing during live log watch windows

### Staged enrichment cutover (2026-04-04)

- Final deployment run: `data/comet-fresh/magnetico/rebuild-runs/20260404T061324Z-enriched-stage-cutover`
- Verified rollback set: `data/comet-fresh/magnetico/backups/20260404T061324Z-enriched-stage-cutover/`
- A builder mirror was refreshed at `data/comet-builder/magnetico/active.search.sqlite3` during this cutover run; later storage consolidation removed that extra copy and made future builder mirroring opt-in only.
- Final validated artifact:
  - `integrity_check = ok`
  - `media_index = 8,223,899`
  - `rows_with_ids = 2,507,559` (`tmdb_id IS NOT NULL OR imdb_id IS NOT NULL`)
  - `live_promotions = 72,479`
  - `sha256 = 1473d7b7fa90a4a812773f47287eaf245fd2ecc5219dfbd0f0e6e411b7b54100`
- Post-cutover runtime:
  - `/status` returns `status=up`
  - live logs continue to show `sqlite promotion completed`

### Enrichment safety rule (2026-04-04)

- Do **not** run `scripts/match_series_to_ids.py`, `scripts/batch_fuzzy_update.py`, or other high-volume enrichment writers directly against `data/comet-fresh/magnetico/active.search.sqlite3`.
- In this environment, direct live exact and fuzzy enrichment eventually corrupted the active DB (`database disk image is malformed`) even after restoring from a clean backup.
- Safe operator pattern:
  1. create a verified backup of the active DB
  2. copy that backup to an offline stage DB
  3. run exact/fuzzy enrichment only on the stage DB
  4. deploy with `python3 scripts/rebuild_live_sqlite.py --search-db data/comet-fresh/magnetico/active.search.sqlite3 --historical-seed-db <stage-db> --integrity-check full --cutover`
- `scripts/backfill_lean_ids.py --apply` now blocks writes to the live `active.search.sqlite3` by default on this host.
  - use an offline stage DB instead
  - `--allow-live-search-db` exists only as an explicit unsafe override for legacy debugging
- `scripts/match_movies_to_tmdb.py` is now the conservative stage-only movie recovery helper for the canonical gap.
  - it uses the representative release name from each unresolved `movie_key` group, runs the same Radarr parsed-title exact cascade used by live smart hints, then falls back to the legacy cleaned-key exact path
  - it ignores numeric-only parsed-title artifacts and skips obvious episodic/broadcast-style representative names before attempting an exact movie lookup
  - it also blocks writes to the live `active.search.sqlite3` by default; run it against an offline stage DB and deploy through the normal rebuild/cutover flow
- `scripts/rebuild_live_sqlite.py` now strips old live-overlay rows from contentless `media_fts` using FTS5 delete tokens; plain `DELETE FROM media_fts` is invalid for the rebuilt schema.
- `scripts/promote_to_sqlite.py` was updated to the rebuilt `media_fts(search_text)` contract and the current container classifier path layout, so inline promotion remains healthy after cutover.
- The destructive unmatched-clear phase remains deferred until a separate audit proves category-level false-positive risk is low enough to justify deletes.
- Post-cutover unmatched audit (`python3 scripts/validate_unmatched_sample.py --category all --sample 200 --db-path data/comet-fresh/magnetico/active.search.sqlite3`) sampled 1,400 unmatched rows and still found 1,089 (`77.8%`) that look recoverable, including `97.0%` of episode samples and `67.5%` of movie samples. Do not mass-clear unmatched rows from this DB.

### Post-cutover coverage snapshot (2026-04-04)

| Category | Coverage |
|----------|----------|
| Movies | 19.8% |
| Episodes | 50.5% |
| Multi-episodes | 60.1% |
| Season packs | 54.8% |
| Anime episodes | 77.1% |
| Anime packs | 32.6% |
| Unknown video | 12.5% |

### SLO proxy + live unknown-video parity (2026-04-04)

- `python3 scripts/check_bitmagnet_id_coverage.py --json` now emits explicit production SLO proxy metrics for:
  - category-level `unknown_video` share of total SQLite rows
  - canonical TMDB/IMDb missing share of total SQLite rows
  - pass/fail status for each proxy threshold
- Current active-DB baseline from that report:
  - overall coverage: `30.52%`
  - `unknown_video`: `2.01%` of total rows
  - canonical missing IDs: `69.48%` of total rows
- Live smart-hint parity has been tightened:
  - `bitmagnet_smart_hint.py` no longer hard-rejects `unknown_video`
  - when `contentType` cannot be safely projected, Go keeps the torrent and simply skips creating a `torrent_hints` row
  - staged Python -> SQLite sync remains the authoritative path that preserves review-tier `unknown_video` rows
- `scripts/promote_bitmagnet_live_to_compact.py` no longer records `unknown_video_dropped` for rows that the staged sync actually inserts.
- Shared runtime/bootstrap pathing now lives in `scripts/lib/classifier_runtime.py`; live/rebuild/reporting utilities should use that helper instead of ad hoc `sys.path` roots.

### Production gate bundle (2026-04-04)

- `python3 scripts/run_production_gates.py --json` is now the host-level production gate bundle.
  - it collects:
    - dual-SLO coverage report from `scripts/check_bitmagnet_id_coverage.py`
    - deterministic unmatched category audit from `scripts/validate_unmatched_sample.py --json --seed 20260404`
    - deterministic top/random movie precision smokes from `scripts/smoke_test_movies.py` and `scripts/smoke_test_movies_random.py`
    - focused regression suites:
      - `python3 scripts/test_match_movies_to_tmdb.py -q`
      - `/home/ubuntu/aiostreams/cometouglas/.venv/bin/python -m unittest tests.test_magnetico_local -q`
  - it exits non-zero when a hard gate fails
- Current reality: the gate bundle is expected to stay red until the canonical SLO is materially improved; use it to measure and guard changes, not to paper over the remaining identity gap.
- Recommended pre-cutover validation checklist:
  1. run the gate bundle against the intended stage or live DB and save the JSON report
  2. confirm `scripts/rebuild_live_sqlite.py` integrity/schema validation passed before cutover
  3. confirm the staged cutover created the expected `rebuild-runs/` + `backups/` bundle and only refreshed a builder mirror if `--builder-deploy-dir` was requested
  4. confirm `/status` is up and live logs still show healthy SQLite promotion after cutover
- Recommended rollback rehearsal checklist:
  1. identify the latest `rebuild-runs/` directory and the paired rollback bundle under `data/comet-fresh/magnetico/backups/`
  2. confirm the bundle still contains the pre-cutover `active.search.sqlite3` copy and replay metadata
  3. rehearse the restore procedure only against an offline copy; do not overwrite the live DB during rehearsal
  4. after any real restore, force-recreate the `bitmagnet` container if WAL/SHM sidecars may be stale, then rerun the gate bundle

### Compose/runtime notes from this incident

- Ensure `bitmagnet` mounts include scripts bridge:
  - `./scripts:/opt/bitmagnetico/scripts:ro`
- Ensure SQLite DB mount is writable for promoter path:
  - `/data/canonical.sqlite3` must not be mounted read-only
- After replacing `active.search.sqlite3`, force-recreate the `bitmagnet` container if sidecar state (`.wal`/`.shm`) mismatch is suspected

## Media Coverage Baseline (2026-04-03)

| Category | Coverage | Target | Status |
|----------|----------|--------|--------|
| Overall | 31.73% | — | — |
| Anime | 76.71% | 80%+ | 🟡 Near target |
| Movies | 22.12% | 60%+ | ❌ Blocked by misclassified content |
| Episodes | 49.41% | 70%+ | 🟡 In progress |

### Post-Fuzzy Matching Potential

Manual validation of unmatched items reveals:
- **Series**: 91% of unmatched are VALID media, only 9% are noise/reject
- **Anime**: 88% of unmatched are VALID media, 12% are noise/reject
- **Movies**: 71% of unmatched are VALID media, 29% are uncertain/noise

This means there's significant recoverable media still unmatched.

Full improvement backlog: `BITMAGNET_PIPELINE_ROADMAP.md`

## Lean SQLite-First Direction

- The long-term authoritative media DB is `data/comet-fresh/magnetico/active.search.sqlite3`
- That SQLite DB already contains the historical smart-filtered corpus built from the Magnetico-side Python pipeline
- Current historical build metadata in SQLite:
  - `accepted_torrents = 8,784,036`
  - `media_index` currently holds about `9.08M` searchable rows (2026-04-03)
  - `source = magnetico-local-strict-video`
- `live_promotions` is incremental BitMagnet bridge state only; it is not the total corpus
- BitMagnet Postgres is temporary staging/workspace for:
  - live DHT crawling
  - raw torrent/file storage
  - Python smart classification at intake
  - downstream enrichment before rows are incorporated into SQLite

### Current lean-production target

- On this host, keep BitMagnet as live DHT intake only and keep the historical corpus in SQLite beside Comet
- Do not import the full curated historical corpus into BitMagnet; keep that corpus in SQLite beside Comet
- Do not carry the legacy pre-filter DHT corpus into the main DB
- If BitMagnet is used for any curated import subset, trusted import must keep `queue_jobs` empty and materialize searchable `torrent_contents`
- Preserve the TSV search index because Torznab/API title queries depend on it
- The current recurring live sync no longer depends on the older `torrent_hints(updated_at, info_hash)` cursor

### Target Architecture

Source of truth: `bitmagnet-media/BITMAGNET_TARGET_LOGIC.md`

Key rules:
- BitMagnet is NOT the source of truth for media classification
- Magnetico filtering/export logic decides what media is imported
- BitMagnet is a temporary live-ingest workspace, not the long-term search database
- Comet reads the compact SQLite media DB directly; BitMagnet only promotes accepted live media forward into it

### Workspace Retention Policy

- Long-term authoritative media storage lives in `data/comet-fresh/magnetico/active.search.sqlite3`
- BitMagnet PostgreSQL should only be pruned after the raw wide corpus has been drained into SQLite under the exact same Python acceptance contract as the historical build
- The prune script now enforces that boundary by checking `live_promotion_meta.wideDrainCompleted`
- The scheduled prune timer has been disabled until that drain is complete
- Manual dry-run / apply:

```bash
./scripts/run_bitmagnet_prune.sh
./scripts/run_bitmagnet_prune.sh --apply --allow-before-wide-drain   # emergency override only
python3 ./scripts/prune_bitmagnet_workspace.py --apply --vacuum-full-tables queue_jobs
```

- Normal pruning frees PostgreSQL pages for reuse.
- If you need the on-disk `bitmagnet-media/runtime/postgres` footprint to shrink immediately, run `VACUUM FULL` manually through `--vacuum-full-tables` during an off-peak maintenance window.

## Import Pipeline

### Source Data
- Curated historical export artifacts and import helpers live under `classifier/vps/` and `classifier/`
- Import script: `scripts/run_bitmagnet_import.sh`
- Chunks: 100k-row chunks with resume support
- Tracking artifacts: historical/import-run notes are now historical context rather than the current operator path

### Import Hints Available
The Python classifier provides these hints for each torrent:
- `contentType` (movie/tv_show)
- `episodes` (season→episode map for TV shows)
- `videoResolution` (V2160p, V1080p, etc.)
- `videoSource` (bluray, webrip, etc.)
- `videoCodec` (x264, x265, etc.)
- `videoModifier` (hdr, remux, etc.)
- `video3D` (3d, sbs, etc.)
- `releaseYear` (2024, etc.)

### Import Hardening (2026-03-10)
- Per-run lock file prevents concurrent imports
- `meta.env` preserved across resumes
- `progress.tsv` no longer overwritten on resume
- Resume continues from the first chunk without a `.done` marker
- Trusted shard import now defaults to `skip_queue=true` and stores progress in `.trusted-import-state.json`
- Trusted import now creates `torrent_contents` directly from import hints so the curated corpus is searchable without a second full queue pass

### Imported Baseline (Verified 2026-03-10)
- `torrents`: ~9.87M
- `torrent_contents`: ~7.32M
- `content`: ~38.35k
- DB size: ~18.5GB

## Smart Hint Pipeline Fixes (2026-03-21)

### Root Cause Analysis
Multiple bugs were found preventing the `torrent_hints` table from being populated:

1. **Mutex deadlock in `processor.go`**: Goroutines had conflicting lock/unlock patterns. Rejected torrents called `mtx.Lock()/Unlock()/return`, but non-rejected goroutines had both `mtx.Lock()` + `defer mtx.Unlock()`. If a goroutine panicked, it never called `wg.Done()`, causing `wg.Wait()` to block forever. Queue jobs would timeout at 10 minutes.

2. **Unbounded goroutine concurrency**: 100 goroutines launched simultaneously, each doing TMDB API lookups. With only 2 DB connections, most blocked. Fixed with semaphore to limit to 4 concurrent classifier calls.

3. **`persist()` never wrote to `torrent_hints`**: The `persistPayload` struct had no `torrentHints` field, and `persist()` had no code to write to the `torrent_hints` table. The smart hint correctly classified torrents but the hint was discarded.

4. **Queue job timeout too short**: Default was 10 minutes, changed to 60 minutes.

5. **`release_year` not carried through**: Python classifier extracted year but smart hint didn't pass it to Go. Added `releaseYear` to import hints.

### Code Changes Made
- **`internal/processor/processor.go`**: Fixed mutex deadlock, added concurrency semaphore (4 max), added hint collection
- **`internal/processor/persist.go`**: Added `torrentHints` to `persistPayload` and persistence logic
- **`internal/processor/queue/handler.go`**: Increased timeout to 60 minutes
- **`internal/processor/smart_hint.go`**: Added `releaseYear` field to `smartHintImportHints` struct
- **`tools/bitmagnet_smart_hint.py`**: Fixed to return `importHints` for non-reject torrents (was incorrectly returning empty object)
- **`tools/magnetico_media_probe.py`**: Added `_extract_year()` function and `releaseYear` to import hints

### Verification
- DHT crawler operational with `smart_hint` enabled
- Smart hint produces ~700 hints/minute
- Live promotion successfully pushes to Comet's search DB
- `release_year` now populated in `torrent_hints` table
- Accepted smart-hint rows now persist even if downstream Go/TMDB enrichment fails
- Live promotion now carries `tmdb_id` / `imdb_id` into the compact SQLite DB when enrichment is available

### Current Live Sync Contract

The long-term target is one smart Python acceptance contract for both historical and live rows:

```
Historical Magnetico dump -> Python smart classifier -> lean SQLite DB
Live BitMagnet torrents/files -> same Python smart classifier -> same lean SQLite DB
```

Important operational note:
- `torrent_hints` is not sufficient as the durable live source projection because `unknown_video` rows do not persist there
- the corrected live bridge now classifies directly from raw `torrents` + `torrent_files` using the same Python contract as the historical build
- the sync cursor is based on `torrents.created_at`, not `torrent_hints.updated_at`

### Live Persistence Semantics
- The Python classifier contract is authoritative for live keep/reject, categorization, canonical matching, and the structured fields that must survive into SQLite.
- Rejected hashes are deleted early.
- Accepted hashes persist to `torrent_hints` even if the downstream Go classifier or TMDB enrichment step fails.
- Failed enrichment hashes are re-queued for retry; persisted hints are not rolled back.
- `torrent_contents` and linked content metadata are still written only when downstream enrichment succeeds.

### Stage Ownership Baseline
- Go / BitMagnet owns DHT crawling, raw persistence, queueing, retries, and operational throughput.
- Python owns media acceptance, reject logic, content class, canonical/TMDB identity, and the structured search-row contract.
- Go enrichment is best-effort only; it can persist or forward metadata, but it must not override a Python decision.
- `active.search.sqlite3` is a derived artifact built from the authoritative Python contract; Comet consumes it read-only and should not re-decide classification.
- The inline Go-triggered `scripts/promote_to_sqlite.py` path is legacy compatibility only. Rebuilds and parity audits should use the raw `torrents` + `torrent_files` -> Python -> lean SQLite path.

### Compact SQLite Sync And Enrichment
- `scripts/promote_bitmagnet_live_to_compact.py` now uses a staged 3-phase pipeline:
  1. flat keyset extraction from Postgres (`torrents` first, `torrent_files` second)
  2. parallel Python classification with the same smart contract as the historical importer
  3. SQLite staging-table merge plus chunked FTS refresh
- the same script writes into the existing lean SQLite schema used by the historical importer
- all non-`reject` smart classes remain eligible, including `unknown_video`
- authoritative Python canonical IDs now land in SQLite first when available; BitMagnet `torrent_contents` enrichment is fallback-only and must not overwrite a Python-authored match
- accepted rows now retain lightweight audit evidence in `media_index` / `live_promotions`: `confidence_tier`, `reason_codes`, `match_source`, and `contract_version`
- `match_source` distinguishes authoritative Python canonical matches from fallback BitMagnet enrichment so operators can see why a promoted row exists without re-deriving the decision
- the drain/writer path now avoids the old `json_agg(...)` file reconstruction and row-by-row FTS update pattern
- the source scan can prepare the required Postgres indexes with `--ensure-source-indexes`
- existing promoted rows can still be backfilled in place with:

```bash
python3 ./scripts/promote_bitmagnet_live_to_compact.py --backfill-ids --backfill-batch-size 2000
```

- bounded audit before mutating the DB:

```bash
python3 ./scripts/promote_bitmagnet_live_to_compact.py \
  --audit-only \
  --created-after 2026-03-21T21:20:50+00:00 \
  --max-batches 1 \
  --batch-size 5
```

- full wide-to-lean drain mode:

```bash
./scripts/run_bitmagnet_wide_drain.sh
```

- clean rebuild / cutover path for `data/comet-fresh/magnetico/active.search.sqlite3`:
  - preferred historical authority: `strictVideo.manifest.json`
  - live authority: raw BitMagnet `torrents` + `torrent_files`
  - the rebuild wrapper stages output under `data/comet-fresh/magnetico/rebuild-runs/`, replays raw live rows from the earliest source cursor up to a captured frontier, verifies SQLite integrity, writes a resumable `live-promotion.checkpoint.json`, and only then offers backup + rollback-aware cutover
  - rebuild validation now also enforces a schema contract before cutover:
    - `media_index.total_size` must keep integer affinity
    - required lookup-index coverage must exist on `movie_key`, `series_key`, `imdb_id`, `tmdb_id`, `exact_episode_key`, and `season_pack_key`
  - if the strict-video manifest is not present on-host, provide it explicitly or use a trusted historical-only seed DB and let the wrapper strip any old live overlay before replaying raw authoritative live rows

```bash
python3 ./scripts/rebuild_live_sqlite.py --historical-manifest /path/to/strictVideo.manifest.json
python3 ./scripts/rebuild_live_sqlite.py --historical-seed-db /path/to/historical.search.sqlite3 --cutover
```

- the recurring live promoter now emits a schema-drift warning (without stopping the run) when the active SQLite file does not match that schema contract; use the rebuild/cutover flow to remediate drift

- the wide-drain entrypoint now uses `scripts/drain_bitmagnet_wide_offline.py`, not the incremental direct-sync loop
- the offline drain keeps durable state in `data/comet-fresh/magnetico/wide-drain-offline.state.json`
- spool and per-slice reports live under `data/comet-fresh/magnetico/wide-drain-offline/`
- progress lines now include export/classify/enrich/merge timings plus scan rate and ETA estimates
- live terminal watcher:

```bash
./scripts/watch_bitmagnet_wide_drain.sh
```

- historical lean-ID enrichment is now handled separately from BitMagnet:
  - the historical SQLite corpus currently has `8,165,894` non-live rows and none of those rows had `imdb_id` / `tmdb_id` after the wide-drain convergence
  - on this host, `scripts/backfill_lean_ids.py --apply` must target an offline stage copy; do not bulk-write directly to `active.search.sqlite3`
  - supported operator flow:
    1. back up the live `active.search.sqlite3`
    2. copy that backup to an offline stage DB
    3. run `scripts/backfill_lean_ids.py` phases against the stage DB
    4. deploy with `python3 ./scripts/rebuild_live_sqlite.py --search-db ./data/comet-fresh/magnetico/active.search.sqlite3 --historical-seed-db <stage-db> --integrity-check full --cutover`
  - `--allow-live-search-db` exists only as an explicit unsafe override for legacy debugging
  - phases are:
    - local propagation from already-enriched groups
    - non-anime series
    - anime through the existing anime/Kitsu-aware mappings
    - movies
  - the non-anime `resolve_series` phase now uses a parser-backed canonicalization layer before TMDB resolution:
    - `guessit` extracts structured title/year/season hints from dirty historical `series_key` values
    - parsed identities are stored in `id_backfill_series_parsed`
    - exact/strong successful resolutions are persisted as reusable aliases in `id_backfill_series_mapping`
    - cluster build uses parsed/mapped canonical titles instead of raw `series_key` cleanup alone
    - a materialized `id_backfill_series_resolved_group` helper avoids repeated anti-lookups against the full `media_index` during the cluster rebuild
  - resolved historical backfill output now also seeds a compact shared canonical-title layer inside the lean DB:
    - `canonical_title_family` stores one durable family per resolved movie/series/anime identity
    - `canonical_title_variant` stores compact learned variants that map noisy historical titles back to those families
    - the canonical layer is storage-light and durable; the large parsed/cluster helper tables remain work tables for the backfill engine
    - live `MagneticoLocal` reads the canonical layer read-only so historical learned title families improve request-time local matching without turning the request path into a writer
  - Sonarr-inspired matching layers that are still intentionally deferred for now:
    - external/provider-fed scene mappings
    - regex-scoped alias activation
    - generic non-anime season-number remapping
    - richer per-mapping search-mode policy
  - current decision:
    - do not add those layers during the current long-running `resolve_series` pass
    - keep the shared parser + canonical-family architecture as the active baseline
    - only add the next Sonarr-style layer if post-run audit data shows a repeated, material miss class that the current canonical-family approach is not handling well
  - the current `resolve_series` strategy version is `cluster-v3`
  - live BitMagnet enrichment remains accepted-hash-only and unchanged

- historical lean-ID backfill commands:

```bash
cp ./data/comet-fresh/magnetico/active.search.sqlite3 ./data/comet-fresh/magnetico/backups/enriched-stage.backup.sqlite3
cp ./data/comet-fresh/magnetico/backups/enriched-stage.backup.sqlite3 ./data/comet-fresh/magnetico/enriched-stage.sqlite3
/home/ubuntu/aiostreams/cometouglas/.venv/bin/python ./scripts/backfill_lean_ids.py --search-db ./data/comet-fresh/magnetico/enriched-stage.sqlite3 --phase resolve_series --max-groups 50 --audit-only
/home/ubuntu/aiostreams/cometouglas/.venv/bin/python ./scripts/backfill_lean_ids.py --search-db ./data/comet-fresh/magnetico/enriched-stage.sqlite3 --phase resolve_anime --max-groups 25 --audit-only
/home/ubuntu/aiostreams/cometouglas/.venv/bin/python ./scripts/backfill_lean_ids.py --search-db ./data/comet-fresh/magnetico/enriched-stage.sqlite3 --phase resolve_movies --max-groups 50 --audit-only
LEAN_ID_BACKFILL_SEARCH_DB=./data/comet-fresh/magnetico/enriched-stage.sqlite3 LEAN_ID_BACKFILL_STATE=./data/comet-fresh/magnetico/enriched-stage.state.json LEAN_ID_BACKFILL_LOG=./logs/enriched-stage.log ./scripts/run_lean_id_backfill.sh
LEAN_ID_BACKFILL_STATE=./data/comet-fresh/magnetico/enriched-stage.state.json LEAN_ID_BACKFILL_LOG=./logs/enriched-stage.log ./scripts/watch_lean_id_backfill.sh
python3 ./scripts/rebuild_live_sqlite.py --search-db ./data/comet-fresh/magnetico/active.search.sqlite3 --historical-seed-db ./data/comet-fresh/magnetico/enriched-stage.sqlite3 --integrity-check full --cutover
```

- durability / persistence notes for the historical backfill:
  - `aiostreams-lean-id-backfill.service` is not installed on this host
  - use the wrapper manually (or under your own `screen`/cron) against a stage DB when you need a long run
  - set `LEAN_ID_BACKFILL_STATE` / `LEAN_ID_BACKFILL_LOG` to stage-specific paths if you want dedicated artifacts
  - the authoritative progress source is whichever `state.json` you choose for the run; the default remains `data/comet-fresh/magnetico/lean-id-backfill.state.json`
  - the log file is mostly startup/error text
  - the propagation phases no longer do Python per-group lookups against `media_index`; they now use:
    - a materialized `id_backfill_propagation_source` table keyed by `(phase, group_key)`
    - set-based `INSERT ... SELECT` staging into `id_backfill_stage`
    - phase-specific partial indexes on unresolved/resolved `series_key` and `movie_key`
  - `resolve_series` now runs on canonical non-anime series clusters instead of raw `series_key` groups:
    - helper table: `id_backfill_series_cluster`
    - cluster key: aggressively cleaned title + year hint + coarse season bucket
    - one representative cluster resolution feeds safe sibling fanout inside the same cluster
    - already-resolved sibling rows can short-circuit TMDB through `local_cluster_propagation`
    - raw-group TMDB fallback is still available, but only inside unresolved clusters
  - later resolver phases also persist results in `id_backfill_resolver_cache` so repeated TMDB/anime lookups are reused across restarts

- the final convergence sequence is:
  - stop `bitmagnet`
  - run the wide drain until near the frontier
  - pause BitMagnet intake briefly
  - run one final catch-up pass to exhaustion
  - confirm `wideDrainCompleted=true`
  - only then reset the wide workspace

### Wide-Drain Completion And Workspace Reset (2026-03-22)

- Verified completion by exact frontier equality, not by ETA:
  - SQLite offline-drain cursor:
    - `lastCreatedAt = 2026-03-21T22:26:48.805553+00:00`
    - `lastInfoHash = ef00aa64a78d96584816a1d0c432dfda33bebe26`
  - latest BitMagnet `torrents` row matched that exact `(created_at, info_hash)` pair
  - rows after the saved cursor: `0`
- Marked completion in `live_promotion_meta`:
  - `wideDrainCompleted=true`
  - `wideDrainCompletionMode="source-frontier-equality"`
  - `wideDrainFrontierCreatedAt="2026-03-21T22:26:48.805553+00:00"`
  - `wideDrainFrontierInfoHash="ef00aa64a78d96584816a1d0c432dfda33bebe26"`
- Final wide-drain state:
  - `scannedRows = 11,732,511`
  - `eligibleRows = 892,683`
  - `insertedRows = 393,543`
  - `updatedRows = 488,652`
- Lean SQLite after convergence:
  - `media_index = 9,076,823`
  - `live_promotions = 910,905`

The recurring prune script was intentionally not used for the final convergence cleanup because it only deletes hashes present in `live_promotions` after a retention window. That is correct for steady-state incremental maintenance, but not for collapsing the full one-time wide staging corpus.

For the full convergence cleanup, use:

```bash
python3 ./scripts/reset_bitmagnet_workspace.py
python3 ./scripts/reset_bitmagnet_workspace.py --apply
```

Results from the verified reset on this host:

- BitMagnet PostgreSQL logical DB size after reset: `824 MB`
- `bitmagnet-media/runtime/postgres` on-disk directory after `CHECKPOINT`: `3.8G`
- Wide staging tables (`torrents`, `torrent_files`, `torrent_contents`, `torrent_hints`, `torrents_torrent_sources`, `queue_jobs`, `content*`) truncated to near-empty baselines
- `prune_bitmagnet_workspace.py` now dry-runs with:
  - `wideDrainCompleted=true`
  - `pruneBlocked=false`
  - `eligiblePromotedInfoHashes=0`

## Recovery

See `bitmagnet-media/RECOVERY.md` for detailed recovery notes.

### Recovery Verification (2026-03-16)
- `bitmagnet-postgres` cluster is reusable and active
- Database `bitmagnet` exists, main app tables present
- `torrents` oldest: `2026-03-09 01:57:02+00`, newest: `2026-03-16 21:43:08+00`
- `queue_jobs`: 136473 processed, 2 pending, 403 failed
- Row counts: torrents ~11.16M, torrent_files ~32.7M, torrent_contents ~11.18M

### Restart Verification (2026-03-16)
- Both `bitmagnet-postgres` and `bitmagnet` restart cleanly
- `/status` returns `status=up`
- Torznab returns valid RSS/XML for queries
- Database survives clean service restart

## Comet Integration

### Current Status
- `SCRAPE_BITMAGNET=False` on both `comet` and `comet-builder`
- `SCRAPE_MAGNETICOLOCAL=True` on both `comet` and `comet-builder`
- BitMagnet is kept out of the live request path; Comet reads the compact SQLite corpus directly

### Series Search Fix (2026-03-10)
- Switched BitMagnet series search from `imdbid+season+ep` to `title+season+ep`
- Added series IMDb base-ID validation/canonicalization before scraping
- Comet now fails clearly on episode-level IMDb IDs instead of scraping with broken base ID
- This keeps Comet IMDb-first while letting BitMagnet exploit the imported media corpus

### Builder Integration (Staged)
- `comet-builder.env` is staged for builder-only BitMagnet use:
  - `SCRAPE_BITMAGNET=background`
  - `BITMAGNET_URL=http://bitmagnet:3333`
  - Conservative limits: `BITMAGNET_MAX_CONCURRENT_PAGES=2`, `BITMAGNET_MAX_OFFSET=1000`
- Integration not yet applied to the live builder container (staged in config only)

## Live A/B Results

### A/B Round 1 (2026-03-10)
Corpus: 6 hard episodic titles (Industry, Drops of God, The Traitors, Paradise, Georgie & Mandy, Daredevil)

| Metric | BitMagnet OFF | BitMagnet ON |
|--------|--------------|-------------|
| median_latency_s | 7.939 | 7.736 |
| p95_latency_s | 8.574 | 8.688 |
| budget_exhausted_count | 3 | 4 |
| rd_cached_positive_count | 4 | 4 |
| median_stream_count | 2.5 | 2.5 |

Decision: keep BitMagnet disabled in default runtime. No clear win; some titles regressed.

### A/B Round 2 (2026-03-10, Title-First Series Path)
Same corpus with corrected title-first series search.

| Metric | BitMagnet OFF | BitMagnet ON |
|--------|--------------|-------------|
| median_latency_s | 6.582 | 7.962 |
| p95_latency_s | 6.678 | 10.343 |
| budget_exhausted_count | 1 | 3 |
| median_stream_count | 4.0 | 4.0 |

Title-level outcomes:
- `Daredevil S01E01`: improved (13 -> 14 cached streams, faster)
- `Industry S01E05`: faster but lost one cached stream
- Others: no stream-count gain; latency/budget regressions

Decision: keep stable title-first code changes, keep `SCRAPE_BITMAGNET=False`. Good enough for targeted experimentation but not strong enough as default live wave-1 source.

## Magnetico Search Layer

Source of truth: `MAGNETICO_SEARCH_MASTER_PLAN.md`

- Added Comet Postgres tables: `magnetico_search_index`, `magnetico_search_builds`
- Builder scripts: `cometouglas/scripts/build_magnetico_search_index.py`, `cometouglas/scripts/query_magnetico_search_index.py`
- Host wrappers: `scripts/run_magnetico_search_index_build.sh`, `scripts/query_magnetico_search_index.sh`
- Current v1 scope: movies + exact single season/episode series rows (packs deferred)
- Local Magnetico-backed index returns 269 rows for Daredevil S01E01 vs BitMagnet's 7 rows
- New preferred direction:
  - compact SQLite historical corpus/search DB under `data/comet-fresh/magnetico/`
  - Comet local scraper reads the SQLite search DB directly
  - BitMagnet promotes only accepted live media forward into the active SQLite search DB
  - staged `scripts/rebuild_live_sqlite.py --cutover` deployments keep the live DB under `data/comet-fresh/magnetico/`; a second builder mirror is created only when `--builder-deploy-dir` is explicitly set

### Local storage consolidation (2026-04-04)

- Live containers mount `data/comet-fresh` and `bitmagnet-media/*`; they do **not** use `data/comet-builder`, `bitmagnet.old`, or `data/postgres-old`.
- The desired on-host retention rule is one large historical search artifact: `data/comet-fresh/magnetico/active.search.sqlite3`.
- `scripts/build_compact_magnetico_search.sh` now deploys only to `data/comet-fresh/magnetico/` by default.
- `scripts/rebuild_live_sqlite.py` now mirrors into a builder path only when `--builder-deploy-dir` (or `REBUILD_LIVE_SQLITE_BUILDER_DEPLOY_DIR`) is set explicitly.
- Post-cleanup snapshot on this host:
  - `/home/ubuntu/aiostreams` -> about `15G`
  - `data/comet-fresh/magnetico` -> about `6.5G`
  - host `/home/ubuntu` usage -> about `37%` used with `124G` free

## Operational Commands

```bash
cd /home/ubuntu/aiostreams

# Start BitMagnet
sudo docker compose --profile bitmagnet up -d --build bitmagnet-postgres bitmagnet

# Check health
sudo docker exec bitmagnet wget -qO- http://127.0.0.1:3333/status

# Torznab smoke test
sudo docker exec bitmagnet wget -qO- "http://127.0.0.1:3333/torznab/api?t=movie&imdbid=tt0133093"

# Legacy helper for the superseded hint-based live cursor
# ./scripts/prepare_bitmagnet_live_indexes.sh

# Manual live promotion pass
./scripts/run_bitmagnet_live_promotion.sh --max-batches 1

# Import status
./scripts/bitmagnet_import_status.sh

# Import watch
./scripts/bitmagnet_import_watch.sh

# Build Magnetico search index
./scripts/run_magnetico_search_index_build.sh

# Query Magnetico search index
./scripts/query_magnetico_search_index.sh
```

## SQLite Promotion (2026-03-31)

### Overview

BitMagnet now supports **inline promotion** of enriched content to SQLite `media_index` immediately after Postgres persistence. This bridges the gap between BitMagnet's Postgres staging and Comet's SQLite search database.

### Status: ENABLED (Production)

The promotion pipeline is now **ENABLED by default** in production. All new content with TMDB/IMDB IDs is automatically promoted to SQLite.

### Architecture

```
DHT → Go Intake Filter → Postgres torrents
        ↓
    Python Smart Hint (TMDB lookup)
        ↓
    Go Classifier (TMDB enrichment)
        ↓
    Postgres torrent_contents
        ↓
    [ENABLED] Go Promotion Trigger → Python promote_to_sqlite.py
        ↓
    SQLite media_index (Comet reads here)
```

### Configuration

**Production config** (`config/bitmagnet.env`):
```bash
# SQLite promotion (ENABLED for production)
PROCESSOR_PROMOTION_ENABLED=true
PROCESSOR_PROMOTION_SCRIPT_PATH=/opt/bitmagnetico/scripts/promote_to_sqlite.py
PROCESSOR_PROMOTION_SEARCH_DB=/data/magnetico/active.search.sqlite3
```

**Docker compose volumes** (required):
```yaml
volumes:
  - ../../scripts:/opt/bitmagnetico/scripts:ro
  - ../../data/comet-fresh/magnetico:/data/magnetico:rw
```

### Behavior

1. **Trigger**: After successful `persist()` to Postgres
2. **Filter**: Only contents with `content_source` AND `content_id` (TMDB/IMDB match)
3. **Failure handling**: Log warning, leave in Postgres, continue
4. **Deduplication**: Check existing `info_hash` before insert

### Monitoring

Check promotion health:
```bash
./scripts/check_bitmagnet_promotion_health.sh
```

Check promotion logs:
```bash
docker logs bitmagnet 2>&1 | grep "sqlite promotion"
```

### Manual Testing

```bash
# Test Python script with sample payload
echo '{"contents": [{"info_hash": "test123...", "name": "Test.Movie.2024", ...}]}' | \
  python3 scripts/promote_to_sqlite.py --stdin --dry-run --verbose

# Check current SQLite state
sqlite3 /home/ubuntu/aiostreams/data/comet-fresh/magnetico/active.search.sqlite3 \
  "SELECT COUNT(*), datetime(MAX(discovered_on), 'unixepoch') FROM media_index;"
```

### Key Metrics

| Metric | Current Value | Notes |
|--------|---------------|-------|
| Postgres TMDB content | 483K | All with TMDB IDs |
| SQLite total | 9.07M | Historical + new |
| Coverage | 78% | Postgres content already in SQLite |
| Promotion rate | ~10-20/min | Inline with DHT discovery |

### Failure Recovery

If promotion fails, content remains in Postgres and will be retried on next classifier run. No data loss.

## Unified Classifier Integration (2026-04-02)

### Overview

The **Unified Classifier** (`classifier/unified_classifier/`) provides Radarr/Sonarr parity for title parsing and Trash-Guides scoring for anime.

### Key Components

| Component | File | Purpose |
|-----------|------|---------|
| RTN Parser | `parsers/rtn_parser.py` | Quality/resolution/codec extraction |
| Sonarr Parser | `parsers/sonarr_parser.py` | 109 episode parsing patterns (from Sonarr's Parser.cs) |
| Trash-Guides Scorer | `scoring/trash_guides_scoring.py` | Anime tier scoring (BD/Web tiers, LQ groups) |
| Kitsu Client | `metadata/kitsu_client.py` | Anime metadata lookup |

### Pattern Coverage Comparison

| Parser | Pattern Count | Coverage |
|--------|---------------|----------|
| Python (Sonarr) | 109 patterns | Standard episodes, anime, date-based, multi-episode, international |
| Go classifier | ~4 patterns | Basic S01E01 and 1x05 formats only |

**Key Gap:** Go re-parses episodes and overwrites Python's more accurate results. See `unified_classifier/WORKFLOW.md` for details.

### Validation Results (5000-torrent test)

| Metric | Value |
|--------|-------|
| Parse rate | 74.86% |
| Single episode accuracy | 99.28% |
| Anime accuracy | 93.91% |
| Multi-episode accuracy | 88.6% |

### Documentation

- `classifier/unified_classifier/README.md` - Component details
- `classifier/unified_classifier/WORKFLOW.md` - Full pipeline flow, data contracts
- `docs/DOCUMENTATION_INDEX.md` - Master documentation index

## Media-Only Workflow (2026-03-30)

### Overview

BitMagnet now operates a **media-only workflow** where only content with TMDB/IMDB IDs is retained. This prevents accumulation of unmatched junk and ensures all indexed content is useful for Comet debrid search.

### Two-Tier Filtering Architecture

```
DHT Discovery
    ↓
┌─────────────────────────────────────────────────────────────┐
│  TIER 1: Go Intake Filter (intake_filter.go)                │
│  - Runs at DHT level BEFORE database persistence            │
│  - Zero latency, instant rejection                          │
│  - Pattern database: 1000+ adult/courseware patterns        │
│  - Filters: adult brands, JAV codes, CJK terms, courseware  │
│  - Size threshold: 50MB minimum                             │
│  - Benign context guards (BBC, Moby Dick, etc.)             │
│  ↓                                                           │
│  BLOCKED if adult/courseware/size ──────────────── BLOCKED  │
└─────────────────────────────────────────────────────────────┘
    ↓ (passes intake filter)
Postgres torrents table
    ↓
┌─────────────────────────────────────────────────────────────┐
│  TIER 2: Python Smart Hint Classifier (bitmagnet_smart_hint) │
│  - Runs AFTER database persistence                          │
│  - Content type classification (movie vs TV)                │
│  - Canonical layer TMDB/IMDB lookup                         │
│  - Episode structure parsing                                │
│  - Rejects if no TMDB match ("no_canonical_match")          │
│  ↓                                                           │
│  BLOCKED if no TMDB/IMDB ID ─────────────────────── BLOCKED │
└─────────────────────────────────────────────────────────────┘
    ↓ (has TMDB/IMDB ID)
Postgres torrent_contents table
```

### Key Components

1. **Go Intake Filter** (`internal/dhtcrawler/intake_filter.go`) - **PRIMARY INTAKE FILTER**
   - Runs at DHT level before persistence
   - **This is the first line of defense**
   - Rejects: adult content, courseware, size < 50MB
   - 1000+ patterns migrated from Python (adultBrands, adultPerformers, adultCodes, cjkAdultTerms)
   - No API calls, instant rejection
    - Location: `bitmagnet-media/internal/dhtcrawler/intake_filter.go`
    - Called from: `bitmagnet-media/internal/dhtcrawler/persist.go:67`

2. **Python Smart Hint Classifier** (`bitmagnet-media/classifier/bitmagnet_smart_hint.py`) - **SECONDARY ENRICHMENT**
   - Runs AFTER Go intake filter (only on content that passed)
   - Classifies content type (movie/tv_show)
   - Checks canonical layer for TMDB/IMDB IDs
   - Provides import hints for matched content
   - Location: `bitmagnet-media/classifier/bitmagnet_smart_hint.py`

3. **Pattern Files** (generated from Python)
    - `bitmagnet-media/internal/dhtcrawler/patterns_gen.go` - Adult brands, performers, codes
    - `bitmagnet-media/internal/dhtcrawler/supplemental_patterns_gen.go` - CJK terms
   - These are Go equivalents of Python patterns

### Pattern Migration History

The Go intake filter was created by migrating patterns from the Python classifier:
- Adult brands: ~200 patterns
- Adult performers: ~300 patterns  
- Adult codes (JAV): ~100 prefixes
- CJK adult terms: ~100 terms
- Explicit patterns: regex for titles
- Courseware patterns: ~20 terms

### Testing

Run comparison between Go and Python:
```bash
python3 scripts/compare_go_vs_python_filter.py --sample-size 1000
```

Go tests:
```bash
cd /home/ubuntu/aiostreams/go
go test ./internal/dhtcrawler -v -run TestIntakeFilter
```

### Storage Impact

Before cleanup:
- Torrents: 1.26M
- TMDB matched: 38%
- Database: 7.6 GB

After cleanup:
- Torrents: 478K
- TMDB matched: 100%
- Database: ~3 GB

### Maintenance

**No cron jobs needed.** The workflow is self-cleaning:

1. **At intake**: Go filter blocks adult/courseware
2. **At classification**: Python rejects content without TMDB match
3. **Automatically**: Rejected torrents are deleted by processor

The cleanup scripts (`cleanup_bitmagnet.sh`, `delete_unmatched_content.sh`) are **one-time tools** for cleaning up historical data. They should NOT be needed going forward.

### Monitoring

Check workflow health:
```bash
# View rejection stats
docker compose logs bitmagnet 2>&1 | grep "smart hint.*rejects"

# Verify only TMDB-matched content exists
docker exec bitmagnet-postgres psql -U postgres -d bitmagnet -c "
SELECT content_source, COUNT(*) FROM torrent_contents GROUP BY content_source;
"
# Expected: only 'tmdb' rows, no NULL

# Check recent intake
docker exec bitmagnet-postgres psql -U postgres -d bitmagnet -c "
SELECT DATE(created_at), COUNT(*) 
FROM torrent_contents 
WHERE created_at > NOW() - INTERVAL '1 day'
GROUP BY DATE(created_at);
"
```

### Failure Modes

If unmatched content accumulates:
1. Check Python classifier logs: `docker compose logs bitmagnet | grep smart_hint`
2. Verify canonical layer is accessible: `ls -la data/comet-fresh/magnetico/active.search.sqlite3`
3. Run one-time cleanup: `./scripts/delete_unmatched_content.sh`
