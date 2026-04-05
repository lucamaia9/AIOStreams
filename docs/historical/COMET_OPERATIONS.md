# Comet Operations

Operational reference for the customized Comet instance (`Cometouglas v2`).

## Source & Build

- Source: `/home/ubuntu/aiostreams/cometouglas`
- Git remotes: `origin` -> `lucamaia9/cometouglas`, `upstream` -> `g0ldyy/comet`
- Build: `compose.yaml` builds from `./cometouglas` (local source)
- Docker build context excludes `.venv` via `cometouglas/.dockerignore`; otherwise a host uv virtualenv can overwrite the image-built venv and force runtime rebuilds on container start
- Active env: `comet-fresh.env`
- Builder env: `comet-builder.env`
- Data volume: `./data/comet-fresh:/app/data`
- Database today: shared PostgreSQL at `postgres:5432`
- Runtime SQLite staging path: `data/comet-fresh/runtime/comet-runtime.sqlite3`
- Offline rebuild wrapper: `scripts/rebuild_comet_runtime_sqlite.py`
- Latest validated rebuild: `data/comet-fresh/runtime-rebuilds/20260404T183000Z-runtime-sqlite/summary.json`
- Current migration status:
  - the offline runtime SQLite rebuild now succeeds end-to-end from live PostgreSQL into `data/comet-fresh/runtime/comet-runtime.sqlite3`
  - current candidate size: `2,201,411,584` bytes with `PRAGMA quick_check=ok`
  - downstream rebalance dry-run can consume the candidate runtime DB after the schema compatibility fix for `updated_at` vs legacy `timestamp`
  - live cutover is still pending because `comet` and `comet-builder` currently share runtime/DMM tables through PostgreSQL and that shared-writer story has not been production-validated on SQLite yet

## MagneticoLocal Historical Search

- New historical-search path: compact SQLite at `/app/data/magnetico/active.search.sqlite3`
- Scraper: `MagneticoLocalScraper`
- Historical source of truth: filtered `strictVideo.manifest.json` + JSONL shards, not the original `database.sqlite3`
- Settings:
  - `SCRAPE_MAGNETICOLOCAL`
  - `MAGNETICOLOCAL_SEARCH_DB`
  - `MAGNETICOLOCAL_CANDIDATE_LIMIT`
  - `MAGNETICOLOCAL_MOVIE_RESULT_LIMIT`
  - `MAGNETICOLOCAL_SERIES_RESULT_LIMIT`
- Intended role:
  - historical movies from the compact SQLite corpus
  - historical exact-episode search from the compact SQLite corpus
  - no dependency on BitMagnet for historical search
- Current build result on this host:
  - `8,784,036` processed rows from `88` shards
  - `8,662,837` inserted searchable rows
  - final artifact size `3.405 GiB`
  - build time `881.999s` (~14.7 min)
- Current runtime on this host:
  - `SCRAPE_MAGNETICOLOCAL=True`
  - `SCRAPE_BITMAGNET=False`
  - both `comet` and `comet-builder` read `/app/data/magnetico/active.search.sqlite3`
  - live accepted BitMagnet rows reach the active SQLite DB via inline promotion, with optional manual catch-up from `scripts/run_bitmagnet_live_promotion.sh`; the old user timer is not installed on this host
  - the currently deployed active SQLite artifact is larger than the initial build because it now includes live BitMagnet promotions and richer ID fields
    - all scrapers fire in parallel: `magneticolocal` + `dmm` + `jackett` + `prowlarr`
    - content-type aware: anime requests filter to anime-only indexers (nyaa, dmhy, mikan, etc.)
    - early return on RD cached hits stops further scraping
  - the active SQLite DB is migrated to a structured search projection with `movie_key`, `series_key`, `exact_episode_key`, and `season_pack_key`
  - `tmdb_id` / `imdb_id` now flow into promoted live rows when enrichment is available, but historical rows should still be treated as key/title-first rather than ID-complete
  - historical ID enrichment is now handled by `scripts/backfill_lean_ids.py`, but on this host it must run against an offline stage copy of the lean SQLite DB and then cut over with `scripts/rebuild_live_sqlite.py --historical-seed-db <stage-db> --cutover`
  - historical enrichment now also seeds a shared compact canonical title layer in the same SQLite DB:
    - durable families live in `canonical_title_family`
    - durable learned variants live in `canonical_title_variant`
    - `MagneticoLocal` consults that layer read-only between exact-ID/key tiers and fuzzy fallback so learned historical title families can improve live local matching without writing on the request path
  - local movie retrieval now runs in tiers:
    - exact `imdb_id`
    - exact `tmdb_id`
    - exact structured keys (`movie_key`, `canonical_title_key`, `title_key`)
    - canonical family title expansion from `canonical_title_variant` / `canonical_title_family`
    - bounded `title_key` prefix fallback
    - bounded FTS fallback
    - `norm_title LIKE` only as a last-resort rescue path
  - `tmdb:` **movie** requests are supported on the public stream endpoint; Comet resolves TMDB -> IMDb when available and passes both IDs into MagneticoLocal
  - `tmdb:` series requests are explicitly unsupported right now (return empty with an explicit log reason) until a dedicated series-TMDB mapping path is implemented
  - request logs now show:
    - MagneticoLocal candidate composition (`exact_id`, `exact_key`, `fallback`)
    - selected hash counts before stream shaping
    - final visible stream counts after cached-only/dedupe shaping
  - standard episodic series search now uses a simplified query approach:
    - `episode_exact` queries: `Title SxxExx`, `Title xxxyyy`
    - `season` queries: `Title Sxx`, `Title Season x`
    - `broad` queries: `Title`
    - all query variants run simultaneously (no sequential stage gating)
  - `TorrentManager` now strips null external ID values before building `ScrapeRequest`, preventing IMDb-only requests from failing validation when `tmdb_id` is absent
  - MagneticoLocal exact-ID retrieval now fetches `torrent_id` values first and then loads rows by id, avoiding SQLite full-table scans on `SELECT m.* WHERE imdb_id=?` / `tmdb_id=?`
  - MagneticoLocal series exact-ID retrieval is now episode-aware for enriched series requests:
    - exact single-episode rows for the requested season/episode first
    - then containing multi-episode rows and season packs for that season
    - broad exact-ID rescue rows are capped so `series_key` / `season_pack_key` title tiers still get a chance to run
  - `MAGNETICOLOCAL_CANDIDATE_LIMIT` is now tuned down to `120`; the live search path stops early on strong exact-ID/exact-key hits instead of always expanding toward the old `250`-row fuzzy budget
  - size-type guardrails (2026-04-02):
    - MagneticoLocal now coerces `media_index.total_size` values to numeric before emitting scraper rows
    - ranking now safely coerces torrent `size`/`maxSize` before size filtering, preventing `str` vs `float` crashes
    - torrent update upserts now coerce numeric fields (`size`, `seeders`, `file_index`, `season`, `episode`) before DB bind
    - MagneticoLocal emits a warning if `media_index.total_size` schema affinity is non-integer so drift is visible early
    - MagneticoLocal now emits one startup drift warning when required lookup indexes are missing (`movie_key`, `series_key`, `imdb_id`, `tmdb_id`, `exact_episode_key`, `season_pack_key`)

### Modified/Untracked Files (Verified 2026-03-27)
- `comet/api/endpoints/stream.py` -- **unified single-wave parallel search**, content-type detection, RD probing, cached-only fast return
- `comet/core/constants.py` -- split artifact + env-driven content/owner filtering (no hardcoded provider/category maps)
- `comet/core/models.py` -- live path settings (wave settings deprecated, kept for backward compat)
- `comet/scrapers/models.py` -- `ScrapeRequest` with new `content_type` field
- `comet/scrapers/jackett.py` -- **content-type filtering** (removed wave-based fast/slow lanes)
- `comet/scrapers/search_plan.py` -- simplified query builder (fallback stages kept for DMM compat)
- `comet/scrapers/prowlarr.py` -- **content-type filtering** (removed wave-based fast/slow lanes)
- `comet/services/orchestration.py` -- unified scrape entry point with `content_type` passthrough
- `comet/services/indexer_health.py` -- circuit breaker (untracked)
- `comet/services/query_dedupe.py` -- TTL-based dedup (untracked)

## Live Path & Unified Search

The custom live path in `stream.py` implements single-wave parallel scraping with content-type awareness and RTN-first RD probing.

### Exact Workflow (Request → Response)

```
1. PARSE
   ├─ Extract media_id, season, episode from Stremio request
   ├─ Resolve aliases (TMDB, Trakt, anime title names)
   └─ Build config from b64-encoded manifest

2. DB CACHE CHECK
   ├─ get_cached_torrents() → load from PostgreSQL
   ├─ primary_cached = True if DB has entries for this media_id
   └─ cache_manager.check_and_decide() → should_scrape_now?

3. CACHEDONLY OVERRIDE
   ├─ if cachedOnly=True AND DB has entries AND no scrape scheduled:
   │     force_scrape_now = True  (ensures fresh results)
   └─ log: "🔄 cachedOnly override: forcing live scrape"

4. CONTENT-TYPE DETECTION
   ├─ kitsu: prefix → "anime"
   ├─ media_type="movie" → "movie"
   ├─ anime_mapper.is_anime_content() → "anime"
   └─ else → "series"

5. UNIFIED SCRAPE (all fire simultaneously)
   ├─ magnetico_local (5s timeout)
   ├─ dmm (5s timeout)
   ├─ jackett (10s timeout) — content-type filtered indexers
   └─ prowlarr (10s timeout) — content-type filtered indexers

   Results stream in via asyncio queue (first-come-first-serve).
   Each batch: filter_manager → dedup(infoHash,title) → RTN filter → add to torrents.
   RD probe callback runs during scrape (checks new hashes as they appear).

6. POST-SCRAPE: DB AVAILABILITY LOOKUP
   └─ check_multi_service_availability()
       └─ PostgreSQL debrid_availability table (previously probed, within TTL)

7. RTN FILTER (before RD API call)
   ├─ rank_torrents() → title match, year, adult, quality ranking
   ├─ Log: "⚖️ Torrents after user RTN filtering: 300/1500"
   └─ Build ranked_torrents dict (only surviving hashes)

8. NEEDS_DEBRID_CHECK (uses ranked counts)
   ├─ unverified_ratio = 1 - (verified_cached / ranked_total)
   ├─ needs_debrid_check = True if:
   │   ├─ no cached torrents, OR
   │   ├─ 0 verified cached, OR
   │   ├─ unverified_ratio > DEBRID_CACHE_CHECK_RATIO, OR
   │   └─ cachedOnly AND verified < target (10)
   └─ Log: "🔄 RD check needed: ranked=300 verified=1 ratio=0.997"

9. RD API CHECK (only ranked hashes, not all)
   └─ get_and_cache_multi_service_availability(ranked_torrents)
       └─ RD API: chunks of 500 in parallel
       └─ Results cached to debrid_availability DB for future requests
       └─ Log: "🔄 Checking availability: realdebrid (300 hashes, saved 1200 by RTN filter)"

10. RESPONSE BUILDING
    ├─ _select_info_hashes_by_resolution() → per-resolution selection
    ├─ cachedOnly=True: skip uncached hashes
    ├─ Build stream objects (name, description, playback URL)
    └─ Return: cached_results + (non_cached_results if not cachedOnly)
```

### Example: Friends S01E16

```
DB cache:         105 entries (1 previously cached on RD)
Scrape:           1202 Jackett + 202 Prowlarr + 32 MagneticoLocal + 5 DMM = 1441
RTN filter:       1441 → ~300 surviving (79% rejected)
RD API check:     300 hashes → 10 cached on RD
Response:         10 cached streams
```

### Content-Type Detection
- `media_id` starts with `kitsu:` → `anime`
- `media_type == "movie"` → `movie`
- `anime_mapper.is_anime_content()` returns True → `anime`
- Otherwise → `series`

### Content-Type Indexer Filtering
Each indexer is mapped to supported content types via runtime split artifact (or env fallback lists):

| Content Type | Jackett Indexers | Prowlarr Indexers |
|-------------|-----------------|-------------------|
| **series** | 13 (all except nyaa, dmhy) | 6-7 |
| **movie** | 15 (all except nyaa, dmhy, eztv) | 7 |
| **anime** | 2-4 (nyaa, dmhy, knaben, bitsearch) | 1 (mikan) |

### Timing Budget

| Phase | Timeout | Notes |
|-------|---------|-------|
| MagneticoLocal | 5s | Local SQLite |
| DMM | 5s | Local SQLite |
| Jackett | 10s | External HTTP |
| Prowlarr | 10s | External HTTP |
| **Total scrape** | **35s** | All parallel, first-come-first-serve |
| RD check | remaining | After scrape, RTN-filtered hashes only |
| Early return | instant | When ≥10 visible cached hits found |

### Key Settings (in `comet-fresh.env`)
- `LIVE_SCRAPE_HARD_TIMEOUT=35` -- absolute scrape ceiling
- `LIVE_TOTAL_BUDGET_S=35` -- total request budget
- `LIVE_SCRAPE_BUDGET_S=30` -- scrape-phase budget
- `LIVE_FAST_RETURN_RD_CACHED_RESULTS=10` -- cached results before fast return
- `LIVE_EARLY_RETURN_ON_RD_CACHED=True`
- `LIVE_MAX_CANDIDATES_BEFORE_RD_CHECK=400`
- `DEBRID_CACHE_CHECK_RATIO=0.0` -- ratio threshold for forcing RD check
- **Wave settings deprecated** (kept in env for backward compat, no longer used by code):
  - `LIVE_CACHED_ONLY_WAVE1_TIMEOUT_S`, `LIVE_PARALLEL_WAVE1_*`, `LIVE_SLOW_WAVE_FALLBACK_TIMEOUT_S`
  - `JACKETT_FAST_INDEXERS`, `JACKETT_SLOW_INDEXERS`, `PROWLARR_FAST_INDEXERS`, `PROWLARR_SLOW_INDEXERS`

### RD Probing
- Chunked and request-local deduped
- Callback probes run during scrape (process hashes as they stream in)
- Post-scrape RD API call processes only RTN-surviving hashes
- Precheck: 240
- Callback: 240
- Hard-title zero-hit requests terminate early instead of burning the full RD budget

### Cached-Only Fast Return
- When `cachedOnly=true` and RD finds ≥`LIVE_FAST_RETURN_RD_CACHED_RESULTS` cached results, scraping stops immediately
- The threshold is server-controlled — not capped by manifest's `maxResultsPerResolution`
- `cachedOnly` override forces live scrape when DB has entries but cached < target
- RTN filter runs BEFORE RD API check — avoids probing hashes that would be rejected

### Optimizations Applied (2026-03-27)
1. **`needs_debrid_check` fix**: `DEBRID_CACHE_CHECK_RATIO=0.0` made ratio check `X < 0.0` (never True). Added `unverified_ratio > threshold` and `cachedOnly < target` conditions. Friends S01E16: 1→10 cached.
2. **RTN filter before RD API**: RTN filtering moved before RD check. Only surviving hashes sent to RD API. 20-50% fewer RD API calls.
3. **`cachedOnly` override**: Forces foreground scrape when DB has entries but not enough cached results.

## Indexer Configuration

### Content-Type Tags
Content-type routing is config-driven (no hardcoded provider/category maps in Python):
- primary source: generated split artifact at `INDEXER_SPLIT_ARTIFACT_PATH` (default `/app/data/indexer_split/current.json`)
- fallback source: env lists in Comet config:
  - `JACKETT_MOVIE_INDEXERS`, `JACKETT_SERIES_INDEXERS`, `JACKETT_ANIME_INDEXERS`
  - `PROWLARR_MOVIE_INDEXERS`, `PROWLARR_SERIES_INDEXERS`, `PROWLARR_ANIME_INDEXERS`
- final fallback: unclassified indexers are treated as `movies+series+anime` to avoid accidental traffic drops

### Indexer Dedup
Owner-lane dedup is now generated by the split routine:
- each provider is assigned exactly one owner lane (`jackett` xor `prowlarr`) in the split artifact
- Comet enforces owner-lane dedup at scrape-time through alias-aware filtering
- content lists (`movies`/`series`/`anime`) can overlap for the same provider while still preserving single-lane ownership

### Indexer Health
- Circuit breaker: `INDEXER_FAIL_STREAK_THRESHOLD=3`, `INDEXER_COOLDOWN_SECONDS=900`
- Name normalization: Comet normalizes Prowlarr/Jackett names before matching env allowlists

## DMM Integration

### Accepted Topology
- **Public `comet`**: `SCRAPE_DMM=live`, `DMM_INGEST_ENABLED=False`
- **Dedicated `comet-builder`**: `DMM_INGEST_ENABLED=True`, shared PostgreSQL refresh path
- Builder is no longer behind a compose profile; survives `docker compose up -d`
- Runtime SQLite is now treated as the staged replacement target, not the active live backend yet. The live cutover remains gated on validating the shared `comet` + `comet-builder` write pattern against the staged SQLite artifact.

### Steady-State Scripts
- `scripts/run_builder_dmm_cycle.sh` -- full-cycle validation
- `scripts/check_builder_dmm_incremental.sh` -- incremental refresh validation
- `scripts/start_builder_dmm.sh`, `scripts/status_builder_dmm.sh`, `scripts/stop_builder_dmm.sh`

### Known Behavior
- DMM prewarming into shared PostgreSQL improves median latency
- Builder DMM ingestion is CPU-heavy; not validated for always-on during live traffic
- `dmm_ingested_files=0` on interrupted runs means file-level checkpoint progress not yet demonstrated

## Background Scraper

Enabled after live path restoration (2026-03-19):
- `BACKGROUND_SCRAPER_ENABLED=True`
- `BACKGROUND_SCRAPER_CONCURRENT_WORKERS=2`
- `BACKGROUND_SCRAPER_MAX_MOVIES_PER_RUN=50`
- `BACKGROUND_SCRAPER_MAX_SERIES_PER_RUN=25`

## Filtering & Matching

- `comet/services/filtering.py` contains a narrow phrase-match fallback for multilingual combined titles (e.g., "Les Gouttes de Dieu / Drops of God")
- RTN (rank-torrent-name) is the upstream Rust filename parser; custom logic lives in the live path, not in RTN

## Adaptive Rebalance

Source of truth: `TESTING_RUNBOOK.md` (ranking/rebalance validation section)

### Scoring Model (per indexer, rolling 3-day window)
- `hit_count`: 0.35
- `seeders_quality`: 0.25
- `recency`: 0.20
- `reliability_norm`: 0.20

### Reliability Penalties
- timeout: 0.45
- forbidden (403): 0.30
- ratelimit (429): 0.15
- hard errors (5xx/connection): 0.10

### Selection Policy
- Core: top 15 indexers with >=20 samples
- Exploration: 10% (currently 2) rotated per day
- Guardrails: sufficient samples, core size >=10, churn <=20%

### 20s Latency Profile
- `INDEXER_MANAGER_TIMEOUT=10`
- `INDEXER_MANAGER_WAIT_TIMEOUT=8`
- `SCRAPE_WAIT_TIMEOUT=18`
- `GET_TORRENT_TIMEOUT=5`
- `DOWNLOAD_TORRENT_FILES=False`
- `HTTP_CLIENT_TIMEOUT_TOTAL=20`
- `HTTP_CLIENT_LIMIT_PER_HOST=10`

## Anti-Burst Request Shaping

- `LIVE_QUERY_DEDUP_TTL_S` -- suppresses duplicate provider/indexer/query calls inside TTL window
- `INDEXER_REQUEST_JITTER_MAX_MS` -- small jitter before outbound indexer fanout
- Implemented in `comet/services/query_dedupe.py`

## Live Path Restoration (2026-03-19)

The v2.53.0 upstream merge (commit `bc86903`) accidentally deleted the entire custom live-path logic from `stream.py` (1909 -> 1068 lines). Restored ~450 lines including:
- Wave orchestration (wave1->wave2->wave3 with budget tracking)

**2026-03-26:** Replaced 3-wave sequential architecture with single-wave unified parallel search:
- All scrapers fire simultaneously (no wave gating)
- Content-type aware indexer filtering (anime/movie/series)
- Indexer deduplication across Jackett/Prowlarr
- Simplified search plan (removed season_fallback/broad_title_rescue stages)
- RD probing (chunked precheck, callback, final phases)
- Cached-only fast return (`_stop_on_first_cached_hit` callback)
- Budget initialization from `LIVE_*` settings

Restoration plan: `cometouglas/docs/historical/LIVE_PATH_RESTORATION_PLAN.md`
Git branch: `restore/live-path` (from `pre-restore-baseline` tag)

## Upstream Merges

### 2026-03-18: v2.53.0 Merge
- Preserved custom optimizations (fast/slow lanes, alias normalization, multilingual matching, BitMagnet series query, DMM unlimited query, Magnetico tables)
- Accepted upstream: EpisodeIndexService, Rust filename parser, scraper orchestration improvements, new schema migration system
- Validation: 167 streams for tt0133093 (The Matrix)

### 2026-03-08: Manual Upstream Patch
- URL-level scrape mode parsing in scraper manager
- Safer lazy Gunicorn import
- Richer StremThru exception logging

## RD Availability Fix (2026-03-18)

Fixed StremThru availability mismatch in `comet/debrid/stremthru.py`:
- Availability parsing now accepts the full ready-status set (`cached`, `downloaded`) before file-level validation
- Previously only counted `cached`, undercounting ready torrents when StremThru returned `downloaded`

## Deployment Commands

```bash
cd /home/ubuntu/aiostreams

# Rebuild/restart Comet
sudo docker compose up -d --build comet

# Rebuild/restart both Comet services
sudo docker compose up -d --build comet comet-builder

# Check service status
sudo docker compose ps comet

# Tail logs
docker logs --since 15m comet

# Smoke-check endpoints
curl -sS https://comet.douglinhas.mywire.org/manifest.json

# Real-user benchmark
export REAL_USER_MANIFEST_URL='<canonical direct Comet manifest URL>'
./scripts/run_real_user_benchmark.sh --limit 5

# Full acceptance benchmark
./scripts/run_real_user_benchmark.sh --limit 20

# Build/deploy the compact historical SQLite search DB from the filtered manifest
./scripts/build_compact_magnetico_search.sh /path/to/strictVideo.manifest.json

# Query the compact historical SQLite search DB
./scripts/query_compact_magnetico_search.sh --title "The Matrix" --year 1999 --limit 20

# Promote accepted live BitMagnet rows into the active compact search DB
python3 ./scripts/promote_bitmagnet_live_to_compact.py --batch-size 5000

# Rebalance dry run
./scripts/rebalance_from_live_usage.py --dry-run

# Rebuild the staged runtime SQLite DB from live Comet PostgreSQL
python3 ./scripts/rebuild_comet_runtime_sqlite.py
```

## Fork Audit

Source of truth: `cometouglas/docs/historical/COMETOUGLAS_AUDIT_20260309.md`

Key conclusions:
1. Main correctness bottleneck is the forked live-path logic in `stream.py`, not upstream filtering
2. Old `DMM=40` cap was a fork-only bottleneck (now removed)
3. Fast-return previously keyed to wrong stage (now corrected)
4. Prowlarr/Jackett laneing adds complexity but is working

## Historical Investigation Notes

These are historical and may not reflect current runtime state. Cross-check against current config before acting on them.

### Jackett Timeout Investigation (Feb 18, 2026)
- Root cause: Jackett calling FlareSolverr via public host IP instead of internal Docker DNS
- Fix: `FlareSolverrUrl` changed to `http://flaresolverr-jackett:8191`
- Jackett proxy lane was later removed entirely because it broke FlareSolverr cookie validation

### Prowlarr 1337x Fix (Feb 18, 2026)
- Root cause: FlareSolverr proxy configured with public IP + 10s timeout
- Fix: host -> `http://flaresolverr:8191`, timeout -> 30s, cleared stale IndexerStatus

### Comet vs Torrentio Baseline (Feb 19, 2026)
- Comet (12 titles): p50=19.3s, p95=21.9s, avg streams=4.67
- Torrentio (12 titles): p50=0.045s, p95=0.057s, avg streams=1.0
- Conclusion: Comet's value is in stream count/quality; latency ceiling is the main gap

### Residual Edge Cases
- `tt0388629:1:2` -- slowest cache-positive title, partial precheck + foreground Prowlarr recovery
- `tt0421357:1:1`, `tt0807832:1:1` -- tail-latency anime cases with 1-2 cached hits
- `tt6473344:1:1` -- hard zero-hit miss, exits early with `reason=completed`

## RD Cache Reuse

- Active TTL policy:
  - `TORRENT_CACHE_TTL=604800`
  - `LIVE_TORRENT_CACHE_TTL=900`
  - `DEBRID_CACHE_TTL=21600`
- `DEBRID_CACHE_TTL` must stay positive. Setting it to `0` causes `get_cached_availability()` to require `updated_at >= now`, which effectively disables RD cache reuse on follow-up requests.
- Comet now logs `♻️ Existing cached torrents on <service>` before deciding whether a fresh debrid probe is needed. Use that line to distinguish:
  - cached RD reuse already present
  - fresh live RD hits from the current request
  - true RD cache misses

## Series Episode Recall

- Torrent-level discovery for IMDb-backed episode requests is intentionally looser than final playback validation.
- `TorrentManager` now keeps season-matched packs and bundles in play even when the torrent title does not expose the exact episode number.
- Final debrid/file validation remains strict on the requested season/episode, so this improves recall without loosening final playback correctness.
- `MagneticoLocal` series exact-ID retrieval now stages exact episode -> containing multi-episode -> season pack rows before any broad exact-ID rescue after indexed IMDb/TMDB lookup.
- Broad exact-ID rescue rows are capped, so structured title tiers (`series_key`, `season_pack_key`, canonical title keys) can still recover valid local candidates when the enriched ID slice is noisy.
- This fixes long-running-series cases like `Grey's Anatomy S01E03` and `The Simpsons S33E05`, where the lean DB already contained valid episode rows but the old exact-ID tier could fill its candidate budget with unrelated seasons.
