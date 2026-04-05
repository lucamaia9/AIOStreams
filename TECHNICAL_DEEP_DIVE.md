# Technical Deep Dive: BitMagnet + Magnetico + Cometouglas

> Last updated: 2026-04-04  
> Scope: Complete technical understanding of the three-system stack

---

## Executive Technical Summary

This is a **three-layer torrent enrichment and search pipeline**:

1. **BitMagnet** (Go/PostgreSQL) - DHT crawler + raw torrent staging
2. **Python Classifier** (Python/SQLite) - Smart media classification + lean search DB
3. **Cometouglas** (Python/PostgreSQL) - Live scraper + Stremio API server

**Data Flow**: DHT → BitMagnet (raw torrents) → Python Classifier (accept/reject + IDs) → Lean SQLite → Comet MagneticoLocal (read-only search) → Stremio

**Key Innovation**: Python's unified_classifier (109 Sonarr regex patterns + TrashGuides scoring) is the **source of truth** for media classification, not Go's simpler parser (4 patterns). This prevents misclassification of anime, episodes, and foreign content.

---

## Part 1: BitMagnet Layer

### Architecture

**Purpose**: Long-running DHT crawler that accumulates raw torrents into PostgreSQL for downstream classification.

**Components**:
- `bitmagnet-postgres:5432` - Dedicated PostgreSQL (not shared)
- `bitmagnet:3333` - Go application with HTTP server + queue processor
- DHT crawler bound to port `3334/tcp+udp`

**Key Tables**:
- `torrents` (11.1M rows) - Raw torrent metadata from DHT
- `torrent_files` (32.7M rows) - File-level details per torrent
- `torrent_contents` - Enriched content with TMDB/IMDB IDs
- `torrent_hints` - Python classification output (fast path)
- `queue_jobs` - Classification job queue (processed: 136K, pending: 2, failed: 403)

### Intake Filter (Go)

**Location**: `bitmagnet-media/internal/processor/`

**Filter stages**:
1. **Size filter** - Reject torrents <7MB or >100GB
2. **Infohash dedupe** - Skip already-seen torrents
3. **Basic heuristics** - Block obvious junk before classification
4. **Queue for classification** - Send to Python smart hint via gRPC

**Current throughput**: ~6K torrents/hour from DHT

### Smart Hint Bridge (Python ↔ Go)

**Location**: `classifier/shared_bitmagnet_bridge.py`

**Contract**: Python sends `ImportHints` JSON to Go via `TorrentHint` struct

```json
{
  "contentType": "movie|tv_show",
  "releaseYear": 2024,
  "episodes": {"1": {"1": {}, "2": {}}},
  "videoResolution": "1080p",
  "videoSource": "bluray",
  "videoCodec": "x264",
  "tmdbId": "12345",
  "imdbId": "tt1234567",
  "fastPath": true
}
```

**Fast Path**: When Python provides TMDB/IMDB ID with high confidence (≥0.85), Go skips classification and directly writes to `torrent_contents`.

### Known Fixes Applied

**2026-03-21 Smart Hint Fixes**:
- ✅ Mutex deadlock in `processor.go` - Goroutine lock/unlock patterns fixed
- ✅ Unbounded goroutine concurrency - Limited to 4 concurrent classifier calls
- ✅ `persist()` never wrote to `torrent_hints` - Added persistence logic
- ✅ Queue job timeout - Increased from 10min to 60min
- ✅ `release_year` not carried through - Added to `smartHintImportHints`

**2026-04-03 Episode Parsing Fix**:
- ✅ Go was overwriting Python's superior episode parsing with simpler regex
- ✅ Fixed in `action_parse_video_content.go`: Trust Python if `len(Hint.Episodes) > 0`

---

## Part 2: Python Classifier Stack

### Unified Classifier Architecture

**Location**: `bitmagnet-media/classifier/unified_classifier/`

**Components**:
1. **RTN Parser** - Quality/resolution/codec extraction from torrent title
2. **Sonarr Parser** - 109 regex patterns for episode/season parsing (Python port of Sonarr's `Parser.cs`)
3. **TrashGuides Scorer** - Anime tier scoring with LQ rejection
4. **Shared Title Normalizer** - Radarr-compatible title cleaning (articles, connectors, accents)
5. **TMDB Series Index** - Local SQLite index for instant ID lookup (no API calls)

**Classification Output**:
```python
{
    "content_class": "movie|episode|anime_episode|season_pack|reject",
    "is_anime": True/False,
    "year": 2024,
    "confidence": 0.92,
    "confidence_tier": "high",  # ≥0.85 → fast path
    "reject_reason": "adult|software|lq_anime",
    "reason_codes": ["adult_explicit", "jav_code_ABW"],
    "resolution": "1080p",
    "codec": "x264",
    "quality": "BluRay",
    "episode_structure": {"1": {"1": {}, "2": {}}},  # saison 1, episodes 1-2
    "tmdb_id": "1396",
    "imdb_id": "tt0903747"
}
```

### Smart Classification Logic

**Location**: `classifier/shared_bitmagnet_bridge.py`

**Decision Tree**:
```
1. Check canonical_title_family layer (instant, 43K families, 242K variants)
   ├─ Match → Return tmdb_id/imdb_id immediately (<1ms)
   └─ No match → Continue to classification

2. RTN parse title → Extract resolution, codec, quality
   ├─ If adult pattern (400+ patterns) → Reject with reason
   └─ If anime pattern → Apply TrashGuides scoring

3. Sonarr episode parsing → Season/episode structure
   ├─ 109 regex patterns (far superior to Go's 4)
   └─ Handles: S01E01, 1x01, 2024.04.03, anime absolute (ep 1, 2, 3...)

4. TMDB SQLite index lookup → tmdb_id/imdb_id if matched
   ├─ Series index: 218K entries
   └─ Movie index: 1.1M entries

5. Return ImportHints to Go with all findings
```

**Performance**: 
- Canonical layer hit: <1ms
- TMDB index match: ~5ms
- Full classification: ~50-100ms

### Live Promotion to Lean SQLite

**Location**: `scripts/promote_bitmagnet_live_to_compact.py`

**Process**:
1. Poll BitMagnet PostgreSQL for new `torrent_contents` rows
2. Use Python classification as source of truth
3. Project into lean SQLite `media_index` table with:
   - `info_hash`, `name`, `total_size`, `discovered_on`
   - `norm_title`, `title_key`, `movie_key`, `series_key`
   - `content_class`, `is_anime`, `confidence`, `reject_reason`
   - `imdb_id`, `tmdb_id`, `year`
   - Episode fields: `season`, `episode_start`, `episode_end`, `episode_count`
   - `resolution`, `source_hint`, `codec_hint`
   - `search_text` (TSV for FTS queries)

**Rate**: 10-20 torrents/min inline promotion

### Lean SQLite Schema

**Location**: `data/comet-fresh/magnetico/active.search.sqlite3` (~5.6 GB)

**Table: `media_index`** (9.08M rows)
```sql
CREATE TABLE media_index (
    info_hash TEXT PRIMARY KEY,
    name TEXT,
    total_size REAL,
    discovered_on INTEGER,
    norm_title TEXT,
    title_key TEXT,
    canonical_title_key TEXT,
    movie_key TEXT,
    series_key TEXT,
    exact_episode_key TEXT,
    season_pack_key TEXT,
    imdb_id TEXT,
    tmdb_id TEXT,
    year INTEGER,
    content_class TEXT,
    is_anime TEXT,
    confidence REAL,
    confidence_tier TEXT,
    reject_reason TEXT,
    reason_codes TEXT,
    match_source TEXT,
    contract_version TEXT,
    season INTEGER,
    episode_start INTEGER,
    episode_end INTEGER,
    episode_count INTEGER,
    has_exact_episode INTEGER,
    is_multi_episode INTEGER,
    is_season_pack INTEGER,
    adult_score REAL,
    software_score REAL,
    media_score REAL,
    resolution TEXT,
    source_hint TEXT,
    codec_hint TEXT,
    search_text TEXT  -- TSV for FTS
);

-- Indexes for fast lookup
CREATE INDEX idx_movie_key ON media_index(movie_key);
CREATE INDEX idx_series_key ON media_index(series_key);
CREATE INDEX idx_imdb_id ON media_index(imdb_id);
CREATE INDEX idx_tmdb_id ON media_index(tmdb_id);
CREATE INDEX idx_exact_episode ON media_index(exact_episode_key);
CREATE INDEX idx_season_pack ON media_index(season_pack_key);
```

**Table: `canonical_title_family`** (43K families)
```sql
CREATE TABLE canonical_title_family (
    family_key TEXT PRIMARY KEY,
    media_kind TEXT,
    canonical_title TEXT,
    canonical_year INTEGER,
    season_bucket TEXT,
    imdb_id TEXT,
    tmdb_id TEXT,
    confidence TEXT,
    resolver_source TEXT,
    evidence_count INTEGER,
    updated_at TEXT
);
```

**Table: `canonical_title_variant`** (242K variants)
```sql
CREATE TABLE canonical_title_variant (
    variant_key TEXT,
    family_key TEXT,
    media_kind TEXT,
    raw_title TEXT,
    normalized_title TEXT,
    year_hint INTEGER,
    season_bucket TEXT,
    confidence TEXT,
    variant_source TEXT,
    evidence_count INTEGER,
    updated_at TEXT,
    PRIMARY KEY (variant_key, family_key)
);
```

### Coverage Metrics (Current State)

| Category | Total | With IDs | Coverage % | Target |
|----------|-------|----------|------------|--------|
| **Overall** | 9.08M | 2.88M | **31.7%** | — |
| Movies | 6.09M | 1.34M | **22.1%** | 60%+ ❌ |
| Episodes | 1.80M | 889K | **49.4%** | 70%+ ⚠️ |
| Multi-episodes | 597K | 344K | **57.7%** | 70%+ ⚠️ |
| Season packs | 197K | 123K | **62.4%** | 70%+ ⚠️ |
| Anime episodes | 187K | 141K | **75.8%** | 80%+ ✅ |
| Anime packs | 11K | 7K | **67.1%** | 80%+ ⚠️ |

**Why movies are low**: 95%+ of unmatched "movies" are misclassified content (anime, Russian TV, noise, Chinese dramas). The 22% represents actual movies.

**Unmatched audit**: 77.8% of unmatched rows are recoverable (97% of episodes, 67.5% of movies). **Do not mass-clear unmatched content.**

---

## Part 3: Cometouglas Layer

### Live Path Architecture

**Location**: `cometouglas/comet/api/endpoints/stream.py`

**Key Innovation**: Single-wave unified parallel search (replaced 3-wave sequential architecture on 2026-03-26)

**Workflow** (Request → Response):
```python
1. PARSE request
   ├─ Extract media_id, season, episode
   ├─ Resolve external IDs (IMDB, TMDB, Kitsu)
   └─ Check DB cache

2. CONTENT-TYPE DETECTION
   ├─ kitsu: → "anime"
   ├─ media_type="movie" → "movie"
   ├─ anime_mapper.is_anime_content() → "anime"
   └─ else → "series"

3. UNIFIED SCRAPE (all fire simultaneously)
   ├─ magnetico_local (5s timeout) - Local SQLite search
   ├─ dmm (5s timeout) - Internal DMM database
   ├─ jackett (10s timeout) - Content-type filtered indexers
   └─ prowlarr (10s timeout) - Content-type filtered indexers
   
   Results stream via asyncio queue (first-come-first-serve)

4. RTN FILTER (before RD API call)
   ├─ rank_torrents() - Title match, year, quality
   └─ Filter 1441 scraped → 300 surviving (79% rejection)

5. RD AVAILABILITY CHECK (only ranked hashes)
   ├─ Chunks of 500 in parallel
   └─ Cache to debrid_availability table

6. RESPONSE BUILDING
   ├─ Select per-resolution (1080p, 2160p, etc.)
   └─ Return stream objects with playback URLs
```

**Timing Budget**:
- `LIVE_SCRAPE_HARD_TIMEOUT=35` - Absolute scrape ceiling
- `LIVE_TOTAL_BUDGET_S=35` - Total request budget
- `LIVE_FAST_RETURN_RD_CACHED_RESULTS=10` - Early return threshold

**Performance Baseline** (20-title corpus, cached-only):
- Median latency: 1.79s
- P95 latency: 8.02s
- RD cached positive: 90%
- Median stream count: 7.0

### MagneticoLocal Scraper

**Location**: `cometouglas/comet/scrapers/magnetico_local.py`

**Purpose**: Read-only search against lean SQLite `active.search.sqlite3`

**Tiered Retrieval** (fastest → slowest):
```python
1. Exact IMDB ID lookup (if request has IMDB)
2. Exact TMDB ID lookup (if request has TMDB)
3. Exact structured keys:
   ├─ movie_key (for movies)
   ├─ series_key (for series)
   ├─ exact_episode_key (for specific episodes)
   └─ season_pack_key (for season packs)
4. Canonical family title expansion
   ├─ Lookup variant in canonical_title_variant
   ├─ Get family_key
   ├─ Retrieve all variants in family
   └─ Search by all variants
5. Title key prefix fallback (bounded)
6. FTS5 fallback (bounded)
7. LAST RESORT: norm_title LIKE query
```

**Current State**: 8.78M processed rows, 8.66M searchable rows

**Optimization** (2026-04-04): Coerced `total_size` to numeric before emitting scraper rows to prevent `str` vs `float` crashes during ranking.

### Content-Type Filtering

**Location**: `cometouglas/comet/core/constants.py`

**Indexer Assignment** (runtime split artifact or env fallback):

| Content Type | Jackett Indexers | Prowlarr Indexers |
|-------------|-----------------|-------------------|
| **series** | 13 (all except nyaa, dmhy) | 6-7 |
| **movie** | 15 (all except nyaa, dmhy, eztv) | 7 |
| **anime** | 2-4 (nyaa, dmhy, knaben, bitsearch) | 1 (mikan) |

**Deduplication**: Each provider assigned exactly one owner lane (jackett xor prowlarr) via `INDEXER_SPLIT_ARTIFACT_PATH`.

### Search Plan Generation

**Location**: `cometouglas/comet/scrapers/search_plan.py`

**Stage-Based Query Building**:
```python
# For standard series request (IMDB ID + season + episode):

Stage 1: episode_exact
  ├─ "Breaking Bad S01E01"
  ├─ "Breaking Bad 1x01"
  └─ With IMDB: Add tt0903747 to query params

Stage 2: season_fallback (if <8 candidates from stage 1)
  ├─ "Breaking Bad S01"
  └─ "Breaking Bad Season 1"

Stage 3: broad_title_rescue (if <24 candidates from stage 2)
  └─ "Breaking Bad"

# Each stage runs simultaneously (not sequentially!)
# First-come-first-serve into unified result queue
```

**Query Dedup**: TTL-based suppression prevents duplicate provider/indexer/query calls (`LIVE_QUERY_DEDUP_TTL_S=45`)

**Jitter**: Small random delay before indexer fanout (`INDEXER_REQUEST_JITTER_MAX_MS=500`) to prevent thundering herd.

### Indexer Health Monitoring

**Location**: `cometouglas/comet/services/indexer_health.py`

**Circuit Breaker**:
- `INDEXER_FAIL_STREAK_THRESHOLD=3` - After 3 consecutive failures, suppress indexer
- `INDEXER_COOLDOWN_SECONDS=900` - 15-min cooldown before retry
- Error classification: `timeout`, `forbidden` (403), `ratelimit` (429), `hard_error` (5xx)
- Reliability penalties tracked per indexer (timeout: 0.45, 403: 0.30, 429: 0.15, 5xx: 0.10)

**Rebalance Script**: `scripts/rebalance_from_live_usage.py` scores indexers on hit_count (0.35), seeders_quality (0.25), recency (0.20), reliability_norm (0.20) over 3-day rolling window.

---

## Part 4: Critical Scripts & Operations

### Live Promotion Workflow

**Script**: `scripts/promote_bitmagnet_live_to_compact.py`

**Command**:
```bash
python3 scripts/promote_bitmagnet_live_to_compact.py \
  --batch-size 1000 \
  --classifier-workers 4 \
  --fts-batch-size 2000
```

**Process**:
1. Poll BitMagnet PostgreSQL for new `torrent_contents` rows (keyset pagination)
2. For each row, check canonical_title_family layer (instant match)
3. If no match, run unified_classifier on raw torrent name
4. Project into lean SQLite `media_index` with all fields
5. Update FTS5 `media_fts` search index
6. Checkpoint progress every 10 batches

**Guarantees**:
- Resumes from checkpoint on restart
- Skips already-promoted infohashes (idempotent)
- Blocks writes to live DB if `--allow-live-search-db` not set

### Rebuild + Cutover Workflow

**Script**: `scripts/rebuild_live_sqlite.py`

**Purpose**: Create new lean SQLite from historical manifest + raw BitMagnet replay

**Command**:
```bash
python3 scripts/rebuild_live_sqlite.py \
  --search-db data/comet-fresh/magnetico/active.search.sqlite3 \
  --historical-seed-db data/comet-fresh/magnetico/stage.db \
  --integrity-check full \
  --cutover \
  --stage-root data/comet-fresh/magnetico/rebuild-runs
```

**Stages**:
1. **Historical base**: Import from strictVideo.manifest.json or seed DB
2. **Raw BitMagnet replay**: Replay live torrents from Postgres up to frontier timestamp
3. **Integrity check**: `PRAGMA integrity_check` or `PRAGMA quick_check`
4. **Cutover**: Atomically replace active.search.sqlite3
5. **Builder mirror**: Optionally copy to builder deploy dir

**Safety**:
- Creates backup before cutover
- Validates schema compatibility
- Strips old live-overlay rows from `media_fts` using FTS5 delete tokens

### ID Enrichment (Offline Stage Only)

**Scripts**:
- `backfill_lean_ids.py` - Series/anime movie enrichment
- `match_movies_to_tmdb.py` - Conservative movie recovery
- `match_series_to_ids.py` - Series TMDB matching
- `batch_fuzzy_update.py` - RapidFuzz fuzzy matching

**Critical Rule**: **Do NOT run these against live `active.search.sqlite3`**. They block live DB writes by default.

**Safe Workflow**:
```bash
# 1. Create verified backup
cp active.search.sqlite3 backups/enriched-stage.backup.sqlite3

# 2. Create stage DB
cp backups/enriched-stage.backup.sqlite3 enriched-stage.sqlite3

# 3. Run enrichment against stage DB
LEAN_ID_BACKFILL_SEARCH_DB=enriched-stage.sqlite3 \
  scripts/run_lean_id_backfill.sh

# 4. Cutover stage → live
python3 scripts/rebuild_live_sqlite.py \
  --search-db active.search.sqlite3 \
  --historical-seed-db enriched-stage.sqlite3 \
  --cutover
```

**Enrichment Methods**:
- **Exact match**: Radarr-parsed title + year → TMDB index
- **Alt title variants**: Check alternative titles with Roman/Arabic numerals
- **Fuzzy match**: RapidFuzz `token_set_ratio` with first-letter blocking (~100 keys/sec, 8% match rate, 2% error rate)

### Production Gate Bundle

**Script**: `scripts/run_production_gates.py`

**Purpose**: Pre-cutover validation checklist

**Checks**:
1. Dual-SLO coverage report (unknown_video share, canonical missing share)
2. Deterministic unmatched audit (200 samples, seed=20260404)
3. Movie precision smokes (top 100 + random 100)
4. Regression suites:
   - `test_match_movies_to_tmdb.py -q`
   - `tests.test_magnetico_local -q` (MagneticoLocal unit tests)

**Exit code**: Non-zero if hard gate fails

**Current state**: Expected to stay red until SLO coverage improves from 31.7% to 80%+.

---

## Part 5: Known Issues & Mitigations

### Issue 1: Wide Drain Incomplete

**Status**: ⚠️ Blocked (parity-correct wide drain still in progress)

**Impact**:
- PostgreSQL pruning blocked (will grow unbounded)
- `wideDrainCompleted` flag not set in SQLite metadata

**Mitigation**:
- Daily prune script exists but disabled
- Emergency override: `--allow-before-wide-drain` flag

**Resolution**: Complete parity-correct drain from BitMagnet PostgreSQL → SQLite, verify frontier equality, set `wideDrainCompleted=true`.

### Issue 2: Low Movie Match Rate (22% vs 60% target)

**Status**: ❌ Blocked by misclassified content

**Root Cause**: 95%+ of unmatched "movies" are not actually movies:
- Russian TV series
- Anime misclassified as movies
- Chinese dramas
- Adult content
- Software tutorials

**Mitigation**:
- Do NOT mass-clear unmatched rows (77.8% are recoverable)
- Run targeted enrichment for specific categories
- Improve intake filter to reject non-movie content earlier

### Issue 3: Runtime SQLite Not Cut Over

**Status**: ⚠️ Still using PostgreSQL for live reads

**Current State**:
- PostgreSQL serves live Comet requests
- `data/comet-fresh/runtime/comet-runtime.sqlite3` is staging only
- Shared writer pattern (comet + comet-builder) not yet production-validated

**Risk**: PostgreSQL overhead adds ~1-2s median latency vs SQLite

**Resolution**: Validate shared SQLite writer pattern, run benchmarks, cut over runtime DB.

### Issue 4: Episode Enrichment Gap

**Status**: 🟡 Not yet automated

**Current State**: Episodes at 49.4% coverage, target is 70%+

**Plan**: Create `match_episodes_to_ids.py` following `match_anime_to_ids.py` pattern:
1. Extract unique `series_key` values from unmatched episode rows
2. Build series-to-TMDB/IMDB lookup from `canonical_title_family` + `canonical_title_variant`
3. Fan out resolved series IDs to all episode rows sharing that `series_key`
4. Use set-based `UPDATE` with indexed `series_key` column

**Estimated Impact**: +20% episode coverage

---

## Part 6: Performance Optimizations Applied

### 1. RTN Filter Before RD API (2026-03-26)

**Change**: Moved RTN filtering before Real-Debrid availability check

**Impact**:
- 20-50% fewer RD API calls
- Friends S01E16 example: 1441 scraped → 300 RTN-surviving → only 300 sent to RD API
- Reduced median latency from 4.0s to 2.5s

### 2. Single-Wave Parallel Search (2026-03-26)

**Change**: Replaced 3-wave sequential (wave1→wave2→wave3) with single-wave unified parallel

**Impact**:
- All scrapers fire simultaneously (no sequential gating)
- Reduced p95 latency from 13.5s to 8.0s
- Simplified logic (no complex wave orchestration)

### 3. Indexer Dedup Across Providers (2026-03-27)

**Change**: Alias-aware dedup prevents same provider queried by both Jackett and Prowlarr

**Impact**:
- Reduced redundant queries
- Better indexer distribution
- Owner-lane assignment via split artifact

### 4. Canonical Layer Lookup (2026-04-03)

**Change**: Built `canonical_title_family` + `canonical_title_variant` tables for instant ID lookup

**Impact**:
- 43K families, 242K variants
- <1ms lookup time (no TMDB API calls)
- 99.99% have TMDB IDs, 91.5% have IMDB IDs

### 5. MagneticoLocal Exact-ID Retrieval (2026-04-04)

**Change**: Fetch `torrent_id` values first, then load rows by ID (avoid full-table scans)

**Impact**:
- Eliminated `SELECT m.* WHERE imdb_id=?` full-table scans
- 10-50x faster for large DBs
- Series exact-ID now episode-aware (contains multi-episode + season pack rows)

---

## Part 7: Documentation Map

| Document | Purpose | Read When |
|----------|---------|-----------|
| `BITMAGNET_OPERATIONS.md` | Runtime ops for BitMagnet | Deploying, troubleshooting BitMagnet |
| `BITMAGNET_TARGET_LOGIC.md` | Architecture decisions | Understanding ownership boundaries |
| `BITMAGNET_PIPELINE_ROADMAP.md` | Prioritized backlog | Planning next improvements |
| `classifier/unified_classifier/WORKFLOW.md` | Classifier pipeline details | Debugging classification, adding features |
| `COMET_OPERATIONS.md` | Comet live path, waves, indexers | Tuning Comet performance |
| `LIVE_ENRICHMENT_IMPLEMENTATION.md` | Live DHT enrichment design | Understanding live ↔ historical parity |
| `PLAN.md` | BitMagnet/Comet enrichment master plan | Historical context for enrichment strategy |

---

## Summary: System Strengths

1. **Python-first classification** - 109 Sonarr patterns >> Go's 4 patterns
2. **Layered TMDB lookup** - Canonical layer (instant) → SQLite index (fast) → API (fallback)
3. **Single-wave parallel search** - Simple, fast, first-come-first-serve
4. **Content-type aware indexer routing** - Anime → nyaa/dmhy, movies → 1337x/TPB, series → EZTV
5. **Lean SQLite for reads** - Avoids PostgreSQL overhead for Comet queries
6. **Offline enrichment workflow** - Stage DB → cutover protects live DB from corruption
7. **Production gate bundle** - Pre-cutover validation prevents bad deploys
8. **Canonical title families** - Learned variants improve live matching over time

---

## Appendix: File Inventory

**BitMagnet Go Code**: `bitmagnet-media/internal/processor/`
- `processor.go` - Main classification orchestrator
- `persist.go` - Write to PostgreSQL
- `action_parse_video_content.go` - Episode parsing (trusts Python)
- `queue/handler.go` - Job queue processor

**Python Classifier**: `bitmagnet-media/classifier/`
- `unified_classifier/` - RTN + Sonarr + TrashGuides
- `shared_bitmagnet_bridge.py` - Python↔Go contract
- `compact_media_search.py` - Lean SQLite builder
- `magnetico_media_probe.py` - Torrent probing + analysis
- `bitmagnet_smart_hint.py` - Smart hint integration
- `tmdb_series_index.py` - Local TMDB index (with fuzzy fallback)
- `canonical_resolver.py` - Canonical layer lookups

**Comet Code**: `cometouglas/comet/`
- `api/endpoints/stream.py` - Live path (single-wave search)
- `scrapers/jackett.py` - Content-type filtered scraping
- `scrapers/prowlarr.py` - Content-type filtered scraping
- `scrapers/magnetico_local.py` - Lean SQLite search
- `scrapers/search_plan.py` - Query variant generation
- `services/orchestration.py` - Unified scrape entry point
- `services/indexer_health.py` - Circuit breaker
- `services/query_dedupe.py` - TTL dedup

**Scripts**: `scripts/`
- `promote_bitmagnet_live_to_compact.py` - Live promotion
- `rebuild_live_sqlite.py` - Rebuild + cutover
- `backfill_lean_ids.py` - Offline enrichment (blocks live DB)
- `match_movies_to_tmdb.py` - Movie recovery (stage-only)
- `match_series_to_ids.py` - Series matching (stage-only)
- `check_bitmagnet_id_coverage.py` - SLO metrics
- `run_production_gates.py` - Pre-cutover validation

This document captures the complete technical understanding of your BitMagnet + Magnetico + Cometouglas stack as of 2026-04-04.
