# Database Storage Analysis: BitMagnet + Cometouglas

> Analysis Date: 2026-04-04  
> Total Disk Usage: 78GB / 194GB (40% used)  
> Total Database Storage: ~10.5GB

---

## Executive Summary

This stack uses **7 distinct databases** across 2 PostgreSQL instances and 4 SQLite files:

| Database | Type | Size | Purpose | Owner |
|----------|------|------|---------|-------|
| BitMagnet PostgreSQL | PostgreSQL | ~2.0GB | Raw DHT torrent storage | Upstream (bitmagnet bitmagnet) |
| Comet PostgreSQL | PostgreSQL | ~3.0GB | Live scraper cache + RD availability | Project-specific (Cometouglas fork) |
| active.search.sqlite3 | SQLite | 6.4GB | Lean media search DB | **Project-specific** (custom schema) |
| comet-runtime.sqlite3 | SQLite | 2.1GB | Runtime torrent cache | Project-specific (Comet fork) |
| movie_index.sqlite3 | SQLite | 335MB | TMDB movie lookup index | Project-specific |
| series_index.sqlite3 | SQLite | 75MB | TMDB series lookup index | Project-specific |
| aiometadata/database.sqlite | SQLite | Small | Metadata service DB | Upstream (aiometadata) |

**Key Finding**: The two SQLite databases (`active.search.sqlite3` + `comet-runtime.sqlite3`) are the **primary storage** for your search infrastructure, representing 81% of total DB storage.

---

## 1. BitMagnet PostgreSQL

**Location**: `bitmagnet-postgres` container  
**Data Path**: `bitmagnet-media/runtime/postgres`  
**Total Size**: ~2.0GB  
**Database Name**: `bitmagnet`

### Purpose

Raw torrent storage from DHT crawling. Temporary staging workspace until content is classified and promoted to SQLite.

### Table Breakdown

| Table | Size | Rows | Purpose | Project-Specific? |
|-------|------|------|---------|-------------------|
| `torrent_files` | 1066 MB | ~32.7M | File-level metadata per torrent | ❌ Upstream |
| `torrent_contents` | 350 MB | ~11.2M | Enriched torrents with TMDB/IMDB IDs | ❌ Upstream (enriched by Python) |
| `torrents` | 123 MB | ~11.2M | Raw torrent metadata from DHT | ❌ Upstream |
| `torrents_torrent_sources` | 82 MB | ~11.2M | Many-to-many: torrents ↔ sources | ❌ Upstream |
| `queue_jobs` | 73 MB | 136K processed, 2 pending, 403 failed | Classification job queue | ❌ Upstream |
| `torrent_hints` | 33 MB | ~11.2M | Python smart hint output (fast path) | ❌ Upstream (populated by Python) |
| `content` | 32 MB | ~38K | TMDB/IMDB content metadata | ❌ Upstream |
| `content_attributes` | 24 MB | ~38K | Content facets (resolution, codec) | ❌ Upstream |
| `content_collections_content` | 17 MB | - | Collection memberships | ❌ Upstream |

**Total**: 1.8GB across 17 tables

### Schema Origin

**Default BitMagnet** with custom Python enrichment:
- Python classifier writes to `torrent_hints` via smart hint bridge
- `torrent_contents` enriched with TMDB/IMDB IDs from Python classification
- Custom fields: `tmdb_id`, `imdb_id`, `content_type`, `episodes` (JSON)

### Current State

- **DHT Crawler**: Active (~6K torrents/hour)
- **Queue Jobs**: 136,473 processed, 2 pending, 403 failed
- **Live Ingestion**: Working (latest torrent: 2026-03-16 21:46:08 UTC)
- **Promotion**: ⚠️ **BLOCKED** - Python module `lean_media_contract` not found in container

### Why It Exists

BitMagnet PostgreSQL serves as:
1. **Raw accumulator** - DHT network floods torrents, this stores them all
2. **Classification workspace** - Python reads from here, classifies, writes hints back
3. **Temporary storage** - Once promoted to SQLite, these rows can eventually be pruned
4. **DHT seed** - Maintains swarm participation for continued crawling

### Problem

**Promotion Failure** (detected 2026-04-04):
```
ModuleNotFoundError: No module named 'lean_media_contract'
```

The `promote_to_sqlite.py` script can't import `lean_media_contract` because container paths changed during `bitmagnet/` → `bitmagnet-media/` reorganization.

**Impact**: Live promotion to SQLite is broken. New DHT torrents accumulate in Postgres but don't reach the lean search DB.

---

## 2. Comet PostgreSQL

**Location**: `postgres` container  
**Data Path**: `postgres/data`  
**Total Size**: ~3.0GB  
**Database Name**: `comet`

### Purpose

Live scraper cache, Real-Debrid availability, DMM entries, anime mappings, background scraper state.

### Table Breakdown

| Table | Size | Purpose | Project-Specific? |
|-------|------|---------|-------------------|
| `torrents` | 2254 MB | Cached scraped torrents from all providers | ⚠️ Custom (Cometouglas fork) |
| `dmm_entries` | 337 MB | DataMediaManagement ingested torrents | **Project-specific** |
| `anime_entries` | 95 MB | Kitsu/Anime provider entries | ⚠️ Custom (Cometouglas fork) |
| `anime_ids` | 50 MB | Anime ID mapping (Kitsu ↔ IMDB ↔ TMDB) | ⚠️ Custom (Cometouglas fork) |
| `background_scraper_episodes` | 44 MB | Background scraper episode state | ⚠️ Custom (Cometouglas fork) |
| `media_demand` | 4.1 MB | On-demand media cache | **Project-specific** |
| `background_scraper_items` | 3.6 MB | Background scraper job queue | ⚠️ Custom (Cometouglas fork) |
| `series_episode_index` | 2.8 MB | Episode index for fast lookup | ⚠️ Custom (Cometouglas fork) |
| `media_metadata_cache` | 2.3 MB | Cached TMDB/API metadata | **Project-specific** |
| `dmm_ingested_files` | 2.3 MB | DMM file ingestion tracking | **Project-specific** |
| `anime_provider_overrides` | 1.6 MB | Manual anime provider overrides | **Project-specific** |
| `debrid_account_magnets` | 1.3 MB | Real-Debrid account magnet cache | ⚠️ Custom (Cometouglas fork) |
| `debrid_availability` | 664 KB | RD hash availability (cached API responses) | ⚠️ Custom (Cometouglas fork) |
| `magnetico_search_index` | 616 KB | Legacy Magnetico index | **Project-specific** |
| `background_scraper_runs` | 184 KB | Scraper run history | ⚠️ Custom (Cometouglas fork) |
| `scrape_locks` | 96 KB | Distributed scrape locks | ⚠️ Custom (Cometouglas fork) |
| `series_episode_index_refresh` | 48 KB | Index refresh state | ⚠️ Custom (Cometouglas fork) |
| `debrid_account_sync_state` | 64 KB | RD sync cursor | ⚠️ Custom (Cometouglas fork) |
| `download_links_cache` | 48 KB | Direct download link cache | **Project-specific** |
| `anime_mapping_state` | 24 KB | Anime mapping build state | ⚠️ Custom (Cometouglas fork) |
| `kodi_setup_codes` | 24 KB | Kodi auth codes | **Project-specific** |
| `metrics_cache` | 16 KB | Performance metrics | **Project-specific** |
| `bandwidth_stats` | 8 KB | Bandwidth statistics | **Project-specific** |

**Total**: ~3.0GB across 27 tables

### Schema Origin

**Base**: Default Comet schema (upstream `g0ldyy/comet`)  
**Custom Additions** (Cometouglas fork `lucamaia9/cometouglas`):
- `dmm_*` tables - DataMediaManagement integration
- `anime_*` tables - Anime provider support
- `background_scraper_*` tables - Background scraping
- `debrid_*` tables - RD-specific caching
- `media_demand` - On-demand request tracking

### Why It Exists

Comet PostgreSQL serves as:
1. **Live scrape cache** - Stores torrents from Jackett/Prowlarr/DMM during live requests
2. **RD availability cache** - Caches Real-Debrid API responses (`debrid_availability`)
3. **DMM workspace** - Separate ingested file tracking
4. **Anime provider** - Kitsu/anime-specific mappings
5. **Background scraper** - Async torrent fetching for popular content

### Key Tables Explained

**`torrents` (2.2GB)**
- NOT BitMagnet torrents - these are from **live scraping** (Jackett, Prowlarr, DMM)
- Cached results with full metadata
- Source for runtime SQLite during rebuilds

**`dmm_entries` (337MB)**
- DataMediaManagement ingested torrents
- Imported from external DM file library
- Separate from DHT crawl data

**`debrid_availability` (664KB)**
- **Critical for performance** - caches RD API responses
- Prevents redundant RD API calls for same infohashes
- TTL-based expiration

---

## 3. active.search.sqlite3

**Location**: `data/comet-fresh/magnetico/active.search.sqlite3`  
**Size**: 6.4GB  
**Rows**: 8,227,482 (media_index table)

### Purpose

**Lean media search database** - the authoritative source of truth for Comet MagneticoLocal queries.

### Schema (Custom Project-Specific)

**Table: `media_index`** (single table, 37 columns)

**Raw Provenance** (5 cols):
- `torrent_id` - BitMagnet PostgreSQL ID
- `info_hash` - Primary key
- `name` - Torrent name
- `total_size` - File size in bytes
- `discovered_on` - Unix timestamp

**Normalized Titles** (6 cols):
- `norm_title` - Normalized (lowercase, no articles)
- `title_key` - Further cleaned title
- `canonical_title_key` - Family-level canonical title
- `movie_key` - Movie-specific key
- `series_key` - Series-specific key
- `aliases_text` - Alternative titles

**Canonical IDs** (3 cols):
- `imdb_id` - IMDB identifier (tt#######)
- `tmdb_id` - TMDB numeric ID
- `year` - Release year

**Classification** (8 cols):
- `content_class` - "movie", "episode", "multi_episode", "season_pack", "unknown_video"
- `is_anime` - Boolean (0/1)
- `confidence` - Float (0.0 - 1.0)
- `confidence_tier` - "none", "low", "medium", "high"
- `reject_reason` - Why rejected (if rejected)
- `reason_codes` - Array of reject codes
- `match_source` - How matched (TMDB index, API, canonical layer)
- `contract_version` - Classifier contract version

**Episodic Structure** (8 cols):
- `season` - Season number
- `episode_start` - First episode
- `episode_end` - Last episode
- `exact_episode_key` - Episode-specific search key
- `season_pack_key` - Season pack search key
- `episode_count` - Total episodes
- `has_exact_episode` - Boolean
- `is_multi_episode` - Boolean
- `is_season_pack` - Boolean

**Quality Metadata** (3 cols):
- `adult_score` - Adult content likelihood
- `software_score` - Software/tutorial likelihood
- `media_score` - Actual media likelihood

**Video Facets** (3 cols):
- `resolution` - "1080p", "2160p", "720p", etc.
- `source_hint` - "BluRay", "WEB-DL", "HDTV"
- `codec_hint` - "x264", "x265", "HEVC"

### Data Breakdown (Current State)

| Category | Count | Percentage |
|----------|-------|------------|
| **Total** | 8,227,482 | 100% |
| Movies | 5,517,145 | 67% |
| Episodes (all types) | 2,357,393 | 29% |
| Anime | 201,510 | 2% |
| Unknown Video | ~151,000 | ~2% |

**Match Rate Analysis**:
- Movies: 22.1% with TMDB/IMDB IDs (5.5M total, 1.2M matched)
- Episodes: 49.4% with IDs (2.4M total, 1.2M matched)
- Anime: 72.1% with IDs (201K total, 145K matched)
- Overall: 31.7% with IDs

### Why It Exists

This is the **most critical database** in your stack:

1. **Read-only for Comet** - MagneticoLocal queries this, never writes
2. **Lean design** - Only essential fields, no bloated metadata
3. **FTS5 search index** - Full-text search via `media_fts` table
4. **Canonical title families** - Learned title variants for better matching
5. **Python-classified** - All rows passed through unified_classifier

### Who Writes It

1. **`promote_bitmagnet_live_to_compact.py`** - Live promotion from BitMagnet PostgreSQL
2. **`rebuild_live_sqlite.py`** - Full rebuilds from historical manifests
3. **`backfill_lean_ids.py`** - Offline ID enrichment (stage DB only)
4. **`match_series_to_ids.py`** - Series TMDB matching (stage DB only)
5. **`match_movies_to_tmdb.py`** - Movie recovery (stage DB only)

### Index Strategy

```sql
-- Primary key
CREATE INDEX media_index_pkey ON media_index(info_hash);

-- Lookup indexes
CREATE INDEX idx_movie_key ON media_index(movie_key);
CREATE INDEX idx_series_key ON media_index(series_key);
CREATE INDEX idx_imdb_id ON media_index(imdb_id);
CREATE INDEX idx_tmdb_id ON media_index(tmdb_id);
CREATE INDEX idx_exact_episode ON media_index(exact_episode_key);
CREATE INDEX idx_season_pack ON media_index(season_pack_key);

-- Content class indexes
CREATE INDEX idx_content_class ON media_index(content_class);
CREATE INDEX idx_is_anime ON media_index(is_anime);

-- Canonical layer indexes
CREATE INDEX canonical_title_family_media_idx
    ON canonical_title_family(media_kind, canonical_title, canonical_year);
CREATE INDEX canonical_title_variant_lookup_idx
    ON canonical_title_variant(media_kind, normalized_title, year_hint);
```

### Growth Rate

- **Ingestion**: ~10-20 torrents/min (live promotion)
- **Daily**: ~15-30K new rows
- **Monthly**: ~500K-1M new rows
- **Current trajectory**: Will hit 10M rows by ~August 2026

---

## 4. comet-runtime.sqlite3

**Location**: `data/comet-fresh/runtime/comet-runtime.sqlite3`  
**Size**: 2.1GB  

### Purpose

**Runtime cache for active scrapes** - stores in-progress and recently-completed scrape results.

### Tables

Based on Comet PostgreSQL schema subset:
- `torrents` - Cached scrape results
- `debrid_availability` - RD hash cache
- `dmm_entries` - DMM ingested files
- `anime_entries` - Anime mappings
- Background scraper tables
- Metadata cache tables

### Why It Exists

1. **Staging for rebuilds** - `rebuild_comet_runtime_sqlite.py` uses this
2. **Runtime cache** - Faster than PostgreSQL for certain queries
3. **Offline mirror** - Can operate independently of PostgreSQL

### Who Writes It

- `rebuild_comet_runtime_sqlite.py` - Periodic rebuilds from Comet PostgreSQL
- Comet runtime - During live scrapes (cache writes)

### Relationship to PostgreSQL

This is a **subset/cache** of Comet PostgreSQL:
- Same schema, smaller footprint
- No historical data beyond active TTL
- Can be rebuilt from PostgreSQL at any time

---

## 5. TMDB Lookup Indexes

### movie_index.sqlite3
**Location**: `data/tmdb/movie_index.sqlite3`  
**Size**: 335MB  
**Rows**: 1,117,308 movies

**Purpose**: Fast TMDB movie ID lookup by title + year (avoids API calls)

**Schema**:
```sql
CREATE TABLE movies (
    tmdb_id INTEGER PRIMARY KEY,
    title TEXT,
    original_title TEXT,
    year INTEGER,
    imdb_id TEXT,
    alt_titles TEXT  -- JSON array
);
```

**Who Built It**: `scripts/build_tmdb_movie_index.py`

### series_index.sqlite3
**Location**: `data/tmdb/series_index.sqlite3`  
**Size**: 75MB  
**Rows**: 218,119 series

**Purpose**: Fast TMDB series ID lookup by title + year

**Schema**:
```sql
CREATE TABLE series (
    tmdb_id INTEGER PRIMARY KEY,
    name TEXT,
    original_name TEXT,
    year INTEGER,
    imdb_id TEXT,
    alt_titles TEXT,  -- JSON array
    seasons INTEGER
);
```

**Who Built It**: `scripts/build_tmdb_series_index.py`

### Why They Exist

- **Avoid TMDB API calls** - Local lookup is ~100x faster
- **Rate limit protection** - No external API dependency
- **Offline operation** - Works without internet
- **Build once, query forever** - Indexes are static once built

---

## 6. GitHub Private Repos

### lucamaia9/bitmagnet-media
**URL**: https://github.com/lucamaia9/bitmagnet-media  
**Description**: BitMagnet fork with smart_hint media filtering  
**Content**: 
- Custom Go processor with Python smart hint integration
- Episode parsing fix (trust Python's 109 patterns)
- Classification queue improvements

### lucamaia9/cometouglas
**URL**: https://github.com/lucamaia9/cometouglas  
**Description**: Private Cometouglas fork for live Stremio scraping  
**Content**:
- Single-wave parallel search architecture
- MagneticoLocal scraper
- Content-type filtering
- DMM integration
- RD fast-return logic

**Neither repo contains custom DB schemas** - both use upstream schemas with Python enrichment.

---

## Storage Summary

| Storage Type | Size | % of Total |
|-------------|------|------------|
| BitMagnet PostgreSQL | 2.0GB | 19% |
| Comet PostgreSQL | 3.0GB | 29% |
| active.search.sqlite3 | 6.4GB | 61% |
| comet-runtime.sqlite3 | 2.1GB | 20% |
| TMDB indexes | 410MB | 4% |
| **Total DB Storage** | **~10.5GB** | **100%** |

**Disk utilization**: 78GB / 194GB (40% used)  
**Remaining space**: 117GB free  
**Growth runway**: ~6+ months at current ingestion rate

---

## Known Storage Issues

### 1. BitMagnet Promotion Broken

**Symptom**: `ModuleNotFoundError: No module named 'lean_media_contract'`

**Root Cause**: Container mount paths changed during `bitmagnet/` → `bitmagnet-media/` reorganization, but `promote_to_sqlite.py` import paths not updated.

**Fix**: Update sys.path in `promote_to_sqlite.py`:
```python
sys.path.insert(0, '/opt/bitmagnetico/classifier')
```

### 2. Comet PostgreSQL Growing Unbounded

**Current**: 2.2GB in `torrents` table  
**Cause**: Live scrape cache with no aggressive cleanup  
**Solution**: Implement TTL-based deletion (e.g., delete rows >30 days old)

### 3. PostgreSQL vs SQLite Redundancy

**Problem**: 
- BitMagnet PostgreSQL (~2GB) stores raw torrents
- Comet PostgreSQL (~3GB) stores scraped torrents
- active.search.sqlite3 (6.4GB) stores classified subset
- comet-runtime.sqlite3 (2.1GB) mirrors Comet PostgreSQL

**Total**: ~13.5GB across 4 databases with overlapping data

**Optimization**: 
- Prune BitMagnet PostgreSQL after wide drain completes
- Consider eliminating comet-runtime.sqlite3 if PostgreSQL performance is acceptable

---

## Recommendations

### Immediate Actions

1. **Fix BitMagnet promotion**
   - Update classifier import paths in container
   - Verify live promotion resumes
   - Monitor `sqlite promotion completed` logs

2. **Implement PostgreSQL cleanup**
   - BitMagnet: Prune torrents older than 90 days
   - Comet: Delete scraped torrents older than 30 days
   - Keep only rows with TMDB/IMDB IDs long-term

3. **Verify backup strategy**
   - `active.search.sqlite3` is most critical (6.4GB)
   - Weekly backups recommended
   - Test restore procedure quarterly

### Medium-Term Optimizations

1. **Evaluate PostgreSQL vs SQLite performance**
   - Benchmark: PostgreSQL reads vs SQLite reads
   - If SQLite is 2-5x faster, migrate Comet runtime fully to SQLite
   - If comparable, eliminate comet-runtime.sqlite3

2. **Implement wide drain**
   - Complete parity-correct drain from BitMagnet PostgreSQL → SQLite
   - Set `wideDrainCompleted=true`
   - Enable pruning to reclaim PostgreSQL space

3. **Index optimization**
   - Audit unused indexes in PostgreSQL (reduce write overhead)
   - Add partial indexes for common query patterns
   - Consider covering indexes for MagneticoLocal queries

### Long-Term Strategy

1. **Consolidate to SQLite-first architecture**
   - BitMagnet → PostgreSQL (temporary staging, pruned weekly)
   - Python Classifier → active.search.sqlite3 (authoritative)
   - Comet → Read from SQLite, write to PostgreSQL only for RD cache

2. **Archive cold data**
   - Move torrents >90 days old to compressed JSONL archives
   - Keep hot data (last 30 days) in databases
   - Re-import from archives if needed

3. **Partition by content type**
   - Split `media_index` into content-specific tables
   - `media_index_movies`, `media_index_episodes`, `media_index_anime`
   - Reduces index size per query, improves performance

---

## Operational Commands

### Check Sizes
```bash
# All databases
du -sh data/comet-fresh/magnetico/*.sqlite3 \
       data/comet-fresh/runtime/*.sqlite3 \
       data/tmdb/*.sqlite3

# BitMagnet PostgreSQL
sudo docker compose exec bitmagnet-postgres psql -U postgres -d bitmagnet -c \
  "SELECT pg_size_pretty(pg_database_size('bitmagnet'));"

# Comet PostgreSQL
sudo docker compose exec postgres psql -U comet -d comet -c \
  "SELECT pg_size_pretty(pg_database_size('comet'));"
```

### Cleanup Commands
```bash
# Prune old BitMagnet torrents (keep last 90 days)
sudo docker compose exec bitmagnet-postgres psql -U postgres -d bitmagnet -c \
  "DELETE FROM torrents WHERE created_at < NOW() - INTERVAL '90 days';"

# Vacuum PostgreSQL (reclaim space)
sudo docker compose exec bitmagnet-postgres psql -U postgres -d bitmagnet -c \
  "VACUUM FULL torrents;"

# Vacuum SQLite
sqlite3 data/comet-fresh/magnetico/active.search.sqlite3 "VACUUM;"
```

### Backup Commands
```bash
# Backup active.search.sqlite3 (hot backup)
cp data/comet-fresh/magnetico/active.search.sqlite3 \
   backups/active-$(date +%Y%m%d).sqlite3

# Backup PostgreSQL (dump)
sudo docker compose exec bitmagnet-postgres pg_dump -U postgres bitmagnet | \
  gzip > backups/bitmagnet-$(date +%Y%m%d).sql.gz
```

This document provides complete visibility into every database in your stack, what it stores, why it exists, and how to manage it.
