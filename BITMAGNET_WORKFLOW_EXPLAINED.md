# BitMagnet End-to-End Workflow

## Overview

BitMagnet is a DHT crawler that discovers torrents, filters unwanted content, classifies media, and promotes TMDB-matched content to a SQLite search database for Comet.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DHT NETWORK                                     │
│                    (Distributed Hash Table - BitTorrent)                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         TIER 1: GO INTAKE FILTER                             │
│                                                                              │
│  Purpose: Block unwanted content BEFORE database write                       │
│  Location: internal/dhtcrawler/intake_filter.go                              │
│                                                                              │
│  Checks:                                                                     │
│  ├── Size threshold (min 50MB)                                              │
│  ├── Adult brands (1000+ patterns) - Brazzers, Naughty America, etc.        │
│  ├── Adult performers - Lisa Ann, Riley Reid, etc.                          │
│  ├── JAV codes - ABP-123, SSIS-456, etc.                                    │
│  ├── CJK adult terms - Chinese/Japanese/Korean                              │
│  ├── Explicit patterns - fuck, anal, blowjob, etc.                          │
│  └── Courseware - Udemy, Coursera, tutorials                                │
│                                                                              │
│  Result: ~60-70% of DHT content rejected instantly                          │
│          Zero database writes for rejected content                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (pass)
┌─────────────────────────────────────────────────────────────────────────────┐
│                         POSTGRESQL DATABASE                                   │
│                                                                              │
│  Tables written:                                                             │
│  ├── torrents (info_hash, name, size, files_status)                         │
│  ├── torrent_files (path, size per file)                                    │
│  └── queue_jobs (pending classification jobs)                              │
│                                                                              │
│  Current state: 484K torrents                                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (queue job)
┌─────────────────────────────────────────────────────────────────────────────┐
│                      TIER 2: PYTHON SMART HINT                               │
│                                                                              │
│  Purpose: Pre-classify and require TMDB match for acceptance                │
│  Location: bitmagnet-media/classifier/bitmagnet_smart_hint.py               │
│                                                                              │
│  Flow:                                                                       │
│  1. Classify content type (movie/episode/season_pack/anime)                 │
│  2. Parse episode structure (S01E05)                                        │
│  3. Extract year and title                                                  │
│  4. Lookup in canonical layer (SQLite - 44K titles)                         │
│  5. If TMDB match found → accept with hints                                 │
│  6. If no TMDB match → reject (useless for Comet)                           │
│                                                                              │
│  Result: ~5-10% of passed content accepted                                  │
│          100% of accepted content has TMDB ID                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (if accepted)
┌─────────────────────────────────────────────────────────────────────────────┐
│                      BITMAGNET CLASSIFIER                                    │
│                                                                              │
│  Purpose: Enrich with full TMDB metadata                                     │
│  Location: internal/classifier/                                              │
│                                                                              │
│  Actions:                                                                    │
│  ├── Parse video metadata (resolution, codec, source)                       │
│  ├── TMDB API search (if not fast-pathed)                                   │
│  ├── Create Content record (title, year, type)                              │
│  └── Link TorrentContent (info_hash -> tmdb_id)                             │
│                                                                              │
│  Tables written:                                                             │
│  ├── contents (tmdb_id, title, type, year, tsv)                             │
│  ├── torrent_contents (info_hash -> content mapping)                        │
│  └── torrent_hints (cached classification)                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (if TMDB matched)
┌─────────────────────────────────────────────────────────────────────────────┐
│                      SQLITE PROMOTION                                        │
│                                                                              │
│  Purpose: Fast local search without PostgreSQL                               │
│  Script: scripts/promote_to_sqlite.py                                        │
│  Target: /data/canonical.sqlite3                                             │
│                                                                              │
│  Requirements for promotion:                                                 │
│  ├── content_source IS NOT NULL (tmdb/imdb)                                 │
│  └── content_id IS NOT NULL (the actual ID)                                 │
│                                                                              │
│  Result: Inserted ~10-30 records/hour                                       │
│          SQLite size: 9M+ entries                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. DHT Crawler Pipeline

The crawler uses a concurrent channel-based pipeline:

```
Bootstrap Nodes → Ping → FindNode → SampleInfoHashes → InfoHashTriage
                                                          │
                                                          ▼
                                                    ┌─────┴─────┐
                                                    │           │
                                                    ▼           ▼
                                                GetPeers    Scrape
                                                    │           │
                                                    ▼           │
                                              RequestMetaInfo    │
                                                    │           │
                                                    ▼           ▼
                                              PersistTorrents ←──┘
                                                    │
                                                    ▼
                                              IntakeFilter.Check()
                                                    │
                                            ┌───────┴───────┐
                                            │               │
                                        REJECT          PASS
                                    (log + block)    (save to DB)
```

### 2. Intake Filter Checks

| Category | Examples | Action |
|----------|----------|--------|
| Size | < 50MB | Reject `size_below_threshold` |
| Adult Brand | "Brazzers", "Naughty America" | Reject `adult_brand` |
| Adult Performer | "Lisa Ann", "Riley Reid" | Reject `adult_performer` |
| JAV Code | "ABP-123", "SSIS-456" | Reject `adult_code` |
| CJK Adult | Chinese/Japanese adult terms | Reject `cjk_adult` |
| Explicit | "cum", "fuck", "anal" | Reject `explicit_pattern` |
| Courseware | "Udemy", "Coursera" | Reject `courseware` |

### 3. Smart Hint Classification

```python
def classify_payload(payload):
    # 1. Create torrent object
    torrent = SampledTorrent(name=payload["name"], size=payload["size"])
    
    # 2. Classify content type
    classification = classify_compact_row(torrent, files)
    # Returns: movie, episode, season_pack, anime_episode, reject
    
    # 3. Lookup in canonical layer
    match = canonical_layer.lookup(
        title=torrent.name,
        year=classification.year,
        content_class=classification.content_class,
        season=classification.season
    )
    
    # 4. Decision
    if match and match.tmdb_id:
        return {"reject": False, "tmdbId": match.tmdb_id, "fastPath": True}
    else:
        return {"reject": True, "reason": "no_canonical_match"}
```

### 4. Canonical Layer

The canonical layer is a SQLite database with pre-computed title→TMDB mappings:

```
canonical_title_family (44K entries)
├── family_key: "series␟71914␟tt7462410␟the wheel of time␟2021␟"
├── tmdb_id: "71914"
├── imdb_id: "tt7462410"
├── canonical_title: "the wheel of time"
└── confidence: "exact"

canonical_title_variant (242K entries)
├── family_key: (foreign key)
├── normalized_title: "star wars the bad batch"
├── media_kind: "series"
├── season_bucket: "s03"
└── evidence_count: 15
```

### 5. Promotion to SQLite

```python
def promote_to_sqlite(contents, search_db):
    conn = sqlite3.connect(search_db)
    
    for content in contents:
        # Check if already exists
        cursor.execute("SELECT 1 FROM media_index WHERE info_hash = ?", 
                       (content.info_hash,))
        if cursor.fetchone():
            skipped += 1
            continue
        
        # Insert new entry
        cursor.execute("""
            INSERT INTO media_index 
            (info_hash, name, size, content_type, tmdb_id, ...)
            VALUES (?, ?, ?, ?, ?, ...)
        """, (content.info_hash, content.name, ...))
        inserted += 1
    
    conn.commit()
    return {"inserted": inserted, "skipped": skipped}
```

## Configuration

### Environment Variables (config/bitmagnet.env)

```bash
# Smart Hint (Tier 2)
PROCESSOR_SMART_HINT_ENABLED=true
PROCESSOR_SMART_HINT_SCRIPT=/opt/bitmagnetico/tools/bitmagnet_smart_hint.py

# Canonical Layer (for TMDB lookups)
SEARCH_DB_PATH=/data/canonical.sqlite3

# SQLite Promotion
PROCESSOR_PROMOTION_ENABLED=true
PROCESSOR_PROMOTION_SCRIPT_PATH=/opt/bitmagnetico/scripts/promote_to_sqlite.py
PROCESSOR_PROMOTION_SEARCH_DB=/data/canonical.sqlite3
```

### Volume Mounts (compose.yaml)

```yaml
volumes:
  - ./bitmagnet-media/classifier:/opt/bitmagnetico/tools:ro
  - ./scripts/promote_to_sqlite.py:/opt/bitmagnetico/scripts/promote_to_sqlite.py:ro
  - ./data/comet-fresh/magnetico/active.search.sqlite3:/data/canonical.sqlite3
```

## Verification Commands

```bash
# Check smart hint acceptance
docker logs bitmagnet 2>&1 | grep "applied smart hint"
# Expected: "rejects": 28-31, "hints": 1-4 per batch

# Check promotion activity
docker logs bitmagnet 2>&1 | grep "sqlite promotion"
# Expected: "inserted": 1-3, "errors": 0

# Check new content
docker exec bitmagnet-postgres psql -U postgres -d bitmagnet -c "
  SELECT COUNT(*) FROM torrent_contents WHERE created_at > NOW() - INTERVAL '10 minutes'
"

# Verify canonical layer
docker exec bitmagnet python3 -c "
import sys; sys.path.insert(0, '/opt/bitmagnetico/tools')
from canonical_resolver import CanonicalLayer
print(CanonicalLayer('/data/canonical.sqlite3').stats())
"
```

## Statistics

| Metric | Value |
|--------|-------|
| DHT discovery rate | ~1000 torrents/hour |
| Go intake filter rejection | ~60-70% |
| Python smart hint rejection | ~90-95% of passed |
| Final acceptance | ~5-10% with TMDB |
| PostgreSQL torrents | 484K |
| SQLite entries | 9M+ |
| Canonical layer families | 44K |
| Canonical layer variants | 242K |
