# BitMagnet Smart Hint Fix Plan

## Issue Summary

**Root Cause Found:** The `SEARCH_DB_PATH` environment variable was pointing to a non-existent database path (`/data/magnetico/active.search.sqlite3`), causing the canonical layer lookup to fail. This resulted in 100% rejection rate because no TMDB matches could be resolved.

**Fix Applied:** Changed `SEARCH_DB_PATH` to point to the correct canonical database at `/data/canonical.sqlite3`.

## Current State

| Component | Status |
|-----------|--------|
| Go Intake Filter (Tier 1) | Working - rejects adult/spam at DHT level |
| Python Smart Hint (Tier 2) | Working - accepts ~3-5% with TMDB matches |
| SQLite Promotion | Working - content flowing to SQLite |
| Canonical Layer | Working - 43,993 families, 242,252 variants |

## Architecture

```
DHT Network
    ↓
Go Intake Filter (Tier 1)
    ├── Rejects: adult_codes, adult_brands, adult_performers, explicit_patterns, size_below_threshold
    └── Passes: ~30-40% of DHT content
    ↓
Python Smart Hint (Tier 2)
    ├── Classifies: movie, episode, season_pack, anime, etc.
    ├── Canonical Lookup: Searches for TMDB/IMDB match
    ├── Rejects: Content without TMDB/IMDB ID (useless for Comet)
    └── Passes: ~3-5% with TMDB matches
    ↓
BitMagnet Classifier
    ├── Enriches with TMDB metadata
    └── Creates torrent_contents
    ↓
SQLite Promotion
    ├── Checks: content_source AND content_id present
    └── Promotes: To SQLite canonical layer
```

## Go vs Python Consistency

### Purpose Difference

| Filter | Purpose | Action |
|--------|---------|--------|
| Go Intake Filter | Block obvious junk at DHT level | Reject adult, spam, courseware |
| Python Smart Hint | Ensure TMDB match for Comet | Reject content without canonical ID |

**They serve DIFFERENT purposes - 100% agreement is NOT expected.**

### Test Results (1000 random torrents)

```
Total: 1000
Agree: 212 (21.2%)
  - Both accept: 212 (TV shows with TMDB matches)
  - Both reject: 0
Disagree: 788
  - Go accepts, Python rejects: 788 (no TMDB match - expected)
  - Go rejects, Python accepts: 0
```

This is **correct behavior**. Go blocks bad content, Python ensures only TMDB-matched content enters the database.

## Long-Term Maintenance

### 1. Canonical Layer Growth

The canonical layer needs to grow to accept more content. Options:
- Backfill from TMDB API periodically
- Add new titles to canonical_title_family/variant tables
- Improve fuzzy matching in canonical_resolver.py

### 2. Monitoring

Add health checks for:
- Canonical layer database accessibility
- Smart hint acceptance rate (should be >1%)
- Promotion success rate

### 3. Pattern Synchronization

The Go intake_filter patterns should be updated when Python classifier adds new adult patterns:
- `internal/dhtcrawler/patterns_gen.go` - Go patterns
- `shared_adult_title_classifier.py` - Python patterns

### 4. Configuration

Ensure these environment variables are correct:
```
SEARCH_DB_PATH=/data/canonical.sqlite3
PROCESSOR_SMART_HINT_ENABLED=true
PROCESSOR_PROMOTION_ENABLED=true
PROCESSOR_PROMOTION_SEARCH_DB=/data/canonical.sqlite3
```

## Verification Steps

1. Check smart hint logs for non-zero hints:
   ```
   docker logs bitmagnet | grep "applied smart hint"
   ```

2. Check promotion logs:
   ```
   docker logs bitmagnet | grep "sqlite promotion"
   ```

3. Verify canonical layer:
   ```
   docker exec bitmagnet python3 -c "
   import sys; sys.path.insert(0, '/opt/bitmagnetico/tools')
   from canonical_resolver import CanonicalLayer
   layer = CanonicalLayer('/data/canonical.sqlite3')
   print(layer.stats())
   "
   ```

## Files Modified

- `/home/ubuntu/aiostreams/config/bitmagnet.env` - Fixed SEARCH_DB_PATH
- `/home/ubuntu/aiostreams/compose.yaml` - Added promotion script mount, fixed sqlite mount

## Commit History

- `6233ff6` - feat: add Go intake filter and SQLite promotion
