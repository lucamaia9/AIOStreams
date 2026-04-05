# Live DHT Enrichment Implementation

## Summary

This document describes the implementation of live DHT enrichment for movies, series, and anime, bringing parity between the historical backfill system and the live DHT ingestion pipeline.

> Historical reference only on this host: bulk ID enrichment now uses the staged SQLite workflow in `BACKFILL_PROCEDURE.md`, and the old backfill/timer assumptions are not active here.

## What Was Implemented

### 1. Canonical Resolver Library (`scripts/lib/canonical_resolver.py`)

A shared library that provides:
- Title normalization (removes quality tags, release group tags, etc.)
- Canonical layer lookups (instant, no API calls)
- Batch lookup support

**Usage:**
```python
from canonical_resolver import CanonicalLayer

layer = CanonicalLayer()
match = layer.lookup("Breaking Bad", year=None, content_class="episode")
if match.matched:
    print(f"tmdb_id={match.tmdb_id}, imdb_id={match.imdb_id}")
```

**Stats:**
- 43,993 title families
- 242,252 title variants
- 99.99% have TMDB IDs
- 91.5% have IMDB IDs

### 2. Extended Smart Hint (`classifier/shared_bitmagnet_bridge.py`)

Modified to:
- Check canonical layer before classification
- Include `tmdbId` and `imdbId` in `importHints` if found
- These IDs flow to the Go processor via the existing smart_hint interface

**Result:**
- Series with canonical hits resolve instantly (<1ms)
- No TMDB API calls needed for known titles
- Breaking Bad example: tmdb_id=1396, imdb_id=tt0903747 resolved from canonical layer

### 3. Enrichment Worker Daemon (`scripts/live_enrichment_worker.py`)

A standalone daemon that:
- Polls Postgres for unenriched torrent_contents
- Checks canonical layer first (instant)
- Batches remaining titles for TMDB resolution
- Updates Postgres and SQLite

**Usage:**
```bash
# Historical reference only for this host's bulk enrichment workflow.
# Dry run (no changes)
python3 scripts/live_enrichment_worker.py --dry-run --verbose --once

# Production mode
python3 scripts/live_enrichment_worker.py --verbose

# With options
python3 scripts/live_enrichment_worker.py \
    --batch-size 100 \
    --batch-window-seconds 30 \
    --poll-interval 10 \
    --verbose
```

**Current State:**
- 706,372 unenriched torrents in Postgres
- Worker can process in dry-run mode
- TMDB API integration ready (needs API key)

## What Remains

### 1. Anime TMDB Extraction (`comet/services/anime.py`)

**Status:** Needs manual edit due to complex indentation

**Required Change:**
In `_persist_mapping()` function (around line 532), modify the fribb processing:

```python
# Original (line 532-550):
fribb_batch = []
for entry in fribb_list:
    imdb_id = entry.get("imdb_id")
    if not imdb_id:
        continue
    # ... rest of processing

# Change to:
fribb_batch = []
for entry in fribb_list:
    imdb_id = entry.get("imdb_id")
    tmdb_id = entry.get("themoviedb_id")  # NEW
    if not imdb_id and not tmdb_id:  # MODIFIED
        continue
    # ... inside the provider loop:
    if found_entry_id is not None:
        if imdb_id:  # NEW
            fribb_batch.append({
                "provider": "imdb",
                "provider_id": imdb_id,
                "entry_id": found_entry_id,
            })
        if tmdb_id:  # NEW
            fribb_batch.append({
                "provider": "tmdb",
                "provider_id": str(tmdb_id),
                "entry_id": found_entry_id,
            })
        break
```

**Also add new methods:**
```python
async def get_tmdb_from_kitsu(self, kitsu_id: str | int) -> str | None:
    """Get TMDB ID from Kitsu ID."""
    if not self.loaded:
        return None
    tmdb_id = await database.fetch_val(
        """SELECT i2.provider_id
           FROM anime_ids i1
           JOIN anime_ids i2 ON i1.entry_id = i2.entry_id
           WHERE i1.provider = 'kitsu' AND i1.provider_id = :kitsu_id
           AND i2.provider = 'tmdb' LIMIT 1""",
        {"kitsu_id": str(kitsu_id)},
    )
    return tmdb_id

async def get_tmdb_from_imdb(self, imdb_id: str | int) -> str | None:
    """Get TMDB ID from IMDB ID."""
    if not self.loaded:
        return None
    tmdb_id = await database.fetch_val(
        """SELECT i2.provider_id
           FROM anime_ids i1
           JOIN anime_ids i2 ON i1.entry_id = i2.entry_id
           WHERE i1.provider = 'imdb' AND i1.provider_id = :imdb_id
           AND i2.provider = 'tmdb' LIMIT 1""",
        {"imdb_id": str(imdb_id)},
    )
    return tmdb_id
```

### 2. Promotion Script Enhancement

The `promote_bitmagnet_live_to_compact.py` already extracts IDs from Postgres. Consider adding:
- Fallback enrichment if IDs are missing
- Update canonical layer on resolution

### 3. End-to-End Testing

Once anime.py is updated:
1. Restart Comet to reload anime mappings
2. Test Kitsu requests resolve TMDB IDs
3. Verify enrichment worker processes 706K pending torrents
4. Check SQLite search results include newly enriched IDs

## Architecture Flow

```
DHT Crawler
    │
    ▼
Go Processor (existing)
    │
    ├──▶ Smart Hint (Python)
    │    ├── Classify (accept/reject)
    │    └── Canonical Lookup (NEW) ──▶ tmdbId/imdbId in importHints
    │
    ├──▶ Go Classifier (uses importHints if provided)
    │    └── TMDB API (fallback)
    │
    └──▶ Persist to Postgres
         └── torrent_contents (with IDs if found)
              │
              ▼
         Enrichment Worker (new daemon)
              │
              ├── Canonical Layer Lookup (instant)
              │
              └── TMDB API (batched, rate-limited)
                   │
                   └── Update Postgres + SQLite
```

## Files Modified

| File | Changes |
|------|---------|
| `scripts/lib/canonical_resolver.py` | NEW - Shared canonical layer library |
| `classifier/shared_bitmagnet_bridge.py` | Added canonical lookup in importHints |
| `scripts/live_enrichment_worker.py` | NEW - Enrichment daemon |
| `comet/services/anime.py` | PENDING - TMDB extraction (needs manual edit) |

## Smoke Tests Passed

1. ✓ Canonical resolver loads and returns correct stats
2. ✓ Breaking Bad lookup returns tmdb_id=1396
3. ✓ Smart hint includes tmdbId/imdbId in importHints
4. ✓ Enrichment worker runs in dry-run mode
5. ✓ 706K pending torrents detected in Postgres

## Next Steps

Historical implementation follow-ups captured at the time:

1. Manually edit `comet/services/anime.py` to add TMDB extraction
2. Restart Comet container to apply changes
3. Validate the worker only as a historical/live-enrichment component, not as the host's bulk backfill path
4. Monitor enrichment progress via logs
5. Verify search results include enriched IDs
