# Magnetico Search Master Plan

## Goal

Build a fast, reliable local search layer for the cleaned Magnetico media corpus without depending on BitMagnet's thin normalized `content` layer.

The authoritative rule is:

- Magnetico filtering/export logic decides what media is worth keeping.
- Comet owns retrieval, matching, RD checks, and ranking.
- BitMagnet remains the background storage and DHT enrichment layer.

## Target Architecture

### 1. Raw approved corpus

Keep the cleaned Magnetico-derived corpus as the large approved source of media torrents.

### 2. Lean authoritative search index inside Comet Postgres

Build a torrent-centric search index in Comet Postgres for:

- movies
- exact season/episode series matches

This index is intentionally larger than any normalized metadata layer and should remain searchable even when metadata mapping is incomplete.

Minimum indexed fields:

- `info_hash`
- `name`
- `size`
- `published_at`
- `source`
- `media_kind`
- `parsed_title`
- `normalized_title`
- `year`
- `season`
- `episode`
- `is_complete`

### 3. Metadata mapping stays auxiliary

IMDb/TMDB/alias metadata helps precision and bad-ID repair, but it does not define the full searchable universe.

### 4. BitMagnet keeps enriching in the background

BitMagnet remains useful for:

- DHT crawling
- long-term torrent accumulation
- future incremental promotion into the authoritative local search corpus

BitMagnet is not the authority for media selection.

## First Milestone

### Scope

- movies
- series exact season/episode matching

### Non-goals

- pack-first retrieval
- trusting BitMagnet classification as the main media gate
- default live integration before shadow validation

## Retrieval Principles

Reuse stable patterns from existing projects:

- movies: title + year aware search
- series: title + season + episode exact matching
- proven parser behavior where possible
- Comet keeps matching and ranking local

## Rollout

1. Build the local search index from the cleaned Magnetico corpus.
2. Query it in shadow mode on hard titles.
3. Compare against current BitMagnet retrieval and live Comet results.
4. Only then A/B it into live search.

## Current First Slice

Implemented foundation:

- `magnetico_search_index` table in Comet Postgres
- `magnetico_search_builds` tracking table
- resumable builder script using Comet's own RTN parser
- local query tool for shadow validation

Scripts:

- `scripts/run_magnetico_search_index_build.sh`
- `scripts/query_magnetico_search_index.sh`

In-container tools:

- `cometouglas/scripts/build_magnetico_search_index.py`
- `cometouglas/scripts/query_magnetico_search_index.py`

## Operational Rule Going Forward

If import quality needs improvement:

- fix the Magnetico exporter/filter pipeline
- do not try to solve media quality by trusting BitMagnet classification after import
