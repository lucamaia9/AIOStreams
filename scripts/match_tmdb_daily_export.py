#!/usr/bin/env python3
"""
Match historical movie torrents against TMDB daily ID exports.

Downloads TMDB's free daily export (no API key needed), builds an in-memory
index, and matches against unresolved movie_key entries in the compact SQLite DB.

Usage:
    python match_tmdb_daily_export.py [--dry-run] [--enrich-imdb] [--batch-size N]

Workflow:
    1. Download daily export (~24MB compressed, 1.1M movies)
    2. Stream parse JSONL, build in-memory index {clean_title: [(tmdb_id, year, popularity)]}
    3. Query SQLite for unique movie_keys without IDs
    4. Match each movie_key against the index
    5. Batch update SQLite (10K rows per transaction)
    6. Optional: enrich matched titles with TMDB API for imdb_id
    7. Clean up (delete compressed file)

Storage: Never stores raw export file. Only lightweight cache table.
"""

import argparse
import gzip
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.request import urlretrieve

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / "bitmagnet-media" / "classifier"))
from shared_title_normalizer import normalize_for_movie_matching

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TMDB_EXPORT_URL = (
    "https://files.tmdb.org/p/exports/movie_ids_{month:02d}_{day:02d}_{year}.json.gz"
)
TMDB_API_BASE = "https://api.themoviedb.org/3"
TMDB_API_TOKEN = os.environ.get("TMDB_API_TOKEN", "")

SQLITE_DB = (
    Path(__file__).parent.parent
    / "data"
    / "comet-fresh"
    / "magnetico"
    / "active.search.sqlite3"
)

BATCH_SIZE = 10000
PROGRESS_INTERVAL = 5000
ENRICH_POPULARITY_THRESHOLD = 10.0
ENRICH_BATCH_SIZE = 100

# ---------------------------------------------------------------------------
# TMDB Daily Export Parser
# ---------------------------------------------------------------------------


def download_daily_export(output_path: Path) -> Path:
    """Download today's TMDB movie daily export."""
    now = datetime.utcnow()
    url = TMDB_EXPORT_URL.format(month=now.month, day=now.day, year=now.year)

    print(f"  Downloading: {url}")
    compressed_path = (
        output_path / f"movie_ids_{now.month:02d}_{now.day:02d}_{now.year}.json.gz"
    )

    try:
        urlretrieve(url, compressed_path)
        size_mb = compressed_path.stat().st_size / (1024 * 1024)
        print(f"  Downloaded: {size_mb:.1f} MB")
        return compressed_path
    except Exception as e:
        # Try yesterday's export if today's isn't ready yet
        from datetime import timedelta

        yesterday = now - timedelta(days=1)
        url = TMDB_EXPORT_URL.format(
            month=yesterday.month, day=yesterday.day, year=yesterday.year
        )
        print(f"  Today's export not ready, trying yesterday: {url}")
        compressed_path = (
            output_path
            / f"movie_ids_{yesterday.month:02d}_{yesterday.day:02d}_{yesterday.year}.json.gz"
        )
        urlretrieve(url, compressed_path)
        size_mb = compressed_path.stat().st_size / (1024 * 1024)
        print(f"  Downloaded: {size_mb:.1f} MB")
        return compressed_path


def build_tmdb_index(compressed_path: Path) -> dict:
    """
    Stream parse the gzipped JSONL export and build an in-memory index.

    Returns: {movie_key_style_title: [(tmdb_id, year, popularity)]}
    The key format matches the movie_key format in SQLite (lowercase, space-separated,
    quality tags stripped).
    """
    index: dict[str, list[tuple[int, Optional[int], float]]] = {}
    total = 0
    skipped_adult = 0
    skipped_video = 0

    print(f"  Building index from: {compressed_path.name}")

    # Patterns to strip from titles to match movie_key format
    title_clean = re.compile(
        r"\[[^\]]*\]|\([^\)]*\)|\{[^\}]*\}|"
        r"\b(?:2160p|1440p|1080p|1080i|720p|576p|480p|360p|4k|8k|uhd|"
        r"hdr10?\+?|hdr|dv|dolby[\s.-]?vision|"
        r"bluray|blu[- ]?ray|brrip|bdrip|webrip|web[- ]?dl|web|hdtv|dvdrip|hdrip|remux|"
        r"x264|x265|h264|h265|hevc|avc|"
        r"aac|dts|truehd|atmos|10bit|8bit|"
        r"ddp[\s.-]?\d\.\d|ac3[\s.-]?\d\.\d|eac3|opus|flac|mp3|"
        r"proper|repack|internal|dual[\s.-]?audio|dubbed|subbed|"
        r"mkv|mp4|avi|wmv|mov|webm|ts|m4v|mpeg|mpg|"
        r"s\d{1,2}e\d{1,3}(?:[ ._\-]*e?\d{1,3})*|"
        r"\d{1,2}x\d{1,3}(?:[ ._\-]*(?:x|e)?\d{1,3})*|"
        r"season\s*\d{1,2}|episode\s*\d{1,3}|ep(?:isode)?\s*\d{1,3}|"
        r"part\s*\d{1,3}|complete\s*(?:season|series)|"
        r"yts|rarbg|ettv|tgx|ion10|exki|exkinoray|prof|bitrip|"
        r"blitzcrieg|memento|galaxyrg|eztv|rartv|psa|joy|hevcbay|pahe|"
        r"x0r|mkvking|shadow|vyndros|ohys[- ]?raws|ohys|subsplease|"
        r"erai[- ]?raws|erai|judas|sam|lostfilm|newstudio|galaxytv|"
        r"megusta|torrentgalaxy|playweb|kovalski|d0ct0rlew|ezzrips|"
        r"killers|bae|msd|ion265|coldfilm)\b",
        re.IGNORECASE,
    )
    multispace = re.compile(r"\s+")

    with gzip.open(compressed_path, "rt", encoding="utf-8") as f:
        for line in f:
            total += 1
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            if entry.get("adult"):
                skipped_adult += 1
                continue
            if entry.get("video"):
                skipped_video += 1
                continue

            tmdb_id = entry["id"]
            original_title = entry.get("original_title", "")
            popularity = entry.get("popularity", 0.0)

            # Extract year from title if present
            year_match = re.search(r"\b(19\d{2}|20\d{2})\b", original_title)
            year = int(year_match.group(1)) if year_match else None

            # Normalize title to match movie_key format
            cleaned = title_clean.sub(" ", original_title)
            cleaned = re.sub(r"[^\w\s-]", " ", cleaned, flags=re.UNICODE)
            cleaned = multispace.sub(" ", cleaned).strip().lower()
            if not cleaned:
                continue

            if cleaned not in index:
                index[cleaned] = []
            index[cleaned].append((tmdb_id, year, popularity))

    print(f"  Index built: {len(index):,} unique titles from {total:,} entries")
    print(f"  Skipped: {skipped_adult:,} adult, {skipped_video:,} video")
    return index


# ---------------------------------------------------------------------------
# SQLite Matching
# ---------------------------------------------------------------------------


def get_unresolved_movie_keys(conn: sqlite3.Connection) -> list[tuple[str, int]]:
    """Get unique movie_keys without IDs, ordered by row count (highest first)."""
    return conn.execute(
        """
        SELECT movie_key, COUNT(*) as cnt
        FROM media_index
        WHERE content_class = 'movie'
          AND movie_key IS NOT NULL
          AND movie_key != ''
          AND (imdb_id IS NULL OR imdb_id = '')
          AND (tmdb_id IS NULL OR tmdb_id = '')
        GROUP BY movie_key
        ORDER BY cnt DESC
        """
    ).fetchall()


def match_movie_key(
    movie_key: str,
    tmdb_index: dict,
) -> Optional[tuple[int, Optional[int], float]]:
    """
    Match a movie_key against the TMDB index.

    movie_key is already normalized (lowercase, space-separated, quality stripped).
    The TMDB index is built with the same normalization.
    """
    # movie_key may contain year like "the matrix 1999" - strip it for matching
    key_for_match = re.sub(r"\b(19\d{2}|20\d{2})\b", "", movie_key).strip()
    if not key_for_match:
        key_for_match = movie_key

    candidates = tmdb_index.get(key_for_match)
    if not candidates:
        return None

    # Pick highest popularity match
    best = max(candidates, key=lambda x: x[2])
    return best


def batch_update_sqlite(
    conn: sqlite3.Connection,
    updates: list[tuple[str, int, Optional[int]]],
    dry_run: bool = False,
) -> int:
    """
    Batch update SQLite with matched TMDB IDs.

    updates: [(name, tmdb_id, year), ...]
    Returns: number of rows updated
    """
    if not updates:
        return 0

    if dry_run:
        return len(updates)

    cursor = conn.cursor()
    total_updated = 0

    for item in updates:
        key = item[0]
        tmdb_id = item[1]
        year = item[2]
        cursor.execute(
            """
            UPDATE media_index
            SET tmdb_id = ?, year = ?
            WHERE movie_key = ?
              AND (tmdb_id IS NULL OR tmdb_id = '')
            """,
            (str(tmdb_id), str(year) if year else None, key),
        )
        total_updated += cursor.rowcount

    return total_updated


# ---------------------------------------------------------------------------
# Optional IMDB Enrichment
# ---------------------------------------------------------------------------


def enrich_with_imdb_api(
    conn: sqlite3.Connection,
    tmdb_ids: list[int],
    api_token: str,
    dry_run: bool = False,
) -> int:
    """Enrich matched TMDB IDs with IMDB IDs via API."""
    if not api_token:
        print("  No TMDB_API_TOKEN set, skipping IMDB enrichment")
        return 0

    import aiohttp
    import asyncio

    async def fetch_imdb_id(
        session: aiohttp.ClientSession, tmdb_id: int
    ) -> Optional[str]:
        url = f"{TMDB_API_BASE}/movie/{tmdb_id}"
        params = {"append_to_response": "external_ids"}
        headers = {"Authorization": f"Bearer {api_token}"}
        try:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("external_ids", {}).get("imdb_id")
        except Exception:
            pass
        return None

    async def enrich_batch(tmdb_ids: list[int]) -> dict[int, Optional[str]]:
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=40)
        async with aiohttp.ClientSession(
            timeout=timeout, connector=connector
        ) as session:
            semaphore = asyncio.Semaphore(40)

            async def fetch_with_limit(tmdb_id: int) -> tuple[int, Optional[str]]:
                async with semaphore:
                    imdb_id = await fetch_imdb_id(session, tmdb_id)
                    return tmdb_id, imdb_id

            results = await asyncio.gather(*(fetch_with_limit(tid) for tid in tmdb_ids))
            return dict(results)

    enriched = 0
    for i in range(0, len(tmdb_ids), ENRICH_BATCH_SIZE):
        batch = tmdb_ids[i : i + ENRICH_BATCH_SIZE]
        results = asyncio.run(enrich_batch(batch))

        if not dry_run:
            for tmdb_id, imdb_id in results.items():
                if imdb_id:
                    conn.execute(
                        "UPDATE media_index SET imdb_id = ? WHERE tmdb_id = ? AND (imdb_id IS NULL OR imdb_id = '')",
                        (imdb_id, str(tmdb_id)),
                    )
                    enriched += conn.rowcount

        if not dry_run:
            conn.commit()

        print(f"  Enriched batch {i // ENRICH_BATCH_SIZE + 1}: {len(batch)} titles")
        time.sleep(0.5)  # Rate limit

    return enriched


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Match movies against TMDB daily exports"
    )
    parser.add_argument("--dry-run", action="store_true", help="Don't update SQLite")
    parser.add_argument(
        "--enrich-imdb", action="store_true", help="Enrich with IMDB IDs via API"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help="Batch size for SQLite updates",
    )
    args = parser.parse_args()

    start_time = time.time()
    print("=" * 60)
    print("TMDB Daily Export Movie Matching")
    print("=" * 60)

    # Step 1: Download daily export
    print("\n[1/5] Downloading TMDB daily export...")
    tmp_dir = Path("/tmp/tmdb-daily")
    tmp_dir.mkdir(exist_ok=True)
    compressed_path = download_daily_export(tmp_dir)

    # Step 2: Build in-memory index
    print("\n[2/5] Building TMDB index...")
    tmdb_index = build_tmdb_index(compressed_path)

    # Step 3: Connect to SQLite
    print("\n[3/5] Connecting to SQLite...")
    conn = sqlite3.connect(str(SQLITE_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Step 4: Match and update
    print("\n[4/5] Matching movie keys...")
    movie_keys = get_unresolved_movie_keys(conn)
    print(f"  Found {len(movie_keys):,} unique movie_keys without IDs")

    matched = 0
    unmatched = 0
    updates = []
    matched_tmdb_ids = []

    for i, (movie_key, count) in enumerate(movie_keys, 1):
        result = match_movie_key(movie_key, tmdb_index)
        if result:
            tmdb_id, year, popularity = result
            updates.append((movie_key, tmdb_id, year))
            matched += 1
            if popularity >= ENRICH_POPULARITY_THRESHOLD:
                matched_tmdb_ids.append(tmdb_id)

            if i % PROGRESS_INTERVAL == 0:
                print(
                    f"  Progress: {i:,}/{len(movie_keys):,} ({matched:,} matched, {unmatched:,} unmatched)"
                )
        else:
            unmatched += 1

        # Batch update
        if len(updates) >= args.batch_size:
            updated = batch_update_sqlite(conn, updates, args.dry_run)
            if not args.dry_run:
                conn.commit()
            print(f"  Batch update: {updated:,} rows updated")
            updates = []

    # Final batch
    if updates:
        updated = batch_update_sqlite(conn, updates, args.dry_run)
        if not args.dry_run:
            conn.commit()
        print(f"  Final batch update: {updated:,} rows updated")

    print(f"\n  Results: {matched:,} matched, {unmatched:,} unmatched")
    print(f"  Match rate: {100.0 * matched / max(matched + unmatched, 1):.1f}%")

    # Step 5: Optional IMDB enrichment
    if args.enrich_imdb and matched_tmdb_ids:
        print(f"\n[5/5] Enriching {len(matched_tmdb_ids):,} TMDB IDs with IMDB...")
        unique_tmdb_ids = list(set(matched_tmdb_ids))
        enriched = enrich_with_imdb_api(
            conn, unique_tmdb_ids, TMDB_API_TOKEN, args.dry_run
        )
        print(f"  Enriched: {enriched:,} IMDB IDs")

    # Cleanup
    print("\n[Cleanup] Removing temporary files...")
    compressed_path.unlink(missing_ok=True)
    print(f"  Deleted: {compressed_path.name}")

    conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"Completed in {elapsed:.0f}s ({elapsed / 60:.1f} minutes)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
