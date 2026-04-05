#!/usr/bin/env python3
"""
V2 Movie ID backfill using exact Radarr matching logic.

Key improvements over v1:
1. Roman/Arabic numeral variants for all lookups
2. Multi-stage matching cascade: main → original → alt → translations
3. Year tolerance (±1 year + secondary_year)
4. Popularity-based tie-breaking

Usage:
    python backfill_movies_v2.py [--limit N] [--dry-run]
"""

import argparse
import re
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib.radarr_parser import (
    clean_movie_title as radarr_clean_title,
    generate_title_variants,
    transliterate_cyrillic,
    has_cyrillic,
)

DB_PATH = (
    Path(__file__).parent.parent
    / "data"
    / "comet-fresh"
    / "magnetico"
    / "active.search.sqlite3"
)
INDEX_PATH = Path(__file__).parent.parent / "data" / "tmdb" / "movie_index.sqlite3"

NOISE_PATTERNS = [
    re.compile(r"\[[^\]]*\]"),
    re.compile(
        r"\([^)]*(?:rus|bg|lat|dub|sub|hddvd|bluray|dvdrip|webrip|web-dl|hdtv|hdrip|audio)[^)]*\)",
        re.I,
    ),
    re.compile(
        r"\b(xvid|divx|x264|x265|h264|h265|hevc|aac|dd5\.?\s?1|5\.1|7\.1|lpcm|dts|ac3|mp3)\b",
        re.I,
    ),
    re.compile(
        r"\b(1080p|720p|480p|2160p|4k|uhd|hdrip|hdtv|webrip|web-dl|bluray|dvdrip|hddvd|open[\W_]?matte|bdremux|hc)\b",
        re.I,
    ),
    re.compile(r"\b\d{2,4}x\d{3,4}\b"),
    re.compile(r"\b\d+[,\.]?\d*\s*(?:mb|gb)\b", re.I),
    re.compile(r"\b(t\d+|e\d+|s\d+)\b", re.I),
    re.compile(
        r"\b(exkinoray|exki|cinecalidad|megapeer|blitzcrieg|satanas|fgt|next|lord|hellywood|galaxyrg|evo|rpg|shadow|hqc|atlas31|kinozal)\b",
        re.I,
    ),
    re.compile(r"\b(bg[\W_]?audio|rus[\W_]?audio|lat[\W_]?audio|mvo|dub)\b", re.I),
]

NOISE_SUFFIX = re.compile(r"\s+(d|l|to|rus|lat|bg|audio)$", re.I)

SERIES_PATTERN = re.compile(
    r"\b[Ss]\d{1,2}\b|\b[Ee]\d{1,3}\b|\bseason\b|\bepisode\b", re.I
)


def clean_movie_key(raw_title: str) -> str:
    result = raw_title
    for pattern in NOISE_PATTERNS:
        result = pattern.sub(" ", result)
    result = re.sub(r"[\._]", " ", result)
    result = re.sub(r"\s+", " ", result)
    result = NOISE_SUFFIX.sub("", result)
    return result.strip()


def find_movie_v2(
    index_conn: sqlite3.Connection, clean: str, year: int | None
) -> dict | None:
    """
    Fast Radarr-style matching with progressive fallback.
    If year is provided, prefers year-constrained matches.
    """
    variants = generate_title_variants(clean)
    placeholders = ",".join("?" * len(variants))

    if year:
        # Phase 1: Main title WITH year (exact)
        match = index_conn.execute(
            f"SELECT * FROM tmdb_movie_index WHERE clean_title IN ({placeholders}) AND year = ? ORDER BY popularity DESC LIMIT 1",
            (*variants, year),
        ).fetchone()
        if match:
            return match

        # Phase 2: Main title WITH year tolerance
        match = index_conn.execute(
            f"SELECT * FROM tmdb_movie_index WHERE clean_title IN ({placeholders}) AND (year IN (?,?,?) OR secondary_year = ?) ORDER BY popularity DESC LIMIT 1",
            (*variants, year - 1, year, year + 1, year),
        ).fetchone()
        if match:
            return match

        # Phase 3: Original title WITH year
        match = index_conn.execute(
            f"SELECT * FROM tmdb_movie_index WHERE clean_original_title IN ({placeholders}) AND (year IN (?,?,?) OR secondary_year = ?) ORDER BY popularity DESC LIMIT 1",
            (*variants, year - 1, year, year + 1, year),
        ).fetchone()
        if match:
            return match

        # Phase 4: Alt titles WITH year
        match = index_conn.execute(
            f"SELECT m.* FROM tmdb_movie_index m JOIN tmdb_alt_titles a ON m.tmdb_id = a.tmdb_id WHERE a.clean_title IN ({placeholders}) AND m.year IN (?,?,?) ORDER BY m.popularity DESC LIMIT 1",
            (*variants, year - 1, year, year + 1),
        ).fetchone()
        if match:
            return match

        # Phase 5: Translations WITH year
        try:
            match = index_conn.execute(
                f"SELECT m.* FROM tmdb_movie_index m JOIN tmdb_translations t ON m.tmdb_id = t.tmdb_id WHERE t.clean_title IN ({placeholders}) AND m.year IN (?,?,?) ORDER BY m.popularity DESC LIMIT 1",
                (*variants, year - 1, year, year + 1),
            ).fetchone()
            if match:
                return match
        except sqlite3.OperationalError:
            pass

    # Phase 6: Main title WITHOUT year (fallback)
    match = index_conn.execute(
        f"SELECT * FROM tmdb_movie_index WHERE clean_title IN ({placeholders}) ORDER BY popularity DESC LIMIT 1",
        (*variants,),
    ).fetchone()
    if match:
        return match

    # Phase 7: Original title WITHOUT year
    match = index_conn.execute(
        f"SELECT * FROM tmdb_movie_index WHERE clean_original_title IN ({placeholders}) ORDER BY popularity DESC LIMIT 1",
        (*variants,),
    ).fetchone()
    if match:
        return match

    # Phase 8: Alt titles WITHOUT year
    match = index_conn.execute(
        f"SELECT m.* FROM tmdb_movie_index m JOIN tmdb_alt_titles a ON m.tmdb_id = a.tmdb_id WHERE a.clean_title IN ({placeholders}) ORDER BY m.popularity DESC LIMIT 1",
        (*variants,),
    ).fetchone()
    if match:
        return match

    # Phase 9: Translations WITHOUT year
    try:
        match = index_conn.execute(
            f"SELECT m.* FROM tmdb_movie_index m JOIN tmdb_translations t ON m.tmdb_id = t.tmdb_id WHERE t.clean_title IN ({placeholders}) ORDER BY m.popularity DESC LIMIT 1",
            (*variants,),
        ).fetchone()
    except sqlite3.OperationalError:
        pass

    return match


def run(limit: int = 0, dry_run: bool = False, batch_size: int = 5000):
    print("V2 Movie ID Backfill (Radarr-Exact Matching)", flush=True)
    print("=" * 50, flush=True)
    print(f"  Database: {DB_PATH}", flush=True)
    print(f"  TMDB Index: {INDEX_PATH}", flush=True)
    print(f"  Dry run: {dry_run}", flush=True)
    print(f"  Limit: {limit if limit else 'none'}", flush=True)
    print(f"  Batch size: {batch_size}", flush=True)
    print(flush=True)

    if not INDEX_PATH.exists():
        print(f"ERROR: TMDB index not found at {INDEX_PATH}", flush=True)
        return

    print("Opening databases...", flush=True)
    index_conn = sqlite3.connect(str(INDEX_PATH))
    index_conn.row_factory = sqlite3.Row

    db_conn = sqlite3.connect(str(DB_PATH), timeout=120)
    db_conn.row_factory = sqlite3.Row
    db_conn.execute("PRAGMA journal_mode=WAL")

    print("Processing movies...", flush=True)

    stats = Counter()
    start_time = time.time()
    last_id = 0
    limit_counter = 0

    while True:
        if limit and limit_counter >= limit:
            break

        actual_batch = min(batch_size, limit - limit_counter) if limit else batch_size

        query = """
            SELECT torrent_id, movie_key
            FROM media_index
            WHERE content_class = 'movie'
              AND movie_key IS NOT NULL AND movie_key != ''
              AND (tmdb_id IS NULL OR tmdb_id = '')
              AND torrent_id > ?
            ORDER BY torrent_id
            LIMIT ?
        """

        rows = db_conn.execute(query, (last_id, actual_batch)).fetchall()
        if not rows:
            break

        last_id = rows[-1]["torrent_id"]
        limit_counter += len(rows)

        for row in rows:
            key = row["movie_key"]
            torrent_id = row["torrent_id"]

            if SERIES_PATTERN.search(key):
                stats["skipped_series"] += 1
                continue

            if "|" in key:
                parts = key.split("|")
                raw_title = parts[0].strip()
                year = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
            else:
                raw_title = key
                year_match = re.search(r"\b((?:19|20)\d{2})\b", key)
                year = int(year_match.group(1)) if year_match else None

            if has_cyrillic(raw_title):
                raw_title = transliterate_cyrillic(raw_title)

            cleaned_title = clean_movie_key(raw_title)
            clean = radarr_clean_title(cleaned_title)

            if not clean or len(clean) < 2:
                stats["skipped_short"] += 1
                continue

            match = find_movie_v2(index_conn, clean, year)

            if match:
                stats["resolved"] += 1
                if not dry_run:
                    db_conn.execute(
                        "UPDATE media_index SET tmdb_id = ?, imdb_id = ? WHERE torrent_id = ?",
                        (str(match["tmdb_id"]), match["imdb_id"], torrent_id),
                    )
            else:
                stats["miss"] += 1

            stats["processed"] += 1

        if not dry_run:
            db_conn.commit()

        elapsed = time.time() - start_time
        rate = stats["processed"] / elapsed if elapsed > 0 else 0
        total_valid = (
            stats["processed"] - stats["skipped_series"] - stats["skipped_short"]
        )
        coverage = stats["resolved"] / max(total_valid, 1) * 100
        print(
            f"  Progress: {stats['processed']:,} processed, resolved={stats['resolved']:,} ({coverage:.1f}%) "
            f"rate={rate:.0f}/s last_id={last_id}",
            flush=True,
        )

    elapsed = time.time() - start_time
    print(flush=True)
    print("Backfill complete!", flush=True)
    print(f"  Processed: {stats['processed']:,}", flush=True)
    print(f"  Resolved: {stats['resolved']:,}", flush=True)
    print(f"  Miss: {stats['miss']:,}", flush=True)
    print(f"  Skipped (series): {stats['skipped_series']:,}", flush=True)
    print(f"  Skipped (short): {stats['skipped_short']:,}", flush=True)
    print(f"  Time: {elapsed / 60:.1f} minutes", flush=True)
    print(f"  Rate: {stats['processed'] / elapsed:.0f} movies/sec", flush=True)

    total_valid = stats["processed"] - stats["skipped_series"] - stats["skipped_short"]
    coverage = stats["resolved"] / max(total_valid, 1) * 100
    print(f"  Coverage: {coverage:.1f}%", flush=True)

    index_conn.close()
    db_conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="V2 movie ID backfill with Radarr-exact matching"
    )
    parser.add_argument(
        "--limit", type=int, default=0, help="Limit number of movies (0 = all)"
    )
    parser.add_argument("--dry-run", action="store_true", help="Don't write changes")
    parser.add_argument(
        "--batch-size", type=int, default=5000, help="Batch size for processing"
    )
    args = parser.parse_args()

    run(limit=args.limit, dry_run=args.dry_run, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
