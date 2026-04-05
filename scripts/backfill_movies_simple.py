#!/usr/bin/env python3
"""
Simple synchronous movie ID backfill using local TMDB index.
Fast local-only matching without API fallback.

Usage:
    python backfill_movies_simple.py [--limit N] [--dry-run]
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
    parse_movie_title,
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
        r"\b(exkinoray|exki|cinecalidad|megapeer|blitzcrieg|satanas|fgt|next|lord|hellywood|galaxyrg|evo|rpg|shadow|hqc|hqc|atlas31|kinozal)\b",
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


def normalize_title(title: str) -> str:
    return re.sub(r"[\W_]+", "", title.lower())


def find_movie(
    index_conn: sqlite3.Connection, clean: str, year: int | None
) -> dict | None:
    if year:
        match = index_conn.execute(
            "SELECT * FROM tmdb_movie_index WHERE clean_title = ? AND year = ? LIMIT 1",
            (clean, year),
        ).fetchone()
        if match:
            return match

    match = index_conn.execute(
        "SELECT * FROM tmdb_movie_index WHERE clean_title = ? ORDER BY popularity DESC LIMIT 1",
        (clean,),
    ).fetchone()
    if match:
        return match

    match = index_conn.execute(
        "SELECT * FROM tmdb_movie_index WHERE clean_original_title = ? ORDER BY popularity DESC LIMIT 1",
        (clean,),
    ).fetchone()
    if match:
        return match

    match = index_conn.execute(
        "SELECT m.* FROM tmdb_movie_index m "
        "JOIN tmdb_alt_titles a ON m.tmdb_id = a.tmdb_id "
        "WHERE a.clean_title = ? ORDER BY m.popularity DESC LIMIT 1",
        (clean,),
    ).fetchone()

    return match


def run(limit: int = 0, dry_run: bool = False, batch_size: int = 1000):
    print("Simple Movie ID Backfill (Local Index Only)", flush=True)
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

        actual_batch = min(batch_size, limit - limit_counter) if limit else batch_size
        rows = db_conn.execute(query, (last_id, actual_batch)).fetchall()
        if not rows:
            break

        last_id = rows[-1]["torrent_id"]
        limit_counter += len(rows)

        batch_resolved = 0
        for row in rows:
            key = row["movie_key"]
            torrent_id = row["torrent_id"]

            if SERIES_PATTERN.search(key):
                stats["skipped_series"] += 1
                continue

            # Extract title and year from key
            if "|" in key:
                parts = key.split("|")
                raw_title = parts[0].strip()
                year = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
            else:
                raw_title = key
                year_match = re.search(r"\b((?:19|20)\d{2})\b", key)
                year = int(year_match.group(1)) if year_match else None

            # Transliterate Cyrillic if present
            if has_cyrillic(raw_title):
                raw_title = transliterate_cyrillic(raw_title)

            # Remove noise (release groups, codecs, etc.) then apply Radarr normalization
            cleaned_title = clean_movie_key(raw_title)
            clean = radarr_clean_title(cleaned_title)

            if not clean or len(clean) < 2:
                stats["skipped_short"] += 1
                continue

            # Try with Roman/Arabic variants
            variants = generate_title_variants(clean)
            match = None
            for variant in variants:
                match = find_movie(index_conn, variant, year)
                if match:
                    break

            if match:
                stats["resolved"] += 1
                batch_resolved += 1
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
    parser = argparse.ArgumentParser(description="Simple movie ID backfill")
    parser.add_argument(
        "--limit", type=int, default=0, help="Limit number of movies (0 = all)"
    )
    parser.add_argument("--dry-run", action="store_true", help="Don't write changes")
    parser.add_argument(
        "--batch-size", type=int, default=10000, help="Batch size for processing"
    )
    args = parser.parse_args()

    run(limit=args.limit, dry_run=args.dry_run, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
