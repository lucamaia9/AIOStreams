#!/usr/bin/env python3
"""
V3 Movie ID backfill - Optimized with in-memory TMDB index.

Architecture:
1. Load entire TMDB index into memory (110MB, ~2s)
2. For each candidate, do O(1) dict lookups
3. Apply Radarr-exact matching cascade
4. Batch write updates

Usage:
    python backfill_movies_v3.py [--limit N] [--dry-run]
"""

import argparse
import re
import sqlite3
import sys
import time
from collections import Counter, defaultdict
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


class TMDBMemoryIndex:
    """
    In-memory TMDB index for O(1) lookups.
    Implements Radarr's exact matching cascade.
    """

    def __init__(self, index_path: Path):
        self.movies_by_clean = defaultdict(
            list
        )  # clean_title -> [(tmdb_id, year, popularity, imdb_id)]
        self.movies_by_orig = defaultdict(list)  # clean_original_title -> [...]
        self.alt_titles = defaultdict(list)  # clean_title -> [tmdb_id, ...]
        self.translations = defaultdict(list)  # clean_title -> [tmdb_id, ...]
        self.movie_data = {}  # tmdb_id -> (imdb_id, year, secondary_year, popularity)

        self._load(index_path)

    def _load(self, index_path: Path):
        print("Loading TMDB index into memory...", flush=True)
        t0 = time.time()

        conn = sqlite3.connect(str(index_path))
        conn.row_factory = sqlite3.Row

        # Load movies
        for row in conn.execute("""
            SELECT tmdb_id, imdb_id, clean_title, clean_original_title, year, secondary_year, popularity
            FROM tmdb_movie_index
        """):
            tmdb_id = row["tmdb_id"]
            imdb_id = row["imdb_id"]
            clean = row["clean_title"]
            clean_orig = row["clean_original_title"]
            year = row["year"]
            sec_year = row["secondary_year"]
            popularity = row["popularity"] or 0

            self.movies_by_clean[clean].append((tmdb_id, year, popularity, imdb_id))
            if clean_orig:
                self.movies_by_orig[clean_orig].append(
                    (tmdb_id, year, popularity, imdb_id)
                )
            self.movie_data[tmdb_id] = (imdb_id, year, sec_year, popularity)

        # Load alt titles
        for row in conn.execute("SELECT tmdb_id, clean_title FROM tmdb_alt_titles"):
            self.alt_titles[row["clean_title"]].append(row["tmdb_id"])

        # Load translations
        try:
            for row in conn.execute(
                "SELECT tmdb_id, clean_title FROM tmdb_translations"
            ):
                self.translations[row["clean_title"]].append(row["tmdb_id"])
        except sqlite3.OperationalError:
            pass

        conn.close()
        elapsed = time.time() - t0
        print(
            f"  Loaded {len(self.movies_by_clean):,} movies, {len(self.alt_titles):,} alt titles, "
            f"{len(self.translations):,} translations in {elapsed:.1f}s",
            flush=True,
        )

    def find(self, clean: str, year: int | None) -> tuple | None:
        """
        Radarr-exact matching cascade.
        Returns (tmdb_id, imdb_id) or None.

        Radarr's approach:
        1. Query all candidates matching variants
        2. Filter by year if provided
        3. Sort by popularity
        4. Return best match
        5. If no matches, try next phase
        """
        variants = generate_title_variants(clean)

        def year_matches(m_year, sec_year):
            if not year:
                return True
            return (
                m_year == year
                or m_year == year - 1
                or m_year == year + 1
                or sec_year == year
            )

        def best_candidate(candidates):
            if not candidates:
                return None
            best = max(candidates, key=lambda x: x[2])
            return (best[0], best[1])

        # Phase 1: Main title (clean_title)
        candidates = []
        for variant in variants:
            for tmdb_id, m_year, popularity, imdb_id in self.movies_by_clean.get(
                variant, []
            ):
                if tmdb_id in self.movie_data:
                    _, _, sec_year, _ = self.movie_data[tmdb_id]
                    if year_matches(m_year, sec_year):
                        candidates.append((tmdb_id, imdb_id, popularity))
        if candidates:
            return best_candidate(candidates)

        # Phase 2: Original title (clean_original_title)
        candidates = []
        for variant in variants:
            for tmdb_id, m_year, popularity, imdb_id in self.movies_by_orig.get(
                variant, []
            ):
                if tmdb_id in self.movie_data:
                    _, _, sec_year, _ = self.movie_data[tmdb_id]
                    if year_matches(m_year, sec_year):
                        candidates.append((tmdb_id, imdb_id, popularity))
        if candidates:
            return best_candidate(candidates)

        # Phase 3: Alt titles
        candidates = []
        for variant in variants:
            for tmdb_id in self.alt_titles.get(variant, []):
                if tmdb_id in self.movie_data:
                    imdb_id, m_year, sec_year, popularity = self.movie_data[tmdb_id]
                    if year_matches(m_year, sec_year):
                        candidates.append((tmdb_id, imdb_id, popularity))
        if candidates:
            return best_candidate(candidates)

        # Phase 4: Translations
        candidates = []
        for variant in variants:
            for tmdb_id in self.translations.get(variant, []):
                if tmdb_id in self.movie_data:
                    imdb_id, m_year, sec_year, popularity = self.movie_data[tmdb_id]
                    if year_matches(m_year, sec_year):
                        candidates.append((tmdb_id, imdb_id, popularity))
        if candidates:
            return best_candidate(candidates)

        return None


def run(limit: int = 0, dry_run: bool = False, batch_size: int = 10000):
    print("V3 Movie ID Backfill (In-Memory Index)", flush=True)
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

    # Load TMDB index into memory
    tmdb = TMDBMemoryIndex(INDEX_PATH)

    print("Opening magnetico database...", flush=True)
    db_conn = sqlite3.connect(str(DB_PATH), timeout=120)
    db_conn.row_factory = sqlite3.Row
    db_conn.execute("PRAGMA journal_mode=WAL")

    print("Processing movies...", flush=True)

    stats = Counter()
    start_time = time.time()
    last_id = 0
    limit_counter = 0
    updates = []

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

            # Try Radarr parser first for keys without |
            if "|" not in key:
                parsed = parse_movie_title(key)
                if parsed and parsed.title:
                    clean = radarr_clean_title(parsed.title)
                    year = parsed.year
                else:
                    # Fallback to manual extraction
                    cleaned = clean_movie_key(key)
                    clean = radarr_clean_title(cleaned)
                    year_match = re.search(r"\b((?:19|20)\d{2})\b", key)
                    year = int(year_match.group(1)) if year_match else None
            else:
                # Standard | split
                parts = key.split("|")
                raw_title = parts[0].strip()
                year = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None

                if has_cyrillic(raw_title):
                    raw_title = transliterate_cyrillic(raw_title)

                cleaned = clean_movie_key(raw_title)
                clean = radarr_clean_title(cleaned)

            if not clean or len(clean) < 2:
                stats["skipped_short"] += 1
                continue

            result = tmdb.find(clean, year)

            if result:
                tmdb_id, imdb_id = result
                stats["resolved"] += 1
                if not dry_run:
                    updates.append((str(tmdb_id), imdb_id, torrent_id))
            else:
                stats["miss"] += 1

            stats["processed"] += 1

        # Batch write updates
        if updates and not dry_run:
            db_conn.executemany(
                "UPDATE media_index SET tmdb_id = ?, imdb_id = ? WHERE torrent_id = ?",
                updates,
            )
            db_conn.commit()
            updates = []

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

    # Final write
    if updates and not dry_run:
        db_conn.executemany(
            "UPDATE media_index SET tmdb_id = ?, imdb_id = ? WHERE torrent_id = ?",
            updates,
        )
        db_conn.commit()

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

    db_conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="V3 movie ID backfill with in-memory index"
    )
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
