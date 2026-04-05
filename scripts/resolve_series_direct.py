#!/usr/bin/env python3
"""
Direct series resolution using TMDB series index.

Instead of trying to match dirty series_key values (which have release group noise),
we extract the clean title from series_key and look it up directly in TMDB.

Strategy:
1. For each unique series_key, extract the "clean" title (strip common noise patterns)
2. Look up in series_index using 4-phase cascade
3. If found, fan out TMDB/IMDB ID to ALL rows with that series_key
"""

import argparse
import re
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "lib"))
sys.path.insert(0, str(Path(__file__).parent.parent / "bitmagnet-media" / "classifier"))
from radarr_parser import generate_title_variants
from shared_title_normalizer import clean_series_title, strip_torrent_noise

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TMDB_INDEX = Path(__file__).parent.parent / "data" / "tmdb" / "series_index.sqlite3"
SQLITE_DB = (
    Path(__file__).parent.parent
    / "data"
    / "comet-fresh"
    / "magnetico"
    / "active.search.sqlite3"
)

BATCH_SIZE = 10000
PROGRESS_INTERVAL = 50000

# Additional patterns to extract clean title from dirty series_key
# The series_key from Go classifier looks like "the walking dead" or "vikings" or
# "arrow dimension" (with release group noise appended)
TITLE_EXTRACT_PATTERNS = [
    # Strip common release group noise at the end
    re.compile(
        r"\s+(?:web|hdtv|webrip|webdl|web[- ]?dl|bluray|blu[- ]?ray|brrip|bdrip|dvdrip|hdrip|remux)\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        r"\s+(?:dimension|eztv|lol|skyfire|sva|usk|dhd|ctv|ctcv|imm|me|eztv|rartv|megusta|kilo|kg|playweb|torrentgalaxy|subth|subeng|anilibria|anidub|animelayer)\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        r"\s+(?:rus|eng|fra|ger|spa|ita|dub|sub| dubbed|subbed)\s*$", re.IGNORECASE
    ),
    re.compile(r"\s+\d{1,3}(?:\.\d+)?(?:gb|mb)\s*$", re.IGNORECASE),
    re.compile(r"\s+[a-z]\s*$", re.IGNORECASE),  # single letter suffix
]

MULTISPACE = re.compile(r"\s+")

# Strip leading "the " for better TMDB matching
# Go classifier keeps "the" but TMDB clean_title strips it
LEADING_ARTICLE = re.compile(r"^the\s+", re.IGNORECASE)


def extract_clean_title(series_key: str) -> list[str]:
    """Extract clean titles from a Go-classifier series_key.

    Returns a list of possible clean titles to try against TMDB.
    The series_key may have release group noise appended.
    We try multiple cleaning approaches.
    """
    results = []

    # Try 1: Direct clean
    clean = clean_series_title(series_key)
    if clean and len(clean) >= 3:
        results.append(clean)

    # Try 2: Strip leading "the " and clean again
    stripped = LEADING_ARTICLE.sub("", series_key, count=1).strip()
    if stripped != series_key:
        clean2 = clean_series_title(stripped)
        if clean2 and len(clean2) >= 3:
            results.append(clean2)

    # Try 3: Strip release group patterns first, then clean
    title = series_key
    for pattern in TITLE_EXTRACT_PATTERNS:
        title = pattern.sub("", title)
    title = MULTISPACE.sub(" ", title).strip()
    if title:
        clean3 = clean_series_title(title)
        if clean3 and len(clean3) >= 3:
            results.append(clean3)

        # Try 3b: Also try with leading article stripped
        stripped2 = LEADING_ARTICLE.sub("", title, count=1).strip()
        if stripped2 != title:
            clean4 = clean_series_title(stripped2)
            if clean4 and len(clean4) >= 3:
                results.append(clean4)

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for c in results:
        if c not in seen:
            seen.add(c)
            unique.append(c)

    return unique


# ---------------------------------------------------------------------------
# TMDB Series Index Loader
# ---------------------------------------------------------------------------


class TMDBSeriesIndex:
    """In-memory TMDB series index with 4-phase cascade matching."""

    def __init__(self):
        self.series_by_clean = {}
        self.series_by_orig = {}
        self.alt_titles = {}
        self.translations = {}
        self.series_data = {}

    def load(self):
        conn = sqlite3.connect(str(TMDB_INDEX))
        conn.execute("PRAGMA journal_mode=WAL")

        print("  Loading TMDB series index...")
        total = 0
        for row in conn.execute(
            "SELECT tmdb_id, imdb_id, clean_title, clean_original_title, "
            "year, popularity FROM tmdb_series_index"
        ):
            tmdb_id, imdb_id, clean_title, clean_orig, year, pop = row
            total += 1

            if clean_title not in self.series_by_clean:
                self.series_by_clean[clean_title] = []
            self.series_by_clean[clean_title].append((tmdb_id, imdb_id, year, pop))

            if clean_orig:
                if clean_orig not in self.series_by_orig:
                    self.series_by_orig[clean_orig] = []
                self.series_by_orig[clean_orig].append((tmdb_id, imdb_id, year, pop))

            self.series_data[tmdb_id] = (imdb_id, year, pop)

        print(
            f"  Loaded {len(self.series_by_clean):,} unique titles from {total:,} series"
        )

        print("  Loading alt titles...")
        alt_count = 0
        for row in conn.execute(
            "SELECT tmdb_id, clean_title FROM tmdb_series_alt_titles"
        ):
            clean = row[1]
            if clean not in self.alt_titles:
                self.alt_titles[clean] = []
            self.alt_titles[clean].append(row[0])
            alt_count += 1
        print(f"  Loaded {alt_count:,} alt title entries")

        print("  Loading translations...")
        trans_count = 0
        try:
            for row in conn.execute(
                "SELECT tmdb_id, clean_title FROM tmdb_series_translations"
            ):
                clean = row[1]
                if clean not in self.translations:
                    self.translations[clean] = []
                self.translations[clean].append(row[0])
                trans_count += 1
        except sqlite3.OperationalError:
            pass
        print(f"  Loaded {trans_count:,} translation entries")

        conn.close()

    def find(self, clean: str) -> tuple | None:
        """4-phase cascade matching with title variants."""
        if not self.series_data:
            return None

        variants = generate_title_variants(clean)

        def best_candidate(candidates):
            if not candidates:
                return None
            best = max(candidates, key=lambda x: x[3])  # by popularity
            return (best[0], best[1], best[2])

        # Phase 1: Main title
        for variant in variants:
            for entry in self.series_by_clean.get(variant, []):
                return best_candidate([entry])

        # Phase 2: Original title
        for variant in variants:
            for entry in self.series_by_orig.get(variant, []):
                return best_candidate([entry])

        # Phase 3: Alt titles
        for variant in variants:
            for tmdb_id in self.alt_titles.get(variant, []):
                if tmdb_id in self.series_data:
                    imdb_id, year, pop = self.series_data[tmdb_id]
                    return (tmdb_id, imdb_id, year)

        # Phase 4: Translations
        for variant in variants:
            for tmdb_id in self.translations.get(variant, []):
                if tmdb_id in self.series_data:
                    imdb_id, year, pop = self.series_data[tmdb_id]
                    return (tmdb_id, imdb_id, year)

        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Match series against TMDB series index"
    )
    parser.add_argument("--dry-run", action="store_true", help="Don't update SQLite")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--min-title-len", type=int, default=3)
    args = parser.parse_args()

    start_time = time.time()
    print("=" * 60)
    print("Direct Series Resolution via TMDB Series Index")
    print("=" * 60)

    print("\n[1/2] Loading TMDB series index...")
    tmdb_index = TMDBSeriesIndex()
    tmdb_index.load()

    print("\n[2/2] Connecting to SQLite...")
    conn = sqlite3.connect(str(SQLITE_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Get unique series_keys without IDs
    print("  Fetching unresolved series_keys...")
    rows = conn.execute(
        """
        SELECT series_key, COUNT(*) as cnt
        FROM media_index
        WHERE content_class IN ('episode', 'multi_episode', 'season_pack')
          AND is_anime = '0'
          AND series_key IS NOT NULL
          AND series_key != ''
          AND (imdb_id IS NULL OR imdb_id = '')
          AND (tmdb_id IS NULL OR tmdb_id = '')
        GROUP BY series_key
        ORDER BY cnt DESC
        """
    ).fetchall()
    print(f"  Found {len(rows):,} unique unresolved series_keys")

    matched_keys = 0
    unmatched_keys = 0
    total_rows_updated = 0
    updates = []

    for i, (series_key, count) in enumerate(rows, 1):
        # Extract clean title variants from dirty series_key
        clean_titles = extract_clean_title(series_key)

        if not clean_titles or len(clean_titles[0]) < args.min_title_len:
            unmatched_keys += 1
            continue

        # Look up in TMDB - try each variant
        result = None
        for clean_title in clean_titles:
            result = tmdb_index.find(clean_title)
            if result:
                break

        if result:
            tmdb_id, imdb_id, year = result
            updates.append((series_key, tmdb_id, imdb_id))
            matched_keys += 1
        else:
            unmatched_keys += 1

        if i % PROGRESS_INTERVAL == 0:
            print(
                f"  Progress: {i:,}/{len(rows):,} "
                f"({matched_keys:,} matched, {unmatched_keys:,} unmatched)"
            )

        # Batch update
        if len(updates) >= args.batch_size:
            cursor = conn.cursor()
            for series_key, tmdb_id, imdb_id in updates:
                cursor.execute(
                    """
                    UPDATE media_index
                    SET tmdb_id = ?, imdb_id = ?
                    WHERE series_key = ?
                      AND (tmdb_id IS NULL OR tmdb_id = '')
                    """,
                    (str(tmdb_id), imdb_id, series_key),
                )
                total_rows_updated += cursor.rowcount

            if not args.dry_run:
                conn.commit()
            print(
                f"  Batch: {len(updates):,} keys, {total_rows_updated:,} rows updated"
            )
            updates = []

    # Final batch
    if updates:
        cursor = conn.cursor()
        for series_key, tmdb_id, imdb_id in updates:
            cursor.execute(
                """
                UPDATE media_index
                SET tmdb_id = ?, imdb_id = ?
                WHERE series_key = ?
                  AND (tmdb_id IS NULL OR tmdb_id = '')
                """,
                (str(tmdb_id), imdb_id, series_key),
            )
            total_rows_updated += cursor.rowcount

        if not args.dry_run:
            conn.commit()
        print(f"  Final: {len(updates):,} keys, {total_rows_updated:,} rows updated")

    print(f"\n  Keys: {matched_keys:,} matched, {unmatched_keys:,} unmatched")
    print(f"  Match rate: {100.0 * matched_keys / max(len(rows), 1):.1f}%")
    print(f"  Total rows updated: {total_rows_updated:,}")

    conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"Completed in {elapsed:.0f}s ({elapsed / 60:.1f} minutes)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
