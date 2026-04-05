#!/usr/bin/env python3
"""
Match historical series torrents against the existing TMDB series index.

Uses the pre-built series_index.sqlite3 (75MB, 218K series) with full
4-phase cascade matching:
    Phase 1: clean_title lookup with title variants (Roman numerals, etc.)
    Phase 2: clean_original_title lookup
    Phase 3: alt_titles lookup
    Phase 4: translations lookup

Workflow:
    1. Load TMDB series index into memory (4-phase cascade indexes)
    2. Query SQLite for unique series_key values without IDs
    3. Strip torrent noise, then apply Sonarr clean_series_title()
    4. Match using 4-phase cascade (no year matching — series keys have no year)
    5. Batch update SQLite (10K rows per transaction) — fan-out to ALL rows with matching series_key

Usage:
    python match_series_to_tmdb.py [--dry-run] [--batch-size N] [--limit N]
      [--db-path PATH]
"""

import argparse
import sqlite3
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent / "lib"))
sys.path.insert(0, str(Path(__file__).parent.parent / "bitmagnet-media" / "classifier"))
import lean_media_contract as lean_contract
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

# ---------------------------------------------------------------------------
# TMDB Series Index Loader (4-phase cascade)
# ---------------------------------------------------------------------------


class TMDBSeriesIndex:
    """In-memory TMDB series index with 4-phase cascade matching."""

    def __init__(self):
        self.series_by_clean = {}  # clean_title -> [(tmdb_id, imdb_id, year, popularity)]
        self.series_by_orig = {}  # clean_original_title -> [...]
        self.alt_titles = {}  # clean_title -> [tmdb_id, ...]
        self.translations = {}  # clean_title -> [tmdb_id, ...]
        self.series_data = {}  # tmdb_id -> (imdb_id, year, popularity)

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

        # Load alt titles
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

        # Load translations
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
        """4-phase cascade matching with title variants (no year matching for series)."""
        if not self.series_data:
            return None

        variants = generate_title_variants(clean)

        def best_candidate(candidates):
            if not candidates:
                return None
            best = max(candidates, key=lambda x: x[3])
            return (best[0], best[1], best[2])

        # Phase 1: Main title (clean_title)
        candidates = []
        for variant in variants:
            for entry in self.series_by_clean.get(variant, []):
                tmdb_id, imdb_id, s_year, pop = entry
                if tmdb_id in self.series_data:
                    candidates.append((tmdb_id, imdb_id, s_year, pop))
        if candidates:
            return best_candidate(candidates)

        # Phase 2: Original title
        candidates = []
        for variant in variants:
            for entry in self.series_by_orig.get(variant, []):
                tmdb_id, imdb_id, s_year, pop = entry
                if tmdb_id in self.series_data:
                    candidates.append((tmdb_id, imdb_id, s_year, pop))
        if candidates:
            return best_candidate(candidates)

        # Phase 3: Alt titles
        candidates = []
        for variant in variants:
            for tmdb_id in self.alt_titles.get(variant, []):
                if tmdb_id in self.series_data:
                    imdb_id, s_year, pop = self.series_data[tmdb_id]
                    candidates.append((tmdb_id, imdb_id, s_year, pop))
        if candidates:
            return best_candidate(candidates)

        # Phase 4: Translations
        candidates = []
        for variant in variants:
            for tmdb_id in self.translations.get(variant, []):
                if tmdb_id in self.series_data:
                    imdb_id, s_year, pop = self.series_data[tmdb_id]
                    candidates.append((tmdb_id, imdb_id, s_year, pop))
        if candidates:
            return best_candidate(candidates)

        return None


# ---------------------------------------------------------------------------
# SQLite Matching
# ---------------------------------------------------------------------------


def get_unresolved_series_keys(conn: sqlite3.Connection) -> list[tuple[str, int]]:
    """Get unique series_key values without IDs, ordered by row count (highest first)."""
    return conn.execute(
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


def match_series_key(
    series_key: str,
    tmdb_index: TMDBSeriesIndex,
) -> tuple[int, str | None, int | None] | None:
    """Match a series_key against the TMDB series index using 4-phase cascade."""
    # Step 1: Strip torrent noise (quality tags, release groups, etc.)
    stripped = strip_torrent_noise(series_key)
    if not stripped:
        return None

    # Step 2: Apply Sonarr clean_series_title (strips articles, accents, etc.)
    clean = clean_series_title(stripped)
    if not clean:
        return None

    # Step 3: 4-phase cascade matching (no year — series keys don't have year)
    result = tmdb_index.find(clean)
    if result:
        tmdb_id, imdb_id, matched_year = result
        return (tmdb_id, imdb_id, matched_year)
    return None


def batch_update_sqlite(
    conn: sqlite3.Connection,
    updates: list[tuple[str, int, str | None, int | None]],
    dry_run: bool = False,
) -> int:
    """Batch update SQLite with matched TMDB/IMDB IDs — fan-out to ALL rows with matching series_key."""
    if not updates:
        return 0

    if dry_run:
        return sum(1 for u in updates if u)

    cursor = conn.cursor()
    total_updated = 0
    fts_deletes: list[tuple[int, str]] = []
    fts_inserts: list[tuple[int, str]] = []

    for item in updates:
        key = item[0]
        tmdb_id = item[1]
        imdb_id = item[2]
        year = item[3]
        matched_rows = cursor.execute(
            """
            SELECT torrent_id, norm_title, name, canonical_title_key, movie_key, series_key,
                   aliases_text, imdb_id, tmdb_id, year, season, episode_start, episode_end,
                   exact_episode_key, season_pack_key
            FROM media_index
            WHERE series_key = ?
              AND (tmdb_id IS NULL OR tmdb_id = '')
            """,
            (key,),
        ).fetchall()
        if not matched_rows:
            continue

        for row in matched_rows:
            row_dict = dict(row)
            fts_deletes.append(
                (int(row["torrent_id"]), lean_contract.search_text_for(row_dict))
            )
            row_dict["tmdb_id"] = str(tmdb_id)
            row_dict["imdb_id"] = imdb_id
            row_dict["year"] = year
            fts_inserts.append(
                (int(row["torrent_id"]), lean_contract.search_text_for(row_dict))
            )

        # Update ALL rows with this series_key (fan-out)
        cursor.execute(
            """
            UPDATE media_index
            SET tmdb_id = ?, imdb_id = ?, year = ?
            WHERE series_key = ?
              AND (tmdb_id IS NULL OR tmdb_id = '')
            """,
            (str(tmdb_id), imdb_id, str(year) if year else None, key),
        )
        total_updated += cursor.rowcount

    if fts_deletes:
        cursor.executemany(
            "INSERT INTO media_fts(media_fts, rowid, search_text) VALUES('delete', ?, ?)",
            fts_deletes,
        )
    if fts_inserts:
        cursor.executemany(
            "INSERT INTO media_fts(rowid, search_text) VALUES (?, ?)",
            fts_inserts,
        )

    return total_updated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Match series against TMDB index")
    parser.add_argument("--dry-run", action="store_true", help="Don't update SQLite")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help="Batch size for SQLite updates",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of unresolved series keys to process",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Path to the SQLite database (defaults to active.search.sqlite3)",
    )
    args = parser.parse_args()

    start_time = time.time()
    print("=" * 60)
    print("TMDB Series Index Matching (4-Phase Cascade)")
    print("=" * 60)

    # Step 1: Load TMDB series index with 4-phase cascade
    print("\n[1/3] Loading TMDB series index...")
    tmdb_index = TMDBSeriesIndex()
    tmdb_index.load()

    # Step 2: Connect to SQLite
    print("\n[2/3] Connecting to SQLite...")
    db_path = Path(args.db_path) if args.db_path else SQLITE_DB
    if not db_path.is_absolute():
        db_path = Path(__file__).parent.parent / db_path
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")

    # Step 3: Match and update
    print("\n[3/3] Matching series keys...")
    series_keys = get_unresolved_series_keys(conn)
    if args.limit is not None:
        series_keys = series_keys[: args.limit]
    print(f"  Found {len(series_keys):,} unique series_keys without IDs")

    matched = 0
    unmatched = 0
    updates = []

    for i, (series_key, count) in enumerate(series_keys, 1):
        result = match_series_key(series_key, tmdb_index)
        if result:
            tmdb_id, imdb_id, year = result
            updates.append((series_key, tmdb_id, imdb_id, year))
            matched += 1
        else:
            unmatched += 1

        if i % PROGRESS_INTERVAL == 0:
            print(
                f"  Progress: {i:,}/{len(series_keys):,} "
                f"({matched:,} matched, {unmatched:,} unmatched, "
                f"{100.0 * matched / max(i, 1):.1f}%)"
            )

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

    conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"Completed in {elapsed:.0f}s ({elapsed / 60:.1f} minutes)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
