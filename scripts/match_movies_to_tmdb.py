#!/usr/bin/env python3
"""
Match historical movie torrents against the existing TMDB movie index.

Uses the pre-built movie_index.sqlite3 (335MB, 1.1M movies) with full
4-phase cascade matching:
    Phase 1: clean_title lookup with title variants (Roman numerals, etc.)
    Phase 2: clean_original_title lookup
    Phase 3: alt_titles lookup (485K entries)
    Phase 4: translations lookup (1M entries)

Workflow:
    1. Load TMDB movie index into memory (4-phase cascade indexes)
    2. Query SQLite for unresolved movie_key groups plus a representative release name
    3. Try Radarr parsed-title exact recovery against the representative release name
    4. Fall back to the legacy cleaned movie_key exact path
    5. Batch update SQLite (10K rows per transaction)

Usage:
    python match_movies_to_tmdb.py [--dry-run] [--batch-size N] [--limit N]
      [--db-path PATH] [--allow-live-search-db]
"""

import argparse
import re
import sqlite3
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent / "lib"))
sys.path.insert(0, str(Path(__file__).parent.parent / "bitmagnet-media" / "classifier"))
from radarr_parser import (
    ParsedMovieInfo,
    clean_movie_title,
    generate_title_variants,
    parse_movie_title,
)
from shared_title_normalizer import strip_torrent_noise

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TMDB_INDEX = Path(__file__).parent.parent / "data" / "tmdb" / "movie_index.sqlite3"
SQLITE_DB = (
    Path(__file__).parent.parent
    / "data"
    / "comet-fresh"
    / "magnetico"
    / "active.search.sqlite3"
)

BATCH_SIZE = 10000
PROGRESS_INTERVAL = 50000
FULL_DATE_PATTERN = re.compile(
    r"\b(?:\d{4}[.\-_]\d{1,2}[.\-_]\d{1,2}|\d{1,2}[.\-_]\d{1,2}[.\-_]\d{4})\b"
)
REPRESENTATIVE_EPISODIC_PATTERN = re.compile(
    r"\b(?:s\d{1,2}(?:[ ._\-]*e\d{1,3})?|\d{1,2}x\d{1,3}|season\s*\d{1,2}|stage\s*\d{1,2}|"
    r"выпуск|эфир|дневной|вечерний|прямой[ ._\-]*эфир|prjamoj[ ._\-]*jefir)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Shared movie matching helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MovieMatchResult:
    tmdb_id: int
    imdb_id: str | None
    year: int | None
    source: str


def resolve_search_db_target(
    db_path: Path,
    *,
    apply_changes: bool,
    allow_live_search_db: bool,
) -> Path:
    resolved = db_path.expanduser().resolve()
    live_target = SQLITE_DB.expanduser().resolve()
    if apply_changes and resolved == live_target and not allow_live_search_db:
        raise SystemExit(
            "Writing directly to the live active.search.sqlite3 is blocked by default. "
            "Use an offline stage copy with --db-path and deploy via the staged "
            "rebuild/cutover path. Pass --allow-live-search-db only for an explicit "
            "unsafe override."
        )
    return resolved


def extract_movie_key_parts(movie_key: str) -> tuple[str, int | None]:
    raw_title = movie_key
    year = None
    parts = movie_key.split("|")
    if parts:
        raw_title = parts[0].strip()
    if len(parts) >= 2:
        try:
            year = int(parts[1])
        except ValueError:
            pass
    return raw_title, year


def iter_clean_movie_lookup_titles(parsed: ParsedMovieInfo | None) -> list[str]:
    if not parsed:
        return []

    raw_titles = list(parsed.titles or [])
    if parsed.title and parsed.title not in raw_titles:
        raw_titles.insert(0, parsed.title)

    clean_titles: list[str] = []
    seen: set[str] = set()
    for raw_title in raw_titles:
        clean = clean_movie_title(raw_title)
        if not clean or clean in seen or clean.isdigit() or len(clean) < 2:
            continue
        clean_titles.append(clean)
        seen.add(clean)
        if len(clean_titles) >= 5:
            break
    return clean_titles


def representative_name_is_safe_for_movie_match(representative_name: str | None) -> bool:
    raw = str(representative_name or "").strip()
    if not raw:
        return False
    if FULL_DATE_PATTERN.search(raw):
        return False
    if REPRESENTATIVE_EPISODIC_PATTERN.search(raw):
        return False
    return True


def match_movie_from_parsed_titles(
    parsed: ParsedMovieInfo | None,
    fallback_year: int | None,
    tmdb_index: "TMDBIndex",
) -> MovieMatchResult | None:
    if not parsed or not tmdb_index.movie_data:
        return None

    match_year = parsed.year or fallback_year
    for clean in iter_clean_movie_lookup_titles(parsed):
        match = tmdb_index.find(clean, match_year)
        if match:
            tmdb_id, imdb_id, matched_year = match
            return MovieMatchResult(
                tmdb_id=tmdb_id,
                imdb_id=imdb_id,
                year=matched_year,
                source="parsed_title",
            )

    return None


def match_movie_from_cleaned_key(
    raw_title: str,
    year: int | None,
    tmdb_index: "TMDBIndex",
) -> MovieMatchResult | None:
    stripped = strip_torrent_noise(raw_title)
    if not stripped:
        return None

    clean = clean_movie_title(stripped)
    if not clean:
        return None

    result = tmdb_index.find(clean, year)
    if not result:
        return None

    tmdb_id, imdb_id, matched_year = result
    return MovieMatchResult(
        tmdb_id=tmdb_id,
        imdb_id=imdb_id,
        year=matched_year,
        source="cleaned_key",
    )


# ---------------------------------------------------------------------------
# TMDB Index Loader (4-phase cascade)
# ---------------------------------------------------------------------------


class TMDBIndex:
    """In-memory TMDB index with 4-phase cascade matching."""

    def __init__(self):
        self.movies_by_clean = {}  # clean_title -> [(tmdb_id, imdb_id, year, popularity)]
        self.movies_by_orig = {}  # clean_original_title -> [...]
        self.alt_titles = {}  # clean_title -> [tmdb_id, ...]
        self.translations = {}  # clean_title -> [tmdb_id, ...]
        self.movie_data = {}  # tmdb_id -> (imdb_id, year, secondary_year, popularity)

    def load(self):
        conn = sqlite3.connect(str(TMDB_INDEX))
        conn.execute("PRAGMA journal_mode=WAL")

        print("  Loading TMDB movie index...")
        total = 0
        for row in conn.execute(
            "SELECT tmdb_id, imdb_id, clean_title, clean_original_title, "
            "year, secondary_year, popularity FROM tmdb_movie_index"
        ):
            tmdb_id, imdb_id, clean_title, clean_orig, year, sec_year, pop = row
            total += 1

            if clean_title not in self.movies_by_clean:
                self.movies_by_clean[clean_title] = []
            self.movies_by_clean[clean_title].append(
                (tmdb_id, imdb_id, year, sec_year, pop)
            )

            if clean_orig:
                if clean_orig not in self.movies_by_orig:
                    self.movies_by_orig[clean_orig] = []
                self.movies_by_orig[clean_orig].append(
                    (tmdb_id, imdb_id, year, sec_year, pop)
                )

            self.movie_data[tmdb_id] = (imdb_id, year, sec_year, pop)

        print(
            f"  Loaded {len(self.movies_by_clean):,} unique titles from {total:,} movies"
        )

        # Load alt titles
        print("  Loading alt titles...")
        alt_count = 0
        for row in conn.execute("SELECT tmdb_id, clean_title FROM tmdb_alt_titles"):
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
                "SELECT tmdb_id, clean_title FROM tmdb_translations"
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

    def find(self, clean: str, year: int | None) -> tuple | None:
        """4-phase cascade matching with title variants."""
        if not self.movie_data:
            return None

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
            best = max(candidates, key=lambda x: x[3])
            return (best[0], best[1], best[2])

        # Phase 1: Main title (clean_title)
        candidates = []
        for variant in variants:
            for entry in self.movies_by_clean.get(variant, []):
                tmdb_id, imdb_id, m_year, sec_year, pop = entry
                if tmdb_id in self.movie_data:
                    if year_matches(m_year, sec_year):
                        candidates.append((tmdb_id, imdb_id, m_year, pop))
        if candidates:
            return best_candidate(candidates)

        # Phase 2: Original title
        candidates = []
        for variant in variants:
            for entry in self.movies_by_orig.get(variant, []):
                tmdb_id, imdb_id, m_year, sec_year, pop = entry
                if tmdb_id in self.movie_data:
                    if year_matches(m_year, sec_year):
                        candidates.append((tmdb_id, imdb_id, m_year, pop))
        if candidates:
            return best_candidate(candidates)

        # Phase 3: Alt titles
        candidates = []
        for variant in variants:
            for tmdb_id in self.alt_titles.get(variant, []):
                if tmdb_id in self.movie_data:
                    imdb_id, m_year, sec_year, pop = self.movie_data[tmdb_id]
                    if year_matches(m_year, sec_year):
                        candidates.append((tmdb_id, imdb_id, m_year, pop))
        if candidates:
            return best_candidate(candidates)

        # Phase 4: Translations
        candidates = []
        for variant in variants:
            for tmdb_id in self.translations.get(variant, []):
                if tmdb_id in self.movie_data:
                    imdb_id, m_year, sec_year, pop = self.movie_data[tmdb_id]
                    if year_matches(m_year, sec_year):
                        candidates.append((tmdb_id, imdb_id, m_year, pop))
        if candidates:
            return best_candidate(candidates)

        return None


# ---------------------------------------------------------------------------
# SQLite Matching
# ---------------------------------------------------------------------------


def get_unresolved_movie_keys(conn: sqlite3.Connection) -> list[tuple[str, int, str]]:
    """Get unresolved movie groups with a representative release name."""
    return conn.execute(
        """
        WITH ranked AS (
            SELECT
                movie_key,
                name,
                COUNT(*) OVER (PARTITION BY movie_key) AS cnt,
                ROW_NUMBER() OVER (
                    PARTITION BY movie_key
                    ORDER BY confidence DESC, media_score DESC, total_size DESC, torrent_id ASC
                ) AS rn
            FROM media_index
            WHERE content_class = 'movie'
              AND movie_key IS NOT NULL
              AND movie_key != ''
              AND (imdb_id IS NULL OR imdb_id = '')
              AND (tmdb_id IS NULL OR tmdb_id = '')
        )
        SELECT movie_key, cnt, COALESCE(name, '') AS representative_name
        FROM ranked
        WHERE rn = 1
        ORDER BY cnt DESC, movie_key
        """
    ).fetchall()


def match_movie_key(
    movie_key: str,
    tmdb_index: TMDBIndex,
    representative_name: str | None = None,
) -> tuple[int, str | None, int | None] | None:
    """Match a movie_key against the TMDB index using parsed-title exact recovery."""
    result = match_movie_key_with_source(
        movie_key,
        tmdb_index,
        representative_name=representative_name,
    )
    if result:
        return (result.tmdb_id, result.imdb_id, result.year)
    return None


def match_movie_key_with_source(
    movie_key: str,
    tmdb_index: TMDBIndex,
    representative_name: str | None = None,
) -> MovieMatchResult | None:
    raw_title, year = extract_movie_key_parts(movie_key)

    if representative_name_is_safe_for_movie_match(representative_name):
        parsed_match = match_movie_from_parsed_titles(
            parse_movie_title(str(representative_name or "").strip()),
            year,
            tmdb_index,
        )
        if parsed_match:
            return MovieMatchResult(
                tmdb_id=parsed_match.tmdb_id,
                imdb_id=parsed_match.imdb_id,
                year=parsed_match.year,
                source="representative_name",
            )

    parsed_match = match_movie_from_parsed_titles(
        parse_movie_title(raw_title),
        year,
        tmdb_index,
    )
    if parsed_match:
        return MovieMatchResult(
            tmdb_id=parsed_match.tmdb_id,
            imdb_id=parsed_match.imdb_id,
            year=parsed_match.year,
            source="parsed_key",
        )

    return match_movie_from_cleaned_key(raw_title, year, tmdb_index)


def batch_update_sqlite(
    conn: sqlite3.Connection,
    updates: list[tuple[str, int, str | None, int | None]],
    dry_run: bool = False,
) -> int:
    """Batch update SQLite with matched TMDB/IMDB IDs."""
    if not updates:
        return 0

    if dry_run:
        return sum(1 for u in updates if u)

    cursor = conn.cursor()
    total_updated = 0

    for item in updates:
        key = item[0]
        tmdb_id = item[1]
        imdb_id = item[2]
        year = item[3]

        cursor.execute(
            """
            UPDATE media_index
            SET tmdb_id = ?, imdb_id = ?, year = ?
            WHERE movie_key = ?
              AND (tmdb_id IS NULL OR tmdb_id = '')
            """,
            (str(tmdb_id), imdb_id, str(year) if year else None, key),
        )
        total_updated += cursor.rowcount

    return total_updated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Match movies against TMDB index")
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
        help="Limit number of unresolved movie keys to process",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Path to the SQLite database (defaults to active.search.sqlite3)",
    )
    parser.add_argument(
        "--allow-live-search-db",
        action="store_true",
        help="Unsafe override: allow writes to the live active.search.sqlite3 database.",
    )
    args = parser.parse_args()

    start_time = time.time()
    print("=" * 60)
    print("TMDB Movie Index Matching (Parsed-Title Exact Recovery)")
    print("=" * 60)

    # Step 1: Load TMDB index with 4-phase cascade
    print("\n[1/3] Loading TMDB movie index...")
    tmdb_index = TMDBIndex()
    tmdb_index.load()

    # Step 2: Connect to SQLite
    print("\n[2/3] Connecting to SQLite...")
    db_path = Path(args.db_path) if args.db_path else SQLITE_DB
    if not db_path.is_absolute():
        db_path = Path(__file__).parent.parent / db_path
    db_path = resolve_search_db_target(
        db_path,
        apply_changes=not args.dry_run,
        allow_live_search_db=bool(args.allow_live_search_db),
    )
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Step 3: Match and update
    print("\n[3/3] Matching movie keys...")
    movie_keys = get_unresolved_movie_keys(conn)
    if args.limit is not None:
        movie_keys = movie_keys[: args.limit]
    print(f"  Found {len(movie_keys):,} unique movie_keys without IDs")

    matched = 0
    unmatched = 0
    updates = []
    source_counts: Counter[str] = Counter()

    for i, (movie_key, count, representative_name) in enumerate(movie_keys, 1):
        result = match_movie_key_with_source(
            movie_key,
            tmdb_index,
            representative_name=representative_name,
        )
        if result:
            updates.append((movie_key, result.tmdb_id, result.imdb_id, result.year))
            matched += 1
            source_counts[result.source] += 1
        else:
            unmatched += 1

        if i % PROGRESS_INTERVAL == 0:
            print(
                f"  Progress: {i:,}/{len(movie_keys):,} "
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
    if source_counts:
        print(
            "  Match sources: "
            + ", ".join(
                f"{source}={count:,}"
                for source, count in sorted(source_counts.items())
            )
        )

    conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"Completed in {elapsed:.0f}s ({elapsed / 60:.1f} minutes)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
