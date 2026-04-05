#!/usr/bin/env python3
"""
Enhanced movie matching with multi-stage noise stripping.

Stage 1: Exact match on movie_key (as-is)
Stage 2: Aggressive noise stripping (release groups, quality, language tags)
Stage 3: Token-based matching (first 3-4 words only)

This handles the fact that Go classifier produces dirty movie_keys like
"the brothers grimsby d exkinoray|2016" while TMDB index has "brothersgrimsby".
"""

import argparse
import re
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "lib"))
sys.path.insert(0, str(Path(__file__).parent.parent / "bitmagnet-media" / "classifier"))
from radarr_parser import clean_movie_title, generate_title_variants
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

# Additional noise patterns not caught by strip_torrent_noise
# These target common release group names, language tags, and quality remnants
AGGRESSIVE_NOISE_PATTERNS = [
    # Single letters that are likely quality tags (D, R, etc.)
    (re.compile(r"\b[a-z]\b", re.IGNORECASE), " "),
    # Common language tags
    (
        re.compile(
            r"\b(?:rus|eng|fra|ger|spa|ita|por|pol|dut|swe|nor|dan|fin|tur|ara|kor|jpn|chi|tha|vie|ind|hin|tam|tel|ben|mar|guj|kan|mal|ori|pan|urd|nep|sin|bur|khm|lao|mon|tib|uig|kaz|uzb|tgk|kir|aze|arm|geo|alb|mac|bos|srp|hrv|slv|slk|cze|hun|rum|bul|ukr|bel|lit|lav|est|mlt|gle|cym|bre|eus|cat|glg|ast|arg|cat|glg|eus|lat|grc|syr|heb|yid|lad|prs|pus|snd|kas|kok|mni|sat|doi|nep|san|mai|bho|mag|awa|hne|gon|kha|lus|miz|nai|inc|dra|sit|aav|poz|paa|aus|afa|nic|ssa|alv|cus|ber|sem|egx|ber|chd|inc|ira|gem|cel|sla|bal|alb|arm|grk|phn|arc|syr|heb|ara|eth|ber|cus|nil|ssa|alv|kho|tok|inc|ira|gem|cel|sla|bal|alb|arm|grk)\b",
            re.IGNORECASE,
        ),
        " ",
    ),
    # Common release group suffixes
    (
        re.compile(
            r"\b(?:megusta|megapeer|exkinoray|blitzcrieg|cinecalidad|kinozal|atrakcion|newstudio|lostfilm|anilibria|anidub|anistar|animelayer|animevost|sovetromantica|baibako|coldfilm|newstudio|lostfilm|anilibria|tv|sub[- ]?th|sub[- ]?eng|clipman|dvb|rmvb|lat|dub|dubl|dubbed|subbed|vostfr|hardsub|softsub)\b",
            re.IGNORECASE,
        ),
        " ",
    ),
    # Quality remnants
    (
        re.compile(
            r"\b(?:bdrip|brrip|webrip|webdl|web[- ]?dl|hdtv|dvdrip|hdrip|remux|hddvd|bluray|blu[- ]?ray|xvid|divx|h264|h265|x264|x265|hevc|avc|aac|dts|truehd|atmos|ac3|eac3|opus|flac|mp3|ddp|dd5|720p|1080p|2160p|4k|8k|uhd|hdr|sdr|dolby|vision|atmos|dts|truehd|atmos|10bit|8bit)\b",
            re.IGNORECASE,
        ),
        " ",
    ),
    # File size patterns
    (re.compile(r"\b\d{1,2}\.\d{1,2}gb\b|\b\d{3,4}mb\b", re.IGNORECASE), " "),
    # Trailing noise after common patterns
    (re.compile(r"\b(?:hq|lq|sd|hd|uhd|fhd|qhd)\b", re.IGNORECASE), " "),
]

MULTISPACE = re.compile(r"\s+")


def aggressive_strip(text: str) -> str:
    """Apply aggressive noise stripping beyond the standard strip_torrent_noise."""
    result = strip_torrent_noise(text)
    for pattern, replacement in AGGRESSIVE_NOISE_PATTERNS:
        result = pattern.sub(replacement, result)
    # Collapse whitespace, strip
    result = MULTISPACE.sub(" ", result).strip()
    return result


def clean_key_for_matching(key: str) -> str:
    """Clean a movie_key for matching against TMDB index.

    Handles the fact that Go classifier produces dirty keys like
    "the brothers grimsby d exkinoray|2016".
    """
    # Extract year if present
    year = None
    parts = key.split("|")
    if len(parts) >= 2:
        try:
            year = int(parts[1])
        except ValueError:
            pass

    # Get title part
    title = parts[0]

    # Stage 1: Standard noise stripping
    stripped = strip_torrent_noise(title)
    clean = clean_movie_title(stripped)

    if clean:
        return clean, year

    # Stage 2: Aggressive noise stripping
    stripped = aggressive_strip(title)
    clean = clean_movie_title(stripped)

    return clean, year


# ---------------------------------------------------------------------------
# TMDB Index Loader (4-phase cascade)
# ---------------------------------------------------------------------------


class TMDBIndex:
    """In-memory TMDB index with 4-phase cascade matching."""

    def __init__(self):
        self.movies_by_clean = {}
        self.movies_by_orig = {}
        self.alt_titles = {}
        self.translations = {}
        self.movie_data = {}

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
    tmdb_index: TMDBIndex,
) -> tuple[int, str | None, int | None] | None:
    """Match a movie_key against the TMDB index using multi-stage cleaning."""
    # Clean the key (handles Go classifier noise)
    clean, year = clean_key_for_matching(movie_key)
    if not clean:
        return None

    # 4-phase cascade matching
    result = tmdb_index.find(clean, year)
    if result:
        tmdb_id, imdb_id, matched_year = result
        return (tmdb_id, imdb_id, matched_year)
    return None


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
    args = parser.parse_args()

    start_time = time.time()
    print("=" * 60)
    print("TMDB Movie Index Matching (Multi-Stage Cleaning)")
    print("=" * 60)

    # Step 1: Load TMDB index with 4-phase cascade
    print("\n[1/3] Loading TMDB movie index...")
    tmdb_index = TMDBIndex()
    tmdb_index.load()

    # Step 2: Connect to SQLite
    print("\n[2/3] Connecting to SQLite...")
    conn = sqlite3.connect(str(SQLITE_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Step 3: Match and update
    print("\n[3/3] Matching movie keys...")
    movie_keys = get_unresolved_movie_keys(conn)
    print(f"  Found {len(movie_keys):,} unique movie_keys without IDs")

    matched = 0
    unmatched = 0
    updates = []

    for i, (movie_key, count) in enumerate(movie_keys, 1):
        result = match_movie_key(movie_key, tmdb_index)
        if result:
            tmdb_id, imdb_id, year = result
            updates.append((movie_key, tmdb_id, imdb_id, year))
            matched += 1
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

    conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"Completed in {elapsed:.0f}s ({elapsed / 60:.1f} minutes)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
