#!/usr/bin/env python3
"""
Smoke tests for movie matching logic.

Tests the v2 matching pipeline against known movie keys and expected TMDB IDs.
Run this before running the full backfill to verify matching correctness.
"""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib.radarr_parser import (
    clean_movie_title as radarr_clean_title,
    generate_title_variants,
)
from backfill_movies_v2 import clean_movie_key, find_movie_v2

INDEX_PATH = Path(__file__).parent.parent / "data" / "tmdb" / "movie_index.sqlite3"

TEST_CASES = [
    # (movie_key, expected_tmdb_id, description)
    # Basic title + year
    ("spectre|2015", 206647, "Simple title+year"),
    ("ready player one|2018", 333339, "Simple title+year"),
    ("the matrix|1999", 603, "Article handling"),
    ("toy story 4|2019", 301528, "Numbered sequel"),
    # DHT noise stripping
    ("insidious the last key exki|2018", 406563, "Release group noise"),
    ("enemy at the gates hddvd dd5 1 lord|2001", 853, "Heavy quality noise"),
    ("toy story 4 800mb galaxyrg|2019", 301528, "File size + release group"),
    (
        "fast and furious presents hobbs and shaw hc hdrip xvid ac3 evo|2019",
        384018,
        "Very noisy (Hobbs & Shaw)",
    ),
    # International titles (via alt titles)
    ("perfetti sconosciuti|2016", 381341, "Italian title (Perfect Strangers)"),
    ("la boum 2|1982", 171, "French title (Party 2)"),
    # Roman/Arabic numerals (via alt titles)
    ("ghostbusters ii|1989", 2978, "Roman numeral"),
    ("ghostbusters 2|1989", 2978, "Arabic numeral variant"),
    ("the godfather ii|1974", 240, "Roman numeral (alt title)"),
    ("the godfather 2|1974", 240, "Arabic variant (alt title)"),
    # Should NOT match (anime/series)
    ("naruto shippuuden 252 306", None, "Anime episode - should not match"),
    ("mahou shoujo ore 01", None, "Anime episode - should not match"),
    # Cyrillic (transliterated)
    ("один дома|1990", 771, "Cyrillic: Home Alone"),
    # Article at end (Radarr handles this)
    ("matrix, the|1999", 603, "Article at end"),
]


def run_tests():
    print("Movie Matching Smoke Tests")
    print("=" * 60)

    if not INDEX_PATH.exists():
        print(f"ERROR: TMDB index not found at {INDEX_PATH}")
        return False

    index_conn = sqlite3.connect(str(INDEX_PATH))
    index_conn.row_factory = sqlite3.Row

    passed = 0
    failed = 0

    for key, expected_tmdb, description in TEST_CASES:
        # Parse key
        if "|" in key:
            parts = key.split("|")
            raw_title = parts[0].strip()
            year = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
        else:
            raw_title = key
            import re

            year_match = re.search(r"\b((?:19|20)\d{2})\b", key)
            year = int(year_match.group(1)) if year_match else None

        # Clean and match
        cleaned = clean_movie_key(raw_title)
        clean = radarr_clean_title(cleaned)

        result = find_movie_v2(index_conn, clean, year)
        actual_tmdb = result["tmdb_id"] if result else None

        # Check result
        if expected_tmdb is None:
            success = actual_tmdb is None
        else:
            success = actual_tmdb == expected_tmdb

        if success:
            passed += 1
            status = "PASS"
        else:
            failed += 1
            status = "FAIL"

        print(f"  {status}: {description}")
        print(f"       key='{key}'")
        print(f"       clean='{clean}', year={year}")
        print(f"       expected={expected_tmdb}, got={actual_tmdb}")
        print()

    index_conn.close()

    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print()

    return failed == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
