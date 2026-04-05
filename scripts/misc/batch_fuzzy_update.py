#!/usr/bin/env python3
"""
Batch update series/anime TMDB IDs using fuzzy matching.

This script processes all unmatched series/anime rows and attempts to find
TMDB IDs using rapidfuzz fuzzy matching.

Usage:
    python3 batch_fuzzy_update.py [--limit N] [--dry-run] [--batch-size BATCH]

Performance:
    - ~100 keys/sec
    - Full run (590K series + 27K anime): ~2 hours
"""

import argparse
import sqlite3
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

# Add paths for imports
sys.path.insert(0, str(REPO_ROOT / "bitmagnet-media" / "classifier"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from fuzzy_series_matcher import FuzzySeriesMatcher
import lean_media_contract as lean_contract
from lib.radarr_parser import clean_movie_title


def resolve_db_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def unresolved_query() -> str:
    return """
        SELECT torrent_id, info_hash, title_key, content_class, year,
               norm_title, name, canonical_title_key, movie_key, series_key,
               aliases_text, imdb_id, tmdb_id, season, episode_start, episode_end,
               exact_episode_key, season_pack_key
        FROM media_index
        WHERE content_class IN ('episode', 'multi_episode', 'season_pack',
                               'anime_episode', 'anime_pack', 'anime')
          AND (tmdb_id IS NULL OR tmdb_id = '')
          AND title_key IS NOT NULL
          AND title_key != ''
        ORDER BY torrent_id
    """


def main():
    parser = argparse.ArgumentParser(
        description="Batch update series TMDB IDs using fuzzy matching"
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Limit number of keys to process"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Don't actually update database"
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Commit every N updates"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="data/comet-fresh/magnetico/active.search.sqlite3",
        help="Path to SQLite database",
    )
    args = parser.parse_args()

    # Connect to database
    db_path = resolve_db_path(args.db_path)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)

    print(f"Connecting to database: {db_path}")
    read_conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    read_conn.row_factory = sqlite3.Row
    write_conn: sqlite3.Connection | None = None
    write_cursor: sqlite3.Cursor | None = None
    if not args.dry_run:
        write_conn = sqlite3.connect(str(db_path))
        write_conn.row_factory = sqlite3.Row
        write_conn.execute("PRAGMA journal_mode=WAL")
        write_conn.execute("PRAGMA synchronous=NORMAL")
        write_conn.execute("PRAGMA busy_timeout=30000")
        write_cursor = write_conn.cursor()

    # Load fuzzy matcher
    print("Loading fuzzy matcher...")
    start = time.time()
    fuzzy_matcher = FuzzySeriesMatcher()
    print(f"Loaded {len(fuzzy_matcher.titles)} titles in {time.time() - start:.1f}s")

    # Get unmatched keys
    print("\nFetching unmatched series keys...")
    read_cursor = read_conn.cursor()
    total_unresolved = read_cursor.execute(
        """
        SELECT COUNT(*)
        FROM media_index
        WHERE content_class IN ('episode', 'multi_episode', 'season_pack',
                               'anime_episode', 'anime_pack', 'anime')
          AND (tmdb_id IS NULL OR tmdb_id = '')
          AND title_key IS NOT NULL
          AND title_key != ''
        """
    ).fetchone()[0]
    total = min(total_unresolved, args.limit) if args.limit else total_unresolved
    read_cursor.execute(unresolved_query())

    print(f"Processing {total} unmatched keys...")

    matches = 0
    no_matches = 0
    errors = 0
    commit_count = 0
    processed = 0
    start_time = time.time()
    read_batch_size = max(args.batch_size * 4, 1000)

    while processed < total:
        fetch_count = min(read_batch_size, total - processed)
        rows = read_cursor.fetchmany(fetch_count)
        if not rows:
            break

        for row in rows:
            try:
                key = row["title_key"]
                year = row["year"]
                row_id = row["info_hash"]

                # Clean the key
                cleaned = clean_movie_title(key)
                if not cleaned:
                    no_matches += 1
                    continue

                # Try fuzzy match
                result = fuzzy_matcher.find(cleaned, year)
                if result:
                    tmdb_id, imdb_id = result
                    if not args.dry_run:
                        row_payload = dict(row)
                        old_search_text = lean_contract.search_text_for(row_payload)
                        row_payload["tmdb_id"] = str(tmdb_id)
                        row_payload["imdb_id"] = imdb_id
                        new_search_text = lean_contract.search_text_for(row_payload)
                        write_cursor.execute(
                            """
                            UPDATE media_index
                            SET tmdb_id = ?, imdb_id = ?
                            WHERE info_hash = ?
                              AND (tmdb_id IS NULL OR tmdb_id = '')
                        """,
                            (str(tmdb_id), imdb_id, row_id),
                        )
                        if write_cursor.rowcount > 0:
                            write_cursor.execute(
                                "INSERT INTO media_fts(media_fts, rowid, search_text) VALUES('delete', ?, ?)",
                                (int(row["torrent_id"]), old_search_text),
                            )
                            write_cursor.execute(
                                "INSERT INTO media_fts(rowid, search_text) VALUES (?, ?)",
                                (int(row["torrent_id"]), new_search_text),
                            )
                            commit_count += 1
                            if commit_count >= args.batch_size:
                                write_conn.commit()
                                commit_count = 0
                    matches += 1
                else:
                    no_matches += 1

            except Exception as e:
                errors += 1
                print(f"Error processing row: {e}")

            processed += 1
            if processed % 10000 == 0:
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                remaining = (total - processed) / rate if rate > 0 else 0
                print(
                    f"  Progress: {processed}/{total} ({processed * 100 / total:.1f}%) - "
                    f"{matches} matches ({matches * 100 / processed:.1f}%) - "
                    f"{rate:.0f} keys/sec - ETA: {remaining / 60:.1f} min"
                )

    # Final commit
    if commit_count > 0 and not args.dry_run:
        write_conn.commit()

    elapsed = time.time() - start_time
    print(f"\n=== Final Results ===")
    print(f"Total processed: {matches + no_matches + errors}")
    print(f"Matches: {matches} ({matches * 100 / max(matches + no_matches, 1):.1f}%)")
    print(f"No matches: {no_matches}")
    print(f"Errors: {errors}")
    print(f"Time: {elapsed / 60:.1f} minutes")
    print(f"Rate: {(matches + no_matches + errors) / elapsed:.0f} keys/sec")

    if args.dry_run:
        print("\n[Dry run - no database updates were made]")

    if write_conn is not None:
        write_conn.close()
    read_conn.close()


if __name__ == "__main__":
    main()
