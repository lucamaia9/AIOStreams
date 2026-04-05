#!/usr/bin/env python3
"""
Smoke test for match_movies_to_tmdb.py - processes N grouped keys to verify speed.
"""

import argparse
import contextlib
import io
import json
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from match_movies_to_tmdb import TMDBIndex, match_movie_key_with_source

TMDB_INDEX = Path(__file__).parent.parent / "data" / "tmdb" / "movie_index.sqlite3"
SQLITE_DB = (
    Path(__file__).parent.parent
    / "data"
    / "comet-fresh"
    / "magnetico"
    / "active.search.sqlite3"
)


def query_top_unresolved_movie_groups(
    conn: sqlite3.Connection, sample_size: int
) -> list[tuple[str, int, str]]:
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
        LIMIT ?
        """,
        (sample_size,),
    ).fetchall()


def count_unresolved_movie_groups(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT 1
                FROM media_index
                WHERE content_class = 'movie'
                  AND movie_key IS NOT NULL
                  AND movie_key != ''
                  AND (imdb_id IS NULL OR imdb_id = '')
                  AND (tmdb_id IS NULL OR tmdb_id = '')
                GROUP BY movie_key
            )
            """
        ).fetchone()[0]
    )


def load_tmdb_index(*, verbose: bool) -> TMDBIndex:
    tmdb_index = TMDBIndex()
    if verbose:
        tmdb_index.load()
        return tmdb_index
    with contextlib.redirect_stdout(io.StringIO()):
        tmdb_index.load()
    return tmdb_index


def run_top_group_smoke(sample_size: int, db_path: Path, *, verbose: bool) -> dict:
    tmdb_index = load_tmdb_index(verbose=verbose)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    rows = query_top_unresolved_movie_groups(conn, sample_size)
    total_keys = count_unresolved_movie_groups(conn)

    start = time.time()
    matched = 0
    unmatched = 0
    examples: list[dict[str, object]] = []
    source_counts: dict[str, int] = {}

    for movie_key, count, representative_name in rows:
        result = match_movie_key_with_source(
            movie_key,
            tmdb_index,
            representative_name=representative_name,
        )
        if result:
            matched += 1
            source_counts[result.source] = source_counts.get(result.source, 0) + 1
            if len(examples) < 5:
                examples.append(
                    {
                        "movie_key": movie_key,
                        "representative_name": representative_name[:160],
                        "matched": True,
                        "tmdb_id": result.tmdb_id,
                        "imdb_id": result.imdb_id,
                        "source": result.source,
                    }
                )
        else:
            unmatched += 1
            if len(examples) < 5:
                examples.append(
                    {
                        "movie_key": movie_key,
                        "representative_name": representative_name[:160],
                        "matched": False,
                    }
                )

    elapsed = time.time() - start
    rate = len(rows) / max(elapsed, 0.001)
    report = {
        "mode": "top_group_smoke",
        "db_path": str(db_path),
        "sample_size": len(rows),
        "matched": matched,
        "unmatched": unmatched,
        "match_rate_pct": round(100.0 * matched / max(len(rows), 1), 1),
        "elapsed_seconds": round(elapsed, 2),
        "keys_per_second": round(rate, 2),
        "estimated_total_keys": total_keys,
        "estimated_runtime_hours": round(total_keys / max(rate, 0.001) / 3600, 2),
        "source_counts": source_counts,
        "examples": examples,
    }
    conn.close()
    return report


def main():
    parser = argparse.ArgumentParser(description="Smoke test the top unresolved movie groups")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="Number of top unresolved movie groups to inspect.",
    )
    parser.add_argument(
        "--db-path",
        default=str(SQLITE_DB),
        help="Path to the SQLite search DB.",
    )
    parser.add_argument("--json", action="store_true", help="Emit a JSON report.")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.is_absolute():
        db_path = (Path(__file__).parent.parent / db_path).resolve()

    if args.json:
        report = run_top_group_smoke(args.sample_size, db_path, verbose=False)
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return

    print(f"Smoke test: Processing {args.sample_size} movie keys")
    print("Loading TMDB index...")
    report = run_top_group_smoke(args.sample_size, db_path, verbose=True)

    print(f"\nFetching {report['sample_size']} unresolved movie keys...")
    print(f"Got {report['sample_size']} keys")
    print(f"\nProcessing {report['sample_size']} keys...")
    for index, example in enumerate(report["examples"], 1):
        if example["matched"]:
            print(
                f"  [{index}] MATCH: '{example['movie_key']}' -> "
                f"TMDB:{example['tmdb_id']}, IMDB:{example['imdb_id']}"
            )
        else:
            print(f"  [{index}] MISS: '{example['movie_key']}'")

    print(f"\nResults: {report['matched']} matched, {report['unmatched']} unmatched")
    print(f"Match rate: {report['match_rate_pct']:.1f}%")
    print(f"Time: {report['elapsed_seconds']:.2f}s ({report['keys_per_second']:.0f} keys/sec)")
    if report["source_counts"]:
        print(
            "Match sources: "
            + ", ".join(
                f"{source}={count}"
                for source, count in sorted(report["source_counts"].items())
            )
        )
    print(f"\nExtrapolation for {report['estimated_total_keys']:,} keys:")
    print(f"  Estimated time: {report['estimated_runtime_hours']:.1f} hours")



if __name__ == "__main__":
    main()
