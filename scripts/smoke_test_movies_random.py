#!/usr/bin/env python3
"""
Smoke test for match_movies_to_tmdb.py - processes N RANDOM grouped keys.
"""

import argparse
import contextlib
import io
import json
import random
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


def query_all_unresolved_movie_groups(conn: sqlite3.Connection) -> list[tuple[str, int, str]]:
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
        ORDER BY movie_key
        """
    ).fetchall()


def load_tmdb_index(*, verbose: bool) -> TMDBIndex:
    tmdb_index = TMDBIndex()
    if verbose:
        tmdb_index.load()
        return tmdb_index
    with contextlib.redirect_stdout(io.StringIO()):
        tmdb_index.load()
    return tmdb_index


def run_random_smoke(sample_size: int, seed: int, db_path: Path, *, verbose: bool) -> dict:
    tmdb_index = load_tmdb_index(verbose=verbose)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    all_keys = query_all_unresolved_movie_groups(conn)
    rng = random.Random(seed)
    sample = rng.sample(all_keys, min(sample_size, len(all_keys)))
    start = time.time()
    matched = 0
    unmatched = 0
    source_counts: dict[str, int] = {}
    examples: list[dict[str, object]] = []

    for movie_key, count, representative_name in sample:
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
    rate = len(sample) / max(elapsed, 0.001)
    report = {
        "mode": "random_smoke",
        "db_path": str(db_path),
        "seed": seed,
        "sample_size": len(sample),
        "total_unresolved_keys": len(all_keys),
        "matched": matched,
        "unmatched": unmatched,
        "match_rate_pct": round(100.0 * matched / max(len(sample), 1), 1),
        "elapsed_seconds": round(elapsed, 2),
        "keys_per_second": round(rate, 2),
        "estimated_runtime_minutes": round(len(all_keys) / max(rate, 0.001) / 60, 1),
        "expected_matches": int(len(all_keys) * matched / max(len(sample), 1)),
        "source_counts": source_counts,
        "examples": examples,
    }
    conn.close()
    return report


def main():
    parser = argparse.ArgumentParser(
        description="Smoke test a deterministic random sample of unresolved movie groups"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="Number of unresolved movie groups to sample.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=20260404,
        help="Deterministic random seed.",
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
        report = run_random_smoke(args.sample_size, args.seed, db_path, verbose=False)
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return

    print(f"Smoke test (RANDOM): Processing {args.sample_size} movie keys")
    print(f"Seed: {args.seed}")
    print("Loading TMDB index...")
    report = run_random_smoke(args.sample_size, args.seed, db_path, verbose=True)

    print(f"\nFetching ALL unresolved movie keys (for sampling)...")
    print(f"Total unresolved keys: {report['total_unresolved_keys']:,}")
    print(f"Sampled {report['sample_size']} random keys")
    print(f"\nProcessing {report['sample_size']} keys...")
    print(f"\nExamples:")
    for example in report["examples"]:
        if example["matched"]:
            print(
                f"  MATCH: '{example['movie_key']}' -> "
                f"TMDB:{example['tmdb_id']}, IMDB:{example['imdb_id']}"
            )
        else:
            print(f"  MISS: '{example['movie_key']}'")

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
    print(f"\nExtrapolation for {report['total_unresolved_keys']:,} keys:")
    print(f"  Estimated time: {report['estimated_runtime_minutes']:.1f} minutes")
    print(f"  Expected matches: {report['expected_matches']:,}")


if __name__ == "__main__":
    main()
