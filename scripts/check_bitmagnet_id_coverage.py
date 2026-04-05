#!/usr/bin/env python3
"""
Check BitMagnet ID coverage and production SLO proxy metrics from SQLite.

Outputs:
- JSON report to stdout (machine-readable)
- key=value pairs to stdout (for systemd/cron integration)
- Human-readable summary to stderr

Usage:
    python check_bitmagnet_id_coverage.py [--json] [--quiet]
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_SQLITE_DB = Path(
    os.environ.get("SEARCH_DB_PATH", "")
    or Path(__file__).parent.parent
    / "data"
    / "comet-fresh"
    / "magnetico"
    / "active.search.sqlite3"
)


def percent(part: int, whole: int) -> float:
    return round(100.0 * part / max(whole, 1), 2)


def query_sqlite(db_path: Path):
    """Query SQLite for ID coverage metrics."""
    conn = sqlite3.connect(str(db_path))
    results = {}

    # Total rows
    results["total_rows"] = conn.execute("SELECT COUNT(*) FROM media_index").fetchone()[
        0
    ]

    # Coverage by content_class
    class_coverage = {}
    for row in conn.execute("""
        SELECT 
            content_class,
            COUNT(*) as total,
            SUM(CASE WHEN imdb_id IS NOT NULL OR tmdb_id IS NOT NULL THEN 1 ELSE 0 END) as with_ids,
            ROUND(100.0 * SUM(CASE WHEN imdb_id IS NOT NULL OR tmdb_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct
        FROM media_index
        GROUP BY content_class
    """):
        class_coverage[row[0]] = {
            "total": row[1],
            "with_ids": row[2],
            "coverage_pct": row[3],
        }
    results["by_content_class"] = class_coverage

    # Overall coverage
    overall = conn.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN imdb_id IS NOT NULL OR tmdb_id IS NOT NULL THEN 1 ELSE 0 END) as with_ids
        FROM media_index
    """).fetchone()
    results["overall"] = {
        "total": overall[0],
        "with_ids": overall[1],
        "coverage_pct": percent(overall[1], overall[0]),
    }

    # Anime coverage
    anime = conn.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN imdb_id IS NOT NULL OR tmdb_id IS NOT NULL THEN 1 ELSE 0 END) as with_ids
        FROM media_index
        WHERE is_anime = '1'
    """).fetchone()
    results["anime"] = {
        "total": anime[0],
        "with_ids": anime[1],
        "coverage_pct": percent(anime[1], anime[0]),
    }

    # Movie coverage
    movie = conn.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN imdb_id IS NOT NULL OR tmdb_id IS NOT NULL THEN 1 ELSE 0 END) as with_ids
        FROM media_index
        WHERE content_class = 'movie'
    """).fetchone()
    results["movies"] = {
        "total": movie[0],
        "with_ids": movie[1],
        "coverage_pct": percent(movie[1], movie[0]),
    }

    # Episode coverage
    episode = conn.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN imdb_id IS NOT NULL OR tmdb_id IS NOT NULL THEN 1 ELSE 0 END) as with_ids
        FROM media_index
        WHERE content_class IN ('episode', 'multi_episode', 'season_pack')
    """).fetchone()
    results["episodes"] = {
        "total": episode[0],
        "with_ids": episode[1],
        "coverage_pct": percent(episode[1], episode[0]),
    }

    unknown = class_coverage.get("unknown_video", {"total": 0, "with_ids": 0})
    unknown_total = int(unknown["total"])
    unknown_with_ids = int(unknown["with_ids"])
    canonical_missing = int(overall[0] - overall[1])
    results["unknown_video"] = {
        "total": unknown_total,
        "with_ids": unknown_with_ids,
        "without_ids": unknown_total - unknown_with_ids,
        "pct_total_rows": percent(unknown_total, int(overall[0])),
    }
    results["canonical_missing"] = {
        "total": canonical_missing,
        "pct_total_rows": percent(canonical_missing, int(overall[0])),
    }

    conn.close()
    return results


def determine_status(coverage_pct):
    """Determine health status based on coverage percentage."""
    if coverage_pct >= 80:
        return "GREEN"
    elif coverage_pct >= 60:
        return "AMBER"
    else:
        return "RED"


def determine_threshold_status(value_pct: float, threshold_pct: float) -> str:
    return "PASS" if value_pct <= threshold_pct else "FAIL"


def main():
    parser = argparse.ArgumentParser(description="Check BitMagnet ID coverage")
    parser.add_argument("--json", action="store_true", help="Output JSON report")
    parser.add_argument(
        "--quiet", action="store_true", help="Only output key=value pairs"
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_SQLITE_DB),
        help="Path to the SQLite search DB (defaults to SEARCH_DB_PATH or the active DB).",
    )
    parser.add_argument(
        "--category-slo-threshold",
        type=float,
        default=2.0,
        help="Maximum allowed unknown_video percentage of total SQLite rows.",
    )
    parser.add_argument(
        "--canonical-slo-threshold",
        type=float,
        default=2.0,
        help="Maximum allowed percentage of rows without TMDB/IMDb IDs.",
    )
    args = parser.parse_args()
    db_path = Path(args.db_path)
    if not db_path.is_absolute():
        db_path = (Path(__file__).parent.parent / db_path).resolve()

    start = time.time()
    results = query_sqlite(db_path)
    elapsed = time.time() - start

    overall_pct = results["overall"]["coverage_pct"]
    status = determine_status(overall_pct)
    unknown_pct = results["unknown_video"]["pct_total_rows"]
    canonical_missing_pct = results["canonical_missing"]["pct_total_rows"]
    category_slo_status = determine_threshold_status(
        unknown_pct, args.category_slo_threshold
    )
    canonical_slo_status = determine_threshold_status(
        canonical_missing_pct, args.canonical_slo_threshold
    )
    combined_slo_status = (
        "PASS"
        if category_slo_status == "PASS" and canonical_slo_status == "PASS"
        else "FAIL"
    )
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    report = {
        "timestamp": timestamp,
        "db_path": str(db_path),
        "status": status,
        "elapsed_seconds": round(elapsed, 2),
        "overall": results["overall"],
        "anime": results["anime"],
        "movies": results["movies"],
        "episodes": results["episodes"],
        "unknown_video": results["unknown_video"],
        "canonical_missing": results["canonical_missing"],
        "slos": {
            "category_unknown": {
                "threshold_pct": args.category_slo_threshold,
                "actual_pct": unknown_pct,
                "status": category_slo_status,
            },
            "canonical_missing": {
                "threshold_pct": args.canonical_slo_threshold,
                "actual_pct": canonical_missing_pct,
                "status": canonical_slo_status,
            },
            "combined_status": combined_slo_status,
        },
        "by_content_class": results["by_content_class"],
    }

    if args.json:
        print(json.dumps(report, indent=2))
        return

    # key=value pairs for systemd/cron
    print(f"run_id=coverage_{timestamp.replace(':', '').replace('-', '')}")
    print(f"status={status}")
    print(f"total_rows={results['overall']['total']}")
    print(f"with_ids={results['overall']['with_ids']}")
    print(f"coverage_pct={overall_pct}")
    print(f"anime_coverage_pct={results['anime']['coverage_pct']}")
    print(f"movie_coverage_pct={results['movies']['coverage_pct']}")
    print(f"episode_coverage_pct={results['episodes']['coverage_pct']}")
    print(f"unknown_video_rows={results['unknown_video']['total']}")
    print(f"unknown_video_pct_total={unknown_pct}")
    print(f"canonical_missing_rows={results['canonical_missing']['total']}")
    print(f"canonical_missing_pct_total={canonical_missing_pct}")
    print(f"category_slo_status={category_slo_status}")
    print(f"canonical_slo_status={canonical_slo_status}")
    print(f"combined_slo_status={combined_slo_status}")
    print(f"elapsed_seconds={round(elapsed, 2)}")

    if not args.quiet:
        # Human-readable summary to stderr
        print(f"\n{'=' * 60}", file=sys.stderr)
        print(f"BitMagnet ID Coverage Report - {timestamp}", file=sys.stderr)
        print(f"Status: {status} ({overall_pct}%)", file=sys.stderr)
        print(f"{'=' * 60}", file=sys.stderr)
        print(
            f"  Overall:    {results['overall']['with_ids']:>10,} / {results['overall']['total']:>10,} ({overall_pct}%)",
            file=sys.stderr,
        )
        print(
            f"  Anime:      {results['anime']['with_ids']:>10,} / {results['anime']['total']:>10,} ({results['anime']['coverage_pct']}%)",
            file=sys.stderr,
        )
        print(
            f"  Movies:     {results['movies']['with_ids']:>10,} / {results['movies']['total']:>10,} ({results['movies']['coverage_pct']}%)",
            file=sys.stderr,
        )
        print(
            f"  Episodes:   {results['episodes']['with_ids']:>10,} / {results['episodes']['total']:>10,} ({results['episodes']['coverage_pct']}%)",
            file=sys.stderr,
        )
        print(
            f"  Unknown:    {results['unknown_video']['total']:>10,} rows ({unknown_pct}% of total rows)",
            file=sys.stderr,
        )
        print(
            f"  Missing IDs:{results['canonical_missing']['total']:>10,} rows ({canonical_missing_pct}% of total rows)",
            file=sys.stderr,
        )
        print("\nSLO proxy checks:", file=sys.stderr)
        print(
            f"  Category unknown <= {args.category_slo_threshold}%: {category_slo_status} ({unknown_pct}%)",
            file=sys.stderr,
        )
        print(
            f"  Canonical missing <= {args.canonical_slo_threshold}%: {canonical_slo_status} ({canonical_missing_pct}%)",
            file=sys.stderr,
        )
        print(f"\nBy content class:", file=sys.stderr)
        for cls, data in sorted(results["by_content_class"].items()):
            print(
                f"  {cls:<20} {data['with_ids']:>10,} / {data['total']:>10,} ({data['coverage_pct']}%)",
                file=sys.stderr,
            )
        print(f"{'=' * 60}", file=sys.stderr)

    # Exit code based on overall coverage or SLO proxy failures
    if status == "RED" or combined_slo_status == "FAIL":
        sys.exit(1)


if __name__ == "__main__":
    main()
