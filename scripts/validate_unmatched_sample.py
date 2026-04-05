#!/usr/bin/env python3
"""
Validation script to determine what percentage of unmatched items are actually
valid media (movie/series/anime) vs noise/rejects.

This helps answer: "How much real media are we actually missing?"

Usage:
    python3 validate_unmatched_sample.py [--sample N] [--category CATEGORY]

Categories:
    - series: episode/multi_episode/season_pack
    - anime: anime_episode/anime_pack/anime
    - movie: movie
    - all: all categories
"""

import argparse
import json
import random
import sqlite3
import sys
from collections import Counter
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from lib.classifier_runtime import bootstrap_classifier_runtime

bootstrap_classifier_runtime(__file__)


CATEGORIES = {
    "series": ("episode", "multi_episode", "season_pack"),
    "anime": ("anime_episode", "anime_pack", "anime"),
    "movie": ("movie",),
    "all": (
        "episode",
        "multi_episode",
        "season_pack",
        "anime_episode",
        "anime_pack",
        "anime",
        "movie",
        "unknown_video",
    ),
}


def classify_sample(name: str, content_class: str) -> str:
    """
    Classify a sample as either:
    - VALID_MEDIA: Looks like real movie/series/anime
    - FOREIGN: Non-English or foreign content
    - NOISE: Scene releases, quality tags, partial titles
    - REJECT: Clearly reject-worthy content
    - UNCERTAIN: Can't determine
    """
    name_lower = name.lower()

    # Check for clearly reject-worthy
    reject_patterns = [
        "sample",
        "preview",
        "trailer",
        "excerpt",
        "1080i",
        "480i",
        "576i",  # Interlaced content
        "xxx",
        "adult",
        "porn",
        "bookmark",
        "catalog",
        "nfo",
        "sfv",
        "m3u",
    ]
    if any(p in name_lower for p in reject_patterns):
        return "REJECT"

    # Check for scene release patterns (often incomplete)
    scene_patterns = [
        "- Simpsons",
        "- Family Guy",
        "- South Park",  # TV but might be scene
    ]

    # Check for partial/fragmentary titles
    if len(name) < 10:
        return "NOISE"

    # Check for quality/release patterns that indicate real media
    quality_patterns = [
        "720p",
        "1080p",
        "2160p",
        "4k",
        "uhd",
        "bluray",
        "blu-ray",
        "dvdrip",
        "webrip",
        "webdl",
        "hdtv",
        "web",
        "bdrip",
        "s01",
        "s02",
        "s03",
        "s04",
        "s05",  # Season patterns
        "e01",
        "e02",
        "e03",
        "e04",
        "e05",  # Episode patterns
        "season",
        "episode",
    ]
    if any(p in name_lower for p in quality_patterns):
        # Has quality info - likely real media
        return "VALID_MEDIA"

    # Check for foreign indicators
    foreign_patterns = [
        # Non-Latin scripts
        "\u4e00-\u9fff",  # Chinese
        "\u3040-\u309f",
        "\u30a0-\u30ff",  # Japanese
        "\uac00-\ud7af",  # Korean
        "\u0400-\u04ff",  # Cyrillic
        # Known foreign words
        "espanol",
        "espa",
        "francais",
        "french",
        "german",
        "italiano",
        "italian",
        "portugues",
        "brasil",
        "hindi",
        "arabic",
        "turkish",
        "korean",
        "chinese",
        "japanese",
        "subtitulado",
        "dubbed",
        "dublado",
    ]
    for p in foreign_patterns:
        if p in name_lower:
            return "FOREIGN"

    # Check for series-like patterns (title with season/episode info)
    series_patterns = [
        "s01",
        "s02",
        "s03",
        "s04",
        "s05",
        "s06",
        "s07",
        "s08",
        "s09",  # Season
        "season",
        "episode",
        "ep01",
        "ep02",
        "ep03",
    ]
    if any(p in name_lower for p in series_patterns):
        return "VALID_MEDIA"

    # If it has a title-like structure (multiple words, reasonable length)
    words = name.split()
    if len(words) >= 2 and all(len(w) >= 2 for w in words[:5]):
        return "VALID_MEDIA"

    return "UNCERTAIN"


def reservoir_sample_rows(
    cursor: sqlite3.Cursor, sample_size: int, seed: int
) -> tuple[list[sqlite3.Row], int]:
    rng = random.Random(seed)
    sample: list[sqlite3.Row] = []
    total = 0
    for row in cursor:
        total += 1
        if len(sample) < sample_size:
            sample.append(row)
            continue
        replacement_index = rng.randrange(total)
        if replacement_index < sample_size:
            sample[replacement_index] = row
    return sample, total


def build_insight(total_valid: int, total_items: int) -> str:
    if total_items <= 0:
        return "No unmatched items sampled."
    if total_valid > total_items * 0.5:
        return "Majority of unmatched items still look like recoverable media."
    if total_valid > total_items * 0.2:
        return "A meaningful minority of unmatched items still look recoverable."
    return "Most unmatched items in this sample look like noise or rejects."


def sample_unmatched_categories(
    conn: sqlite3.Connection,
    *,
    category: str,
    sample_size: int,
    seed: int,
) -> dict:
    categories = CATEGORIES.get(category, CATEGORIES["all"])
    report: dict[str, object] = {
        "category": category,
        "seed": seed,
        "sample_size_requested": sample_size,
        "categories": {},
    }
    total_valid = 0
    total_items = 0

    for index, cat in enumerate(categories):
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT torrent_id, name, title_key, content_class
            FROM media_index
            WHERE content_class = ?
              AND tmdb_id IS NULL
              AND imdb_id IS NULL
              AND name IS NOT NULL
            ORDER BY torrent_id
            """,
            (cat,),
        )
        rows, population = reservoir_sample_rows(cursor, sample_size, seed + index)
        if not rows:
            report["categories"][cat] = {
                "population": population,
                "sampled": 0,
                "counts": {},
                "percentages": {},
                "samples": [],
            }
            continue

        classification_counts = Counter()
        samples = []
        for row in rows:
            name = row["name"] or ""
            title_key = row["title_key"] or ""
            classification = classify_sample(name, cat)
            classification_counts[classification] += 1
            samples.append(
                {
                    "torrent_id": int(row["torrent_id"]),
                    "name": name[:160],
                    "title_key": title_key[:160],
                    "classification": classification,
                }
            )

        valid = classification_counts.get("VALID_MEDIA", 0)
        foreign = classification_counts.get("FOREIGN", 0)
        noise = classification_counts.get("NOISE", 0)
        reject = classification_counts.get("REJECT", 0)
        uncertain = classification_counts.get("UNCERTAIN", 0)
        total = len(rows)
        total_valid += valid + foreign
        total_items += total

        report["categories"][cat] = {
            "population": population,
            "sampled": total,
            "counts": dict(classification_counts),
            "percentages": {
                "valid_media": round(valid * 100 / max(total, 1), 1),
                "foreign": round(foreign * 100 / max(total, 1), 1),
                "noise": round(noise * 100 / max(total, 1), 1),
                "reject": round(reject * 100 / max(total, 1), 1),
                "uncertain": round(uncertain * 100 / max(total, 1), 1),
            },
            "samples": samples[:20],
        }

    overall = {
        "sampled": total_items,
        "recoverable_media": total_valid,
        "recoverable_pct": round(total_valid * 100 / max(total_items, 1), 1),
        "true_losses": total_items - total_valid,
        "true_losses_pct": round((total_items - total_valid) * 100 / max(total_items, 1), 1),
    }
    overall["insight"] = build_insight(total_valid, total_items)
    report["overall"] = overall
    return report


def main():
    parser = argparse.ArgumentParser(description="Validate unmatched items sample")
    parser.add_argument(
        "--sample", type=int, default=500, help="Number of samples per category"
    )
    parser.add_argument(
        "--category", default="all", choices=["series", "anime", "movie", "all"]
    )
    parser.add_argument(
        "--db-path",
        default="data/comet-fresh/magnetico/active.search.sqlite3",
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=20260404,
        help="Deterministic sampling seed used by per-category reservoir sampling.",
    )
    parser.add_argument("--json", action="store_true", help="Emit a JSON report.")
    args = parser.parse_args()

    db_path = Path(__file__).parent.parent / args.db_path
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    report = sample_unmatched_categories(
        conn,
        category=args.category,
        sample_size=args.sample,
        seed=args.seed,
    )

    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
        conn.close()
        return

    categories = CATEGORIES.get(args.category, CATEGORIES["all"])
    print(f"=== Unmatched Items Validation ===")
    print(f"Categories: {args.category} ({', '.join(categories)})")
    print(f"Sample size: {args.sample} per category")
    print(f"Seed: {args.seed}\n")

    for cat in categories:
        data = report["categories"].get(cat, {})
        sampled = int(data.get("sampled", 0))
        if sampled <= 0:
            print(f"[{cat}] No unmatched items found")
            continue

        counts = data["counts"]
        percentages = data["percentages"]
        print(
            f"[{cat}] Sampling {sampled} unmatched items "
            f"(population={int(data.get('population', 0))})..."
        )
        print(
            f"  VALID_MEDIA: {int(counts.get('VALID_MEDIA', 0)):4d} ({float(percentages.get('valid_media', 0.0)):5.1f}%)"
        )
        print(
            f"  FOREIGN:     {int(counts.get('FOREIGN', 0)):4d} ({float(percentages.get('foreign', 0.0)):5.1f}%)"
        )
        print(
            f"  NOISE:       {int(counts.get('NOISE', 0)):4d} ({float(percentages.get('noise', 0.0)):5.1f}%)"
        )
        print(
            f"  REJECT:      {int(counts.get('REJECT', 0)):4d} ({float(percentages.get('reject', 0.0)):5.1f}%)"
        )
        print(
            f"  UNCERTAIN:   {int(counts.get('UNCERTAIN', 0)):4d} ({float(percentages.get('uncertain', 0.0)):5.1f}%)"
        )
        print()

    overall = report["overall"]
    print("=" * 50)
    print("OVERALL SUMMARY")
    print("=" * 50)
    print(f"Total unmatched items sampled: {int(overall['sampled'])}")
    print(
        f"Potentially recoverable media: {int(overall['recoverable_media'])} ({float(overall['recoverable_pct']):.1f}%)"
    )
    print(
        f"True losses (noise/reject): {int(overall['true_losses'])} ({float(overall['true_losses_pct']):.1f}%)"
    )
    print()
    print("KEY INSIGHT:")
    print(f"  {overall['insight']}")

    print("\nSample items for manual review:")
    for cat, data in report["categories"].items():
        print(f"\n[{cat}] - Sample unmatched names:")
        for sample in data["samples"][:5]:
            print(f"  [{sample['classification']:12s}] {sample['name']}")

    conn.close()


if __name__ == "__main__":
    main()
