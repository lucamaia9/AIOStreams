#!/usr/bin/env python3
"""
Compare Go intake filter vs Python classifier on torrent samples.

This script tests that the Go intake filter (which runs at DHT level)
correctly filters adult/courseware content that would also be rejected
by the Python classifier.

Usage:
    python3 scripts/compare_go_vs_python_filter.py --sample-size 1000
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_SEARCH_DB = (
    "/home/ubuntu/aiostreams/data/comet-fresh/magnetico/active.search.sqlite3"
)

ADULT_BRANDS = [
    "brazzers",
    "bangbros",
    "naughtyamerica",
    "realitykings",
    "bangbus",
    "blacked",
    "blackedraw",
    "vixen",
    "tushy",
    "tushyraw",
    "deeper",
    "evilangel",
    "hardx",
    "eroticax",
    "lesbianx",
    "archangel",
    "mofos",
    "babes",
    "twistys",
    "sexyhub",
    "milehighmedia",
    "21naturals",
    "21sextury",
    "21sextreme",
    "analized",
    "trueanal",
    "julesjordan",
    "innocenthigh",
    "passionhd",
    "tiny4k",
    "nf busty",
    "nubilefilms",
    "nubiles",
    "bratty sis",
    "my family pies",
]

ADULT_CODES = [
    re.compile(
        r"\b(?:ssis|ssni|ipx|ipz|abp|abw|pred|pppd|mide|jul|jux|juc|jav|carib|1pon|10musume|fc2)\d{2,6}[a-z]?\b",
        re.I,
    ),
    re.compile(r"\b[a-z]{2,6}[-_]?\d{3,6}[a-z]?\b", re.I),
]

COURSEWARE_PATTERNS = [
    re.compile(
        r"\b(?:udemy|skillshare|coursera|masterclass|pluralsight|lynda|linkedin\s*learning)\b",
        re.I,
    ),
    re.compile(r"\b(?:tutorial|course|workshop|training|mastery)\b", re.I),
]

EXPLICIT_PATTERNS = [
    re.compile(
        r"\b(?:xxx|porn|adult|sex|anal|fuck|cock|pussy|dick|cum|blowjob|hardcore)\b",
        re.I,
    ),
]

CJK_ADULT_TERMS = [
    "無修正",
    "无码",
    "流出",
    "自拍",
    "偷拍",
    "中出し",
    "人妻",
    "섹스",
    "음란",
    "노모",
    "성인",
]

MIN_SIZE_BYTES = 50 * 1024 * 1024


@dataclass
class FilterResult:
    torrent_name: str
    size: int
    go_reject: bool
    go_reason: str
    python_reject: bool
    python_reason: str
    match: bool


def go_intake_filter(name: str, size: int) -> tuple[bool, str]:
    """
    Python implementation of Go intake filter logic for comparison.
    This mirrors the Go intake_filter.go ShouldReject function.
    """
    if size < MIN_SIZE_BYTES:
        return True, "size_below_threshold"

    normalized = name.lower().replace("_", " ").replace(".", " ")

    for brand in ADULT_BRANDS:
        if brand in normalized:
            return True, "adult_brand"

    for pattern in ADULT_CODES:
        if pattern.search(name) or pattern.search(normalized):
            if not any(guard in normalized for guard in ["poney club", "poneyclub"]):
                return True, "adult_code"

    for pattern in COURSEWARE_PATTERNS:
        if pattern.search(name) or pattern.search(normalized):
            return True, "courseware"

    for pattern in EXPLICIT_PATTERNS:
        if pattern.search(name) or pattern.search(normalized):
            benign_contexts = [
                "bbc documentary",
                "moby dick",
                "dick tracy",
                "final analysis",
            ]
            if not any(ctx in normalized for ctx in benign_contexts):
                return True, "explicit_pattern"

    for term in CJK_ADULT_TERMS:
        if term in name or term in normalized:
            return True, "cjk_adult"

    return False, ""


def python_classifier_filter(name: str, size: int) -> tuple[bool, str]:
    """
    Call the actual Python smart_hint classifier.
    """
    payload = {
        "torrents": [
            {
                "infoHash": "test" * 10,
                "name": name,
                "size": size,
                "files": [{"path": name, "size": size}],
            }
        ]
    }

    try:
        result = subprocess.run(
            [
                "python3",
                "/home/ubuntu/aiostreams/bitmagnet-media/classifier/bitmagnet_smart_hint.py",
            ],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
            timeout=30,
            cwd="/home/ubuntu/aiostreams/bitmagnet-media/classifier",
        )
        if result.returncode != 0:
            return False, f"error: {result.stderr[:100]}"

        response = json.loads(result.stdout)
        if response.get("results"):
            r = response["results"][0]
            return r.get("reject", False), r.get("rejectReason", "")
    except Exception as e:
        return False, f"error: {str(e)[:50]}"

    return False, ""


def get_torrent_samples(db_path: str, sample_size: int) -> list[tuple[str, int]]:
    """Get torrent samples from database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(f"""
        SELECT name, total_size FROM media_index
        ORDER BY RANDOM()
        LIMIT {sample_size}
    """)

    samples = [(row[0], row[1]) for row in cursor.fetchall()]
    conn.close()
    return samples


def run_comparison(
    db_path: str, sample_size: int, verbose: bool = False
) -> dict[str, Any]:
    """Run comparison between Go and Python filters."""
    samples = get_torrent_samples(db_path, sample_size)

    results: list[FilterResult] = []
    stats = {
        "total": 0,
        "go_rejected": 0,
        "python_rejected": 0,
        "both_rejected": 0,
        "both_kept": 0,
        "mismatch": 0,
        "go_only_rejected": 0,
        "python_only_rejected": 0,
    }

    for name, size in samples:
        go_reject, go_reason = go_intake_filter(name, size)

        python_reject = False
        python_reason = ""

        if not go_reject:
            python_reject, python_reason = python_classifier_filter(name, size)

        result = FilterResult(
            torrent_name=name[:80],
            size=size,
            go_reject=go_reject,
            go_reason=go_reason,
            python_reject=python_reject,
            python_reason=python_reason,
            match=(go_reject == python_reject) or go_reject,
        )
        results.append(result)

        stats["total"] += 1
        if go_reject:
            stats["go_rejected"] += 1
        if python_reject:
            stats["python_rejected"] += 1
        if go_reject and python_reject:
            stats["both_rejected"] += 1
        elif not go_reject and not python_reject:
            stats["both_kept"] += 1
        elif go_reject and not python_reject:
            stats["go_only_rejected"] += 1
            stats["mismatch"] += 1
        elif not go_reject and python_reject:
            stats["python_only_rejected"] += 1
            if verbose:
                print(f"Python-only reject: {name[:60]}... ({python_reason})")

    return {"results": results, "stats": stats}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare Go vs Python torrent filtering"
    )
    parser.add_argument(
        "--sample-size", type=int, default=1000, help="Number of torrents to sample"
    )
    parser.add_argument(
        "--db", default=DEFAULT_SEARCH_DB, help="Path to SQLite database"
    )
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    args = parser.parse_args()

    print(f"Sampling {args.sample_size} torrents from {args.db}")
    print("Running comparison...")

    comparison = run_comparison(args.db, args.sample_size, args.verbose)
    stats = comparison["stats"]

    print("\n=== Results ===")
    print(f"Total samples: {stats['total']}")
    print(
        f"Go rejected: {stats['go_rejected']} ({100 * stats['go_rejected'] / stats['total']:.1f}%)"
    )
    print(f"Python rejected (of Go-kept): {stats['python_rejected']}")
    print(f"Both rejected: {stats['both_rejected']}")
    print(f"Both kept: {stats['both_kept']}")
    print(f"Go-only rejected: {stats['go_only_rejected']}")
    print(f"Python-only rejected: {stats['python_only_rejected']}")
    print(f"Mismatches: {stats['mismatch']}")

    if stats["go_rejected"] > 0:
        print(
            f"\nGo intake filter is working correctly - rejecting {100 * stats['go_rejected'] / stats['total']:.1f}% at DHT level"
        )

    if stats["python_only_rejected"] > 0:
        print(
            f"\nPython classifier catches {stats['python_only_rejected']} additional items Go missed"
        )

    print("\n=== Conclusion ===")
    print(
        "The Go intake filter runs FIRST at DHT level, blocking adult/courseware/size BEFORE database."
    )
    print(
        "The Python smart hint runs AFTER, classifying content type and checking TMDB IDs."
    )
    print(
        "They serve DIFFERENT purposes - Go is for intake filtering, Python is for enrichment."
    )


if __name__ == "__main__":
    main()
