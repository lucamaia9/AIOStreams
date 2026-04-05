#!/usr/bin/env python3
"""
Canonical Promotion Worker

Syncs enriched content from Postgres to SQLite canonical layer.
Uses cursor-based polling for efficiency (no full table scans).

Usage:
    python3 canonical_promotion_worker.py [--options]

Environment:
    SEARCH_DB_PATH: Path to SQLite search database
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_SEARCH_DB = Path(
    "/home/ubuntu/aiostreams/data/comet-fresh/magnetico/active.search.sqlite3"
)
DEFAULT_CHECKPOINT = Path(
    "/home/ubuntu/aiostreams/data/comet-fresh/magnetico/canonical-promotion.checkpoint.json"
)
DEFAULT_CONTAINER = "bitmagnet-postgres"
BATCH_SIZE = 200
SLEEP_INTERVAL = 30

YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")
WORD_PATTERN = re.compile(r"[^\W_]+", re.UNICODE)
MULTISPACE = re.compile(r"\s+")

TITLE_CLEAN_PATTERNS = [
    re.compile(r"\[[^\]]*\]"),
    re.compile(r"\([^\)]*\)"),
    re.compile(r"\{[^\}]*\}"),
    re.compile(
        r"\b(?:2160p|1080p|1080i|720p|576p|540p|480p|360p|4k|8k|uhd|hdr|webrip|web[- .]?dl|bluray|brrip|bdrip|hdtv|dvdrip|x264|x265|hevc|avc|aac|dts|truehd|atmos|10bit|remux|proper|repack|internal|multi|dual)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\b(?:mkv|mp4|avi|wmv|mov|webm|ts|m4v|mpeg|mpg)\b", re.IGNORECASE),
    re.compile(
        r"\b(?:s\d{1,2}e\d{1,3}(?:[ ._\-]*e?\d{1,3})*|\d{1,2}x\d{1,3}(?:[ ._\-]*(?:x|e)?\d{1,3})*|season\s*\d{1,2}|episode\s*\d{1,3}|ep\s*\d{1,3}|complete\s*(?:season|series))\b",
        re.IGNORECASE,
    ),
    YEAR_PATTERN,
]


def normalize_title(text: str) -> str:
    """Normalize a title for matching."""
    cleaned = unicodedata.normalize("NFKC", str(text or ""))
    cleaned = (
        cleaned.replace("_", " ").replace(".", " ").replace("/", " ").replace("\\", " ")
    )
    for pattern in TITLE_CLEAN_PATTERNS:
        cleaned = pattern.sub(" ", cleaned)
    cleaned = re.sub(r"[^\w\s-]", " ", cleaned, flags=re.UNICODE)
    return MULTISPACE.sub(" ", cleaned).strip().lower()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Canonical promotion worker")
    parser.add_argument("--search-db", default=str(DEFAULT_SEARCH_DB))
    parser.add_argument("--checkpoint", default=str(DEFAULT_CHECKPOINT))
    parser.add_argument("--container", default=DEFAULT_CONTAINER)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--sleep-interval", type=int, default=SLEEP_INTERVAL)
    parser.add_argument("--dry-run", action="store_true", help="Don't write changes")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    parser.add_argument(
        "--max-cycles", type=int, default=0, help="Max cycles (0 = unlimited)"
    )
    return parser.parse_args()


def run_psql(container: str, query: str) -> list[dict]:
    """Execute a query in Postgres container."""
    cmd = [
        "sudo",
        "docker",
        "exec",
        "-i",
        container,
        "psql",
        "-U",
        "postgres",
        "-d",
        "bitmagnet",
        "-t",
        "-A",
        "-c",
        query,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Postgres query failed: {result.stderr}")

    rows = []
    for line in result.stdout.strip().split("\n"):
        if line.strip():
            values = [v.strip() for v in line.split("|")]
            if values and any(v for v in values if v):
                rows.append(values)
    return rows


def load_checkpoint(path: Path) -> dict[str, Any]:
    """Load checkpoint from file."""
    if not path.exists():
        return {"lastCreatedAt": "1970-01-01T00:00:00+00:00", "lastInfoHash": "0" * 40}
    with open(path) as f:
        return json.load(f)


def save_checkpoint(path: Path, checkpoint: dict[str, Any]) -> None:
    """Save checkpoint to file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(checkpoint, f, indent=2)


def fetch_enriched_batch(
    container: str, checkpoint: dict[str, Any], batch_size: int
) -> list[dict]:
    """Fetch newly enriched torrents from Postgres."""
    last_created = checkpoint["lastCreatedAt"]
    last_hash = checkpoint["lastInfoHash"]

    query = f"""
        SELECT 
            encode(t.info_hash, 'hex') as info_hash,
            t.name,
            tc.content_source,
            tc.content_id,
            tc.content_type,
            t.created_at
        FROM torrent_contents tc
        JOIN torrents t ON t.info_hash = tc.info_hash
        WHERE tc.content_source IS NOT NULL
          AND tc.content_id IS NOT NULL
          AND (t.created_at > '{last_created}'::timestamptz
               OR (t.created_at = '{last_created}'::timestamptz AND encode(t.info_hash, 'hex') > '{last_hash}'))
        ORDER BY t.created_at, t.info_hash
        LIMIT {batch_size}
    """

    rows = run_psql(container, query)

    result = []
    for row in rows:
        if len(row) >= 6:
            result.append(
                {
                    "info_hash": row[0].lower(),
                    "name": row[1],
                    "content_source": row[2],
                    "content_id": row[3],
                    "content_type": row[4],
                    "created_at": row[5],
                }
            )
    return result


def get_media_kind(content_type: str) -> str:
    """Map content_type to media_kind."""
    if content_type == "movie":
        return "movie"
    elif content_type == "tv_show":
        return "series"
    return "movie"


def write_to_canonical_layer(
    conn: sqlite3.Connection, batch: list[dict], args: argparse.Namespace
) -> int:
    """Write batch to canonical layer and media_index."""
    if args.dry_run:
        return len(batch)

    written = 0
    cursor = conn.cursor()

    for row in batch:
        try:
            normalized = normalize_title(row["name"])
            if not normalized:
                continue

            media_kind = get_media_kind(row["content_type"])
            family_key = f"{media_kind}:{row['content_id']}"

            # Check if variant already exists
            existing = cursor.execute(
                "SELECT 1 FROM canonical_title_variant WHERE normalized_title = ? AND family_key = ?",
                (normalized, family_key),
            ).fetchone()

            if not existing:
                # Insert family if not exists
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO canonical_title_family (family_key, tmdb_id, imdb_id, canonical_title, canonical_year, media_kind, evidence_count)
                    VALUES (?, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        family_key,
                        row["content_id"] if row["content_source"] == "tmdb" else None,
                        None,
                        None,
                        None,
                        media_kind,
                    ),
                )

                # Insert variant
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO canonical_title_variant (family_key, media_kind, normalized_title, year_hint, season_bucket, evidence_count)
                    VALUES (?, ?, ?, ?, '', 1)
                    """,
                    (family_key, media_kind, normalized, None),
                )

                written += 1
        except Exception as e:
            if args.verbose:
                print(f"Error writing {row['info_hash']}: {e}")

    conn.commit()
    return written


def mark_exported(container: str, info_hashes: list) -> int:
    """Mark torrents as exported in Postgres."""
    if not info_hashes:
        return 0

    # Build the SQL to mark as exported
    hex_hashes = []
    for h in info_hashes:
        if isinstance(h, bytes):
            hex_hashes.append(f"'\\x{h.hex()}'")
        else:
            hex_hashes.append(f"'\\x{h}'")

    sql = f"""
    UPDATE torrent_contents 
    SET exported = true, exported_at = NOW()
    WHERE info_hash IN ({",".join(hex_hashes)})
    AND exported = false
    """

    cmd = [
        "docker",
        "exec",
        "-i",
        container,
        "psql",
        "-U",
        "postgres",
        "-d",
        "bitmagnet",
        "-c",
        sql,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        # Parse "UPDATE N" from output
        match = re.search(r"UPDATE (\d+)", result.stdout)
        return int(match.group(1)) if match else 0
    except Exception:
        return 0


def run_cycle(args: argparse.Namespace, conn: sqlite3.Connection) -> dict[str, int]:
    """Run one promotion cycle."""
    checkpoint = load_checkpoint(Path(args.checkpoint))

    batch = fetch_enriched_batch(args.container, checkpoint, args.batch_size)

    stats = {
        "fetched": len(batch),
        "written": 0,
    }

    if batch:
        written = write_to_canonical_layer(conn, batch, args)
        stats["written"] = written

        # Mark as exported in Postgres
        if written > 0 and not args.dry_run:
            info_hashes = [
                bytes.fromhex(
                    row["info_hash"][2:]
                    if row["info_hash"].startswith("\\x")
                    else row["info_hash"]
                )
                for row in batch
            ]
            marked = mark_exported(args.container, info_hashes)
            stats["marked_exported"] = marked

        # Update checkpoint
        last_row = batch[-1]
        new_checkpoint = {
            "lastCreatedAt": last_row["created_at"],
            "lastInfoHash": last_row["info_hash"],
        }
        save_checkpoint(Path(args.checkpoint), new_checkpoint)

    return stats


def main():
    args = parse_args()

    # Connect to SQLite
    conn = sqlite3.connect(args.search_db)
    conn.row_factory = sqlite3.Row

    print(f"Starting canonical promotion worker (dry_run={args.dry_run})")

    # Check canonical layer
    stats = conn.execute("SELECT COUNT(*) FROM canonical_title_family").fetchone()
    print(f"Canonical families: {stats[0]}")

    cycle_count = 0
    total_written = 0

    while True:
        cycle_count += 1

        try:
            cycle_stats = run_cycle(args, conn)
            total_written += cycle_stats["written"]

            if args.verbose or cycle_stats["fetched"] > 0:
                ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
                print(
                    f"[{ts}] Cycle {cycle_count}: fetched={cycle_stats['fetched']}, written={cycle_stats['written']}, total={total_written}"
                )

        except Exception as e:
            print(f"Error in cycle {cycle_count}: {e}")
            import traceback

            traceback.print_exc()

        if args.once:
            break

        if args.max_cycles > 0 and cycle_count >= args.max_cycles:
            print(f"Reached max cycles ({args.max_cycles})")
            break

        time.sleep(args.sleep_interval)

    conn.close()
    print(f"Done. Total written: {total_written}")


if __name__ == "__main__":
    main()
