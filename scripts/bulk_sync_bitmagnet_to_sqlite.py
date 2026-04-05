#!/usr/bin/env python3
"""
Bulk sync existing BitMagnet content to SQLite.

This script handles the backlog of content already in Postgres
that hasn't been promoted to SQLite yet.

Usage:
    python3 scripts/bulk_sync_bitmagnet_to_sqlite.py --batch-size 1000 --limit 10000
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

DEFAULT_POSTGRES_CONTAINER = "bitmagnet-postgres"
DEFAULT_SEARCH_DB = (
    "/home/ubuntu/aiostreams/data/comet-fresh/magnetico/active.search.sqlite3"
)
DEFAULT_PROMOTION_SCRIPT = "/home/ubuntu/aiostreams/scripts/promote_to_sqlite.py"


def get_postgres_connection():
    """Get connection to BitMagnet Postgres."""
    import psycopg2

    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="bitmagnet",
        user="postgres",
        password="postgres",
    )


def fetch_batch(conn, offset: int, limit: int) -> list[dict]:
    """Fetch a batch of content from Postgres."""
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT 
            encode(t.info_hash, 'hex') as info_hash,
            t.name,
            t.size,
            tc.content_type,
            tc.content_source,
            tc.content_id,
            tc.episodes::text,
            tc.video_resolution::text,
            tc.video_source::text,
            tc.video_codec::text,
            EXTRACT(EPOCH FROM tc.created_at)::int as created_at_ts
        FROM torrents t
        JOIN torrent_contents tc ON tc.info_hash = t.info_hash
        WHERE tc.content_source IS NOT NULL
        ORDER BY tc.created_at DESC
        OFFSET {offset} LIMIT {limit}
    """)

    columns = [desc[0] for desc in cursor.description]
    rows = []
    for row in cursor.fetchall():
        row_dict = dict(zip(columns, row))
        # Parse episodes JSON
        if row_dict.get("episodes"):
            try:
                row_dict["episodes"] = json.loads(row_dict["episodes"])
            except json.JSONDecodeError:
                row_dict["episodes"] = None
        rows.append(row_dict)

    cursor.close()
    return rows


def get_existing_hashes(db_path: str) -> set[str]:
    """Get set of existing info_hashes in SQLite."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT info_hash FROM media_index")
    hashes = {row[0] for row in cursor.fetchall()}
    conn.close()
    return hashes


def promote_batch(contents: list[dict], script_path: str, db_path: str) -> dict:
    """Promote a batch of content to SQLite."""
    payload = {
        "contents": contents,
        "search_db": db_path,
    }

    result = subprocess.run(
        ["python3", script_path, "--stdin"],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=60,
    )

    if result.returncode != 0:
        print(f"Error: {result.stderr}", file=sys.stderr)
        return {"inserted": 0, "skipped": 0, "errors": len(contents)}

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"inserted": 0, "skipped": 0, "errors": len(contents)}


def main():
    parser = argparse.ArgumentParser(description="Bulk sync BitMagnet to SQLite")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size")
    parser.add_argument("--limit", type=int, default=0, help="Total limit (0 = all)")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually promote")
    parser.add_argument("--db", default=DEFAULT_SEARCH_DB, help="SQLite database path")
    parser.add_argument(
        "--script", default=DEFAULT_PROMOTION_SCRIPT, help="Promotion script path"
    )
    args = parser.parse_args()

    print(f"Connecting to Postgres...")
    try:
        conn = get_postgres_connection()
    except Exception as e:
        print(f"Failed to connect to Postgres: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading existing hashes from {args.db}...")
    existing = get_existing_hashes(args.db)
    print(f"Found {len(existing)} existing hashes in SQLite")

    # Get total count
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM torrent_contents WHERE content_source IS NOT NULL"
    )
    total = cursor.fetchone()[0]
    cursor.close()
    print(f"Total content with TMDB IDs in Postgres: {total}")

    to_promote = total - len(existing)
    print(f"Content to promote: ~{to_promote}")

    if args.dry_run:
        print("\n[DRY RUN] Would promote content...")
        conn.close()
        return

    # Process in batches
    offset = 0
    total_inserted = 0
    total_skipped = 0
    total_errors = 0

    batch_num = 0
    while True:
        if args.limit > 0 and offset >= args.limit:
            break

        batch = fetch_batch(conn, offset, args.batch_size)
        if not batch:
            break

        # Filter out already-existing
        new_batch = [c for c in batch if c["info_hash"] not in existing]

        if new_batch:
            result = promote_batch(new_batch, args.script, args.db)
            total_inserted += result.get("inserted", 0)
            total_skipped += result.get("skipped", 0)
            total_errors += result.get("errors", 0)

            # Update existing set
            for c in new_batch:
                existing.add(c["info_hash"])

        batch_num += 1
        offset += args.batch_size

        if batch_num % 10 == 0:
            print(
                f"Batch {batch_num}: inserted={total_inserted}, skipped={total_skipped}, errors={total_errors}"
            )

    conn.close()

    print(f"\n=== Final Results ===")
    print(f"Total inserted: {total_inserted}")
    print(f"Total skipped: {total_skipped}")
    print(f"Total errors: {total_errors}")


if __name__ == "__main__":
    main()
