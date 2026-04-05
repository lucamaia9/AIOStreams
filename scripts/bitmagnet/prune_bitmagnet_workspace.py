#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

DEFAULT_SEARCH_DB = "/home/ubuntu/aiostreams/data/comet-fresh/magnetico/active.search.sqlite3"
DEFAULT_CONTAINER = "bitmagnet-postgres"
PROMOTED_TABLES = (
    "torrents",
    "torrent_files",
    "torrent_contents",
    "torrent_hints",
    "torrents_torrent_sources",
    "torrent_tags",
    "torrent_pieces",
)


@dataclass
class CutoffCursor:
    promoted_at: int
    info_hash: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prune the BitMagnet PostgreSQL workspace after promoted rows have aged past the retention window."
    )
    parser.add_argument("--search-db", default=DEFAULT_SEARCH_DB)
    parser.add_argument("--docker-container", default=DEFAULT_CONTAINER)
    parser.add_argument("--retention-days", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--queue-batch-size", type=int, default=5000)
    parser.add_argument("--orphan-batch-size", type=int, default=1000)
    parser.add_argument("--max-batches", type=int, default=0)
    parser.add_argument(
        "--allow-before-wide-drain",
        action="store_true",
        help="Override the safety gate that blocks torrent pruning before the wide BitMagnet drain is marked complete in SQLite metadata.",
    )
    parser.add_argument(
        "--vacuum-full-tables",
        default="",
        help="Comma-separated PostgreSQL tables to VACUUM FULL after deletes. Use only during off-peak maintenance windows.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute deletes. Without this flag, the script only reports what would be removed.",
    )
    return parser.parse_args()


def run_psql(container: str, sql: str) -> str:
    command = [
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
        "-qAt",
        "-c",
        sql,
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return result.stdout.strip()


def run_psql_json(container: str, sql: str) -> list[dict[str, Any]]:
    wrapped = f"""
    COPY (
      SELECT row_to_json(q)
      FROM (
        {sql}
      ) q
    ) TO STDOUT
    """
    raw = run_psql(container, wrapped)
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def hex_decode_list(info_hashes: list[str]) -> str:
    return ", ".join(f"decode('{info_hash.lower()}', 'hex')" for info_hash in info_hashes)


def load_candidate_count(connection: sqlite3.Connection, cutoff_epoch: int) -> int:
    row = connection.execute(
        "SELECT count(*) FROM live_promotions WHERE promoted_at <= ?",
        (cutoff_epoch,),
    ).fetchone()
    return int(row[0] or 0)


def load_meta_flag(connection: sqlite3.Connection, key: str) -> bool:
    row = connection.execute(
        "SELECT value FROM live_promotion_meta WHERE key = ?",
        (key,),
    ).fetchone()
    if row is None:
        return False
    raw = str(row[0] or "").strip()
    if not raw:
        return False
    try:
        return bool(json.loads(raw))
    except json.JSONDecodeError:
        return raw.lower() in {"1", "true", "yes"}


def fetch_candidate_batch(
    connection: sqlite3.Connection,
    *,
    cutoff_epoch: int,
    cursor: CutoffCursor,
    batch_size: int,
) -> list[str]:
    rows = connection.execute(
        """
        SELECT info_hash
        FROM live_promotions
        WHERE promoted_at <= ?
          AND (
            promoted_at > ?
            OR (promoted_at = ? AND info_hash > ?)
          )
        ORDER BY promoted_at, info_hash
        LIMIT ?
        """,
        (
            cutoff_epoch,
            cursor.promoted_at,
            cursor.promoted_at,
            cursor.info_hash,
            batch_size,
        ),
    ).fetchall()
    return [str(row[0]).lower() for row in rows]


def advance_cursor(connection: sqlite3.Connection, info_hash: str) -> CutoffCursor:
    row = connection.execute(
        "SELECT promoted_at, info_hash FROM live_promotions WHERE info_hash = ?",
        (info_hash,),
    ).fetchone()
    if row is None:
        return CutoffCursor(promoted_at=0, info_hash="")
    return CutoffCursor(promoted_at=int(row[0]), info_hash=str(row[1]).lower())


def fetch_table_stats(container: str) -> dict[str, dict[str, float]]:
    rows = run_psql_json(
        container,
        """
        SELECT
          c.relname AS table_name,
          GREATEST(COALESCE(c.reltuples, 0), 0) AS estimated_rows,
          pg_total_relation_size(c.oid) AS total_bytes
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind = 'r'
          AND c.relname IN (
            'torrents',
            'torrent_files',
            'torrent_contents',
            'torrent_hints',
            'torrents_torrent_sources',
            'torrent_tags',
            'torrent_pieces',
            'queue_jobs',
            'content',
            'content_attributes',
            'content_collections_content'
          )
        ORDER BY c.relname
        """
    )
    return {
        str(row["table_name"]): {
            "estimated_rows": float(row["estimated_rows"] or 0),
            "total_bytes": float(row["total_bytes"] or 0),
        }
        for row in rows
    }


def count_promoted_rows_for_hashes(container: str, info_hashes: list[str]) -> dict[str, int]:
    if not info_hashes:
        return {table: 0 for table in PROMOTED_TABLES}
    in_list = hex_decode_list(info_hashes)
    sql = f"""
    SELECT 'torrents' AS table_name, count(*)::bigint AS row_count
    FROM torrents
    WHERE info_hash IN ({in_list})
    UNION ALL
    SELECT 'torrent_files' AS table_name, count(*)::bigint AS row_count
    FROM torrent_files
    WHERE info_hash IN ({in_list})
    UNION ALL
    SELECT 'torrent_contents' AS table_name, count(*)::bigint AS row_count
    FROM torrent_contents
    WHERE info_hash IN ({in_list})
    UNION ALL
    SELECT 'torrent_hints' AS table_name, count(*)::bigint AS row_count
    FROM torrent_hints
    WHERE info_hash IN ({in_list})
    UNION ALL
    SELECT 'torrents_torrent_sources' AS table_name, count(*)::bigint AS row_count
    FROM torrents_torrent_sources
    WHERE info_hash IN ({in_list})
    UNION ALL
    SELECT 'torrent_tags' AS table_name, count(*)::bigint AS row_count
    FROM torrent_tags
    WHERE info_hash IN ({in_list})
    UNION ALL
    SELECT 'torrent_pieces' AS table_name, count(*)::bigint AS row_count
    FROM torrent_pieces
    WHERE info_hash IN ({in_list})
    """
    rows = run_psql_json(container, sql)
    counts = {table: 0 for table in PROMOTED_TABLES}
    for row in rows:
        counts[str(row["table_name"])] = int(row["row_count"] or 0)
    return counts


def count_queue_candidates(container: str, cutoff_iso: str) -> int:
    raw = run_psql(
        container,
        f"""
        SELECT count(*)
        FROM queue_jobs
        WHERE status = 'processed'
          AND COALESCE(ran_at, created_at) < TIMESTAMPTZ '{cutoff_iso}'
        """,
    )
    return int(raw or 0)


def delete_promoted_batch(container: str, info_hashes: list[str]) -> int:
    if not info_hashes:
        return 0
    in_list = hex_decode_list(info_hashes)
    raw = run_psql(
        container,
        f"""
        WITH deleted AS (
          DELETE FROM torrents
          WHERE info_hash IN ({in_list})
          RETURNING 1
        )
        SELECT count(*) FROM deleted
        """,
    )
    return int(raw or 0)


def delete_queue_batch(container: str, cutoff_iso: str, batch_size: int) -> int:
    raw = run_psql(
        container,
        f"""
        WITH doomed AS (
          SELECT id
          FROM queue_jobs
          WHERE status = 'processed'
            AND COALESCE(ran_at, created_at) < TIMESTAMPTZ '{cutoff_iso}'
          ORDER BY COALESCE(ran_at, created_at), id
          LIMIT {int(batch_size)}
        ),
        deleted AS (
          DELETE FROM queue_jobs q
          USING doomed d
          WHERE q.id = d.id
          RETURNING 1
        )
        SELECT count(*) FROM deleted
        """,
    )
    return int(raw or 0)


def delete_orphan_content_batch(container: str, batch_size: int) -> int:
    raw = run_psql(
        container,
        f"""
        WITH doomed AS (
          SELECT c.type, c.source, c.id
          FROM content c
          WHERE NOT EXISTS (
            SELECT 1
            FROM torrent_contents tc
            WHERE tc.content_type = c.type
              AND tc.content_source = c.source
              AND tc.content_id = c.id
          )
          LIMIT {int(batch_size)}
        ),
        deleted AS (
          DELETE FROM content c
          USING doomed d
          WHERE c.type = d.type
            AND c.source = d.source
            AND c.id = d.id
          RETURNING 1
        )
        SELECT count(*) FROM deleted
        """,
    )
    return int(raw or 0)


def vacuum_full_tables(container: str, tables: list[str]) -> list[str]:
    allowed = {
        "queue_jobs",
        "torrents",
        "torrent_files",
        "torrent_contents",
        "torrent_hints",
        "torrents_torrent_sources",
        "content",
        "content_attributes",
    }
    executed: list[str] = []
    for table in tables:
        clean = table.strip()
        if not clean:
            continue
        if clean not in allowed:
            raise SystemExit(f"unsupported table for VACUUM FULL: {clean}")
        run_psql(container, f"VACUUM FULL ANALYZE {clean}")
        executed.append(clean)
    return executed


def estimate_reclaim_bytes(table_stats: dict[str, dict[str, float]], table_name: str, row_count: int) -> int:
    stats = table_stats.get(table_name)
    if not stats:
        return 0
    estimated_rows = float(stats["estimated_rows"])
    total_bytes = float(stats["total_bytes"])
    if estimated_rows <= 0 or total_bytes <= 0 or row_count <= 0:
        return 0
    return int((total_bytes / estimated_rows) * row_count)


def build_report(
    *,
    mode: str,
    cutoff_epoch: int,
    cutoff_iso: str,
    candidate_count: int,
    queue_candidate_count: int,
    promoted_row_counts: dict[str, int],
    orphan_counts: dict[str, int | None],
    table_stats: dict[str, dict[str, float]],
    deleted_torrents: int = 0,
    deleted_queue_jobs: int = 0,
    deleted_orphan_content: int = 0,
    vacuum_full: list[str] | None = None,
    wide_drain_completed: bool = False,
    prune_blocked: bool = False,
) -> dict[str, Any]:
    estimated_reclaim = {
        table: estimate_reclaim_bytes(table_stats, table, count)
        for table, count in promoted_row_counts.items()
    }
    estimated_reclaim["queue_jobs"] = estimate_reclaim_bytes(
        table_stats, "queue_jobs", queue_candidate_count
    )
    estimated_reclaim["content"] = estimate_reclaim_bytes(
        table_stats, "content", int(orphan_counts["content"] or 0)
    )
    estimated_reclaim["content_attributes"] = estimate_reclaim_bytes(
        table_stats, "content_attributes", int(orphan_counts["content_attributes"] or 0)
    )
    return {
        "mode": mode,
        "cutoffEpoch": cutoff_epoch,
        "cutoffIso": cutoff_iso,
        "eligiblePromotedInfoHashes": candidate_count,
        "eligibleQueueJobs": queue_candidate_count,
        "eligibleRows": promoted_row_counts,
        "eligibleOrphans": orphan_counts,
        "wideDrainCompleted": wide_drain_completed,
        "pruneBlocked": prune_blocked,
        "estimatedReclaimBytes": estimated_reclaim,
        "deleted": {
            "torrents": deleted_torrents,
            "queueJobs": deleted_queue_jobs,
            "orphanContent": deleted_orphan_content,
        },
        "vacuumFullTables": vacuum_full or [],
    }


def main() -> None:
    args = parse_args()
    search_db = Path(args.search_db)
    if not search_db.exists():
        raise SystemExit(f"search db not found: {search_db}")

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=args.retention_days)
    cutoff_epoch = int(cutoff.timestamp())
    cutoff_iso = cutoff.isoformat()

    connection = sqlite3.connect(search_db)
    connection.row_factory = sqlite3.Row
    try:
        wide_drain_completed = load_meta_flag(connection, "wideDrainCompleted")
        candidate_count = load_candidate_count(connection, cutoff_epoch)
        queue_candidate_count = count_queue_candidates(args.docker_container, cutoff_iso)
        table_stats = fetch_table_stats(args.docker_container)

        promoted_row_counts = {table: 0 for table in PROMOTED_TABLES}
        cursor = CutoffCursor(promoted_at=0, info_hash="")
        batches = 0
        while True:
            if args.max_batches and batches >= args.max_batches:
                break
            batch = fetch_candidate_batch(
                connection,
                cutoff_epoch=cutoff_epoch,
                cursor=cursor,
                batch_size=args.batch_size,
            )
            if not batch:
                break
            batch_counts = count_promoted_rows_for_hashes(args.docker_container, batch)
            for table_name, row_count in batch_counts.items():
                promoted_row_counts[table_name] += row_count
            cursor = advance_cursor(connection, batch[-1])
            batches += 1

        orphan_counts: dict[str, int | None] = {
            "content": None,
            "content_attributes": None,
        }

        if not args.apply:
            print(
                json.dumps(
                    build_report(
                        mode="dry-run",
                        cutoff_epoch=cutoff_epoch,
                        cutoff_iso=cutoff_iso,
                        candidate_count=candidate_count,
                        queue_candidate_count=queue_candidate_count,
                        promoted_row_counts=promoted_row_counts,
                        orphan_counts=orphan_counts,
                        table_stats=table_stats,
                        wide_drain_completed=wide_drain_completed,
                        prune_blocked=(not wide_drain_completed and not args.allow_before_wide_drain),
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return

        deleted_torrents = 0
        deleted_queue_jobs = 0
        deleted_orphan_content = 0
        vacuum_tables = [
            item.strip()
            for item in str(args.vacuum_full_tables or "").split(",")
            if item.strip()
        ]
        vacuum_executed: list[str] = []

        if not wide_drain_completed and not args.allow_before_wide_drain:
            print(
                json.dumps(
                    build_report(
                        mode="blocked",
                        cutoff_epoch=cutoff_epoch,
                        cutoff_iso=cutoff_iso,
                        candidate_count=candidate_count,
                        queue_candidate_count=queue_candidate_count,
                        promoted_row_counts=promoted_row_counts,
                        orphan_counts=orphan_counts,
                        table_stats=table_stats,
                        wide_drain_completed=wide_drain_completed,
                        prune_blocked=True,
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return

        cursor = CutoffCursor(promoted_at=0, info_hash="")
        batches = 0
        while True:
            if args.max_batches and batches >= args.max_batches:
                break
            batch = fetch_candidate_batch(
                connection,
                cutoff_epoch=cutoff_epoch,
                cursor=cursor,
                batch_size=args.batch_size,
            )
            if not batch:
                break
            deleted_torrents += delete_promoted_batch(args.docker_container, batch)
            cursor = advance_cursor(connection, batch[-1])
            batches += 1

        while True:
            deleted = delete_queue_batch(
                args.docker_container,
                cutoff_iso=cutoff_iso,
                batch_size=args.queue_batch_size,
            )
            deleted_queue_jobs += deleted
            if deleted == 0:
                break

        if deleted_torrents > 0:
            while True:
                deleted = delete_orphan_content_batch(
                    args.docker_container,
                    batch_size=args.orphan_batch_size,
                )
                deleted_orphan_content += deleted
                if deleted == 0:
                    break

        if vacuum_tables:
            vacuum_executed = vacuum_full_tables(args.docker_container, vacuum_tables)

        print(
            json.dumps(
                build_report(
                    mode="apply",
                    cutoff_epoch=cutoff_epoch,
                    cutoff_iso=cutoff_iso,
                    candidate_count=candidate_count,
                    queue_candidate_count=queue_candidate_count,
                    promoted_row_counts=promoted_row_counts,
                    orphan_counts=orphan_counts,
                    table_stats=table_stats,
                    deleted_torrents=deleted_torrents,
                    deleted_queue_jobs=deleted_queue_jobs,
                    deleted_orphan_content=deleted_orphan_content,
                    vacuum_full=vacuum_executed,
                    wide_drain_completed=wide_drain_completed,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
    finally:
        connection.close()


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stderr or str(exc))
        raise
