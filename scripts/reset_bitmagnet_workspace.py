#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
from pathlib import Path
from typing import Any

DEFAULT_SEARCH_DB = "/home/ubuntu/aiostreams/data/comet-fresh/magnetico/active.search.sqlite3"
DEFAULT_STATE_PATH = "/home/ubuntu/aiostreams/data/comet-fresh/magnetico/wide-drain-offline.state.json"
DEFAULT_CONTAINER = "bitmagnet-postgres"
WORKSPACE_TABLES = (
    "queue_jobs",
    "torrent_files",
    "torrent_contents",
    "torrent_hints",
    "torrents_torrent_sources",
    "torrent_tags",
    "torrent_pieces",
    "torrents",
    "content_attributes",
    "content",
    "content_collections_content",
    "content_collections",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reset the BitMagnet PostgreSQL workspace after the wide drain has been verified complete."
    )
    parser.add_argument("--search-db", default=DEFAULT_SEARCH_DB)
    parser.add_argument("--state", default=DEFAULT_STATE_PATH)
    parser.add_argument("--docker-container", default=DEFAULT_CONTAINER)
    parser.add_argument(
        "--bitmagnet-container",
        default="bitmagnet",
        help="Refuse reset if this container is running, unless --allow-running-bitmagnet is set.",
    )
    parser.add_argument(
        "--allow-running-bitmagnet",
        action="store_true",
        help="Override the safety check that requires the BitMagnet app container to be stopped.",
    )
    parser.add_argument(
        "--vacuum-full-tables",
        default="",
        help="Optional comma-separated list of tables to VACUUM FULL after reset. Usually unnecessary after TRUNCATE.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute the workspace reset. Without this flag, print a dry-run report only.",
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


def docker_container_running(name: str) -> bool:
    result = subprocess.run(
        ["sudo", "docker", "ps", "--filter", f"name=^{name}$", "--format", "{{.Names}}"],
        capture_output=True,
        text=True,
        check=True,
    )
    return any(line.strip() == name for line in result.stdout.splitlines())


def fetch_latest_source_cursor(container: str) -> dict[str, str] | None:
    rows = run_psql_json(
        container,
        """
        SELECT
          to_char(
            t.created_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'
          ) AS created_at,
          encode(t.info_hash, 'hex') AS info_hash
        FROM torrents t
        ORDER BY t.created_at DESC, t.info_hash DESC
        LIMIT 1
        """,
    )
    return rows[0] if rows else None


def count_rows_after_cursor(container: str, created_at: str, info_hash: str) -> int:
    raw = run_psql(
        container,
        f"""
        WITH s AS (
          SELECT
            TIMESTAMPTZ '{created_at}' AS created_at,
            decode('{info_hash.lower()}', 'hex') AS info_hash
        )
        SELECT count(*)
        FROM torrents t, s
        WHERE t.created_at > s.created_at
           OR (t.created_at = s.created_at AND t.info_hash > s.info_hash)
        """,
    )
    return int(raw or 0)


def fetch_table_stats(container: str) -> list[dict[str, Any]]:
    return run_psql_json(
        container,
        """
        SELECT
          c.relname AS table_name,
          GREATEST(COALESCE(c.reltuples, 0), 0)::bigint AS estimated_rows,
          pg_total_relation_size(c.oid)::bigint AS total_bytes
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind = 'r'
          AND c.relname IN (
            'queue_jobs',
            'torrent_files',
            'torrent_contents',
            'torrent_hints',
            'torrents_torrent_sources',
            'torrent_tags',
            'torrent_pieces',
            'torrents',
            'content_attributes',
            'content',
            'content_collections_content',
            'content_collections'
          )
        ORDER BY pg_total_relation_size(c.oid) DESC
        """
    )


def truncate_workspace(container: str) -> None:
    table_list = ", ".join(WORKSPACE_TABLES)
    run_psql(container, f"TRUNCATE TABLE {table_list} RESTART IDENTITY CASCADE")


def vacuum_full_tables(container: str, tables: list[str]) -> list[str]:
    allowed = set(WORKSPACE_TABLES)
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


def main() -> None:
    args = parse_args()
    search_db = Path(args.search_db)
    state_path = Path(args.state)
    if not search_db.exists():
        raise SystemExit(f"search db not found: {search_db}")
    if not state_path.exists():
        raise SystemExit(f"state file not found: {state_path}")

    with sqlite3.connect(search_db) as connection:
        wide_drain_completed = load_meta_flag(connection, "wideDrainCompleted")

    state = json.loads(state_path.read_text())
    last_created_at = str(state.get("lastCreatedAt") or "")
    last_info_hash = str(state.get("lastInfoHash") or "").lower()
    if not last_created_at or not last_info_hash:
        raise SystemExit("offline drain state does not contain a completion cursor")

    latest_source = fetch_latest_source_cursor(args.docker_container)
    rows_after_cursor = count_rows_after_cursor(args.docker_container, last_created_at, last_info_hash)
    bitmagnet_running = docker_container_running(args.bitmagnet_container)
    table_stats = fetch_table_stats(args.docker_container)

    report: dict[str, Any] = {
        "mode": "apply" if args.apply else "dry-run",
        "wideDrainCompleted": wide_drain_completed,
        "stateCursor": {
            "lastCreatedAt": last_created_at,
            "lastInfoHash": last_info_hash,
        },
        "latestSource": latest_source,
        "rowsAfterCursor": rows_after_cursor,
        "bitmagnetContainerRunning": bitmagnet_running,
        "workspaceTables": table_stats,
        "vacuumFullTables": [],
    }

    if not args.apply:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return

    if not wide_drain_completed:
        raise SystemExit("wideDrainCompleted is false in SQLite metadata")
    if rows_after_cursor != 0:
        raise SystemExit(f"refusing workspace reset: {rows_after_cursor} source rows remain after the saved cursor")
    if bitmagnet_running and not args.allow_running_bitmagnet:
        raise SystemExit(
            f"refusing workspace reset while {args.bitmagnet_container!r} is running; stop it first or pass --allow-running-bitmagnet"
        )

    truncate_workspace(args.docker_container)
    vacuum_tables = [
        item.strip()
        for item in str(args.vacuum_full_tables or "").split(",")
        if item.strip()
    ]
    report["vacuumFullTables"] = vacuum_full_tables(args.docker_container, vacuum_tables)
    report["workspaceTablesAfter"] = fetch_table_stats(args.docker_container)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
