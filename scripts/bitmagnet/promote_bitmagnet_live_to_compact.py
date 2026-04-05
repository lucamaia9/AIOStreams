#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sqlite3
import subprocess
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# Add tools path for lean_media_contract (in container: /opt/bitmagnetico/tools)
TOOLS_DIR = SCRIPT_DIR.parent / "bitmagnet-media" / "classifier"
if TOOLS_DIR.exists() and str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

# Also try container mount path
CONTAINER_TOOLS = Path("/opt/bitmagnetico/tools")
if CONTAINER_TOOLS.exists() and str(CONTAINER_TOOLS) not in sys.path:
    sys.path.insert(0, str(CONTAINER_TOOLS))

from lib.classifier_runtime import bootstrap_classifier_runtime

RUNTIME_PATHS = bootstrap_classifier_runtime(__file__)
REPO_ROOT = RUNTIME_PATHS.repo_root

import lean_media_contract as lean_contract
from bitmagnet_smart_hint import BROAD_MIN_SIZE, STRICT_MIN_SIZE
from compact_media_search import classify_compact_row
from magnetico_media_probe import SampledTorrent, display_text
from shared_bitmagnet_bridge import (
    authoritative_import_hints,
    authoritative_match_details,
)
from shared_classifier_contract import (
    AUTHORITATIVE_CONTRACT_VERSION,
    serialize_reason_codes,
)


def _get_default_search_db() -> Path:
    """Get default search DB path from environment or repo."""
    env_path = os.environ.get("SEARCH_DB_PATH") or os.environ.get("MAGNETICO_SEARCH_DB")
    if env_path:
        return Path(env_path)
    return REPO_ROOT / "data" / "comet-fresh" / "magnetico" / "active.search.sqlite3"


DEFAULT_SEARCH_DB = str(_get_default_search_db())
DEFAULT_INCREMENTAL_CHECKPOINT = str(
    REPO_ROOT / "data" / "comet-fresh" / "magnetico" / "live-promotion.checkpoint.json"
)
DEFAULT_DRAIN_CHECKPOINT = str(
    REPO_ROOT / "data" / "comet-fresh" / "magnetico" / "wide-drain.checkpoint.json"
)
DEFAULT_CONTAINER = "bitmagnet-postgres"
CHECKPOINT_SCHEMA_VERSION = 3
ZERO_CURSOR = "1970-01-01T00:00:00+00:00"
ZERO_HASH = "0000000000000000000000000000000000000000"
SOURCE_INDEX_STATEMENTS = (
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS torrents_created_at_info_hash_idx ON torrents (created_at, info_hash)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS torrent_contents_info_hash_idx ON torrent_contents (info_hash)",
)
SOURCE_INDEX_NAMES = (
    "torrents_created_at_info_hash_idx",
    "torrent_contents_info_hash_idx",
)
MEDIA_COMPARE_COLUMNS = (
    "info_hash",
    "name",
    "total_size",
    "discovered_on",
    "norm_title",
    "title_key",
    "canonical_title_key",
    "movie_key",
    "series_key",
    "aliases_text",
    "imdb_id",
    "tmdb_id",
    "year",
    "content_class",
    "is_anime",
    "confidence",
    "confidence_tier",
    "reject_reason",
    "reason_codes",
    "match_source",
    "contract_version",
    "season",
    "episode_start",
    "episode_end",
    "exact_episode_key",
    "season_pack_key",
    "episode_count",
    "has_exact_episode",
    "is_multi_episode",
    "is_season_pack",
    "adult_score",
    "software_score",
    "media_score",
    "resolution",
    "source_hint",
    "codec_hint",
)
MEDIA_STAGE_COLUMNS = (
    "torrent_id",
    "info_hash",
    "name",
    "total_size",
    "discovered_on",
    "norm_title",
    "title_key",
    "canonical_title_key",
    "movie_key",
    "series_key",
    "aliases_text",
    "imdb_id",
    "tmdb_id",
    "year",
    "content_class",
    "is_anime",
    "confidence",
    "confidence_tier",
    "reject_reason",
    "reason_codes",
    "match_source",
    "contract_version",
    "season",
    "episode_start",
    "episode_end",
    "exact_episode_key",
    "season_pack_key",
    "episode_count",
    "has_exact_episode",
    "is_multi_episode",
    "is_season_pack",
    "adult_score",
    "software_score",
    "media_score",
    "resolution",
    "source_hint",
    "codec_hint",
    "search_text",
)
PROMOTION_STAGE_COLUMNS = (
    "info_hash",
    "torrent_id",
    "source_updated_at",
    "promoted_at",
    "content_class",
    "reject_reason",
    "reason_codes",
    "confidence_tier",
    "match_source",
    "contract_version",
)
REQUIRED_LOOKUP_INDEX_COLUMNS = (
    "movie_key",
    "series_key",
    "imdb_id",
    "tmdb_id",
    "exact_episode_key",
    "season_pack_key",
)
DEFAULT_CLASSIFIER_WORKER_CAP = 8
INFO_HASH_LOOKUP_BATCH_SIZE = 400
ID_LOOKUP_MAX_PARALLEL_QUERIES = 4


def default_classifier_workers() -> int:
    cpu_count = max(1, os.cpu_count() or 1)
    return max(1, min(DEFAULT_CLASSIFIER_WORKER_CAP, cpu_count - 1))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync BitMagnet raw torrents into the lean SQLite DB using the authoritative Python media classifier."
    )
    parser.add_argument("--search-db", default=DEFAULT_SEARCH_DB)
    parser.add_argument("--checkpoint", default=DEFAULT_INCREMENTAL_CHECKPOINT)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument(
        "--classifier-workers", type=int, default=default_classifier_workers()
    )
    parser.add_argument("--fts-batch-size", type=int, default=1000)
    parser.add_argument("--progress-every-batches", type=int, default=10)
    parser.add_argument("--backfill-batch-size", type=int, default=1000)
    parser.add_argument("--max-batches", type=int, default=0)
    parser.add_argument("--docker-container", default=DEFAULT_CONTAINER)
    parser.add_argument(
        "--ensure-source-indexes",
        action="store_true",
        help="Ensure the Postgres source indexes required for fast keyset scans exist.",
    )
    parser.add_argument(
        "--backfill-ids",
        action="store_true",
        help="Backfill tmdb_id/imdb_id for already-promoted live rows in the SQLite DB.",
    )
    parser.add_argument(
        "--bootstrap-from-latest",
        action="store_true",
        help="Initialize the checkpoint to the latest BitMagnet torrent.created_at cursor without syncing rows.",
    )
    parser.add_argument(
        "--bootstrap-now",
        action="store_true",
        help="Initialize the checkpoint to the current UTC time without scanning BitMagnet.",
    )
    parser.add_argument(
        "--drain-all",
        action="store_true",
        help="Run a full wide-to-lean drain using a dedicated checkpoint instead of the incremental cursor.",
    )
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help="Run the parity audit and planned action analysis without mutating the SQLite DB.",
    )
    parser.add_argument(
        "--audit-report",
        help="Optional path for a machine-readable JSON audit report.",
    )
    parser.add_argument(
        "--audit-sample-limit",
        type=int,
        default=5,
        help="Sample rows to keep per audit reason category.",
    )
    parser.add_argument(
        "--created-after",
        help="Optional lower bound on torrent.created_at (ISO-8601 UTC) for audit or drain runs.",
    )
    parser.add_argument(
        "--created-before",
        help="Optional upper bound on torrent.created_at (ISO-8601 UTC) for audit or drain runs.",
    )
    return parser.parse_args()


def normalize_cursor(value: str | None) -> str:
    if not value:
        return ZERO_CURSOR
    return (
        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        .astimezone(timezone.utc)
        .isoformat()
    )


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def load_checkpoint(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "schemaVersion": CHECKPOINT_SCHEMA_VERSION,
            "cursorType": "torrent_created_at",
            "lastCreatedAt": ZERO_CURSOR,
            "lastInfoHash": ZERO_HASH,
            "scannedRows": 0,
            "eligibleRows": 0,
            "insertedRows": 0,
            "updatedRows": 0,
            "noopRows": 0,
            "skippedRows": 0,
            "extractSeconds": 0.0,
            "classifySeconds": 0.0,
            "mergeSeconds": 0.0,
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    if int(payload.get("schemaVersion", 0)) == CHECKPOINT_SCHEMA_VERSION:
        payload["lastCreatedAt"] = normalize_cursor(payload.get("lastCreatedAt"))
        payload["lastInfoHash"] = str(payload.get("lastInfoHash") or ZERO_HASH).lower()
        for key in ("extractSeconds", "classifySeconds", "mergeSeconds"):
            payload[key] = float(payload.get(key, 0.0) or 0.0)
        return payload
    return {
        "schemaVersion": CHECKPOINT_SCHEMA_VERSION,
        "cursorType": "torrent_created_at",
        "lastCreatedAt": normalize_cursor(
            payload.get("lastCreatedAt") or payload.get("lastUpdatedAt")
        ),
        "lastInfoHash": str(payload.get("lastInfoHash") or ZERO_HASH).lower(),
        "scannedRows": int(payload.get("scannedRows", 0) or 0),
        "eligibleRows": int(
            payload.get("eligibleRows", payload.get("promotedRows", 0)) or 0
        ),
        "insertedRows": int(payload.get("insertedRows", 0) or 0),
        "updatedRows": int(payload.get("updatedRows", 0) or 0),
        "noopRows": int(payload.get("noopRows", 0) or 0),
        "skippedRows": int(payload.get("skippedRows", 0) or 0),
        "extractSeconds": float(payload.get("extractSeconds", 0.0) or 0.0),
        "classifySeconds": float(payload.get("classifySeconds", 0.0) or 0.0),
        "mergeSeconds": float(payload.get("mergeSeconds", 0.0) or 0.0),
        "migratedFromLegacyCheckpoint": True,
    }


def effective_checkpoint_path(args: argparse.Namespace) -> Path:
    requested = Path(args.checkpoint)
    if args.drain_all and str(requested) == DEFAULT_INCREMENTAL_CHECKPOINT:
        return Path(DEFAULT_DRAIN_CHECKPOINT)
    return requested


def run_psql(container: str, sql: str, *, tuples_only: bool = True) -> str:
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
        "-X",
        "-v",
        "ON_ERROR_STOP=1",
    ]
    if tuples_only:
        command.extend(["-qAt"])
    else:
        command.append("-q")
    command.extend(["-c", sql])
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return result.stdout.strip()


def run_psql_copy_rows(container: str, sql: str) -> list[dict[str, str]]:
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
        "-X",
        "-v",
        "ON_ERROR_STOP=1",
        "-q",
        "-c",
        f"COPY ({sql}) TO STDOUT WITH (FORMAT csv, HEADER true)",
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    raw = result.stdout.strip()
    if not raw:
        return []
    return list(csv.DictReader(io.StringIO(raw)))


def sql_literal(value: str) -> str:
    return value.replace("'", "''")


def progress_line(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def estimated_source_rows(container: str) -> int | None:
    raw = run_psql(
        container,
        """
        SELECT COALESCE(reltuples::bigint, 0)
        FROM pg_class
        WHERE oid = 'public.torrents'::regclass
        """,
    )
    if not raw:
        return None
    return int(raw)


def source_index_status(container: str) -> dict[str, dict[str, bool]]:
    rows = run_psql_copy_rows(
        container,
        f"""
        SELECT
          c.relname AS index_name,
          i.indisvalid AS is_valid,
          i.indisready AS is_ready,
          i.indislive AS is_live
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.relname IN ({", ".join(f"'{name}'" for name in SOURCE_INDEX_NAMES)})
        """,
    )
    status: dict[str, dict[str, bool]] = {}
    for row in rows:
        status[str(row["index_name"])] = {
            "is_valid": str(row.get("is_valid") or "").lower() in {"t", "true"},
            "is_ready": str(row.get("is_ready") or "").lower() in {"t", "true"},
            "is_live": str(row.get("is_live") or "").lower() in {"t", "true"},
        }
    return status


def source_indexes_in_progress(container: str) -> set[str]:
    rows = run_psql_copy_rows(
        container,
        f"""
        SELECT index_relid::regclass::text AS index_name
        FROM pg_stat_progress_create_index
        WHERE index_relid::regclass::text IN ({", ".join(f"'{name}'" for name in SOURCE_INDEX_NAMES)})
        """,
    )
    return {str(row["index_name"]) for row in rows if row.get("index_name")}


def ensure_source_indexes(container: str) -> None:
    progress = source_indexes_in_progress(container)
    status = source_index_status(container)
    for index_name, statement in zip(
        SOURCE_INDEX_NAMES, SOURCE_INDEX_STATEMENTS, strict=True
    ):
        if index_name in progress:
            continue
        current = status.get(index_name)
        if (
            current
            and current["is_valid"]
            and current["is_ready"]
            and current["is_live"]
        ):
            continue
        if current is not None:
            run_psql(
                container,
                f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}",
                tuples_only=False,
            )
        run_psql(container, statement, tuples_only=False)


def latest_source_cursor(container: str) -> dict[str, str] | None:
    rows = run_psql_copy_rows(
        container,
        """
        SELECT
          to_char(
            t.created_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'
          ) AS source_created_at,
          encode(t.info_hash, 'hex') AS info_hash
        FROM torrents t
        ORDER BY t.created_at DESC, t.info_hash DESC
        LIMIT 1
        """,
    )
    return rows[0] if rows else None


def earliest_source_cursor(container: str) -> dict[str, str] | None:
    rows = run_psql_copy_rows(
        container,
        """
        SELECT
          to_char(
            t.created_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'
          ) AS source_created_at,
          encode(t.info_hash, 'hex') AS info_hash
        FROM torrents t
        ORDER BY t.created_at, t.info_hash
        LIMIT 1
        """,
    )
    return rows[0] if rows else None


def build_cursor_filters(
    *,
    last_created_at: str,
    last_info_hash: str,
    created_after: str | None,
    created_before: str | None,
) -> str:
    filters = [
        f"""(
            t.created_at > TIMESTAMPTZ '{sql_literal(last_created_at)}'
            OR (
              t.created_at = TIMESTAMPTZ '{sql_literal(last_created_at)}'
              AND t.info_hash > decode('{sql_literal(last_info_hash)}', 'hex')
            )
          )"""
    ]
    if created_after:
        filters.append(
            f"t.created_at >= TIMESTAMPTZ '{sql_literal(normalize_cursor(created_after))}'"
        )
    if created_before:
        filters.append(
            f"t.created_at <= TIMESTAMPTZ '{sql_literal(normalize_cursor(created_before))}'"
        )
    return "\n          AND ".join(filters)


def fetch_header_batch(
    container: str,
    *,
    last_created_at: str,
    last_info_hash: str,
    batch_size: int,
    created_after: str | None = None,
    created_before: str | None = None,
) -> list[dict[str, Any]]:
    where_clause = build_cursor_filters(
        last_created_at=last_created_at,
        last_info_hash=last_info_hash,
        created_after=created_after,
        created_before=created_before,
    )
    rows = run_psql_copy_rows(
        container,
        f"""
        SELECT
          encode(t.info_hash, 'hex') AS info_hash,
          t.name AS torrent_name,
          t.size AS total_size,
          to_char(
            t.created_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'
          ) AS source_created_at,
          to_char(
            t.created_at AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'
          ) AS discovered_at
        FROM torrents t
        WHERE {where_clause}
        ORDER BY t.created_at, t.info_hash
        LIMIT {int(batch_size)}
        """,
    )
    for row in rows:
        row["info_hash"] = str(row["info_hash"]).lower()
        row["total_size"] = int(row.get("total_size") or 0)
    return rows


def fetch_file_rows(
    container: str, info_hashes: Sequence[str]
) -> dict[str, list[dict[str, Any]]]:
    if not info_hashes:
        return {}
    values = ", ".join(
        f"(decode('{sql_literal(info_hash.lower())}', 'hex'))"
        for info_hash in info_hashes
    )
    rows = run_psql_copy_rows(
        container,
        f"""
        WITH wanted(info_hash) AS (
          VALUES {values}
        )
        SELECT
          encode(tf.info_hash, 'hex') AS info_hash,
          tf.index AS file_index,
          tf.path AS path,
          tf.size AS size
        FROM torrent_files tf
        JOIN wanted w
          ON w.info_hash = tf.info_hash
        ORDER BY tf.info_hash, tf.index
        """,
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["info_hash"]).lower()].append(
            {
                "path": str(row.get("path") or ""),
                "size": int(row.get("size") or 0),
            }
        )
    return grouped


def fetch_id_rows(
    container: str, info_hashes: Sequence[str]
) -> dict[str, dict[str, str | None]]:
    if not info_hashes:
        return {}
    batches = [
        list(batch)
        for batch in chunked(list(info_hashes), INFO_HASH_LOOKUP_BATCH_SIZE)
        if batch
    ]
    merged: dict[str, dict[str, str | None]] = {}
    if len(batches) == 1:
        row_batches = [_fetch_id_rows_batch(container, batches[0])]
    else:
        max_workers = min(ID_LOOKUP_MAX_PARALLEL_QUERIES, len(batches))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_fetch_id_rows_batch, container, batch)
                for batch in batches
            ]
            row_batches = [future.result() for future in futures]

    for rows in row_batches:
        for row in rows:
            if not row.get("info_hash"):
                continue
            merged[str(row["info_hash"]).lower()] = {
                "tmdb_id": str(row["tmdb_id"]).strip() if row.get("tmdb_id") else None,
                "imdb_id": str(row["imdb_id"]).strip() if row.get("imdb_id") else None,
            }
    return merged


def _fetch_id_rows_batch(
    container: str, info_hashes: Sequence[str]
) -> list[dict[str, str]]:
    values = ", ".join(
        f"(decode('{sql_literal(info_hash.lower())}', 'hex'))"
        for info_hash in info_hashes
    )
    return run_psql_copy_rows(
        container,
        f"""
        WITH wanted(info_hash) AS (
          VALUES {values}
        )
        SELECT
          encode(w.info_hash, 'hex') AS info_hash,
          MAX(CASE WHEN tc.content_source = 'tmdb' THEN tc.content_id ELSE NULL END) AS tmdb_id,
          MAX(
            CASE
              WHEN tc.content_source = 'tmdb'
               AND ca.source = 'imdb'
               AND ca.key = 'id'
              THEN ca.value
              ELSE NULL
            END
          ) AS imdb_id
        FROM wanted w
        LEFT JOIN torrent_contents tc
          ON tc.info_hash = w.info_hash
        LEFT JOIN content_attributes ca
          ON ca.content_type = tc.content_type
         AND ca.content_source = tc.content_source
         AND ca.content_id = tc.content_id
         AND ca.source = 'imdb'
         AND ca.key = 'id'
        GROUP BY w.info_hash
        """,
    )


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        f"""
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA temp_store=MEMORY;
        PRAGMA busy_timeout=30000;
        PRAGMA cache_size=-200000;
        PRAGMA mmap_size=30000000000;
        PRAGMA foreign_keys=OFF;

        CREATE TABLE IF NOT EXISTS live_promotions (
            info_hash TEXT PRIMARY KEY,
            torrent_id INTEGER NOT NULL UNIQUE,
            source_updated_at TEXT NOT NULL,
            promoted_at INTEGER NOT NULL,
            content_class TEXT NOT NULL,
            reject_reason TEXT,
            reason_codes TEXT NOT NULL DEFAULT '',
            confidence_tier TEXT NOT NULL DEFAULT 'none',
            match_source TEXT,
            contract_version TEXT NOT NULL DEFAULT '{AUTHORITATIVE_CONTRACT_VERSION}'
        );

        CREATE TABLE IF NOT EXISTS live_promotion_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )
    existing_columns = {
        row[1]
        for row in connection.execute("PRAGMA table_info(media_index)").fetchall()
    }
    for name, sql_type, default in [
        ("canonical_title_key", "TEXT", "''"),
        ("movie_key", "TEXT", "''"),
        ("series_key", "TEXT", "''"),
        ("aliases_text", "TEXT", "''"),
        ("imdb_id", "TEXT", "NULL"),
        ("tmdb_id", "TEXT", "NULL"),
        ("exact_episode_key", "TEXT", "NULL"),
        ("season_pack_key", "TEXT", "NULL"),
        ("confidence_tier", "TEXT", "'none'"),
        ("reject_reason", "TEXT", "NULL"),
        ("reason_codes", "TEXT", "''"),
        ("match_source", "TEXT", "NULL"),
        ("contract_version", "TEXT", f"'{AUTHORITATIVE_CONTRACT_VERSION}'"),
    ]:
        if name not in existing_columns:
            connection.execute(
                f"ALTER TABLE media_index ADD COLUMN {name} {sql_type} DEFAULT {default}"
            )
    promotion_columns = {
        row[1]
        for row in connection.execute("PRAGMA table_info(live_promotions)").fetchall()
    }
    for name, sql_type, default in [
        ("reject_reason", "TEXT", "NULL"),
        ("reason_codes", "TEXT", "''"),
        ("confidence_tier", "TEXT", "'none'"),
        ("match_source", "TEXT", "NULL"),
        ("contract_version", "TEXT", f"'{AUTHORITATIVE_CONTRACT_VERSION}'"),
    ]:
        if name not in promotion_columns:
            connection.execute(
                f"ALTER TABLE live_promotions ADD COLUMN {name} {sql_type} DEFAULT {default}"
            )
    connection.executescript(
        f"""
        CREATE INDEX IF NOT EXISTS media_index_canonical_title_key_idx ON media_index(canonical_title_key);
        CREATE INDEX IF NOT EXISTS media_index_movie_key_idx ON media_index(movie_key);
        CREATE INDEX IF NOT EXISTS media_index_series_key_idx ON media_index(series_key);
        CREATE INDEX IF NOT EXISTS media_index_imdb_id_idx ON media_index(imdb_id);
        CREATE INDEX IF NOT EXISTS media_index_tmdb_id_idx ON media_index(tmdb_id);
        CREATE INDEX IF NOT EXISTS media_index_exact_episode_key_idx ON media_index(exact_episode_key);
        CREATE INDEX IF NOT EXISTS media_index_season_pack_key_idx ON media_index(season_pack_key);
        CREATE TEMP TABLE IF NOT EXISTS staging_media_batch (
            torrent_id INTEGER NOT NULL,
            info_hash TEXT NOT NULL,
            name TEXT NOT NULL,
            total_size INTEGER NOT NULL,
            discovered_on INTEGER NOT NULL,
            norm_title TEXT NOT NULL,
            title_key TEXT NOT NULL,
            canonical_title_key TEXT NOT NULL,
            movie_key TEXT NOT NULL,
            series_key TEXT NOT NULL,
            aliases_text TEXT NOT NULL,
            imdb_id TEXT,
            tmdb_id TEXT,
            year INTEGER,
            content_class TEXT NOT NULL,
            is_anime INTEGER NOT NULL,
            confidence REAL NOT NULL,
            confidence_tier TEXT NOT NULL,
            reject_reason TEXT,
            reason_codes TEXT NOT NULL,
            match_source TEXT,
            contract_version TEXT NOT NULL,
            season INTEGER,
            episode_start INTEGER,
            episode_end INTEGER,
            exact_episode_key TEXT,
            season_pack_key TEXT,
            episode_count INTEGER NOT NULL,
            has_exact_episode INTEGER NOT NULL,
            is_multi_episode INTEGER NOT NULL,
            is_season_pack INTEGER NOT NULL,
            adult_score INTEGER NOT NULL,
            software_score INTEGER NOT NULL,
            media_score REAL NOT NULL,
            resolution TEXT,
            source_hint TEXT,
            codec_hint TEXT,
            search_text TEXT NOT NULL
        );
        CREATE TEMP TABLE IF NOT EXISTS staging_promotion_batch (
            info_hash TEXT NOT NULL,
            torrent_id INTEGER NOT NULL,
            source_updated_at TEXT NOT NULL,
            promoted_at INTEGER NOT NULL,
            content_class TEXT NOT NULL,
            reject_reason TEXT,
            reason_codes TEXT NOT NULL,
            confidence_tier TEXT NOT NULL,
            match_source TEXT,
            contract_version TEXT NOT NULL DEFAULT '{AUTHORITATIVE_CONTRACT_VERSION}'
        );
        """
    )
    media_columns = {
        str(row["name"]): str(row["type"] or "").upper()
        for row in connection.execute("PRAGMA table_info(media_index)").fetchall()
    }
    total_size_type = media_columns.get("total_size", "")
    total_size_integer_affinity = "INT" in total_size_type
    indexed_columns: set[str] = set()
    for index_row in connection.execute("PRAGMA index_list(media_index)").fetchall():
        index_name = str(index_row["name"] or "")
        if not index_name:
            continue
        escaped_index_name = index_name.replace("'", "''")
        for column_row in connection.execute(
            f"PRAGMA index_info('{escaped_index_name}')"
        ).fetchall():
            column_name = str(column_row["name"] or "")
            if column_name:
                indexed_columns.add(column_name)
    missing_lookup_indexes = [
        name for name in REQUIRED_LOOKUP_INDEX_COLUMNS if name not in indexed_columns
    ]
    if not total_size_integer_affinity or missing_lookup_indexes:
        print(
            "[warn] SQLite schema drift detected: "
            f"total_size_type={total_size_type or 'missing'} "
            f"missing_lookup_indexes={','.join(missing_lookup_indexes) or '-'}; "
            "consider rebuild via scripts/rebuild_live_sqlite.py",
            file=sys.stderr,
        )
    connection.commit()


def next_torrent_id(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT MAX(torrent_id)
        FROM (
            SELECT MAX(torrent_id) AS torrent_id FROM media_index
            UNION ALL
            SELECT MAX(torrent_id) AS torrent_id FROM live_promotions
        )
        """
    ).fetchone()
    return int(row[0] or 0) + 1


def chunked(values: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def fetch_existing_media_rows(
    connection: sqlite3.Connection, info_hashes: Sequence[str]
) -> dict[str, sqlite3.Row]:
    rows: dict[str, sqlite3.Row] = {}
    for batch in chunked(list(info_hashes), INFO_HASH_LOOKUP_BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        for row in connection.execute(
            f"""
            SELECT torrent_id, {", ".join(MEDIA_COMPARE_COLUMNS)}
            FROM media_index
            WHERE info_hash IN ({placeholders})
            """,
            list(batch),
        ):
            rows[str(row["info_hash"]).lower()] = row
    return rows


def fetch_existing_promotions(
    connection: sqlite3.Connection, info_hashes: Sequence[str]
) -> dict[str, sqlite3.Row]:
    rows: dict[str, sqlite3.Row] = {}
    for batch in chunked(list(info_hashes), INFO_HASH_LOOKUP_BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        for row in connection.execute(
            f"""
            SELECT
                info_hash,
                torrent_id,
                source_updated_at,
                promoted_at,
                content_class,
                reject_reason,
                reason_codes,
                confidence_tier,
                match_source,
                contract_version
            FROM live_promotions
            WHERE info_hash IN ({placeholders})
            """,
            list(batch),
        ):
            rows[str(row["info_hash"]).lower()] = row
    return rows


def media_row_matches(existing: sqlite3.Row, payload: dict[str, Any]) -> bool:
    for column in MEDIA_COMPARE_COLUMNS:
        if existing[column] != payload[column]:
            return False
    return True


def promotion_row_matches(
    existing: sqlite3.Row | None,
    *,
    torrent_id: int,
    source_created_at: str,
    content_class: str,
    reject_reason: str | None,
    reason_codes: str,
    confidence_tier: str,
    match_source: str | None,
    contract_version: str,
) -> bool:
    if existing is None:
        return False
    return (
        int(existing["torrent_id"]) == int(torrent_id)
        and str(existing["source_updated_at"]) == str(source_created_at)
        and str(existing["content_class"]) == str(content_class)
        and _clean_optional_text(existing["reject_reason"])
        == _clean_optional_text(reject_reason)
        and str(existing["reason_codes"] or "") == str(reason_codes or "")
        and str(existing["confidence_tier"] or "") == str(confidence_tier or "")
        and _clean_optional_text(existing["match_source"])
        == _clean_optional_text(match_source)
        and str(existing["contract_version"] or "") == str(contract_version or "")
    )


def record_audit_sample(
    bucket: dict[str, list[dict[str, Any]]],
    reason: str,
    sample_limit: int,
    row: dict[str, Any],
) -> None:
    samples = bucket.setdefault(reason, [])
    if len(samples) >= sample_limit:
        return
    samples.append(
        {
            "infoHash": str(row["info_hash"]).lower(),
            "name": str(row.get("torrent_name") or ""),
            "contentClass": str(row.get("content_class") or ""),
            "year": row.get("year"),
            "sourceCreatedAt": str(row.get("source_created_at") or ""),
            "rejectReason": row.get("reject_reason"),
            "reasonCodes": str(row.get("reason_codes") or ""),
            "confidenceTier": str(row.get("confidence_tier") or ""),
            "matchSource": row.get("match_source"),
            "contractVersion": str(row.get("contract_version") or ""),
        }
    )


def classify_work_item(row: dict[str, Any]) -> dict[str, Any]:
    torrent = SampledTorrent(
        id=0,
        info_hash=str(row["info_hash"]).lower(),
        name=display_text(str(row["torrent_name"])),
        total_size=int(row.get("total_size") or 0),
        discovered_at=str(
            row.get("discovered_at") or row.get("source_created_at") or ""
        ),
    )
    file_rows = [
        {
            "path": str(file_row.get("path") or ""),
            "size": int(file_row.get("size") or 0),
        }
        for file_row in (row.get("files") or [])
        if str(file_row.get("path") or "").strip()
    ]
    classification = classify_compact_row(
        torrent,
        file_rows,
        STRICT_MIN_SIZE,
        BROAD_MIN_SIZE,
    )
    match_details = authoritative_match_details(torrent, classification)
    import_hints = authoritative_import_hints(
        torrent,
        file_rows,
        classification,
        match_details=match_details,
    )
    match_source = (
        str(
            getattr(classification, "match_source", "")
            or match_details.get("match_source")
            or ""
        ).strip()
        or None
    )
    contract_version = (
        str(
            getattr(classification, "contract_version", "")
            or AUTHORITATIVE_CONTRACT_VERSION
        ).strip()
        or AUTHORITATIVE_CONTRACT_VERSION
    )
    if match_source and not getattr(classification, "match_source", None):
        setattr(classification, "match_source", match_source)
    if contract_version != getattr(classification, "contract_version", ""):
        setattr(classification, "contract_version", contract_version)
    content_class = str(getattr(classification, "content_class", "") or "")
    result = {
        "info_hash": str(row["info_hash"]).lower(),
        "torrent_name": str(row.get("torrent_name") or ""),
        "source_created_at": str(row.get("source_created_at") or ""),
        "content_class": content_class,
        "year": getattr(classification, "year", None),
        "legacy_drop_reason": None,
        "payload": None,
        "skip_reason": None,
        "reject_reason": str(getattr(classification, "reject_reason", "") or "").strip()
        or None,
        "reason_codes": serialize_reason_codes(
            getattr(classification, "reason_codes", ()) or ()
        ),
        "confidence_tier": str(
            getattr(classification, "confidence_tier", "") or ""
        ).strip()
        or "none",
        "match_source": match_source,
        "contract_version": contract_version,
    }
    if not import_hints.get("contentType"):
        result["legacy_drop_reason"] = "missing_bitmagnet_content_type"

    if content_class == "reject":
        result["reject_reason"] = result["reject_reason"] or "other"
        return result

    payload, skip_reason = lean_contract.row_from_classification(
        torrent_id=0,
        info_hash=str(row["info_hash"]).lower(),
        name=str(row.get("torrent_name") or ""),
        total_size=int(row.get("total_size") or 0),
        discovered_at=str(
            row.get("discovered_at") or row.get("source_created_at") or ""
        ),
        classification=classification,
        imdb_id=match_details.get("imdb_id"),
        tmdb_id=match_details.get("tmdb_id"),
        match_source=match_source,
        contract_version=contract_version,
    )
    result["payload"] = payload
    result["skip_reason"] = skip_reason
    return result


def classify_rows(
    rows: list[dict[str, Any]],
    *,
    workers: int,
    executor: ProcessPoolExecutor | None,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    if workers <= 1 or executor is None:
        return [classify_work_item(row) for row in rows]
    chunksize = max(1, len(rows) // max(1, workers * 4))
    return list(executor.map(classify_work_item, rows, chunksize=chunksize))


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def apply_canonical_enrichment(
    payload: dict[str, Any],
    enrichment: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(payload)
    enrichment = enrichment or {}
    if merged.get("tmdb_id") is None:
        merged["tmdb_id"] = _clean_optional_text(enrichment.get("tmdb_id"))
    if merged.get("imdb_id") is None:
        merged["imdb_id"] = _clean_optional_text(enrichment.get("imdb_id"))
    if merged.get("match_source") is None and (
        merged.get("tmdb_id") is not None or merged.get("imdb_id") is not None
    ):
        merged["match_source"] = "bitmagnet_enrichment"
    return merged


def set_meta(connection: sqlite3.Connection, key: str, value: Any) -> None:
    connection.execute(
        """
        INSERT INTO live_promotion_meta(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, json.dumps(value, ensure_ascii=False)),
    )


def stage_media_rows(
    connection: sqlite3.Connection, rows: list[dict[str, Any]]
) -> None:
    connection.execute("DELETE FROM staging_media_batch")
    if not rows:
        return
    placeholders = ",".join("?" for _ in MEDIA_STAGE_COLUMNS)
    connection.executemany(
        f"""
        INSERT INTO staging_media_batch ({", ".join(MEDIA_STAGE_COLUMNS)})
        VALUES ({placeholders})
        """,
        [tuple(row[column] for column in MEDIA_STAGE_COLUMNS) for row in rows],
    )


def stage_promotion_rows(
    connection: sqlite3.Connection, rows: list[dict[str, Any]]
) -> None:
    connection.execute("DELETE FROM staging_promotion_batch")
    if not rows:
        return
    placeholders = ",".join("?" for _ in PROMOTION_STAGE_COLUMNS)
    connection.executemany(
        f"""
        INSERT INTO staging_promotion_batch ({", ".join(PROMOTION_STAGE_COLUMNS)})
        VALUES ({placeholders})
        """,
        [tuple(row[column] for column in PROMOTION_STAGE_COLUMNS) for row in rows],
    )


def merge_staged_media(connection: sqlite3.Connection) -> None:
    connection.execute(
        f"""
        INSERT OR REPLACE INTO media_index ({", ".join(MEDIA_STAGE_COLUMNS[:-1])})
        SELECT {", ".join(column for column in MEDIA_STAGE_COLUMNS if column != "search_text")}
        FROM staging_media_batch
        """
    )


def merge_staged_promotions(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO live_promotions (
            info_hash,
            torrent_id,
            source_updated_at,
            promoted_at,
            content_class,
            reject_reason,
            reason_codes,
            confidence_tier,
            match_source,
            contract_version
        )
        SELECT
            info_hash,
            torrent_id,
            source_updated_at,
            promoted_at,
            content_class,
            reject_reason,
            reason_codes,
            confidence_tier,
            match_source,
            contract_version
        FROM staging_promotion_batch
        """
    )


def refresh_fts(
    connection: sqlite3.Connection,
    *,
    deleted_rows: list[tuple[int, str]],
    inserted_rows: list[tuple[int, str]],
    batch_size: int,
) -> None:
    for batch in chunked(deleted_rows, batch_size):
        connection.executemany(
            "INSERT INTO media_fts(media_fts, rowid, search_text) VALUES('delete', ?, ?)",
            list(batch),
        )
    for batch in chunked(inserted_rows, batch_size):
        connection.executemany(
            "INSERT INTO media_fts(rowid, search_text) VALUES (?, ?)",
            list(batch),
        )


def sync_batch(
    connection: sqlite3.Connection,
    container: str,
    rows: list[dict[str, Any]],
    *,
    apply_changes: bool,
    sample_limit: int,
    classifier_workers: int,
    executor: ProcessPoolExecutor | None,
    fts_batch_size: int,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "scannedRows": len(rows),
        "eligibleRows": 0,
        "rejectedRows": 0,
        "insertedRows": 0,
        "updatedRows": 0,
        "noopRows": 0,
        "skippedRows": 0,
        "classCounts": Counter(),
        "rejectReasons": Counter(),
        "skipReasons": Counter(),
        "legacyBridgeDropReasons": Counter(),
        "enrichmentCoverage": Counter(),
        "timings": {
            "classifySeconds": 0.0,
            "enrichSeconds": 0.0,
            "mergeSeconds": 0.0,
        },
        "samples": {
            "skipReasons": {},
            "legacyBridgeDropReasons": {},
            "rejectReasons": {},
        },
    }

    classify_started = time.perf_counter()
    classified_rows = classify_rows(
        rows,
        workers=classifier_workers,
        executor=executor,
    )
    report["timings"]["classifySeconds"] = time.perf_counter() - classify_started

    accepted_hashes = [
        row["info_hash"]
        for row in classified_rows
        if row["content_class"] != "reject" and row.get("payload") is not None
    ]

    enrich_started = time.perf_counter()
    ids_by_hash = fetch_id_rows(container, accepted_hashes)
    report["timings"]["enrichSeconds"] = time.perf_counter() - enrich_started

    existing_media = fetch_existing_media_rows(connection, accepted_hashes)
    existing_promotions = fetch_existing_promotions(connection, accepted_hashes)
    current_id = next_torrent_id(connection)
    promoted_at = int(time.time())
    staged_media: list[dict[str, Any]] = []
    staged_promotions: list[dict[str, Any]] = []
    fts_deletes: list[tuple[int, str]] = []
    fts_inserts: list[tuple[int, str]] = []

    for result in classified_rows:
        content_class = str(result["content_class"] or "")
        report["classCounts"][content_class] += 1

        if content_class == "reject":
            report["rejectedRows"] += 1
            reason = str(result.get("reject_reason") or "other")
            report["rejectReasons"][reason] += 1
            record_audit_sample(
                report["samples"]["rejectReasons"], reason, sample_limit, result
            )
            continue

        report["eligibleRows"] += 1
        legacy_reason = result.get("legacy_drop_reason")
        if legacy_reason:
            report["legacyBridgeDropReasons"][str(legacy_reason)] += 1
            record_audit_sample(
                report["samples"]["legacyBridgeDropReasons"],
                str(legacy_reason),
                sample_limit,
                result,
            )

        payload = result.get("payload")
        if payload is None:
            report["skippedRows"] += 1
            reason = str(result.get("skip_reason") or "bridge_invariant")
            report["skipReasons"][reason] += 1
            record_audit_sample(
                report["samples"]["skipReasons"], reason, sample_limit, result
            )
            continue

        payload = dict(payload)
        info_hash = str(payload["info_hash"]).lower()
        enrichment = ids_by_hash.get(info_hash) or {}
        payload = apply_canonical_enrichment(payload, enrichment)
        payload["search_text"] = lean_contract.search_text_for(payload)
        if payload["tmdb_id"] is not None:
            report["enrichmentCoverage"]["tmdbId"] += 1
        if payload["imdb_id"] is not None:
            report["enrichmentCoverage"]["imdbId"] += 1

        existing_media_row = existing_media.get(info_hash)
        existing_promotion_row = existing_promotions.get(info_hash)
        if existing_media_row is not None:
            torrent_id = int(existing_media_row["torrent_id"])
        elif existing_promotion_row is not None:
            torrent_id = int(existing_promotion_row["torrent_id"])
        else:
            torrent_id = current_id
            current_id += 1
        payload["torrent_id"] = torrent_id

        media_same = existing_media_row is not None and media_row_matches(
            existing_media_row, payload
        )
        promotion_same = promotion_row_matches(
            existing_promotion_row,
            torrent_id=torrent_id,
            source_created_at=str(result["source_created_at"]),
            content_class=str(payload["content_class"]),
            reject_reason=payload.get("reject_reason"),
            reason_codes=str(payload.get("reason_codes") or ""),
            confidence_tier=str(payload.get("confidence_tier") or ""),
            match_source=payload.get("match_source"),
            contract_version=str(
                payload.get("contract_version") or AUTHORITATIVE_CONTRACT_VERSION
            ),
        )

        if existing_media_row is None:
            action = "insert"
        elif media_same and promotion_same:
            action = "noop"
        else:
            action = "update"

        if action == "insert":
            report["insertedRows"] += 1
        elif action == "update":
            report["updatedRows"] += 1
        else:
            report["noopRows"] += 1

        if not media_same:
            staged_media.append(payload)
            if existing_media_row is not None:
                fts_deletes.append(
                    (
                        torrent_id,
                        lean_contract.search_text_for(dict(existing_media_row)),
                    )
                )
            fts_inserts.append((torrent_id, payload["search_text"]))

        if not promotion_same:
            staged_promotions.append(
                {
                    "info_hash": payload["info_hash"],
                    "torrent_id": torrent_id,
                    "source_updated_at": str(result["source_created_at"]),
                    "promoted_at": promoted_at,
                    "content_class": payload["content_class"],
                    "reject_reason": payload.get("reject_reason"),
                    "reason_codes": str(payload.get("reason_codes") or ""),
                    "confidence_tier": str(payload.get("confidence_tier") or ""),
                    "match_source": payload.get("match_source"),
                    "contract_version": str(
                        payload.get("contract_version")
                        or AUTHORITATIVE_CONTRACT_VERSION
                    ),
                }
            )

    if not apply_changes:
        for key in (
            "classCounts",
            "rejectReasons",
            "skipReasons",
            "legacyBridgeDropReasons",
            "enrichmentCoverage",
        ):
            report[key] = dict(report[key])
        return report

    merge_started = time.perf_counter()
    connection.execute("BEGIN IMMEDIATE")
    try:
        stage_media_rows(connection, staged_media)
        stage_promotion_rows(connection, staged_promotions)
        if staged_media:
            merge_staged_media(connection)
            refresh_fts(
                connection,
                deleted_rows=fts_deletes,
                inserted_rows=fts_inserts,
                batch_size=fts_batch_size,
            )
        if staged_promotions:
            merge_staged_promotions(connection)
        set_meta(connection, "lastRunAt", datetime.now(timezone.utc).isoformat())
        set_meta(connection, "cursorType", "torrent_created_at")
        set_meta(
            connection, "authoritativeContractVersion", AUTHORITATIVE_CONTRACT_VERSION
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    report["timings"]["mergeSeconds"] = time.perf_counter() - merge_started

    for key in (
        "classCounts",
        "rejectReasons",
        "skipReasons",
        "legacyBridgeDropReasons",
        "enrichmentCoverage",
    ):
        report[key] = dict(report[key])
    return report


def merge_reports(total: dict[str, Any], batch: dict[str, Any]) -> None:
    for key in (
        "scannedRows",
        "eligibleRows",
        "rejectedRows",
        "insertedRows",
        "updatedRows",
        "noopRows",
        "skippedRows",
    ):
        total[key] += int(batch.get(key, 0) or 0)
    for key in (
        "classCounts",
        "rejectReasons",
        "skipReasons",
        "legacyBridgeDropReasons",
        "enrichmentCoverage",
    ):
        counter = Counter(total.get(key, {}))
        counter.update(batch.get(key, {}))
        total[key] = dict(counter)
    for key in ("extractSeconds", "classifySeconds", "enrichSeconds", "mergeSeconds"):
        total["timings"][key] += float(batch.get("timings", {}).get(key, 0.0) or 0.0)
    for bucket in ("skipReasons", "legacyBridgeDropReasons", "rejectReasons"):
        for reason, samples in batch.get("samples", {}).get(bucket, {}).items():
            current = total["samples"][bucket].setdefault(reason, [])
            if len(current) < total["sampleLimit"]:
                remaining = total["sampleLimit"] - len(current)
                current.extend(samples[:remaining])


def backfill_ids(
    connection: sqlite3.Connection, container: str, batch_size: int
) -> dict[str, int]:
    scanned = 0
    updated_tmdb = 0
    updated_imdb = 0
    last_source_updated_at = ""
    last_info_hash = ""

    while True:
        batch = connection.execute(
            """
            SELECT lp.info_hash, lp.torrent_id, lp.source_updated_at
            FROM live_promotions lp
            WHERE (
                lp.source_updated_at > ?
                OR (lp.source_updated_at = ? AND lp.info_hash > ?)
            )
            ORDER BY lp.source_updated_at, lp.info_hash
            LIMIT ?
            """,
            (
                last_source_updated_at,
                last_source_updated_at,
                last_info_hash,
                batch_size,
            ),
        ).fetchall()
        if not batch:
            break

        info_hashes = [str(row[0]).lower() for row in batch]
        ids_by_hash = fetch_id_rows(container, info_hashes)

        connection.execute("BEGIN IMMEDIATE")
        try:
            for info_hash, torrent_id, source_updated_at in batch:
                scanned += 1
                lookup = ids_by_hash.get(str(info_hash).lower())
                if not lookup:
                    last_source_updated_at = str(source_updated_at)
                    last_info_hash = str(info_hash).lower()
                    continue
                tmdb_id = lookup.get("tmdb_id")
                imdb_id = lookup.get("imdb_id")
                if tmdb_id is None and imdb_id is None:
                    last_source_updated_at = str(source_updated_at)
                    last_info_hash = str(info_hash).lower()
                    continue

                current = connection.execute(
                    "SELECT tmdb_id, imdb_id FROM media_index WHERE torrent_id = ?",
                    (torrent_id,),
                ).fetchone()
                if current is None:
                    last_source_updated_at = str(source_updated_at)
                    last_info_hash = str(info_hash).lower()
                    continue

                current_tmdb, current_imdb = current
                next_tmdb = current_tmdb if current_tmdb is not None else tmdb_id
                next_imdb = current_imdb if current_imdb is not None else imdb_id
                if next_tmdb == current_tmdb and next_imdb == current_imdb:
                    last_source_updated_at = str(source_updated_at)
                    last_info_hash = str(info_hash).lower()
                    continue

                connection.execute(
                    """
                    UPDATE media_index
                    SET tmdb_id = ?, imdb_id = ?
                    WHERE torrent_id = ?
                    """,
                    (next_tmdb, next_imdb, torrent_id),
                )
                if current_tmdb is None and next_tmdb is not None:
                    updated_tmdb += 1
                if current_imdb is None and next_imdb is not None:
                    updated_imdb += 1
                last_source_updated_at = str(source_updated_at)
                last_info_hash = str(info_hash).lower()
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return {
        "scannedRows": scanned,
        "updatedTmdbIds": updated_tmdb,
        "updatedImdbIds": updated_imdb,
    }


def write_report(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    save_json(path, payload)


def print_progress(
    *,
    mode: str,
    batches: int,
    checkpoint: dict[str, Any],
    batch_report: dict[str, Any],
    estimated_rows: int | None,
    last_created_at: str,
) -> None:
    scanned_total = int(checkpoint.get("scannedRows", 0) or 0)
    elapsed_total = (
        float(checkpoint.get("extractSeconds", 0.0) or 0.0)
        + float(checkpoint.get("classifySeconds", 0.0) or 0.0)
        + float(checkpoint.get("mergeSeconds", 0.0) or 0.0)
    )
    rate = scanned_total / elapsed_total if elapsed_total > 0 else 0.0
    remaining = None
    eta_seconds = None
    if estimated_rows is not None and estimated_rows > scanned_total and rate > 0:
        remaining = estimated_rows - scanned_total
        eta_seconds = remaining / rate
    print(
        progress_line(
            {
                "event": "bitmagnet-sync-progress",
                "mode": mode,
                "batches": batches,
                "lastCreatedAt": last_created_at,
                "scannedRows": scanned_total,
                "eligibleRows": int(checkpoint.get("eligibleRows", 0) or 0),
                "insertedRows": int(checkpoint.get("insertedRows", 0) or 0),
                "updatedRows": int(checkpoint.get("updatedRows", 0) or 0),
                "noopRows": int(checkpoint.get("noopRows", 0) or 0),
                "lastBatchScannedRows": int(batch_report.get("scannedRows", 0) or 0),
                "lastBatchEligibleRows": int(batch_report.get("eligibleRows", 0) or 0),
                "extractSeconds": round(
                    float(
                        batch_report.get("timings", {}).get("extractSeconds", 0.0)
                        or 0.0
                    ),
                    3,
                ),
                "classifySeconds": round(
                    float(
                        batch_report.get("timings", {}).get("classifySeconds", 0.0)
                        or 0.0
                    ),
                    3,
                ),
                "enrichSeconds": round(
                    float(
                        batch_report.get("timings", {}).get("enrichSeconds", 0.0) or 0.0
                    ),
                    3,
                ),
                "mergeSeconds": round(
                    float(
                        batch_report.get("timings", {}).get("mergeSeconds", 0.0) or 0.0
                    ),
                    3,
                ),
                "rowsPerSecond": round(rate, 2),
                "estimatedSourceRows": estimated_rows,
                "estimatedRemainingRows": remaining,
                "etaSeconds": round(eta_seconds, 1)
                if eta_seconds is not None
                else None,
            }
        ),
        flush=True,
    )


def main() -> None:
    args = parse_args()
    checkpoint_path = effective_checkpoint_path(args)
    checkpoint = load_checkpoint(checkpoint_path)
    search_db = Path(args.search_db)
    if not search_db.exists():
        raise SystemExit(f"search db not found: {search_db}")

    if args.ensure_source_indexes:
        ensure_source_indexes(args.docker_container)

    if args.bootstrap_from_latest or args.bootstrap_now:
        if args.bootstrap_now:
            now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
            checkpoint["lastCreatedAt"] = now
            checkpoint["lastInfoHash"] = "ffffffffffffffffffffffffffffffffffffffff"
        else:
            latest = latest_source_cursor(args.docker_container)
            if latest is None:
                checkpoint["lastCreatedAt"] = ZERO_CURSOR
                checkpoint["lastInfoHash"] = ZERO_HASH
            else:
                checkpoint["lastCreatedAt"] = str(latest["source_created_at"])
                checkpoint["lastInfoHash"] = str(latest["info_hash"]).lower()
        checkpoint["schemaVersion"] = CHECKPOINT_SCHEMA_VERSION
        checkpoint["cursorType"] = "torrent_created_at"
        checkpoint["updatedAt"] = datetime.now(timezone.utc).isoformat()
        checkpoint["bootstrapFromLatest"] = bool(args.bootstrap_from_latest)
        checkpoint["bootstrapNow"] = bool(args.bootstrap_now)
        save_json(checkpoint_path, checkpoint)
        print(
            json.dumps(
                {
                    "searchDb": str(search_db),
                    "checkpoint": str(checkpoint_path),
                    "bootstrapFromLatest": bool(args.bootstrap_from_latest),
                    "bootstrapNow": bool(args.bootstrap_now),
                    "lastCreatedAt": checkpoint["lastCreatedAt"],
                    "lastInfoHash": checkpoint["lastInfoHash"],
                },
                ensure_ascii=False,
            )
        )
        return

    estimated_rows = (
        estimated_source_rows(args.docker_container) if args.drain_all else None
    )

    connection = sqlite3.connect(search_db)
    connection.row_factory = sqlite3.Row
    try:
        ensure_schema(connection)
        if args.backfill_ids:
            result = backfill_ids(
                connection, args.docker_container, args.backfill_batch_size
            )
            result["searchDb"] = str(search_db)
            result["backfillBatchSize"] = args.backfill_batch_size
            print(json.dumps(result, ensure_ascii=False))
            return

        total_report: dict[str, Any] = {
            "mode": "audit"
            if args.audit_only
            else ("wide-drain" if args.drain_all else "incremental-sync"),
            "searchDb": str(search_db),
            "checkpoint": str(checkpoint_path),
            "cursorType": "torrent_created_at",
            "sampleLimit": args.audit_sample_limit,
            "batches": 0,
            "scannedRows": 0,
            "eligibleRows": 0,
            "rejectedRows": 0,
            "insertedRows": 0,
            "updatedRows": 0,
            "noopRows": 0,
            "skippedRows": 0,
            "classCounts": {},
            "rejectReasons": {},
            "skipReasons": {},
            "legacyBridgeDropReasons": {},
            "enrichmentCoverage": {},
            "timings": {
                "extractSeconds": 0.0,
                "classifySeconds": 0.0,
                "enrichSeconds": 0.0,
                "mergeSeconds": 0.0,
            },
            "estimatedSourceRows": estimated_rows,
            "samples": {
                "skipReasons": {},
                "legacyBridgeDropReasons": {},
                "rejectReasons": {},
            },
        }

        last_created_at = normalize_cursor(
            args.created_after
            if args.audit_only and args.created_after
            else checkpoint.get("lastCreatedAt")
        )
        last_info_hash = (
            ZERO_HASH
            if args.audit_only and args.created_after
            else str(checkpoint.get("lastInfoHash") or ZERO_HASH).lower()
        )
        exhausted = False
        executor: ProcessPoolExecutor | None = None
        if args.classifier_workers > 1:
            executor = ProcessPoolExecutor(max_workers=args.classifier_workers)

        try:
            while True:
                if args.max_batches and total_report["batches"] >= args.max_batches:
                    break

                extract_started = time.perf_counter()
                header_batch = fetch_header_batch(
                    args.docker_container,
                    last_created_at=last_created_at,
                    last_info_hash=last_info_hash,
                    batch_size=args.batch_size,
                    created_after=args.created_after if not args.audit_only else None,
                    created_before=args.created_before,
                )
                if not header_batch:
                    exhausted = True
                    break
                file_map = fetch_file_rows(
                    args.docker_container,
                    [str(row["info_hash"]).lower() for row in header_batch],
                )
                for row in header_batch:
                    row["files"] = file_map.get(str(row["info_hash"]).lower(), [])
                extract_seconds = time.perf_counter() - extract_started

                batch_report = sync_batch(
                    connection,
                    args.docker_container,
                    header_batch,
                    apply_changes=not args.audit_only,
                    sample_limit=args.audit_sample_limit,
                    classifier_workers=args.classifier_workers,
                    executor=executor,
                    fts_batch_size=args.fts_batch_size,
                )
                batch_report["timings"]["extractSeconds"] = extract_seconds
                total_report["batches"] += 1
                merge_reports(total_report, batch_report)

                last_created_at = str(header_batch[-1]["source_created_at"])
                last_info_hash = str(header_batch[-1]["info_hash"]).lower()

                if not args.audit_only:
                    checkpoint["schemaVersion"] = CHECKPOINT_SCHEMA_VERSION
                    checkpoint["cursorType"] = "torrent_created_at"
                    checkpoint["lastCreatedAt"] = last_created_at
                    checkpoint["lastInfoHash"] = last_info_hash
                    checkpoint["scannedRows"] = int(
                        checkpoint.get("scannedRows", 0)
                    ) + int(batch_report["scannedRows"])
                    checkpoint["eligibleRows"] = int(
                        checkpoint.get("eligibleRows", 0)
                    ) + int(batch_report["eligibleRows"])
                    checkpoint["insertedRows"] = int(
                        checkpoint.get("insertedRows", 0)
                    ) + int(batch_report["insertedRows"])
                    checkpoint["updatedRows"] = int(
                        checkpoint.get("updatedRows", 0)
                    ) + int(batch_report["updatedRows"])
                    checkpoint["noopRows"] = int(checkpoint.get("noopRows", 0)) + int(
                        batch_report["noopRows"]
                    )
                    checkpoint["skippedRows"] = int(
                        checkpoint.get("skippedRows", 0)
                    ) + int(batch_report["skippedRows"])
                    checkpoint["extractSeconds"] = (
                        float(checkpoint.get("extractSeconds", 0.0)) + extract_seconds
                    )
                    checkpoint["classifySeconds"] = float(
                        checkpoint.get("classifySeconds", 0.0)
                    ) + float(batch_report["timings"]["classifySeconds"])
                    checkpoint["mergeSeconds"] = float(
                        checkpoint.get("mergeSeconds", 0.0)
                    ) + float(batch_report["timings"]["mergeSeconds"])
                    checkpoint["updatedAt"] = datetime.now(timezone.utc).isoformat()
                    save_json(checkpoint_path, checkpoint)
                    if (
                        args.progress_every_batches
                        and total_report["batches"] % args.progress_every_batches == 0
                    ):
                        print_progress(
                            mode=total_report["mode"],
                            batches=total_report["batches"],
                            checkpoint=checkpoint,
                            batch_report=batch_report,
                            estimated_rows=estimated_rows,
                            last_created_at=last_created_at,
                        )
        finally:
            if executor is not None:
                executor.shutdown()

        if not args.audit_only and args.drain_all and exhausted:
            connection.execute("BEGIN IMMEDIATE")
            try:
                set_meta(connection, "wideDrainCompleted", True)
                set_meta(
                    connection,
                    "wideDrainCompletedAt",
                    datetime.now(timezone.utc).isoformat(),
                )
                set_meta(connection, "wideDrainCheckpoint", str(checkpoint_path))
                set_meta(
                    connection,
                    "wideDrainCheckpointSchemaVersion",
                    CHECKPOINT_SCHEMA_VERSION,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            total_report["wideDrainCompleted"] = True
        else:
            total_report["wideDrainCompleted"] = False

        total_report["lastCreatedAt"] = last_created_at
        total_report["lastInfoHash"] = last_info_hash
        total_report["createdAfter"] = args.created_after
        total_report["createdBefore"] = args.created_before
        write_report(
            Path(args.audit_report) if args.audit_report else None, total_report
        )
        print(json.dumps(total_report, ensure_ascii=False))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
