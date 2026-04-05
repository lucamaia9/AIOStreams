#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from lib.classifier_runtime import bootstrap_classifier_runtime

RUNTIME_PATHS = bootstrap_classifier_runtime(__file__, include_export_dir=True)
ROOT_DIR = RUNTIME_PATHS.repo_root
EXPORT_DIR = RUNTIME_PATHS.export_dir
LEGACY_TOOLS_DIR = RUNTIME_PATHS.legacy_tools_dir

import import_compact_search_jsonl as compact_import
import promote_bitmagnet_live_to_compact as live_sync

DEFAULT_SEARCH_DB = ROOT_DIR / "data" / "comet-fresh" / "magnetico" / "active.search.sqlite3"
DEFAULT_STAGE_ROOT = DEFAULT_SEARCH_DB.parent / "rebuild-runs"
DEFAULT_BUILDER_DEPLOY_DIR = os.environ.get("REBUILD_LIVE_SQLITE_BUILDER_DEPLOY_DIR", "")
IMPORT_COMPACT_SEARCH_PATH = EXPORT_DIR / "import_compact_search_jsonl.py"
if not IMPORT_COMPACT_SEARCH_PATH.exists():
    IMPORT_COMPACT_SEARCH_PATH = LEGACY_TOOLS_DIR / "import_compact_search_jsonl.py"
ZERO_HASH = live_sync.ZERO_HASH

CANONICAL_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS canonical_title_family (
    family_key TEXT PRIMARY KEY,
    media_kind TEXT NOT NULL,
    canonical_title TEXT NOT NULL,
    canonical_year INTEGER,
    season_bucket TEXT NOT NULL,
    imdb_id TEXT,
    tmdb_id TEXT,
    confidence TEXT NOT NULL,
    resolver_source TEXT NOT NULL,
    evidence_count INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS canonical_title_family_media_idx
    ON canonical_title_family(media_kind, canonical_title, canonical_year, season_bucket);
CREATE INDEX IF NOT EXISTS canonical_title_family_ids_idx
    ON canonical_title_family(media_kind, tmdb_id, imdb_id);

CREATE TABLE IF NOT EXISTS canonical_title_variant (
    variant_key TEXT NOT NULL,
    family_key TEXT NOT NULL,
    media_kind TEXT NOT NULL,
    raw_title TEXT NOT NULL,
    normalized_title TEXT NOT NULL,
    year_hint INTEGER,
    season_bucket TEXT NOT NULL,
    confidence TEXT NOT NULL,
    variant_source TEXT NOT NULL,
    evidence_count INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (variant_key, family_key),
    FOREIGN KEY (family_key) REFERENCES canonical_title_family(family_key) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS canonical_title_variant_lookup_idx
    ON canonical_title_variant(media_kind, normalized_title, year_hint, season_bucket, confidence);
CREATE INDEX IF NOT EXISTS canonical_title_variant_family_idx
    ON canonical_title_variant(family_key, normalized_title);
"""
REQUIRED_LOOKUP_INDEX_COLUMNS = (
    "movie_key",
    "series_key",
    "imdb_id",
    "tmdb_id",
    "exact_episode_key",
    "season_pack_key",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild the live active.search.sqlite3 artifact from trusted historical input "
            "plus raw BitMagnet torrents/torrent_files, verify it, and optionally cut it over."
        )
    )
    parser.add_argument("--search-db", default=str(DEFAULT_SEARCH_DB))
    parser.add_argument(
        "--historical-manifest",
        help="Preferred strictVideo.manifest.json path for rebuilding the historical base.",
    )
    parser.add_argument(
        "--historical-seed-db",
        help=(
            "Fallback trusted historical seed DB when the strictVideo manifest is unavailable. "
            "Any tracked live overlay is stripped and rebuilt from raw BitMagnet data."
        ),
    )
    parser.add_argument(
        "--allow-empty-base",
        action="store_true",
        help="Create an empty lean DB instead of a historical base. Intended for bounded validation only.",
    )
    parser.add_argument("--stage-root", default=str(DEFAULT_STAGE_ROOT))
    parser.add_argument("--run-label", default="")
    parser.add_argument("--docker-container", default=live_sync.DEFAULT_CONTAINER)
    parser.add_argument("--cutover", action="store_true")
    parser.add_argument(
        "--builder-deploy-dir",
        default=DEFAULT_BUILDER_DEPLOY_DIR,
        help=(
            "Optional path for a second deployed copy of the rebuilt DB. "
            "Leave unset to keep only the live active.search.sqlite3 on-host."
        ),
    )
    parser.add_argument(
        "--skip-builder-mirror",
        action="store_true",
        help=(
            "Force-disable builder mirroring even when --builder-deploy-dir "
            "or REBUILD_LIVE_SQLITE_BUILDER_DEPLOY_DIR is set."
        ),
    )
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--checkpoint-every-batches", type=int, default=10)
    parser.add_argument("--progress-every-batches", type=int, default=5)
    parser.add_argument("--slice-minutes", type=int, default=30)
    parser.add_argument("--classifier-workers", type=int, default=live_sync.default_classifier_workers())
    parser.add_argument("--fts-batch-size", type=int, default=2000)
    parser.add_argument(
        "--live-created-before",
        help="Optional UTC upper bound for the live raw replay. Defaults to the source frontier captured at rebuild start.",
    )
    parser.add_argument(
        "--live-max-slices",
        type=int,
        default=0,
        help="Optional bounded replay limit for smoke validation. 0 means drain to the requested frontier.",
    )
    parser.add_argument(
        "--skip-live-drain",
        action="store_true",
        help="Skip the raw BitMagnet replay stage after preparing the base DB.",
    )
    parser.add_argument(
        "--integrity-check",
        choices=("quick", "full"),
        default="full",
        help="Integrity PRAGMA to require before reporting success or applying cutover.",
    )
    return parser.parse_args()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def sanitize_label(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip())
    cleaned = cleaned.strip("-._")
    return cleaned or "rebuild"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sqlite_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def run_checked(command: list[str]) -> None:
    subprocess.run(command, check=True)


def cleanup_sqlite_artifacts(path: Path) -> None:
    for suffix in ("", "-wal", "-shm", "-journal"):
        candidate = Path(f"{path}{suffix}")
        if candidate.exists():
            candidate.unlink()


def json_loads_or_raw(value: str) -> Any:
    try:
        return json.loads(value)
    except Exception:
        return value


def int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def int_or_none(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def float_or_default(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def text_or_none(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def confidence_tier_for(confidence: float) -> str:
    if confidence >= 0.9:
        return "high"
    if confidence >= 0.75:
        return "medium"
    return "low"


def prepare_existing_base_db(path: Path) -> dict[str, Any]:
    print(f"[base] preparing {path}", flush=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        live_sync.ensure_schema(connection)
        ensure_canonical_schema(connection)
        print("[base] stripping any tracked live overlay", flush=True)
        reset_report = reset_live_overlay(connection)
        print("[base] running ANALYZE", flush=True)
        connection.execute("ANALYZE")
        connection.commit()
        return reset_report
    finally:
        connection.close()


def copy_table_rows(
    source_connection: sqlite3.Connection,
    target_connection: sqlite3.Connection,
    table_name: str,
) -> int:
    source_tables = {
        row["name"]
        for row in source_connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    if table_name not in source_tables:
        return 0

    columns = [
        str(row["name"])
        for row in source_connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    ]
    if not columns:
        return 0

    column_sql = ", ".join(columns)
    rows = source_connection.execute(f"SELECT {column_sql} FROM {table_name}").fetchall()
    if not rows:
        return 0

    placeholders = ", ".join("?" for _ in columns)
    target_connection.executemany(
        f"INSERT OR REPLACE INTO {table_name} ({column_sql}) VALUES ({placeholders})",
        [tuple(row[column] for column in columns) for row in rows],
    )
    target_connection.commit()
    return len(rows)


def salvage_seed_historical_base(
    source_db: Path,
    build_db: Path,
    *,
    batch_size: int,
    progress_every_batches: int,
) -> dict[str, Any]:
    print(
        "[salvage] seed DB is malformed while stripping live overlay; "
        "rebuilding historical base from readable non-live rows",
        flush=True,
    )

    cleanup_sqlite_artifacts(build_db)

    source_connection = sqlite3.connect(f"file:{source_db}?mode=ro", uri=True)
    source_connection.row_factory = sqlite3.Row
    target_connection = compact_import.configure_connection(sqlite3.connect(build_db))
    target_connection.row_factory = sqlite3.Row

    try:
        compact_import.create_schema(target_connection)
        live_sync.ensure_schema(target_connection)
        ensure_canonical_schema(target_connection)

        source_build_meta = {
            str(key): json_loads_or_raw(value)
            for key, value in source_connection.execute(
                "SELECT key, value FROM build_meta"
            ).fetchall()
        }

        historical_count = int(
            source_connection.execute(
                """
                SELECT COUNT(*)
                FROM media_index
                WHERE torrent_id NOT IN (SELECT torrent_id FROM live_promotions)
                """
            ).fetchone()[0]
        )

        processed = 0
        last_torrent_id = 0
        batch_number = 0
        while True:
            source_rows = source_connection.execute(
                """
                SELECT
                    torrent_id,
                    info_hash,
                    name,
                    total_size,
                    discovered_on,
                    norm_title,
                    title_key,
                    canonical_title_key,
                    movie_key,
                    series_key,
                    aliases_text,
                    imdb_id,
                    tmdb_id,
                    year,
                    content_class,
                    is_anime,
                    confidence,
                    season,
                    episode_start,
                    episode_end,
                    exact_episode_key,
                    season_pack_key,
                    episode_count,
                    has_exact_episode,
                    is_multi_episode,
                    is_season_pack,
                    adult_score,
                    software_score,
                    media_score,
                    resolution,
                    source_hint,
                    codec_hint
                FROM media_index
                WHERE torrent_id > ?
                  AND torrent_id NOT IN (SELECT torrent_id FROM live_promotions)
                ORDER BY torrent_id
                LIMIT ?
                """,
                (last_torrent_id, batch_size),
            ).fetchall()
            if not source_rows:
                break

            batch_rows = []
            for source_row in source_rows:
                norm_title = str(source_row["norm_title"] or "").strip()
                title_key = str(source_row["title_key"] or "").strip()
                if not title_key:
                    title_key = compact_import.title_key_for(
                        str(source_row["name"] or ""),
                        norm_title,
                    )
                year = int_or_none(source_row["year"])
                movie_key = str(source_row["movie_key"] or "").strip()
                if not movie_key:
                    movie_key = compact_import.movie_key_for(title_key, year)
                series_key = str(source_row["series_key"] or "").strip() or title_key
                canonical_title_key = (
                    str(source_row["canonical_title_key"] or "").strip() or title_key
                )
                imdb_id = text_or_none(source_row["imdb_id"])
                tmdb_id = text_or_none(source_row["tmdb_id"])
                confidence = float_or_default(source_row["confidence"], 0.0)
                row = {
                    "torrent_id": int_or_default(source_row["torrent_id"]),
                    "info_hash": str(source_row["info_hash"] or ""),
                    "name": str(source_row["name"] or ""),
                    "total_size": int_or_default(source_row["total_size"]),
                    "discovered_on": int_or_default(source_row["discovered_on"]),
                    "norm_title": norm_title,
                    "title_key": title_key,
                    "canonical_title_key": canonical_title_key,
                    "movie_key": movie_key,
                    "series_key": series_key,
                    "aliases_text": str(source_row["aliases_text"] or ""),
                    "imdb_id": imdb_id,
                    "tmdb_id": tmdb_id,
                    "year": year,
                    "content_class": str(source_row["content_class"] or "unknown_video"),
                    "is_anime": int_or_default(source_row["is_anime"]),
                    "confidence": confidence,
                    "confidence_tier": confidence_tier_for(confidence),
                    "reject_reason": None,
                    "reason_codes": "",
                    "match_source": "legacy-salvage" if imdb_id or tmdb_id else None,
                    "contract_version": "legacy-salvage",
                    "season": int_or_none(source_row["season"]),
                    "episode_start": int_or_none(source_row["episode_start"]),
                    "episode_end": int_or_none(source_row["episode_end"]),
                    "exact_episode_key": text_or_none(source_row["exact_episode_key"]),
                    "season_pack_key": text_or_none(source_row["season_pack_key"]),
                    "episode_count": int_or_default(source_row["episode_count"]),
                    "has_exact_episode": int_or_default(source_row["has_exact_episode"]),
                    "is_multi_episode": int_or_default(source_row["is_multi_episode"]),
                    "is_season_pack": int_or_default(source_row["is_season_pack"]),
                    "adult_score": int_or_default(source_row["adult_score"]),
                    "software_score": int_or_default(source_row["software_score"]),
                    "media_score": float_or_default(source_row["media_score"], 0.0),
                    "resolution": text_or_none(source_row["resolution"]),
                    "source_hint": text_or_none(source_row["source_hint"]),
                    "codec_hint": text_or_none(source_row["codec_hint"]),
                }
                row["search_text"] = compact_import.search_text_for(row)
                batch_rows.append(row)

            inserted_rows, duplicate_rows = compact_import.flush_batch(
                target_connection, batch_rows
            )
            processed += len(batch_rows)
            batch_number += 1
            last_torrent_id = int_or_default(source_rows[-1]["torrent_id"])

            if batch_number % max(progress_every_batches, 1) == 0:
                print(
                    f"[salvage] processed {processed:,}/{historical_count:,} "
                    f"(inserted={inserted_rows:,} duplicate={duplicate_rows:,})",
                    flush=True,
                )

        canonical_family_rows = copy_table_rows(
            source_connection, target_connection, "canonical_title_family"
        )
        canonical_variant_rows = copy_table_rows(
            source_connection, target_connection, "canonical_title_variant"
        )

        build_meta = dict(source_build_meta)
        build_meta["source"] = "historical-seed-salvage"
        build_meta["salvaged_from"] = str(source_db)
        build_meta["salvaged_at"] = utc_now_iso()
        build_meta["historical_row_count"] = historical_count
        compact_import.write_build_meta(target_connection, build_meta)
        compact_import.finalize_schema(target_connection)
    finally:
        source_connection.close()
        target_connection.close()

    return {
        "mode": "historical-seed-salvage",
        "historicalRowCount": historical_count,
        "canonicalFamilyRows": canonical_family_rows,
        "canonicalVariantRows": canonical_variant_rows,
    }


def manifest_candidates(search_db: Path) -> list[Path]:
    parent = search_db.parent
    candidates: list[Path] = []
    for meta_name in ("active.import.report.json", "active.import.checkpoint.json"):
        meta_path = parent / meta_name
        if not meta_path.exists():
            continue
        try:
            payload = load_json(meta_path)
        except Exception:
            continue
        manifest_value = payload.get("manifestPath")
        if manifest_value:
            candidates.append(Path(str(manifest_value)).expanduser())
    default_manifest = (
        ROOT_DIR
        / "bitmagnet-media"
        / "deploy"
        / "upload"
        / "extracted"
        / "full-run-strictvideo"
        / "export"
        / "strictVideo.manifest.json"
    )
    candidates.append(default_manifest)
    legacy_manifest = (
        ROOT_DIR
        / "bitmagnet"
        / "classifier-stack"
        / "vps"
        / "upload"
        / "extracted"
        / "full-run-strictvideo"
        / "export"
        / "strictVideo.manifest.json"
    )
    candidates.append(legacy_manifest)
    for pipeline_manifest in (
        default_manifest.parent.parent / "full-pipeline.manifest.json",
        legacy_manifest.parent.parent / "full-pipeline.manifest.json",
    ):
        if pipeline_manifest.exists():
            candidates.append(pipeline_manifest.parent / "export" / "strictVideo.manifest.json")
    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def resolve_historical_input(args: argparse.Namespace, search_db: Path) -> dict[str, Any]:
    searched_candidates = [str(path) for path in manifest_candidates(search_db)]
    if args.historical_manifest:
        manifest_path = Path(args.historical_manifest).expanduser().resolve()
        if not manifest_path.exists():
            raise SystemExit(f"historical manifest not found: {manifest_path}")
        return {"mode": "manifest", "path": manifest_path, "searchedCandidates": searched_candidates}
    for candidate in manifest_candidates(search_db):
        if candidate.exists():
            return {"mode": "manifest", "path": candidate.resolve(), "searchedCandidates": searched_candidates}
    if args.historical_seed_db:
        seed_db = Path(args.historical_seed_db).expanduser().resolve()
        if not seed_db.exists():
            raise SystemExit(f"historical seed DB not found: {seed_db}")
        return {"mode": "seed-db", "path": seed_db, "searchedCandidates": searched_candidates}
    if args.allow_empty_base:
        return {"mode": "empty-base", "path": None, "searchedCandidates": searched_candidates}
    raise SystemExit(
        "No trusted historical manifest is available on this host. "
        "Provide --historical-manifest or --historical-seed-db. "
        f"Checked: {', '.join(searched_candidates)}"
    )


def ensure_canonical_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(CANONICAL_SCHEMA_SQL)
    connection.commit()


def reset_live_overlay(connection: sqlite3.Connection) -> dict[str, int]:
    existing_live_rows = int(connection.execute("SELECT count(*) FROM live_promotions").fetchone()[0])
    if existing_live_rows == 0:
        cleared_live_meta_rows = int(
            connection.execute("SELECT count(*) FROM live_promotion_meta").fetchone()[0]
        )
        connection.execute("DELETE FROM live_promotion_meta")
        connection.commit()
        return {
            "existingLivePromotionRows": 0,
            "removedMediaRows": 0,
            "clearedLiveMetaRows": cleared_live_meta_rows,
        }
    connection.execute("BEGIN IMMEDIATE")
    try:
        live_rows = connection.execute(
            """
            SELECT mi.torrent_id, mi.norm_title, mi.name, mi.canonical_title_key, mi.movie_key,
                   mi.series_key, mi.aliases_text, mi.imdb_id, mi.tmdb_id, mi.year, mi.season,
                   mi.episode_start, mi.episode_end, mi.exact_episode_key, mi.season_pack_key
            FROM media_index mi
            WHERE mi.info_hash IN (SELECT info_hash FROM live_promotions)
            """
        ).fetchall()
        removed_media_rows = int(
            connection.execute(
                "SELECT count(*) FROM media_index WHERE info_hash IN (SELECT info_hash FROM live_promotions)"
            ).fetchone()[0]
        )
        cleared_live_meta_rows = int(
            connection.execute("SELECT count(*) FROM live_promotion_meta").fetchone()[0]
        )
        connection.execute(
            "DELETE FROM media_index WHERE info_hash IN (SELECT info_hash FROM live_promotions)"
        )
        if live_rows:
            connection.executemany(
                "INSERT INTO media_fts(media_fts, rowid, search_text) VALUES('delete', ?, ?)",
                [
                    (
                        int(row["torrent_id"]),
                        compact_import.search_text_for(dict(row)),
                    )
                    for row in live_rows
                ],
            )
        connection.execute("DELETE FROM live_promotion_meta")
        connection.execute("DELETE FROM live_promotions")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return {
        "existingLivePromotionRows": existing_live_rows,
        "removedMediaRows": removed_media_rows,
        "clearedLiveMetaRows": cleared_live_meta_rows,
    }


def create_empty_base_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = compact_import.configure_connection(sqlite3.connect(path))
    try:
        compact_import.create_schema(connection)
        compact_import.finalize_schema(connection)
        compact_import.write_build_meta(
            connection,
            {
                "source": "empty-base-validation",
                "generated_at": utc_now_iso(),
                "schema_version": compact_import.SCHEMA_VERSION,
            },
        )
    finally:
        connection.close()


def write_seed_sidecars(base_db: Path, spec: dict[str, Any], reset_report: dict[str, int]) -> tuple[Path, Path]:
    checkpoint_path = base_db.with_suffix(base_db.suffix + ".import.checkpoint.json")
    report_path = base_db.with_suffix(base_db.suffix + ".import.report.json")
    payload = {
        "schemaVersion": compact_import.SCHEMA_VERSION,
        "phase": "completed",
        "sourceMode": spec["mode"],
        "sourcePath": str(spec["path"]) if spec.get("path") else None,
        "generatedAt": utc_now_iso(),
        "outputDb": str(base_db),
        "resetLiveOverlay": reset_report,
    }
    save_json(checkpoint_path, payload)
    save_json(report_path, payload)
    return checkpoint_path, report_path


def build_base_db(
    args: argparse.Namespace,
    spec: dict[str, Any],
    *,
    build_db: Path,
    build_dir: Path,
) -> dict[str, Any]:
    build_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = build_db.with_suffix(build_db.suffix + ".import.checkpoint.json")
    report_path = build_db.with_suffix(build_db.suffix + ".import.report.json")
    cleanup_sqlite_artifacts(build_db)
    for path in (checkpoint_path, report_path):
        if path.exists():
            path.unlink()
    start = time.perf_counter()
    reset_report: dict[str, Any]
    if spec["mode"] == "manifest":
        run_checked(
            [
                sys.executable,
                str(IMPORT_COMPACT_SEARCH_PATH),
                str(spec["path"]),
                str(build_db),
                "--checkpoint",
                str(checkpoint_path),
                "--report",
                str(report_path),
                "--batch-size",
                str(args.batch_size),
                "--checkpoint-every-batches",
                str(args.checkpoint_every_batches),
                "--progress-every-batches",
                str(args.progress_every_batches),
            ]
        )
        reset_report = prepare_existing_base_db(build_db)
    elif spec["mode"] == "seed-db":
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{spec['path']}{suffix}")
            if sidecar.exists():
                raise SystemExit(
                    f"historical seed DB has an active SQLite sidecar ({sidecar}); checkpoint it before using it as a trusted seed"
                )
        print(f"[base] copying historical seed DB from {spec['path']}", flush=True)
        shutil.copy2(spec["path"], build_db)
        try:
            reset_report = prepare_existing_base_db(build_db)
        except sqlite3.DatabaseError as exc:
            if "malformed" not in str(exc).lower():
                raise
            print(f"[base] reset failed: {exc}", flush=True)
            reset_report = salvage_seed_historical_base(
                spec["path"],
                build_db,
                batch_size=args.batch_size,
                progress_every_batches=args.progress_every_batches,
            )
            reset_report["postSalvageReset"] = prepare_existing_base_db(build_db)
    elif spec["mode"] == "empty-base":
        create_empty_base_db(build_db)
        reset_report = prepare_existing_base_db(build_db)
    else:
        raise ValueError(f"Unsupported historical input mode: {spec['mode']}")

    if spec["mode"] != "manifest":
        checkpoint_path, report_path = write_seed_sidecars(build_db, spec, reset_report)
    elapsed = round(time.perf_counter() - start, 3)
    return {
        "mode": spec["mode"],
        "sourcePath": str(spec["path"]) if spec.get("path") else None,
        "buildDb": str(build_db),
        "checkpoint": str(checkpoint_path),
        "report": str(report_path),
        "elapsedSeconds": elapsed,
        "resetLiveOverlay": reset_report,
    }


def earliest_source_cursor(container: str) -> dict[str, str] | None:
    return live_sync.earliest_source_cursor(container)


def create_seed_checkpoint(path: Path, *, earliest_created_at: str) -> None:
    save_json(
        path,
        {
            "schemaVersion": live_sync.CHECKPOINT_SCHEMA_VERSION,
            "cursorType": "torrent_created_at",
            "lastCreatedAt": live_sync.normalize_cursor(earliest_created_at),
            "lastInfoHash": ZERO_HASH,
            "createdAt": utc_now_iso(),
            "bootstrapMode": "earliest-source-row",
        },
    )


def update_live_meta(
    db_path: Path,
    *,
    state_path: Path,
    state: dict[str, Any],
    target_created_before: str | None,
    target_frontier_hash: str | None,
    frontier_stable: bool,
    replay_complete: bool,
) -> None:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("BEGIN IMMEDIATE")
        live_sync.set_meta(connection, "cursorType", "torrent_created_at")
        live_sync.set_meta(connection, "lastRunAt", utc_now_iso())
        live_sync.set_meta(connection, "wideDrainCheckpoint", str(state_path))
        live_sync.set_meta(connection, "wideDrainCheckpointSchemaVersion", state.get("schemaVersion"))
        live_sync.set_meta(connection, "wideDrainScannedRows", int(state.get("scannedRows", 0)))
        live_sync.set_meta(connection, "rebuildLiveTargetCreatedBefore", target_created_before)
        live_sync.set_meta(connection, "rebuildLiveTargetInfoHash", target_frontier_hash)
        live_sync.set_meta(connection, "rebuildLiveReplayComplete", replay_complete)
        live_sync.set_meta(connection, "rebuildLiveFrontierStable", frontier_stable)
        if replay_complete and frontier_stable:
            live_sync.set_meta(connection, "wideDrainCompleted", True)
            live_sync.set_meta(connection, "wideDrainCompletedAt", utc_now_iso())
            live_sync.set_meta(connection, "wideDrainCompletionMode", "rebuild-snapshot-cutover")
            live_sync.set_meta(connection, "wideDrainFrontierCreatedAt", target_created_before)
            live_sync.set_meta(connection, "wideDrainFrontierInfoHash", target_frontier_hash)
        else:
            live_sync.set_meta(connection, "wideDrainCompleted", False)
            live_sync.set_meta(connection, "wideDrainCompletedAt", None)
            live_sync.set_meta(connection, "wideDrainCompletionMode", "rebuild-partial" if not replay_complete else "rebuild-needs-catchup")
            live_sync.set_meta(connection, "wideDrainFrontierCreatedAt", target_created_before)
            live_sync.set_meta(connection, "wideDrainFrontierInfoHash", target_frontier_hash)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def build_live_checkpoint(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "schemaVersion": live_sync.CHECKPOINT_SCHEMA_VERSION,
        "cursorType": "torrent_created_at",
        "lastCreatedAt": live_sync.normalize_cursor(state.get("lastCreatedAt")),
        "lastInfoHash": str(state.get("lastInfoHash") or ZERO_HASH).lower(),
        "scannedRows": int(state.get("scannedRows", 0) or 0),
        "eligibleRows": int(state.get("eligibleRows", 0) or 0),
        "insertedRows": int(state.get("insertedRows", 0) or 0),
        "updatedRows": int(state.get("updatedRows", 0) or 0),
        "noopRows": int(state.get("noopRows", 0) or 0),
        "skippedRows": int(state.get("skippedRows", 0) or 0),
        "extractSeconds": float(state.get("exportSeconds", 0.0) or 0.0),
        "classifySeconds": float(state.get("classifySeconds", 0.0) or 0.0),
        "mergeSeconds": float(state.get("mergeSeconds", 0.0) or 0.0),
        "updatedAt": utc_now_iso(),
    }


def run_live_replay(
    args: argparse.Namespace,
    *,
    build_db: Path,
    live_dir: Path,
) -> dict[str, Any]:
    live_dir.mkdir(parents=True, exist_ok=True)
    workdir = live_dir / "wide-drain-offline"
    state_path = live_dir / "wide-drain-offline.state.json"
    seed_checkpoint = live_dir / "seed.checkpoint.json"

    if args.skip_live_drain:
        state = {
            "schemaVersion": 1,
            "mode": "offline-wide-drain",
            "lastCreatedAt": live_sync.ZERO_CURSOR,
            "lastInfoHash": ZERO_HASH,
            "scannedRows": 0,
            "eligibleRows": 0,
            "insertedRows": 0,
            "updatedRows": 0,
            "noopRows": 0,
            "skippedRows": 0,
            "rejectedRows": 0,
        }
        save_json(state_path, state)
        update_live_meta(
            build_db,
            state_path=state_path,
            state=state,
            target_created_before=None,
            target_frontier_hash=None,
            frontier_stable=False,
            replay_complete=False,
        )
        return {
            "skipped": True,
            "statePath": str(state_path),
            "seedCheckpoint": str(seed_checkpoint),
            "targetCreatedBefore": None,
            "targetInfoHash": None,
            "frontierStable": False,
            "replayComplete": False,
            "state": state,
        }

    earliest = earliest_source_cursor(args.docker_container)
    latest = live_sync.latest_source_cursor(args.docker_container)
    if earliest is None or latest is None:
        state = {
            "schemaVersion": 1,
            "mode": "offline-wide-drain",
            "lastCreatedAt": live_sync.ZERO_CURSOR,
            "lastInfoHash": ZERO_HASH,
            "scannedRows": 0,
            "eligibleRows": 0,
            "insertedRows": 0,
            "updatedRows": 0,
            "noopRows": 0,
            "skippedRows": 0,
            "rejectedRows": 0,
        }
        save_json(state_path, state)
        update_live_meta(
            build_db,
            state_path=state_path,
            state=state,
            target_created_before=None,
            target_frontier_hash=None,
            frontier_stable=True,
            replay_complete=True,
        )
        return {
            "skipped": False,
            "emptySource": True,
            "statePath": str(state_path),
            "seedCheckpoint": str(seed_checkpoint),
            "targetCreatedBefore": None,
            "targetInfoHash": None,
            "frontierStable": True,
            "replayComplete": True,
            "state": state,
        }

    target_created_before = (
        live_sync.normalize_cursor(args.live_created_before)
        if args.live_created_before
        else str(latest["source_created_at"])
    )
    earliest_created_at = live_sync.normalize_cursor(str(earliest["source_created_at"]))
    if target_created_before < earliest_created_at:
        raise SystemExit(
            f"--live-created-before ({target_created_before}) is earlier than the earliest source row ({earliest_created_at})."
        )

    create_seed_checkpoint(seed_checkpoint, earliest_created_at=earliest_created_at)

    command = [
        sys.executable,
        str(SCRIPT_DIR / "drain_bitmagnet_wide_offline.py"),
        "--search-db",
        str(build_db),
        "--state",
        str(state_path),
        "--workdir",
        str(workdir),
        "--seed-checkpoint",
        str(seed_checkpoint),
        "--docker-container",
        args.docker_container,
        "--slice-minutes",
        str(args.slice_minutes),
        "--classifier-workers",
        str(args.classifier_workers),
        "--fts-batch-size",
        str(args.fts_batch_size),
        "--progress-every-slices",
        "1",
        "--ensure-source-indexes",
        "--created-before",
        target_created_before,
    ]
    if args.live_max_slices:
        command.extend(["--max-slices", str(args.live_max_slices)])
    run_checked(command)

    state = load_json(state_path)
    last_created_at = live_sync.normalize_cursor(state.get("lastCreatedAt"))
    replay_complete = args.live_max_slices == 0 and last_created_at >= target_created_before
    latest_after = live_sync.latest_source_cursor(args.docker_container)
    frontier_stable = latest_after == latest
    update_live_meta(
        build_db,
        state_path=state_path,
        state=state,
        target_created_before=target_created_before,
        target_frontier_hash=str(latest.get("info_hash") or "").lower() or None,
        frontier_stable=frontier_stable,
        replay_complete=replay_complete,
    )
    return {
        "skipped": False,
        "emptySource": False,
        "statePath": str(state_path),
        "seedCheckpoint": str(seed_checkpoint),
        "targetCreatedBefore": target_created_before,
        "targetInfoHash": str(latest.get("info_hash") or "").lower() or None,
        "frontierStable": frontier_stable,
        "replayComplete": replay_complete,
        "state": state,
        "latestBefore": latest,
        "latestAfter": latest_after,
        "earliest": earliest,
    }


def vacuum_into(source_db: Path, final_db: Path) -> None:
    if final_db.exists():
        final_db.unlink()
    connection = sqlite3.connect(source_db)
    try:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.execute("ANALYZE")
        connection.commit()
        connection.execute(f"VACUUM INTO {sqlite_quote(str(final_db))}")
        connection.commit()
    finally:
        connection.close()


def validate_db(path: Path, *, integrity_mode: str) -> dict[str, Any]:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        pragma = "quick_check" if integrity_mode == "quick" else "integrity_check"
        rows = connection.execute(f"PRAGMA {pragma}").fetchall()
        integrity = "\n".join(str(row[0]) for row in rows)
        if integrity.strip().lower() != "ok":
            raise RuntimeError(f"SQLite {pragma} failed for {path}: {integrity}")
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        required_tables = {
            "media_index",
            "media_fts",
            "live_promotions",
            "live_promotion_meta",
            "canonical_title_family",
            "canonical_title_variant",
        }
        missing_tables = sorted(required_tables - tables)
        if missing_tables:
            raise RuntimeError(f"Missing required tables in {path}: {', '.join(missing_tables)}")
        counts = {}
        for table in (
            "media_index",
            "live_promotions",
            "canonical_title_family",
            "canonical_title_variant",
        ):
            counts[table] = int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
        meta = {
            key: value
            for key, value in connection.execute(
                "SELECT key, value FROM live_promotion_meta ORDER BY key"
            ).fetchall()
        }
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
            for column_row in connection.execute(
                f"PRAGMA index_info({sqlite_quote(index_name)})"
            ).fetchall():
                column_name = str(column_row["name"] or "")
                if column_name:
                    indexed_columns.add(column_name)

        missing_lookup_indexes = sorted(
            name for name in REQUIRED_LOOKUP_INDEX_COLUMNS if name not in indexed_columns
        )
        if not total_size_integer_affinity or missing_lookup_indexes:
            raise RuntimeError(
                "Schema contract validation failed for "
                f"{path}: total_size_type={total_size_type or 'missing'}, "
                f"missing_lookup_indexes={','.join(missing_lookup_indexes) or '-'}"
            )

        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        return {
            "path": str(path),
            "sizeBytes": path.stat().st_size,
            "sha256": sha256_file(path),
            "integrityPragma": pragma,
            "integrity": integrity,
            "counts": counts,
            "pageSize": page_size,
            "pageCount": page_count,
            "livePromotionMeta": meta,
            "schemaContract": {
                "totalSizeType": total_size_type,
                "totalSizeIntegerAffinity": total_size_integer_affinity,
                "missingLookupIndexes": missing_lookup_indexes,
            },
        }
    finally:
        connection.close()


def prepare_final_artifacts(
    run_dir: Path,
    *,
    build_db: Path,
    base_info: dict[str, Any],
    live_info: dict[str, Any],
    integrity_mode: str,
) -> dict[str, Any]:
    final_dir = run_dir / "final"
    final_dir.mkdir(parents=True, exist_ok=True)
    final_db = final_dir / "active.search.sqlite3"
    vacuum_into(build_db, final_db)
    validation = validate_db(final_db, integrity_mode=integrity_mode)

    build_report_src = Path(base_info["report"])
    build_checkpoint_src = Path(base_info["checkpoint"])
    import_report_target = final_dir / "active.import.report.json"
    import_checkpoint_target = final_dir / "active.import.checkpoint.json"

    report_payload = load_json(build_report_src) if build_report_src.exists() else {}
    report_payload["outputDb"] = str(final_db)
    report_payload["rebuiltAt"] = utc_now_iso()
    save_json(import_report_target, report_payload)

    checkpoint_payload = load_json(build_checkpoint_src) if build_checkpoint_src.exists() else {}
    checkpoint_payload["outputDb"] = str(final_db)
    checkpoint_payload["rebuiltAt"] = utc_now_iso()
    save_json(import_checkpoint_target, checkpoint_payload)

    live_state_target = final_dir / "wide-drain-offline.state.json"
    if Path(live_info["statePath"]).exists():
        shutil.copy2(live_info["statePath"], live_state_target)
    else:
        save_json(live_state_target, live_info["state"])
    live_checkpoint_target = final_dir / "live-promotion.checkpoint.json"
    save_json(live_checkpoint_target, build_live_checkpoint(live_info["state"]))

    return {
        "finalDir": str(final_dir),
        "finalDb": str(final_db),
        "importReport": str(import_report_target),
        "importCheckpoint": str(import_checkpoint_target),
        "liveState": str(live_state_target),
        "liveCheckpoint": str(live_checkpoint_target),
        "validation": validation,
    }


def related_artifact_paths(search_db: Path) -> list[Path]:
    return [
        search_db,
        Path(f"{search_db}-wal"),
        Path(f"{search_db}-shm"),
        search_db.parent / "active.import.report.json",
        search_db.parent / "active.import.checkpoint.json",
        search_db.parent / "live-promotion.checkpoint.json",
        search_db.parent / "wide-drain-offline.state.json",
        search_db.parent / "active.rebuild.report.json",
    ]


def write_rollback_script(backup_dir: Path, *, search_db: Path) -> Path:
    rollback_path = backup_dir / "rollback.sh"
    lines = ["#!/usr/bin/env bash", "set -euo pipefail"]
    main_backup = backup_dir / search_db.name
    if main_backup.exists():
        lines.append(f"cp {shlex_quote(str(main_backup))} {shlex_quote(str(search_db))}")
    else:
        lines.append(f"rm -f {shlex_quote(str(search_db))}")
    for suffix in ("-wal", "-shm"):
        backup_file = backup_dir / f"{search_db.name}{suffix}"
        target_file = Path(f"{search_db}{suffix}")
        if backup_file.exists():
            lines.append(f"cp {shlex_quote(str(backup_file))} {shlex_quote(str(target_file))}")
        else:
            lines.append(f"rm -f {shlex_quote(str(target_file))}")
    for name in (
        "active.import.report.json",
        "active.import.checkpoint.json",
        "live-promotion.checkpoint.json",
        "wide-drain-offline.state.json",
        "active.rebuild.report.json",
    ):
        backup_file = backup_dir / name
        target_file = search_db.parent / name
        if backup_file.exists():
            lines.append(f"cp {shlex_quote(str(backup_file))} {shlex_quote(str(target_file))}")
        else:
            lines.append(f"rm -f {shlex_quote(str(target_file))}")
    rollback_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    rollback_path.chmod(0o755)
    return rollback_path


def shlex_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def atomic_copy_replace(source: Path, target: Path, *, run_id: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.parent / f".{target.name}.{run_id}.next"
    if staging.exists():
        staging.unlink()
    shutil.copy2(source, staging)
    os.replace(staging, target)


def apply_cutover(
    args: argparse.Namespace,
    *,
    search_db: Path,
    final_info: dict[str, Any],
    run_id: str,
) -> dict[str, Any]:
    if args.allow_empty_base:
        raise SystemExit("Cutover refuses --allow-empty-base; provide a real historical manifest or seed DB.")
    if args.live_max_slices:
        raise SystemExit("Cutover refuses partial live replay (--live-max-slices must be 0).")
    if args.skip_live_drain:
        raise SystemExit("Cutover refuses --skip-live-drain; the live raw replay must run before deployment.")
    backup_dir = search_db.parent / "backups" / run_id
    backup_dir.mkdir(parents=True, exist_ok=True)
    backed_up: list[str] = []
    for path in related_artifact_paths(search_db):
        if not path.exists():
            continue
        destination = backup_dir / path.name
        shutil.copy2(path, destination)
        backed_up.append(str(destination))
    rollback_script = write_rollback_script(backup_dir, search_db=search_db)

    final_db = Path(final_info["finalDb"])
    atomic_copy_replace(final_db, search_db, run_id=run_id)
    for stale_path in (Path(f"{search_db}-wal"), Path(f"{search_db}-shm")):
        if stale_path.exists():
            stale_path.unlink()
    for key, target_name in (
        ("importReport", "active.import.report.json"),
        ("importCheckpoint", "active.import.checkpoint.json"),
        ("liveCheckpoint", "live-promotion.checkpoint.json"),
        ("liveState", "wide-drain-offline.state.json"),
    ):
        atomic_copy_replace(Path(final_info[key]), search_db.parent / target_name, run_id=run_id)

    rebuild_report_target = search_db.parent / "active.rebuild.report.json"
    atomic_copy_replace(Path(final_info["rebuildReport"]), rebuild_report_target, run_id=run_id)

    builder_mirror: list[str] = []
    builder_deploy_dir = str(args.builder_deploy_dir).strip()
    if not args.skip_builder_mirror and builder_deploy_dir:
        builder_dir = Path(builder_deploy_dir)
        builder_dir.mkdir(parents=True, exist_ok=True)
        atomic_copy_replace(final_db, builder_dir / "active.search.sqlite3", run_id=run_id)
        atomic_copy_replace(
            Path(final_info["importReport"]),
            builder_dir / "active.import.report.json",
            run_id=run_id,
        )
        atomic_copy_replace(
            Path(final_info["importCheckpoint"]),
            builder_dir / "active.import.checkpoint.json",
            run_id=run_id,
        )
        builder_mirror = [
            str(builder_dir / "active.search.sqlite3"),
            str(builder_dir / "active.import.report.json"),
            str(builder_dir / "active.import.checkpoint.json"),
        ]

    return {
        "applied": True,
        "backupDir": str(backup_dir),
        "backedUpFiles": backed_up,
        "rollbackScript": str(rollback_script),
        "builderMirrorFiles": builder_mirror,
    }


def create_run_id(label: str) -> str:
    timestamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    suffix = sanitize_label(label) if label else "rebuild"
    return f"{timestamp}-{suffix}"


def main() -> None:
    args = parse_args()
    search_db = Path(args.search_db).expanduser().resolve()
    stage_root = Path(args.stage_root).expanduser().resolve()
    run_id = create_run_id(args.run_label)
    run_dir = stage_root / run_id
    build_dir = run_dir / "build"
    live_dir = run_dir / "live"
    final_dir = run_dir / "final"
    build_db = build_dir / "active.search.sqlite3"

    historical_spec = resolve_historical_input(args, search_db)
    base_info = build_base_db(args, historical_spec, build_db=build_db, build_dir=build_dir)
    live_info = run_live_replay(args, build_db=build_db, live_dir=live_dir)
    final_info = prepare_final_artifacts(
        run_dir,
        build_db=build_db,
        base_info=base_info,
        live_info=live_info,
        integrity_mode=args.integrity_check,
    )

    historical_report = {
        "mode": historical_spec["mode"],
        "path": str(historical_spec["path"]) if historical_spec.get("path") else None,
        "searchedCandidates": list(historical_spec.get("searchedCandidates", [])),
    }

    rebuild_report = {
        "generatedAt": utc_now_iso(),
        "runId": run_id,
        "runDir": str(run_dir),
        "searchDb": str(search_db),
        "historicalInput": historical_report,
        "baseBuild": base_info,
        "liveReplay": {
            key: value
            for key, value in live_info.items()
            if key not in {"state"}
        },
        "finalArtifact": {
            key: value
            for key, value in final_info.items()
            if key != "rebuildReport"
        },
        "diskFreeBytes": shutil.disk_usage(search_db.parent).free,
        "cutover": {"applied": False},
    }
    rebuild_report_path = final_dir / "active.rebuild.report.json"
    save_json(rebuild_report_path, rebuild_report)
    final_info["rebuildReport"] = str(rebuild_report_path)

    if args.cutover:
        cutover_info = apply_cutover(args, search_db=search_db, final_info=final_info, run_id=run_id)
        rebuild_report["cutover"] = cutover_info
        save_json(rebuild_report_path, rebuild_report)
        atomic_copy_replace(rebuild_report_path, search_db.parent / "active.rebuild.report.json", run_id=run_id)

    print(json.dumps(rebuild_report, ensure_ascii=False))


if __name__ == "__main__":
    main()
