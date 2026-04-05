#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import promote_bitmagnet_live_to_compact as live_sync


DEFAULT_SEARCH_DB = live_sync.DEFAULT_SEARCH_DB
DEFAULT_CONTAINER = live_sync.DEFAULT_CONTAINER
DEFAULT_SEED_CHECKPOINT = live_sync.DEFAULT_DRAIN_CHECKPOINT
DEFAULT_STATE_PATH = "/home/ubuntu/aiostreams/data/comet-fresh/magnetico/wide-drain-offline.state.json"
DEFAULT_WORKDIR = "/home/ubuntu/aiostreams/data/comet-fresh/magnetico/wide-drain-offline"
STATE_SCHEMA_VERSION = 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="High-throughput offline BitMagnet wide drain into the lean SQLite DB."
    )
    parser.add_argument("--search-db", default=DEFAULT_SEARCH_DB)
    parser.add_argument("--state", default=DEFAULT_STATE_PATH)
    parser.add_argument("--workdir", default=DEFAULT_WORKDIR)
    parser.add_argument("--seed-checkpoint", default=DEFAULT_SEED_CHECKPOINT)
    parser.add_argument("--docker-container", default=DEFAULT_CONTAINER)
    parser.add_argument("--slice-minutes", type=int, default=30)
    parser.add_argument("--classifier-workers", type=int, default=live_sync.default_classifier_workers())
    parser.add_argument("--fts-batch-size", type=int, default=2000)
    parser.add_argument("--max-slices", type=int, default=0)
    parser.add_argument("--progress-every-slices", type=int, default=1)
    parser.add_argument("--created-before")
    parser.add_argument("--keep-spool-files", action="store_true")
    parser.add_argument("--ensure-source-indexes", action="store_true")
    parser.add_argument("--audit-only", action="store_true")
    parser.add_argument("--audit-sample-limit", type=int, default=5)
    return parser.parse_args()


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def load_seed_cursor(path: Path) -> tuple[str, str]:
    if not path.exists():
        return live_sync.ZERO_CURSOR, live_sync.ZERO_HASH
    payload = json.loads(path.read_text(encoding="utf-8"))
    return (
        live_sync.normalize_cursor(payload.get("lastCreatedAt") or payload.get("lastUpdatedAt")),
        str(payload.get("lastInfoHash") or live_sync.ZERO_HASH).lower(),
    )


def load_state(path: Path, seed_checkpoint: Path) -> dict[str, Any]:
    if not path.exists():
        last_created_at, last_info_hash = load_seed_cursor(seed_checkpoint)
        return {
            "schemaVersion": STATE_SCHEMA_VERSION,
            "mode": "offline-wide-drain",
            "lastCreatedAt": last_created_at,
            "lastInfoHash": last_info_hash,
            "exportedRows": 0,
            "scannedRows": 0,
            "eligibleRows": 0,
            "insertedRows": 0,
            "updatedRows": 0,
            "noopRows": 0,
            "skippedRows": 0,
            "rejectedRows": 0,
            "slicesCompleted": 0,
            "exportSeconds": 0.0,
            "classifySeconds": 0.0,
            "enrichSeconds": 0.0,
            "mergeSeconds": 0.0,
            "seedCheckpoint": str(seed_checkpoint),
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["lastCreatedAt"] = live_sync.normalize_cursor(payload.get("lastCreatedAt"))
    payload["lastInfoHash"] = str(payload.get("lastInfoHash") or live_sync.ZERO_HASH).lower()
    for key in ("exportSeconds", "classifySeconds", "enrichSeconds", "mergeSeconds"):
        payload[key] = float(payload.get(key, 0.0) or 0.0)
    return payload


def run_copy_to_file(container: str, sql: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
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
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        subprocess.run(command, check=True, stdout=handle, text=True)


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def latest_source_cursor(container: str) -> tuple[str, str] | None:
    latest = live_sync.latest_source_cursor(container)
    if latest is None:
        return None
    return str(latest["source_created_at"]), str(latest["info_hash"]).lower()


def compute_slice_end(last_created_at: str, *, slice_minutes: int, latest_created_at: str, created_before: str | None) -> str:
    start_dt = datetime.fromisoformat(last_created_at.replace("Z", "+00:00"))
    end_dt = start_dt + timedelta(minutes=slice_minutes)
    latest_dt = datetime.fromisoformat(latest_created_at.replace("Z", "+00:00"))
    if end_dt > latest_dt:
        end_dt = latest_dt
    if created_before:
        before_dt = datetime.fromisoformat(live_sync.normalize_cursor(created_before).replace("Z", "+00:00"))
        if end_dt > before_dt:
            end_dt = before_dt
    return end_dt.astimezone(timezone.utc).isoformat()


def slice_predicate(last_created_at: str, last_info_hash: str, slice_end: str) -> str:
    return f"""
        (
          t.created_at > TIMESTAMPTZ '{live_sync.sql_literal(last_created_at)}'
          OR (
            t.created_at = TIMESTAMPTZ '{live_sync.sql_literal(last_created_at)}'
            AND t.info_hash > decode('{live_sync.sql_literal(last_info_hash)}', 'hex')
          )
        )
        AND t.created_at <= TIMESTAMPTZ '{live_sync.sql_literal(slice_end)}'
    """


def export_slice(
    container: str,
    *,
    last_created_at: str,
    last_info_hash: str,
    slice_end: str,
    slice_dir: Path,
) -> dict[str, Any]:
    headers_path = slice_dir / "headers.csv"
    files_path = slice_dir / "files.csv"
    predicate = slice_predicate(last_created_at, last_info_hash, slice_end)
    headers_sql = f"""
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
        WHERE {predicate}
        ORDER BY t.created_at, t.info_hash
    """
    files_sql = f"""
        WITH slice AS (
          SELECT
            t.info_hash,
            t.created_at
          FROM torrents t
          WHERE {predicate}
        )
        SELECT
          encode(tf.info_hash, 'hex') AS info_hash,
          tf.index AS file_index,
          tf.path AS path,
          tf.size AS size
        FROM torrent_files tf
        JOIN slice s ON s.info_hash = tf.info_hash
        ORDER BY tf.info_hash, tf.index
    """
    export_started = time.perf_counter()
    run_copy_to_file(container, headers_sql, headers_path)
    header_rows = count_csv_rows(headers_path)
    if header_rows:
        run_copy_to_file(container, files_sql, files_path)
    else:
        files_path.write_text("info_hash,file_index,path,size\n", encoding="utf-8")
    file_rows = count_csv_rows(files_path)
    return {
        "headersPath": str(headers_path),
        "filesPath": str(files_path),
        "headerRows": header_rows,
        "fileRows": file_rows,
        "exportSeconds": time.perf_counter() - export_started,
    }


def load_slice_rows(headers_path: Path, files_path: Path) -> list[dict[str, Any]]:
    headers: list[dict[str, Any]] = []
    with headers_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            headers.append(
                {
                    "info_hash": str(row["info_hash"]).lower(),
                    "torrent_name": row["torrent_name"],
                    "total_size": int(row.get("total_size") or 0),
                    "source_created_at": row["source_created_at"],
                    "discovered_at": row["discovered_at"],
                    "files": [],
                }
            )
    by_hash = {row["info_hash"]: row for row in headers}
    with files_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            info_hash = str(row["info_hash"]).lower()
            target = by_hash.get(info_hash)
            if target is None:
                continue
            target["files"].append(
                {
                    "path": str(row.get("path") or ""),
                    "size": int(row.get("size") or 0),
                }
            )
    return headers


def classify_rows(rows: list[dict[str, Any]], workers: int) -> tuple[list[dict[str, Any]], float]:
    started = time.perf_counter()
    if workers <= 1:
        results = [live_sync.classify_work_item(row) for row in rows]
    else:
        chunksize = max(1, len(rows) // max(1, workers * 4))
        with ProcessPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(live_sync.classify_work_item, rows, chunksize=chunksize))
    return results, time.perf_counter() - started


def record_sample(bucket: dict[str, list[dict[str, Any]]], reason: str, sample_limit: int, row: dict[str, Any]) -> None:
    items = bucket.setdefault(reason, [])
    if len(items) >= sample_limit:
        return
    items.append(
        {
            "infoHash": row["info_hash"],
            "name": row["torrent_name"],
            "contentClass": row["content_class"],
            "sourceCreatedAt": row["source_created_at"],
            "year": row.get("year"),
            "rejectReason": row.get("reject_reason"),
            "reasonCodes": str(row.get("reason_codes") or ""),
            "confidenceTier": str(row.get("confidence_tier") or ""),
            "matchSource": row.get("match_source"),
            "contractVersion": str(row.get("contract_version") or ""),
        }
    )


def merge_slice(
    connection: sqlite3.Connection,
    container: str,
    classified_rows: list[dict[str, Any]],
    *,
    audit_only: bool,
    sample_limit: int,
    fts_batch_size: int,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "scannedRows": len(classified_rows),
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
        "samples": {
            "skipReasons": {},
            "legacyBridgeDropReasons": {},
            "rejectReasons": {},
        },
        "enrichSeconds": 0.0,
        "mergeSeconds": 0.0,
    }
    accepted_hashes = []
    for row in classified_rows:
        content_class = str(row["content_class"] or "")
        report["classCounts"][content_class] += 1
        if content_class == "reject":
            report["rejectedRows"] += 1
            reason = str(row.get("reject_reason") or "other")
            report["rejectReasons"][reason] += 1
            record_sample(report["samples"]["rejectReasons"], reason, sample_limit, row)
            continue
        report["eligibleRows"] += 1
        legacy_reason = row.get("legacy_drop_reason")
        if legacy_reason:
            report["legacyBridgeDropReasons"][str(legacy_reason)] += 1
            record_sample(report["samples"]["legacyBridgeDropReasons"], str(legacy_reason), sample_limit, row)
        if row.get("payload") is not None:
            accepted_hashes.append(row["info_hash"])
        else:
            report["skippedRows"] += 1
            reason = str(row.get("skip_reason") or "bridge_invariant")
            report["skipReasons"][reason] += 1
            record_sample(report["samples"]["skipReasons"], reason, sample_limit, row)

    enrich_started = time.perf_counter()
    ids_by_hash = live_sync.fetch_id_rows(container, accepted_hashes)
    report["enrichSeconds"] = time.perf_counter() - enrich_started

    existing_media = live_sync.fetch_existing_media_rows(connection, accepted_hashes)
    existing_promotions = live_sync.fetch_existing_promotions(connection, accepted_hashes)
    current_id = live_sync.next_torrent_id(connection)
    promoted_at = int(time.time())
    staged_media: list[dict[str, Any]] = []
    staged_promotions: list[dict[str, Any]] = []
    fts_deletes: list[tuple[int, str]] = []
    fts_inserts: list[tuple[int, str]] = []

    for row in classified_rows:
        payload = row.get("payload")
        if payload is None:
            continue
        payload = dict(payload)
        info_hash = str(payload["info_hash"]).lower()
        enrichment = ids_by_hash.get(info_hash) or {}
        payload = live_sync.apply_canonical_enrichment(payload, enrichment)
        payload["search_text"] = live_sync.lean_contract.search_text_for(payload)
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

        media_same = existing_media_row is not None and live_sync.media_row_matches(existing_media_row, payload)
        promotion_same = live_sync.promotion_row_matches(
            existing_promotion_row,
            torrent_id=torrent_id,
            source_created_at=str(row["source_created_at"]),
            content_class=str(payload["content_class"]),
            reject_reason=payload.get("reject_reason"),
            reason_codes=str(payload.get("reason_codes") or ""),
            confidence_tier=str(payload.get("confidence_tier") or ""),
            match_source=payload.get("match_source"),
            contract_version=str(
                payload.get("contract_version") or live_sync.AUTHORITATIVE_CONTRACT_VERSION
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
                    (torrent_id, live_sync.lean_contract.search_text_for(dict(existing_media_row)))
                )
            fts_inserts.append((torrent_id, payload["search_text"]))
        if not promotion_same:
            staged_promotions.append(
                {
                    "info_hash": payload["info_hash"],
                    "torrent_id": torrent_id,
                    "source_updated_at": str(row["source_created_at"]),
                    "promoted_at": promoted_at,
                    "content_class": payload["content_class"],
                    "reject_reason": payload.get("reject_reason"),
                    "reason_codes": str(payload.get("reason_codes") or ""),
                    "confidence_tier": str(payload.get("confidence_tier") or ""),
                    "match_source": payload.get("match_source"),
                    "contract_version": str(
                        payload.get("contract_version")
                        or live_sync.AUTHORITATIVE_CONTRACT_VERSION
                    ),
                }
            )

    if audit_only:
        for key in ("classCounts", "rejectReasons", "skipReasons", "legacyBridgeDropReasons", "enrichmentCoverage"):
            report[key] = dict(report[key])
        return report

    merge_started = time.perf_counter()
    connection.execute("BEGIN IMMEDIATE")
    try:
        live_sync.stage_media_rows(connection, staged_media)
        live_sync.stage_promotion_rows(connection, staged_promotions)
        if staged_media:
            live_sync.merge_staged_media(connection)
            live_sync.refresh_fts(
                connection,
                deleted_rows=fts_deletes,
                inserted_rows=fts_inserts,
                batch_size=fts_batch_size,
            )
        if staged_promotions:
            live_sync.merge_staged_promotions(connection)
        live_sync.set_meta(connection, "lastRunAt", datetime.now(timezone.utc).isoformat())
        live_sync.set_meta(connection, "cursorType", "torrent_created_at")
        live_sync.set_meta(
            connection,
            "authoritativeContractVersion",
            live_sync.AUTHORITATIVE_CONTRACT_VERSION,
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    report["mergeSeconds"] = time.perf_counter() - merge_started

    for key in ("classCounts", "rejectReasons", "skipReasons", "legacyBridgeDropReasons", "enrichmentCoverage"):
        report[key] = dict(report[key])
    return report


def json_progress(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)


def main() -> None:
    args = parse_args()
    search_db = Path(args.search_db)
    state_path = Path(args.state)
    seed_checkpoint = Path(args.seed_checkpoint)
    workdir = Path(args.workdir)
    slices_dir = workdir / "slices"
    reports_dir = workdir / "reports"
    if not search_db.exists():
        raise SystemExit(f"search db not found: {search_db}")
    workdir.mkdir(parents=True, exist_ok=True)
    slices_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    if args.ensure_source_indexes:
        live_sync.ensure_source_indexes(args.docker_container)

    state = load_state(state_path, seed_checkpoint)
    connection = sqlite3.connect(search_db)
    connection.row_factory = sqlite3.Row
    live_sync.ensure_schema(connection)

    latest_cursor = latest_source_cursor(args.docker_container)
    if latest_cursor is None:
        json_progress({"event": "wide-drain-offline-empty", "searchDb": str(search_db)})
        return
    latest_created_at, _ = latest_cursor

    slices_run = 0
    try:
        while True:
            if args.max_slices and slices_run >= args.max_slices:
                break
            slice_end = compute_slice_end(
                state["lastCreatedAt"],
                slice_minutes=args.slice_minutes,
                latest_created_at=latest_created_at,
                created_before=args.created_before,
            )
            if datetime.fromisoformat(slice_end.replace("Z", "+00:00")) <= datetime.fromisoformat(state["lastCreatedAt"].replace("Z", "+00:00")):
                break
            slice_key = datetime.fromisoformat(slice_end.replace("Z", "+00:00")).strftime("%Y%m%dT%H%M%S")
            slice_dir = slices_dir / slice_key
            slice_dir.mkdir(parents=True, exist_ok=True)
            slice_manifest = {
                "sliceStartCreatedAt": state["lastCreatedAt"],
                "sliceStartInfoHash": state["lastInfoHash"],
                "sliceEndCreatedAt": slice_end,
                "auditOnly": bool(args.audit_only),
            }
            save_json(slice_dir / "manifest.json", slice_manifest)

            export_info = export_slice(
                args.docker_container,
                last_created_at=state["lastCreatedAt"],
                last_info_hash=state["lastInfoHash"],
                slice_end=slice_end,
                slice_dir=slice_dir,
            )
            if export_info["headerRows"] == 0:
                state["lastCreatedAt"] = slice_end
                state["lastInfoHash"] = live_sync.ZERO_HASH
                save_json(state_path, state)
                if slice_dir.exists() and not args.keep_spool_files:
                    shutil.rmtree(slice_dir)
                if state["lastCreatedAt"] >= latest_created_at:
                    break
                continue

            rows = load_slice_rows(Path(export_info["headersPath"]), Path(export_info["filesPath"]))
            classified_rows, classify_seconds = classify_rows(rows, args.classifier_workers)
            merge_report = merge_slice(
                connection,
                args.docker_container,
                classified_rows,
                audit_only=args.audit_only,
                sample_limit=args.audit_sample_limit,
                fts_batch_size=args.fts_batch_size,
            )

            last_row = rows[-1]
            slice_report = {
                "sliceKey": slice_key,
                "sliceStartCreatedAt": state["lastCreatedAt"],
                "sliceStartInfoHash": state["lastInfoHash"],
                "sliceEndCreatedAt": slice_end,
                "headerRows": export_info["headerRows"],
                "fileRows": export_info["fileRows"],
                "exportSeconds": export_info["exportSeconds"],
                "classifySeconds": classify_seconds,
                **merge_report,
                "lastCreatedAt": last_row["source_created_at"],
                "lastInfoHash": last_row["info_hash"],
            }
            save_json(reports_dir / f"{slice_key}.json", slice_report)

            state["lastCreatedAt"] = last_row["source_created_at"]
            state["lastInfoHash"] = last_row["info_hash"]
            state["exportedRows"] = int(state.get("exportedRows", 0)) + int(export_info["headerRows"])
            for key in ("scannedRows", "eligibleRows", "insertedRows", "updatedRows", "noopRows", "skippedRows", "rejectedRows"):
                state[key] = int(state.get(key, 0)) + int(slice_report.get(key, 0))
            state["slicesCompleted"] = int(state.get("slicesCompleted", 0)) + 1
            state["exportSeconds"] = float(state.get("exportSeconds", 0.0)) + float(export_info["exportSeconds"])
            state["classifySeconds"] = float(state.get("classifySeconds", 0.0)) + float(classify_seconds)
            state["enrichSeconds"] = float(state.get("enrichSeconds", 0.0)) + float(slice_report.get("enrichSeconds", 0.0))
            state["mergeSeconds"] = float(state.get("mergeSeconds", 0.0)) + float(slice_report.get("mergeSeconds", 0.0))
            state["updatedAt"] = datetime.now(timezone.utc).isoformat()
            save_json(state_path, state)

            if not args.keep_spool_files:
                for path in (slice_dir / "headers.csv", slice_dir / "files.csv"):
                    if path.exists():
                        path.unlink()

            slices_run += 1
            if args.progress_every_slices and slices_run % args.progress_every_slices == 0:
                elapsed = float(state.get("exportSeconds", 0.0)) + float(state.get("classifySeconds", 0.0)) + float(state.get("enrichSeconds", 0.0)) + float(state.get("mergeSeconds", 0.0))
                rows_per_second = int(state.get("scannedRows", 0)) / elapsed if elapsed > 0 else 0.0
                estimated_total = live_sync.estimated_source_rows(args.docker_container)
                remaining = None
                eta_seconds = None
                if estimated_total and rows_per_second > 0 and estimated_total > int(state["scannedRows"]):
                    remaining = estimated_total - int(state["scannedRows"])
                    eta_seconds = remaining / rows_per_second
                json_progress(
                    {
                        "event": "bitmagnet-wide-offline-progress",
                        "slicesCompleted": state["slicesCompleted"],
                        "sliceKey": slice_key,
                        "scannedRows": state["scannedRows"],
                        "eligibleRows": state["eligibleRows"],
                        "insertedRows": state["insertedRows"],
                        "updatedRows": state["updatedRows"],
                        "noopRows": state["noopRows"],
                        "rowsPerSecond": round(rows_per_second, 2),
                        "estimatedSourceRows": estimated_total,
                        "estimatedRemainingRows": remaining,
                        "etaSeconds": round(eta_seconds, 1) if eta_seconds is not None else None,
                        "exportSeconds": round(slice_report["exportSeconds"], 3),
                        "classifySeconds": round(slice_report["classifySeconds"], 3),
                        "enrichSeconds": round(slice_report["enrichSeconds"], 3),
                        "mergeSeconds": round(slice_report["mergeSeconds"], 3),
                        "lastCreatedAt": state["lastCreatedAt"],
                    }
                )

            if state["lastCreatedAt"] >= latest_created_at:
                break
    finally:
        connection.close()


if __name__ == "__main__":
    main()
