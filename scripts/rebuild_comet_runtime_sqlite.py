#!/usr/bin/env python3

from __future__ import annotations

import argparse
import gzip
import json
import os
import socket
import shutil
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from lib.env_files import dump_env, load_env, resolve_project_path

ROOT_DIR = Path(__file__).resolve().parent.parent
COMET_DIR = ROOT_DIR / "cometouglas"
DEFAULT_SOURCE_ENV = ROOT_DIR / "config" / "comet-fresh.env"
DEFAULT_TARGET_DB = ROOT_DIR / "data" / "comet-fresh" / "runtime" / "comet-runtime.sqlite3"
DEFAULT_WORK_ROOT = ROOT_DIR / "data" / "comet-fresh" / "runtime-rebuilds"
EXCLUDED_EXPORT_TABLES = {
    "active_connections",
    "db_maintenance",
    "magnetico_search_builds",
    "magnetico_search_index",
    "metrics_cache",
    "schema_migrations",
}


def utc_now() -> str:
    return (
        datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a SQLite Comet runtime DB from the live PostgreSQL runtime using "
            "Comet's built-in import/export CLI."
        )
    )
    parser.add_argument("--source-env-file", default=str(DEFAULT_SOURCE_ENV))
    parser.add_argument("--target-db-path", default=str(DEFAULT_TARGET_DB))
    parser.add_argument("--work-root", default=str(DEFAULT_WORK_ROOT))
    parser.add_argument("--run-label", default="")
    parser.add_argument(
        "--tables",
        default="",
        help="Optional comma-separated subset of tables to export/import.",
    )
    parser.add_argument(
        "--keep-export-dir",
        action="store_true",
        help="Keep the intermediate export directory after a successful build.",
    )
    return parser.parse_args()


def parse_table_list(raw: str) -> list[str]:
    value = str(raw or "").strip()
    if not value:
        return []
    return [table.strip() for table in value.split(",") if table.strip()]


def run_db_cli(env_file: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["COMET_ENV_FILE"] = str(env_file)
    command = [str(COMET_DIR / ".venv" / "bin" / "python"), "-m", "comet.db_cli", *args]
    try:
        return subprocess.run(
            command,
            cwd=COMET_DIR,
            env=env,
            check=True,
            text=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as exc:
        message = (
            f"db_cli command failed: {' '.join(command)}\n"
            f"stdout:\n{exc.stdout}\n"
            f"stderr:\n{exc.stderr}"
        )
        raise SystemExit(message) from exc


def copy_env_with_overrides(source_env: Path, destination_env: Path, overrides: dict[str, str]) -> None:
    shutil.copy2(source_env, destination_env)
    dump_env(destination_env, overrides)


def parse_database_host(raw_url: str) -> str:
    normalized = raw_url if "://" in raw_url else f"postgresql://{raw_url}"
    parsed = urlsplit(normalized)
    return parsed.hostname or ""


def replace_database_host(raw_url: str, new_host: str) -> str:
    had_scheme = "://" in raw_url
    normalized = raw_url if had_scheme else f"postgresql://{raw_url}"
    parsed = urlsplit(normalized)
    if parsed.hostname is None:
        return raw_url

    credentials = ""
    if parsed.username:
        credentials = parsed.username
        if parsed.password:
            credentials += f":{parsed.password}"
        credentials += "@"

    host_port = new_host
    if parsed.port is not None:
        host_port += f":{parsed.port}"

    rebuilt = parsed._replace(netloc=f"{credentials}{host_port}")
    updated = urlunsplit(rebuilt)
    if had_scheme:
        return updated
    return updated.removeprefix("postgresql://")


def resolve_docker_container_ip(container_name: str) -> str:
    result = subprocess.run(
        [
            "sudo",
            "docker",
            "inspect",
            "-f",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            container_name,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def ensure_host_reachable_source_env(source_env_copy: Path) -> None:
    env_map = load_env(source_env_copy)
    database_type = str(env_map.get("DATABASE_TYPE", "") or "").strip().lower()
    if database_type not in {"postgresql", "postgres", "pgsql", "psql"}:
        return

    raw_url = str(env_map.get("DATABASE_URL", "") or "").strip()
    if not raw_url:
        return

    host = parse_database_host(raw_url)
    if not host:
        return

    try:
        socket.gethostbyname(host)
        return
    except OSError:
        pass

    try:
        container_ip = resolve_docker_container_ip(host)
    except subprocess.CalledProcessError:
        return

    if not container_ip:
        return

    dump_env(
        source_env_copy,
        {
            "DATABASE_URL": replace_database_host(raw_url, container_ip),
            "DATABASE_FORCE_IPV4_RESOLUTION": "True",
        },
    )


def count_export_rows(export_file: Path) -> int:
    if export_file.suffix == ".gz":
        opener = gzip.open
        mode = "rt"
    else:
        opener = open
        mode = "r"

    with opener(export_file, mode, encoding="utf-8") as handle:
        handle.readline()
        return sum(1 for line in handle if line.strip())


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def collect_sqlite_counts(db_path: Path, tables: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    with sqlite3.connect(db_path) as connection:
        for table in tables:
            row = connection.execute(
                f"SELECT COUNT(*) FROM {quote_identifier(table)}"
            ).fetchone()
            counts[table] = int(row[0] or 0)
    return counts


def finalize_sqlite(db_path: Path) -> dict[str, str]:
    with sqlite3.connect(db_path) as connection:
        quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    return {"quick_check": quick_check, "journal_mode": journal_mode}


def remove_sqlite_sidecars(db_path: Path) -> None:
    for suffix in ("-shm", "-wal"):
        sidecar = Path(f"{db_path}{suffix}")
        sidecar.unlink(missing_ok=True)


def backup_existing_sqlite(db_path: Path, backup_dir: Path) -> dict[str, str]:
    backup_paths: dict[str, str] = {}
    if not db_path.exists():
        return backup_paths

    backup_dir.mkdir(parents=True, exist_ok=True)
    main_backup = backup_dir / db_path.name
    shutil.copy2(db_path, main_backup)
    backup_paths["db"] = str(main_backup)

    for suffix in ("-shm", "-wal"):
        sidecar = Path(f"{db_path}{suffix}")
        if sidecar.exists():
            target = backup_dir / sidecar.name
            shutil.copy2(sidecar, target)
            backup_paths[suffix.lstrip("-")] = str(target)

    return backup_paths


def main() -> int:
    args = parse_args()
    source_env = resolve_project_path(ROOT_DIR, args.source_env_file)
    target_db_path = resolve_project_path(ROOT_DIR, args.target_db_path)
    work_root = resolve_project_path(ROOT_DIR, args.work_root)
    requested_tables = parse_table_list(args.tables)

    if not source_env.exists():
        raise SystemExit(f"source env file not found: {source_env}")
    if not (COMET_DIR / ".venv" / "bin" / "python").exists():
        raise SystemExit("Comet virtualenv not found at cometouglas/.venv")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    label = args.run_label.strip() or "runtime-sqlite"
    run_dir = work_root / f"{run_id}-{label}"
    export_dir = run_dir / "exports"
    temp_dir = run_dir / "tmp"
    backup_dir = run_dir / "previous-target"
    temp_dir.mkdir(parents=True, exist_ok=True)
    export_dir.mkdir(parents=True, exist_ok=True)

    source_env_copy = temp_dir / "source.env"
    target_env_copy = temp_dir / "target.env"
    stage_db_path = temp_dir / target_db_path.name
    copy_env_with_overrides(
        source_env,
        source_env_copy,
        {"DATABASE_STARTUP_CLEANUP_INTERVAL": "-1"},
    )
    ensure_host_reachable_source_env(source_env_copy)
    copy_env_with_overrides(
        source_env,
        target_env_copy,
        {
            "DATABASE_TYPE": "sqlite",
            "DATABASE_PATH": str(stage_db_path),
            "DATABASE_STARTUP_CLEANUP_INTERVAL": "-1",
        },
    )

    export_args = ["--skip-setup", "export", "--output", str(export_dir)]
    if requested_tables:
        export_args.extend(["--tables", ",".join(requested_tables)])
    export_result = run_db_cli(source_env_copy, export_args)

    removed_exports: list[str] = []
    for table in EXCLUDED_EXPORT_TABLES:
        for suffix in (".json", ".json.gz"):
            export_file = export_dir / f"{table}{suffix}"
            if export_file.exists():
                export_file.unlink()
                removed_exports.append(table)

    export_files = sorted(
        [
            path
            for path in export_dir.iterdir()
            if path.is_file() and any(path.name.endswith(ext) for ext in (".json", ".json.gz"))
        ]
    )
    if not export_files:
        raise SystemExit("no export files were produced")

    imported_tables = [
        path.name.removesuffix(".json.gz").removesuffix(".json") for path in export_files
    ]
    source_counts = {table: count_export_rows(path) for table, path in zip(imported_tables, export_files)}

    import_result = run_db_cli(
        target_env_copy,
        ["import", "--input", str(export_dir), "--no-parallel"],
    )

    sqlite_counts = collect_sqlite_counts(stage_db_path, imported_tables)
    mismatched_tables = {
        table: {"source": source_counts[table], "sqlite": sqlite_counts[table]}
        for table in imported_tables
        if source_counts[table] != sqlite_counts[table]
    }
    if mismatched_tables:
        raise SystemExit(
            "sqlite row-count mismatch after import:\n"
            + json.dumps(mismatched_tables, indent=2, ensure_ascii=True)
        )

    sqlite_checks = finalize_sqlite(stage_db_path)
    if sqlite_checks["quick_check"].lower() != "ok":
        raise SystemExit(f"sqlite quick_check failed: {sqlite_checks['quick_check']}")

    target_db_path.parent.mkdir(parents=True, exist_ok=True)
    previous_target = backup_existing_sqlite(target_db_path, backup_dir)
    remove_sqlite_sidecars(target_db_path)
    stage_db_path.replace(target_db_path)
    remove_sqlite_sidecars(stage_db_path)

    summary = {
        "generated_at_utc": utc_now(),
        "source_env_file": str(source_env),
        "target_db_path": str(target_db_path),
        "container_runtime_db_path": "/app/data/runtime/comet-runtime.sqlite3",
        "requested_tables": requested_tables,
        "excluded_exports": sorted(set(removed_exports)),
        "table_row_counts": source_counts,
        "sqlite_checks": sqlite_checks,
        "target_db_size_bytes": target_db_path.stat().st_size,
        "previous_target_backup": previous_target,
        "db_cli_export_stdout": export_result.stdout,
        "db_cli_import_stdout": import_result.stdout,
    }
    summary_path = run_dir / "summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=True) + "\n", encoding="utf-8"
    )

    if not args.keep_export_dir:
        shutil.rmtree(export_dir, ignore_errors=True)

    print(json.dumps(summary, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
