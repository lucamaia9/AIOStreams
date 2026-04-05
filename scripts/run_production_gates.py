#!/usr/bin/env python3
"""
Run the BitMagnet production gate bundle on this host.

The gate runner keeps destructive actions out of scope: it collects SLO reports,
category audit samples, movie recovery smoke metrics, and regression-suite
results, then emits a single JSON report. Exit status is non-zero when a hard
gate fails.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_SEARCH_DB = ROOT_DIR / "data" / "comet-fresh" / "magnetico" / "active.search.sqlite3"
DEFAULT_COMET_VENV_PYTHON = ROOT_DIR / "cometouglas" / ".venv" / "bin" / "python"


def run_json_command(command: list[str], *, cwd: Path | None = None) -> dict:
    completed = subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"Command failed ({completed.returncode}): {' '.join(command)}\n{completed.stderr.strip()}"
        )
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Command did not return JSON: {' '.join(command)}\n{completed.stdout[:4000]}"
        ) from exc


def run_test_command(command: list[str], *, cwd: Path | None = None) -> dict:
    completed = subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=True,
        check=False,
    )
    return {
        "command": command,
        "cwd": str(cwd) if cwd else str(ROOT_DIR),
        "returncode": completed.returncode,
        "status": "PASS" if completed.returncode == 0 else "FAIL",
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
    }


def build_checklists(search_db: Path) -> dict:
    return {
        "cutover_validation": [
            f"Run ./scripts/check_bitmagnet_id_coverage.py --json against {search_db}",
            "Confirm the stage DB passed integrity and schema checks before cutover.",
            "Confirm rebuild_live_sqlite.py emitted a rebuild-runs bundle and only refreshed a builder mirror if one was explicitly requested.",
            "Verify /status is up and live logs still show sqlite promotion completed with no readonly/malformed signatures.",
            "Verify any explicitly requested builder mirror points to the same freshly cut-over artifact as active.search.sqlite3.",
        ],
        "rollback_rehearsal": [
            "Identify the latest rebuild-runs directory and its paired backups rollback bundle.",
            "Confirm the rollback bundle contains the pre-cutover active.search.sqlite3 copy and replay metadata.",
            "Practice the restore commands against an offline copy first; do not rehearse on the live DB in place.",
            "After any real rollback, force-recreate the bitmagnet container if WAL/SHM sidecars may be stale.",
            "Re-run the production gate bundle after rollback to confirm the restored artifact is consistent.",
        ],
        "docs_reference": "BITMAGNET_OPERATIONS.md",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the BitMagnet production gate bundle")
    parser.add_argument(
        "--search-db",
        default=str(DEFAULT_SEARCH_DB),
        help="Path to the SQLite search DB to audit.",
    )
    parser.add_argument(
        "--category-sample-size",
        type=int,
        default=200,
        help="Per-category sample size for validate_unmatched_sample.py",
    )
    parser.add_argument(
        "--movie-top-sample-size",
        type=int,
        default=1000,
        help="Top grouped movie sample size for smoke_test_movies.py",
    )
    parser.add_argument(
        "--movie-random-sample-size",
        type=int,
        default=1000,
        help="Random grouped movie sample size for smoke_test_movies_random.py",
    )
    parser.add_argument(
        "--movie-random-seed",
        type=int,
        default=20260404,
        help="Deterministic seed for smoke_test_movies_random.py",
    )
    parser.add_argument(
        "--comet-python",
        default=str(DEFAULT_COMET_VENV_PYTHON),
        help="Python interpreter for cometouglas regression tests.",
    )
    parser.add_argument(
        "--report-path",
        help="Optional path to write the JSON gate report.",
    )
    parser.add_argument("--json", action="store_true", help="Emit the JSON report to stdout.")
    args = parser.parse_args()

    search_db = Path(args.search_db)
    if not search_db.is_absolute():
        search_db = (ROOT_DIR / search_db).resolve()

    coverage_report = run_json_command(
        [
            "python3",
            "scripts/check_bitmagnet_id_coverage.py",
            "--db-path",
            str(search_db),
            "--json",
        ],
        cwd=ROOT_DIR,
    )
    category_audit_report = run_json_command(
        [
            "python3",
            "scripts/validate_unmatched_sample.py",
            "--db-path",
            str(search_db),
            "--category",
            "all",
            "--sample",
            str(args.category_sample_size),
            "--seed",
            str(args.movie_random_seed),
            "--json",
        ],
        cwd=ROOT_DIR,
    )
    movie_top_smoke = run_json_command(
        [
            "python3",
            "scripts/smoke_test_movies.py",
            "--db-path",
            str(search_db),
            "--sample-size",
            str(args.movie_top_sample_size),
            "--json",
        ],
        cwd=ROOT_DIR,
    )
    movie_random_smoke = run_json_command(
        [
            "python3",
            "scripts/smoke_test_movies_random.py",
            "--db-path",
            str(search_db),
            "--sample-size",
            str(args.movie_random_sample_size),
            "--seed",
            str(args.movie_random_seed),
            "--json",
        ],
        cwd=ROOT_DIR,
    )

    movie_regressions = run_test_command(
        ["python3", "scripts/test_match_movies_to_tmdb.py", "-q"],
        cwd=ROOT_DIR,
    )
    magnetico_local_regressions = run_test_command(
        [args.comet_python, "-m", "unittest", "tests.test_magnetico_local", "-q"],
        cwd=ROOT_DIR / "cometouglas",
    )

    failures: list[str] = []
    if coverage_report["slos"]["combined_status"] != "PASS":
        failures.append("coverage_slos")
    if movie_regressions["status"] != "PASS":
        failures.append("movie_regressions")
    if magnetico_local_regressions["status"] != "PASS":
        failures.append("magnetico_local_regressions")

    report = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "search_db": str(search_db),
        "coverage": coverage_report,
        "category_audit": category_audit_report,
        "canonical_precision": {
            "movie_top_smoke": movie_top_smoke,
            "movie_random_smoke": movie_random_smoke,
        },
        "regressions": {
            "movie_recovery": movie_regressions,
            "magnetico_local": magnetico_local_regressions,
        },
        "checklists": build_checklists(search_db),
        "status": {
            "production_gates": "PASS" if not failures else "FAIL",
            "failures": failures,
        },
    }

    if args.report_path:
        report_path = Path(args.report_path)
        if not report_path.is_absolute():
            report_path = (ROOT_DIR / report_path).resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")

    if args.json or not args.report_path:
        print(json.dumps(report, indent=2, ensure_ascii=False))

    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
