#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import random
import re
import shutil
import sqlite3
import subprocess
import tempfile
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from lib.env_files import (
    dump_env,
    load_env,
    resolve_comet_data_path,
    resolve_project_path,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
HIT_W = 0.35
SEEDERS_W = 0.25
RECENCY_W = 0.20
RELIABILITY_W = 0.20

P_TIMEOUT_W = 0.45
P_FORBIDDEN_W = 0.30
P_RATELIMIT_W = 0.15
P_HARD_W = 0.10


def utc_now() -> str:
    return (
        dt.datetime.now(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build and optionally apply adaptive indexer allowlists from live Comet usage."
    )
    p.add_argument(
        "--db-path",
        default="",
        help=(
            "Explicit path to the SQLite runtime DB used for usage signals. "
            "When omitted, resolve from --env-file if DATABASE_TYPE=sqlite, "
            "otherwise fall back to REBALANCE_RUNTIME_DB_PATH or the legacy "
            "data/comet-fresh/comet-fresh.db path."
        ),
    )
    p.add_argument("--jackett-log-dir", default="jackett/data/Jackett")
    p.add_argument("--prowlarr-log-dir", default="prowlarr/config/logs")
    p.add_argument("--window-days", type=int, default=3)
    p.add_argument("--top-n", type=int, default=15)
    p.add_argument("--exploration-ratio", type=float, default=0.10)
    p.add_argument("--min-samples", type=int, default=20)
    p.add_argument("--max-membership-change", type=float, default=0.20)
    p.add_argument("--env-file", default="config/comet-fresh.env")
    p.add_argument("--compose-file", default="compose.yaml")
    p.add_argument("--service-name", default="comet")
    p.add_argument("--out-dir", default="")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--apply", action="store_true")
    return p.parse_args()


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    vals = sorted(values)
    if len(vals) == 1:
        return vals[0]
    idx = (len(vals) - 1) * p
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return vals[lo]
    return vals[lo] * (hi - idx) + vals[hi] * (idx - lo)


def normalize(value: float, low: float, high: float, invert: bool = False) -> float:
    if high <= low:
        return 1.0
    ratio = (value - low) / (high - low)
    ratio = max(0.0, min(1.0, ratio))
    if invert:
        ratio = 1.0 - ratio
    return ratio

def parse_env_list(raw: str) -> list[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    return [str(x) for x in data if str(x).strip()]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def resolve_runtime_db_path(db_path_arg: str, env_map: dict[str, str]) -> Path:
    explicit = str(db_path_arg or "").strip()
    if explicit:
        return resolve_project_path(PROJECT_ROOT, explicit)

    db_type = str(env_map.get("DATABASE_TYPE", "") or "").strip().lower()
    if db_type in {"sqlite", "sqlite3"}:
        raw_path = str(env_map.get("DATABASE_PATH", "") or "").strip()
        if raw_path:
            return resolve_comet_data_path(PROJECT_ROOT, raw_path)

    for key in ("REBALANCE_RUNTIME_DB_PATH", "COMET_REBALANCE_RUNTIME_DB_PATH"):
        override = str(env_map.get(key, "") or "").strip()
        if override:
            return resolve_comet_data_path(PROJECT_ROOT, override)

    return resolve_project_path(PROJECT_ROOT, "data/comet-fresh/comet-fresh.db")


def resolve_docker_cmd() -> list[str]:
    for cmd in (["docker"], ["sudo", "docker"]):
        try:
            subprocess.run(
                cmd + ["ps"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return cmd
        except Exception:
            continue
    raise RuntimeError("Docker is unavailable (direct or sudo).")


def get_jackett_indexers(jackett_log_dir: Path) -> set[str]:
    idx_dir = jackett_log_dir / "Indexers"
    out: set[str] = set()
    if not idx_dir.exists():
        return out
    for p in idx_dir.glob("*.json"):
        if p.name.endswith(".bak"):
            continue
        out.add(p.stem.strip())
    return out


def get_prowlarr_indexers(log_dir: Path, env_map: dict[str, str]) -> set[str]:
    # Prefer API if possible, fallback to names from logs.
    out: set[str] = set()
    base = env_map.get("PROWLARR_URL", "").rstrip("/")
    api_key = env_map.get("PROWLARR_API_KEY", "")
    if base and api_key:
        import urllib.error
        import urllib.request

        req = urllib.request.Request(
            f"{base}/api/v1/indexer", headers={"X-Api-Key": api_key}, method="GET"
        )
        try:
            with urllib.request.urlopen(req, timeout=8) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode("utf-8", errors="replace"))
                    if isinstance(data, list):
                        for row in data:
                            name = str(row.get("name", "")).strip()
                            def_name = str(row.get("definitionName", "")).strip()
                            if name:
                                out.add(name)
                            if def_name:
                                out.add(def_name)
        except Exception:
            pass

    if out:
        return out

    # Fallback.
    pat = re.compile(r"Adding request for ([^:]+):")
    for p in sorted(log_dir.glob("prowlarr.debug*.txt"))[-10:]:
        text = p.read_text(encoding="utf-8", errors="replace")
        for m in pat.finditer(text):
            out.add(m.group(1).strip())
    return out


def read_comet_usage(db_path: Path, window_days: int) -> dict[str, dict[str, Any]]:
    if not db_path.exists():
        return {}
    cutoff = int(time.time()) - (window_days * 86400)
    with tempfile.NamedTemporaryFile(
        prefix="comet_db_", suffix=".db", delete=False
    ) as tmp:
        tmp_path = Path(tmp.name)
    try:
        shutil.copyfile(db_path, tmp_path)
    except Exception:
        tmp_path = db_path

    con = sqlite3.connect(str(tmp_path))
    cur = con.cursor()
    try:
        columns = {
            str(row[1])
            for row in cur.execute("PRAGMA table_info(torrents)").fetchall()
            if len(row) > 1
        }
        if "updated_at" in columns:
            ts_column = "updated_at"
        elif "timestamp" in columns:
            ts_column = "timestamp"
        else:
            return {}

        cur.execute(
            f"""
            SELECT tracker, COUNT(*) AS hits, AVG(COALESCE(seeders, 0)) AS avg_seeders,
                   MAX({ts_column}) AS last_ts, AVG(COALESCE(seeders, 0)) AS seeders_quality
            FROM torrents
            WHERE {ts_column} >= ? AND tracker IS NOT NULL AND tracker != ''
            GROUP BY tracker
            """,
            (cutoff,),
        )
        rows = cur.fetchall()
    finally:
        con.close()
    if tmp_path != db_path:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
    out = {}
    for tracker, hits, avg_seeders, last_ts, seeders_quality in rows:
        out[str(tracker)] = {
            "tracker": str(tracker),
            "hit_count": int(hits or 0),
            "avg_seeders": float(avg_seeders or 0.0),
            "seeders_quality": float(seeders_quality or 0.0),
            "last_ts": int(last_ts or 0),
        }
    return out


def classify_error(msg: str) -> str:
    m = msg.lower()
    if "timeout" in m or "timed out" in m:
        return "timeout"
    if "403" in m or "forbidden" in m:
        return "forbidden"
    if "429" in m or "too many requests" in m or "rate limit" in m:
        return "ratelimit"
    if (
        "500" in m
        or "502" in m
        or "503" in m
        or "connection" in m
        or "unable to connect" in m
    ):
        return "hard"
    return "hard"


def parse_jackett_penalties(
    log_root: Path, window_days: int
) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    cutoff = dt.datetime.now() - dt.timedelta(days=window_days)
    pat = re.compile(r"Exception \(([^)]+)\): (.+)")
    paths = sorted(log_root.glob("log.txt*"))[-8:]
    for p in paths:
        text = p.read_text(encoding="utf-8", errors="replace")
        for line in text.splitlines():
            m = pat.search(line)
            if not m:
                continue
            # Timestamp format: 2026-02-19 13:02:33
            ts_ok = True
            if len(line) >= 19 and line[4] == "-" and line[13] == ":":
                try:
                    when = dt.datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S")
                    ts_ok = when >= cutoff
                except Exception:
                    ts_ok = True
            if not ts_ok:
                continue
            idx = m.group(1).strip()
            kind = classify_error(m.group(2))
            out[idx][kind] += 1
            out[idx]["total"] += 1
    return out


def parse_prowlarr_penalties(
    log_dir: Path, window_days: int
) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    cutoff = dt.datetime.now() - dt.timedelta(days=window_days)
    paths = sorted(log_dir.glob("prowlarr.debug*.txt"))[-16:]
    add_pat = re.compile(r"Adding request for ([^:]+):")
    ts_pat = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
    recent_idx = "unknown"
    for p in paths:
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
        for line in lines:
            m_ts = ts_pat.search(line)
            if m_ts:
                try:
                    when = dt.datetime.strptime(m_ts.group(1), "%Y-%m-%d %H:%M:%S")
                    if when < cutoff:
                        continue
                except Exception:
                    pass
            m_add = add_pat.search(line)
            if m_add:
                recent_idx = m_add.group(1).strip()
                continue
            if "|Warn|" not in line and "|Error|" not in line:
                continue
            if (
                "HTTP Error" in line
                or "Unable to connect to indexer" in line
                or "request failed" in line.lower()
            ):
                kind = classify_error(line)
                out[recent_idx][kind] += 1
                out[recent_idx]["total"] += 1
    return out


def build_provider_rows(
    provider: str,
    usage: dict[str, dict[str, Any]],
    penalties: dict[str, dict[str, int]],
    indexer_names: set[str],
    now_ts: int,
    window_days: int,
    min_samples: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not usage:
        return rows

    # filter by provider known indexers (case-insensitive)
    name_map = {n.lower(): n for n in indexer_names}
    for tracker, u in usage.items():
        t_lower = tracker.lower()
        if t_lower in name_map:
            canonical = name_map[t_lower]
        else:
            # Skip unknown trackers for this provider to avoid cross-provider pollution.
            continue
        p = penalties.get(canonical, penalties.get(tracker, {}))
        total_err = int(p.get("total", 0))
        timeout = int(p.get("timeout", 0))
        forbidden = int(p.get("forbidden", 0))
        ratelimit = int(p.get("ratelimit", 0))
        hard = int(p.get("hard", 0))
        denom = max(total_err, 1)
        penalty_norm = (
            P_TIMEOUT_W * (timeout / denom)
            + P_FORBIDDEN_W * (forbidden / denom)
            + P_RATELIMIT_W * (ratelimit / denom)
            + P_HARD_W * (hard / denom)
        )
        penalty_norm = max(0.0, min(1.0, penalty_norm))
        recency_sec = max(0, now_ts - int(u["last_ts"]))
        recency_days = recency_sec / 86400.0
        rows.append(
            {
                "provider": provider,
                "indexer": canonical,
                "samples": int(u["hit_count"]),
                "seeders_quality": float(u["seeders_quality"]),
                "last_seen_days": recency_days,
                "timeout_count": timeout,
                "forbidden_count": forbidden,
                "ratelimit_count": ratelimit,
                "hard_error_count": hard,
                "error_events": total_err,
                "penalty_norm": penalty_norm,
                "eligible": int(u["hit_count"]) >= min_samples,
                "score": 0.0,
                "role": "excluded",
            }
        )

    if not rows:
        return rows

    hits = [r["samples"] for r in rows]
    seeds = [r["seeders_quality"] for r in rows]
    recs = [r["last_seen_days"] for r in rows]
    min_h, max_h = min(hits), max(hits)
    min_s, max_s = min(seeds), max(seeds)
    min_r, max_r = min(recs), max(recs)

    for r in rows:
        hit_norm = normalize(float(r["samples"]), float(min_h), float(max_h))
        seed_norm = normalize(float(r["seeders_quality"]), float(min_s), float(max_s))
        recency_norm = normalize(
            float(r["last_seen_days"]), float(min_r), float(max_r), invert=True
        )
        reliability_norm = 1.0 - float(r["penalty_norm"])
        score = (
            HIT_W * hit_norm
            + SEEDERS_W * seed_norm
            + RECENCY_W * recency_norm
            + RELIABILITY_W * reliability_norm
        )
        r["score"] = max(0.0, min(1.0, score))
        r["hit_norm"] = hit_norm
        r["seed_norm"] = seed_norm
        r["recency_norm"] = recency_norm
        r["reliability_norm"] = reliability_norm

    rows.sort(
        key=lambda x: (x["score"], x["samples"], x["seeders_quality"]), reverse=True
    )
    return rows


def choose_allowlist(
    rows: list[dict[str, Any]], top_n: int, exploration_ratio: float, seed: int
) -> tuple[list[str], list[str], list[str]]:
    core: list[str] = []
    low_conf: list[str] = []
    for r in rows:
        if r["eligible"] and r["score"] >= 0.50 and len(core) < top_n:
            r["role"] = "core"
            core.append(r["indexer"])
    if len(core) < top_n:
        for r in rows:
            if r["indexer"] in core:
                continue
            if len(core) >= top_n:
                break
            r["role"] = "low_confidence_core"
            core.append(r["indexer"])
            low_conf.append(r["indexer"])

    explore_n = max(1, int(math.ceil(top_n * exploration_ratio)))
    pool = [r["indexer"] for r in rows if r["indexer"] not in core]
    rnd = random.Random(seed)
    rnd.shuffle(pool)
    exploration = pool[:explore_n]
    for r in rows:
        if r["indexer"] in exploration:
            r["role"] = "exploration"
    return core, exploration, low_conf


def write_ranked_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "provider",
                "indexer",
                "samples",
                "seeders_quality",
                "last_seen_days",
                "penalty_norm",
                "timeout_count",
                "forbidden_count",
                "ratelimit_count",
                "hard_error_count",
                "score",
                "eligible",
                "role",
            ],
        )
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "provider": r["provider"],
                    "indexer": r["indexer"],
                    "samples": r["samples"],
                    "seeders_quality": f"{r['seeders_quality']:.3f}",
                    "last_seen_days": f"{r['last_seen_days']:.3f}",
                    "penalty_norm": f"{r['penalty_norm']:.4f}",
                    "timeout_count": r["timeout_count"],
                    "forbidden_count": r["forbidden_count"],
                    "ratelimit_count": r["ratelimit_count"],
                    "hard_error_count": r["hard_error_count"],
                    "score": f"{r['score']:.4f}",
                    "eligible": str(bool(r["eligible"])).lower(),
                    "role": r["role"],
                }
            )


def write_allowlist_json(path: Path, provider: str, indexers: list[str]) -> None:
    ensure_dir(path.parent)
    payload = {
        "generated_at_utc": utc_now(),
        "provider": provider,
        "indexers": indexers,
    }
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8"
    )


def membership_change_ratio(prev: list[str], new: list[str]) -> float:
    a = set(prev)
    b = set(new)
    if not a and not b:
        return 0.0
    if not a:
        return 1.0
    changed = len(a.symmetric_difference(b))
    return changed / max(len(a), len(b), 1)


FAST_SPLIT_TOP_N = 8
FAST_MIN_FLOOR = 5


def split_fast_slow(rows: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    if not rows:
        return [], []
    sorted_rows = sorted(
        rows,
        key=lambda r: (r["score"], r["samples"], r["seeders_quality"]),
        reverse=True,
    )
    fast: list[str] = []
    slow: list[str] = []
    for r in sorted_rows:
        if len(fast) < FAST_SPLIT_TOP_N:
            fast.append(r["indexer"])
        else:
            slow.append(r["indexer"])
    if len(fast) < FAST_MIN_FLOOR:
        fast = [r["indexer"] for r in sorted_rows[:FAST_MIN_FLOOR]]
        slow = [r["indexer"] for r in sorted_rows[FAST_MIN_FLOOR:]]
    return fast, slow


def resolve_overlaps(
    j_fast: list[str],
    p_fast: list[str],
    jackett_rows: list[dict[str, Any]],
    prowlarr_rows: list[dict[str, Any]],
) -> tuple[list[str], list[str], list[dict[str, str]]]:
    j_overlap = set(j_fast) & set(p_fast)
    if not j_overlap:
        return j_fast, p_fast, []
    j_score_map = {r["indexer"]: r["score"] for r in jackett_rows}
    p_score_map = {r["indexer"]: r["score"] for r in prowlarr_rows}
    reassignments: list[dict[str, str]] = []
    j_borrowed: set[str] = set()
    p_borrowed: set[str] = set()
    j_fast_set = set(j_fast)
    p_fast_set = set(p_fast)
    for idx in j_overlap:
        j_score = j_score_map.get(idx, 0.0)
        p_score = p_score_map.get(idx, 0.0)
        if j_score >= p_score:
            winner, loser = "jackett", "prowlarr"
            winner_fast_set, loser_fast_set = j_fast_set, p_fast_set
        else:
            winner, loser = "prowlarr", "jackett"
            winner_fast_set, loser_fast_set = p_fast_set, j_fast_set
        loser_fast_set.discard(idx)
        reassignments.append(
            {
                "indexer": idx,
                "winner": winner,
                "loser": loser,
                "j_score": f"{j_score:.4f}",
                "p_score": f"{p_score:.4f}",
            }
        )

    def fill_to_min(
        provider_fast: set[str], all_rows: list[dict[str, Any]], provider: str
    ) -> list[str]:
        current = list(provider_fast)
        if len(current) >= FAST_MIN_FLOOR:
            return current
        filled: list[str] = list(current)
        sorted_all = sorted(
            all_rows,
            key=lambda r: (r["score"], r["samples"], r["seeders_quality"]),
            reverse=True,
        )
        for r in sorted_all:
            if r["indexer"] not in provider_fast and r["indexer"] not in j_overlap:
                if r["indexer"] not in filled:
                    filled.append(r["indexer"])
                    reassignments.append(
                        {
                            "indexer": r["indexer"],
                            "winner": provider,
                            "loser": "none",
                            "j_score": j_score_map.get(r["indexer"], 0.0).__format__(
                                ".4f"
                            ),
                            "p_score": p_score_map.get(r["indexer"], 0.0).__format__(
                                ".4f"
                            ),
                        }
                    )
                    if len(filled) >= FAST_MIN_FLOOR:
                        break
        return filled

    j_final_fast = fill_to_min(j_fast_set, jackett_rows, "jackett")
    p_final_fast = fill_to_min(p_fast_set, prowlarr_rows, "prowlarr")
    return j_final_fast, p_final_fast, reassignments


def write_report(
    path: Path,
    run_id: str,
    status: str,
    reasons: list[str],
    jackett_rows: list[dict[str, Any]],
    prowlarr_rows: list[dict[str, Any]],
    jackett_final: list[str],
    prowlarr_final: list[str],
    j_fast: list[str] | None = None,
    j_slow: list[str] | None = None,
    p_fast: list[str] | None = None,
    p_slow: list[str] | None = None,
    overlap_reassignments: list[dict[str, str]] | None = None,
) -> None:
    lines = [
        "# Adaptive Rebalance Report",
        "",
        f"- run_id: `{run_id}`",
        f"- generated_at_utc: `{utc_now()}`",
        f"- status: `{status}`",
    ]
    if reasons:
        lines.append("- guardrail_reasons:")
        lines.extend([f"  - {r}" for r in reasons])
    lines.extend(
        [
            "",
            f"- jackett_final_count: {len(jackett_final)}",
            f"- prowlarr_final_count: {len(prowlarr_final)}",
            "",
            "## Fast/Slow Split",
        ]
    )
    if j_fast is not None and j_slow is not None:
        lines.append(f"### Jackett fast ({len(j_fast)}): {', '.join(j_fast)}")
        lines.append(
            f"### Jackett slow ({len(j_slow or [])}): {', '.join(j_slow or [])}"
        )
        lines.append(
            f"### Prowlarr fast ({len(p_fast or [])}): {', '.join(p_fast or [])}"
        )
        lines.append(
            f"### Prowlarr slow ({len(p_slow or [])}): {', '.join(p_slow or [])}"
        )
    if overlap_reassignments:
        lines.append("")
        lines.append("## Overlap Reassignments")
        for r in overlap_reassignments:
            lines.append(
                f"- {r['indexer']}: winner={r['winner']} "
                f"(j_score={r['j_score']}, p_score={r['p_score']})"
            )
    lines.extend(
        [
            "",
            "## Top 10 Jackett",
        ]
    )
    for r in jackett_rows[:10]:
        lines.append(
            f"- {r['indexer']} score={r['score']:.3f} samples={r['samples']} role={r['role']}"
        )
    lines.append("")
    lines.append("## Top 10 Prowlarr")
    for r in prowlarr_rows[:10]:
        lines.append(
            f"- {r['indexer']} score={r['score']:.3f} samples={r['samples']} role={r['role']}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def health_check(base_url: str, timeout_s: int = 120) -> bool:
    import urllib.request
    import urllib.error

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(
                f"{base_url.rstrip('/')}/health", timeout=5
            ) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(3)
    return False


def restart_service(
    docker_cmd: list[str], compose_file: Path, service: str
) -> tuple[bool, str]:
    try:
        res = subprocess.run(
            docker_cmd + ["compose", "-f", str(compose_file), "up", "-d", service],
            check=False,
            capture_output=True,
            text=True,
        )
        ok = res.returncode == 0
        return ok, (res.stdout + "\n" + res.stderr).strip()
    except Exception as ex:
        return False, f"restart_exception:{type(ex).__name__}:{ex}"


def main() -> int:
    args = parse_args()
    if not args.apply:
        args.dry_run = True

    run_id = dt.datetime.now(dt.timezone.utc).strftime("rebalance_%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir) if args.out_dir else Path("test_runs") / run_id
    ranked_dir = out_dir / "ranked"
    metrics_dir = out_dir / "metrics"
    apply_dir = out_dir / "apply"
    ensure_dir(ranked_dir)
    ensure_dir(metrics_dir)
    ensure_dir(apply_dir)

    env_path = resolve_project_path(PROJECT_ROOT, args.env_file)
    env_map = load_env(env_path)
    usage_db_path = resolve_runtime_db_path(args.db_path, env_map)

    if not usage_db_path.exists():
        raise SystemExit(f"usage db not found: {usage_db_path}")

    usage = read_comet_usage(usage_db_path, args.window_days)
    jackett_indexers = get_jackett_indexers(Path(args.jackett_log_dir))
    prowlarr_indexers = get_prowlarr_indexers(Path(args.prowlarr_log_dir), env_map)
    jackett_pen = parse_jackett_penalties(Path(args.jackett_log_dir), args.window_days)
    prowlarr_pen = parse_prowlarr_penalties(
        Path(args.prowlarr_log_dir), args.window_days
    )

    now_ts = int(time.time())
    jackett_rows = build_provider_rows(
        "jackett",
        usage,
        jackett_pen,
        jackett_indexers,
        now_ts,
        args.window_days,
        args.min_samples,
    )
    prowlarr_rows = build_provider_rows(
        "prowlarr",
        usage,
        prowlarr_pen,
        prowlarr_indexers,
        now_ts,
        args.window_days,
        args.min_samples,
    )

    date_seed = int(dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d"))
    j_core, j_exp, j_low = choose_allowlist(
        jackett_rows, args.top_n, args.exploration_ratio, date_seed + 11
    )
    p_core, p_exp, p_low = choose_allowlist(
        prowlarr_rows, args.top_n, args.exploration_ratio, date_seed + 29
    )
    j_final = j_core + j_exp
    p_final = p_core + p_exp

    j_fast, j_slow = split_fast_slow(jackett_rows)
    p_fast, p_slow = split_fast_slow(prowlarr_rows)
    j_fast, p_fast, overlap_reassignments = resolve_overlaps(
        j_fast, p_fast, jackett_rows, prowlarr_rows
    )

    write_ranked_csv(ranked_dir / "jackett_ranked.csv", jackett_rows)
    write_ranked_csv(ranked_dir / "prowlarr_ranked.csv", prowlarr_rows)
    write_allowlist_json(ranked_dir / "jackett_allowlist.json", "jackett", j_final)
    write_allowlist_json(ranked_dir / "prowlarr_allowlist.json", "prowlarr", p_final)
    write_allowlist_json(ranked_dir / "jackett_fast.json", "jackett", j_fast)
    write_allowlist_json(ranked_dir / "jackett_slow.json", "jackett", j_slow)
    write_allowlist_json(ranked_dir / "prowlarr_fast.json", "prowlarr", p_fast)
    write_allowlist_json(ranked_dir / "prowlarr_slow.json", "prowlarr", p_slow)
    (ranked_dir / "allowlists_env_snippets.txt").write_text(
        "\n".join(
            [
                "# Comet env snippets generated from adaptive rebalance",
                f"JACKETT_INDEXERS={json.dumps(j_final, ensure_ascii=True)}",
                f"PROWLARR_INDEXERS={json.dumps(p_final, ensure_ascii=True)}",
                f"JACKETT_FAST_INDEXERS={json.dumps(j_fast, ensure_ascii=True)}",
                f"JACKETT_SLOW_INDEXERS={json.dumps(j_slow, ensure_ascii=True)}",
                f"PROWLARR_FAST_INDEXERS={json.dumps(p_fast, ensure_ascii=True)}",
                f"PROWLARR_SLOW_INDEXERS={json.dumps(p_slow, ensure_ascii=True)}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    source_counts = {
        "window_days": args.window_days,
        "usage_db_path": str(usage_db_path),
        "usage_trackers": len(usage),
        "jackett_known_indexers": len(jackett_indexers),
        "prowlarr_known_indexers": len(prowlarr_indexers),
        "jackett_ranked": len(jackett_rows),
        "prowlarr_ranked": len(prowlarr_rows),
        "jackett_total_samples": sum(r["samples"] for r in jackett_rows),
        "prowlarr_total_samples": sum(r["samples"] for r in prowlarr_rows),
    }
    (metrics_dir / "source_counts.json").write_text(
        json.dumps(source_counts, indent=2, ensure_ascii=True) + "\n", encoding="utf-8"
    )
    (metrics_dir / "penalty_breakdown.json").write_text(
        json.dumps(
            {"jackett": jackett_pen, "prowlarr": prowlarr_pen},
            indent=2,
            ensure_ascii=True,
        )
        + "\n",
        encoding="utf-8",
    )

    prev_j = parse_env_list(env_map.get("JACKETT_INDEXERS", ""))
    prev_p = parse_env_list(env_map.get("PROWLARR_INDEXERS", ""))
    churn_j = membership_change_ratio(prev_j, j_final)
    churn_p = membership_change_ratio(prev_p, p_final)

    guardrail_reasons: list[str] = []
    if (
        source_counts["jackett_total_samples"] + source_counts["prowlarr_total_samples"]
        < 30
    ):
        guardrail_reasons.append("insufficient_total_sample_volume(<30)")
    if len(j_core) < 10:
        guardrail_reasons.append("jackett_core_size_below_10")
    if len(p_core) < 10:
        guardrail_reasons.append("prowlarr_core_size_below_10")
    if prev_j and churn_j > args.max_membership_change:
        guardrail_reasons.append(
            f"jackett_membership_change_exceeds_{args.max_membership_change:.2f}"
        )
    if prev_p and churn_p > args.max_membership_change:
        guardrail_reasons.append(
            f"prowlarr_membership_change_exceeds_{args.max_membership_change:.2f}"
        )

    status = "DRY_RUN" if args.dry_run else "HOLD"
    apply_result: dict[str, Any] = {
        "run_id": run_id,
        "timestamp_utc": utc_now(),
        "dry_run": bool(args.dry_run),
        "churn": {"jackett": churn_j, "prowlarr": churn_p},
        "core_sizes": {"jackett": len(j_core), "prowlarr": len(p_core)},
        "final_sizes": {"jackett": len(j_final), "prowlarr": len(p_final)},
        "fast_slow": {
            "jackett_fast": j_fast,
            "jackett_slow": j_slow,
            "prowlarr_fast": p_fast,
            "prowlarr_slow": p_slow,
        },
        "overlap_reassignments": overlap_reassignments,
        "low_confidence": {"jackett": j_low, "prowlarr": p_low},
        "guardrail_reasons": guardrail_reasons,
    }

    if args.apply and not guardrail_reasons:
        docker_cmd = resolve_docker_cmd()
        backup = env_path.with_name(f"{env_path.name}.bak.{run_id}")
        backup.write_text(
            env_path.read_text(encoding="utf-8", errors="replace"), encoding="utf-8"
        )
        dump_env(
            env_path,
            {
                "JACKETT_INDEXERS": json.dumps(j_final, ensure_ascii=True),
                "PROWLARR_INDEXERS": json.dumps(p_final, ensure_ascii=True),
                "JACKETT_FAST_INDEXERS": json.dumps(j_fast, ensure_ascii=True),
                "JACKETT_SLOW_INDEXERS": json.dumps(j_slow, ensure_ascii=True),
                "PROWLARR_FAST_INDEXERS": json.dumps(p_fast, ensure_ascii=True),
                "PROWLARR_SLOW_INDEXERS": json.dumps(p_slow, ensure_ascii=True),
            },
        )
        ok_restart, restart_msg = restart_service(
            docker_cmd, Path(args.compose_file), args.service_name
        )
        base = (
            env_map.get("PUBLIC_BASE_URL", "").strip() or "https://comet.mrdouglas.uk"
        )
        healthy = ok_restart and health_check(base, timeout_s=120)
        if healthy:
            status = "APPLIED"
            apply_result["result"] = "applied"
            apply_result["restart"] = restart_msg
            apply_result["health_url"] = f"{base.rstrip('/')}/health"
        else:
            # Roll back.
            dump_env(env_path, load_env(backup))
            restart_service(docker_cmd, Path(args.compose_file), args.service_name)
            status = "ROLLED_BACK"
            apply_result["result"] = "rolled_back"
            apply_result["restart"] = restart_msg
            apply_result["reason"] = "health_check_failed_after_apply"
    elif args.apply and guardrail_reasons:
        status = "HOLD"
        apply_result["result"] = "hold"
    else:
        status = "DRY_RUN"
        apply_result["result"] = "dry_run_only"

    (apply_dir / "apply_result.json").write_text(
        json.dumps(apply_result, indent=2, ensure_ascii=True) + "\n", encoding="utf-8"
    )
    write_report(
        ranked_dir / "rebalance_report.md",
        run_id,
        status,
        guardrail_reasons,
        jackett_rows,
        prowlarr_rows,
        j_final,
        p_final,
        j_fast,
        j_slow,
        p_fast,
        p_slow,
        overlap_reassignments,
    )

    print(f"run_id={run_id}")
    print(f"out_dir={out_dir}")
    print(f"status={status}")
    print(
        f"jackett_core={len(j_core)} jackett_final={len(j_final)} churn={churn_j:.3f}"
    )
    print(
        f"prowlarr_core={len(p_core)} prowlarr_final={len(p_final)} churn={churn_p:.3f}"
    )
    if guardrail_reasons:
        print("guardrail_reasons=" + ",".join(guardrail_reasons))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
