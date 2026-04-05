#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import re
import shutil
import statistics
import subprocess
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests


REQ_TIMEOUT_PADDING_S = 15


def run(cmd: list[str], cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=check,
    )


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = (len(s) - 1) * p
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return s[lo]
    return (s[lo] * (hi - idx)) + (s[hi] * (idx - lo))


def parse_manifest_base_b64(manifest_url: str) -> tuple[str, str]:
    u = urlparse(manifest_url)
    parts = [p for p in u.path.split("/") if p]
    if len(parts) < 2 or parts[-1] != "manifest.json":
        raise ValueError(f"Invalid manifest url: {manifest_url}")
    return f"{u.scheme}://{u.netloc}", parts[-2]


def load_titles(path: Path, limit: int = 0) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    if limit > 0:
        rows = rows[:limit]
    return rows


def load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


def set_env_values(path: Path, updates: dict[str, str]) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    seen: set[str] = set()
    out: list[str] = []
    for line in lines:
        if "=" in line and not line.lstrip().startswith("#"):
            k = line.split("=", 1)[0].strip()
            if k in updates:
                out.append(f"{k}={updates[k]}")
                seen.add(k)
                continue
        out.append(line)
    for k, v in updates.items():
        if k not in seen:
            out.append(f"{k}={v}")
    path.write_text("\n".join(out) + "\n", encoding="utf-8")


def docker_compose(repo: Path, *args: str) -> subprocess.CompletedProcess:
    return run(["sudo", "docker", "compose", *args], cwd=repo)


def docker_logs(container: str, since: str) -> str:
    cp = run(["sudo", "docker", "logs", "--since", since, container], check=False)
    return f"{cp.stdout}\n{cp.stderr}".strip()


def health_check(repo: Path, timeout_s: int = 120) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        cp = run(
            ["sudo", "docker", "exec", "comet", "sh", "-lc", "wget -qO- http://127.0.0.1:8000/health"],
            check=False,
        )
        if cp.returncode == 0 and "ok" in cp.stdout.lower():
            return
        time.sleep(2)
    raise RuntimeError("Comet health check failed after restart window")


def wait_public_manifest(manifest_url: str, timeout_s: int = 90) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            r = requests.get(manifest_url, timeout=8)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError(f"Public manifest not ready after restart: {manifest_url}")


def apply_jackett_bypass_mode(repo: Path, mode: str) -> str:
    config_path = repo / "jackett/data/Jackett/ServerConfig.json"
    if not config_path.exists():
        return "jackett_config_missing"
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    desired = "http://byparr:8191" if mode in {"jackett", "both"} else "http://flaresolverr-jackett:8191"
    current = cfg.get("FlareSolverrUrl")
    if current == desired:
        return f"unchanged:{desired}"
    cfg["FlareSolverrUrl"] = desired
    payload = json.dumps(cfg, indent=2) + "\n"
    try:
        config_path.write_text(payload, encoding="utf-8")
    except PermissionError:
        tmp_path = Path("/tmp/jackett_serverconfig_tmp.json")
        tmp_path.write_text(payload, encoding="utf-8")
        run(["sudo", "cp", str(tmp_path), str(config_path)], check=True)
    return f"updated:{desired}"


def request_stream(manifest_url: str, media_type: str, media_id: str, timeout_s: float) -> dict[str, Any]:
    base, b64 = parse_manifest_base_b64(manifest_url)
    url = f"{base}/{b64}/stream/{media_type}/{media_id}.json"
    t0 = time.perf_counter()
    error = ""
    status = 0
    streams = 0
    try:
        r = requests.get(url, timeout=timeout_s)
        status = r.status_code
        payload = r.json() if "application/json" in r.headers.get("content-type", "") else {}
        streams = len(payload.get("streams", [])) if isinstance(payload, dict) else 0
    except Exception as e:  # noqa: BLE001
        error = str(e)
    elapsed = time.perf_counter() - t0
    return {
        "url": url,
        "status": status,
        "elapsed_s": round(elapsed, 3),
        "streams": streams,
        "error": error,
    }


PROVIDER_RE = re.compile(r"Scraper (?P<provider>\w+) #\d+ found (?P<count>\d+) torrents")
RD_RE = re.compile(r"Available cached torrents on (?P<service>[a-zA-Z0-9_]+): (?P<cached>\d+)/(?P<total>\d+)")
ERROR_RE = re.compile(r"(timeout|403|429|cooldown|rate.?limit|failed|forbidden)", re.IGNORECASE)


def parse_provider_events(log_text: str) -> list[dict[str, Any]]:
    out = []
    for line in log_text.splitlines():
        m = PROVIDER_RE.search(line)
        if not m:
            continue
        out.append(
            {
                "ts_line": line.split(" | ")[0].strip() if " | " in line else "",
                "provider": m.group("provider").lower(),
                "count": int(m.group("count")),
                "line": line,
            }
        )
    return out


def parse_rd_events(log_text: str) -> list[dict[str, Any]]:
    out = []
    for line in log_text.splitlines():
        m = RD_RE.search(line)
        if not m:
            continue
        out.append(
            {
                "service": m.group("service").lower(),
                "cached": int(m.group("cached")),
                "total": int(m.group("total")),
                "line": line,
            }
        )
    return out


def parse_error_events(log_text: str, source: str) -> list[dict[str, Any]]:
    out = []
    for line in log_text.splitlines():
        if not ERROR_RE.search(line):
            continue
        out.append({"source": source, "line": line})
    return out


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")


def compute_summary(request_rows: list[dict[str, Any]], error_rows: list[dict[str, Any]]) -> dict[str, Any]:
    ok = [r for r in request_rows if r["status"] == 200 and not r["error"]]
    lat = [float(r["elapsed_s"]) for r in ok]
    streams = [int(r["streams"]) for r in ok]
    success = len([s for s in streams if s > 0])
    zero = len([s for s in streams if s == 0])
    p50 = statistics.median(lat) if lat else 0.0
    p95 = percentile(lat, 0.95) if lat else 0.0
    p99 = percentile(lat, 0.99) if lat else 0.0
    return {
        "total_requests": len(request_rows),
        "ok_requests": len(ok),
        "p50_s": round(p50, 3),
        "p95_s": round(p95, 3),
        "p99_s": round(p99, 3),
        "avg_streams": round(statistics.mean(streams), 3) if streams else 0.0,
        "median_streams": statistics.median(streams) if streams else 0,
        "success_rate": round((success / len(ok)), 4) if ok else 0.0,
        "zero_stream_rate": round((zero / len(ok)), 4) if ok else 1.0,
        "error_events": len(error_rows),
    }


def norm(v: float, lo: float, hi: float, invert: bool = False) -> float:
    if hi <= lo:
        return 1.0
    x = (v - lo) / (hi - lo)
    x = max(0.0, min(1.0, x))
    return 1.0 - x if invert else x


def rank_variants(summaries: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    med_streams = [float(v.get("median_streams", 0)) for v in summaries.values()]
    p95s = [float(v.get("p95_s", 0)) for v in summaries.values()]
    success_rates = [float(v.get("success_rate", 0)) for v in summaries.values()]
    error_events = [float(v.get("error_events", 0)) for v in summaries.values()]

    min_streams, max_streams = min(med_streams), max(med_streams)
    min_p95, max_p95 = min(p95s), max(p95s)
    min_succ, max_succ = min(success_rates), max(success_rates)
    min_err, max_err = min(error_events), max(error_events)

    for name, s in summaries.items():
        rd_cached_streams_norm = norm(float(s.get("median_streams", 0)), min_streams, max_streams)
        latency_norm = norm(float(s.get("p95_s", 0)), min_p95, max_p95, invert=True)
        success_norm = norm(float(s.get("success_rate", 0)), min_succ, max_succ)
        stability_norm = norm(float(s.get("error_events", 0)), min_err, max_err, invert=True)
        score = (
            (0.50 * rd_cached_streams_norm)
            + (0.25 * latency_norm)
            + (0.15 * success_norm)
            + (0.10 * stability_norm)
        )
        hard_gate_ok = (
            float(s.get("p95_s", 9999)) <= 20.0
            and float(s.get("zero_stream_rate", 1.0)) <= 0.75
        )
        rows.append(
            {
                "variant": name,
                "score": round(score, 4),
                "hard_gate_ok": hard_gate_ok,
                "rd_cached_streams_norm": round(rd_cached_streams_norm, 4),
                "latency_norm": round(latency_norm, 4),
                "success_norm": round(success_norm, 4),
                "stability_norm": round(stability_norm, 4),
                **s,
            }
        )

    rows.sort(key=lambda r: (r["hard_gate_ok"], r["score"]), reverse=True)
    return rows


def build_variant_matrix(base_env: dict[str, str], phase: str) -> dict[str, dict[str, str]]:
    variants: dict[str, dict[str, str]] = {
        "phase0_baseline_current": {}
    }

    if phase in {"phase1", "phase2", "phase3", "full"}:
        variants.update(
            {
                "phase1_progressive_only": {
                    "PROVIDER_PARTIAL_MERGE": "True",
                    "PROVIDER_PROGRESSIVE_EMIT": "True",
                    "JACKETT_AS_COMPLETED": "True",
                    "PROWLARR_AS_COMPLETED": "True",
                    "LIVE_EARLY_RETURN_ON_RD_CACHED": "False",
                },
                "phase1_progressive_rd_threshold": {
                    "PROVIDER_PARTIAL_MERGE": "True",
                    "PROVIDER_PROGRESSIVE_EMIT": "True",
                    "JACKETT_AS_COMPLETED": "True",
                    "PROWLARR_AS_COMPLETED": "True",
                    "LIVE_EARLY_RETURN_ON_RD_CACHED": "True",
                    "LIVE_MIN_RD_CACHED_RESULTS": "10",
                },
                "phase1_split_budget": {
                    "LIVE_TOTAL_BUDGET_S": "20",
                    "LIVE_SCRAPE_BUDGET_S": "14",
                    "LIVE_RD_CHECK_BUDGET_S": "6",
                    "LIVE_EARLY_RETURN_ON_RD_CACHED": "True",
                    "LIVE_MIN_RD_CACHED_RESULTS": "10",
                },
            }
        )

    if phase in {"phase2", "phase3", "full"}:
        jackett_all = json.loads(base_env.get("JACKETT_INDEXERS", "[]"))
        prowlarr_all = json.loads(base_env.get("PROWLARR_INDEXERS", "[]"))
        for n in (8, 12, 17):
            j = json.dumps(jackett_all[: min(n, len(jackett_all))], ensure_ascii=True)
            p = json.dumps(prowlarr_all[: min(n, len(prowlarr_all))], ensure_ascii=True)
            variants[f"phase2_allow_{n}_group1_timeout10"] = {
                "JACKETT_INDEXERS": j,
                "PROWLARR_INDEXERS": p,
                "PROWLARR_GROUP_SIZE": "1",
                "INDEXER_MANAGER_TIMEOUT": "10",
            }
        variants["phase2_allow_12_group2_timeout14"] = {
            "JACKETT_INDEXERS": json.dumps(jackett_all[: min(12, len(jackett_all))], ensure_ascii=True),
            "PROWLARR_INDEXERS": json.dumps(prowlarr_all[: min(12, len(prowlarr_all))], ensure_ascii=True),
            "PROWLARR_GROUP_SIZE": "2",
            "INDEXER_MANAGER_TIMEOUT": "14",
        }
        variants["phase2_allow_12_group4_timeout20"] = {
            "JACKETT_INDEXERS": json.dumps(jackett_all[: min(12, len(jackett_all))], ensure_ascii=True),
            "PROWLARR_INDEXERS": json.dumps(prowlarr_all[: min(12, len(prowlarr_all))], ensure_ascii=True),
            "PROWLARR_GROUP_SIZE": "4",
            "INDEXER_MANAGER_TIMEOUT": "20",
        }

    if phase in {"phase3", "full"}:
        variants.update(
            {
                "phase3_byparr_off": {"BYPARR_MODE": "off"},
                "phase3_byparr_jackett": {"BYPARR_MODE": "jackett"},
                "phase3_byparr_prowlarr": {"BYPARR_MODE": "prowlarr"},
                "phase3_byparr_both": {"BYPARR_MODE": "both"},
            }
        )

    return variants


def run_variant(
    repo: Path,
    variant_name: str,
    overrides: dict[str, str],
    env_file: Path,
    titles: list[dict[str, str]],
    repeats: int,
    manifest_comet: str,
    out_dir: Path,
) -> dict[str, Any]:
    variant_dir = out_dir / variant_name
    variant_dir.mkdir(parents=True, exist_ok=True)
    variant_start = utc_now()

    set_env_values(env_file, overrides)
    mode = load_env(env_file).get("BYPARR_MODE", "off").lower()
    jackett_bypass_state = apply_jackett_bypass_mode(repo, mode)

    # Restart services at variant boundary.
    docker_compose(repo, "up", "-d", "comet", "jackett", "prowlarr", "flaresolverr", "flaresolverr-jackett")
    health_check(repo, timeout_s=180)
    wait_public_manifest(manifest_comet, timeout_s=120)

    req_rows: list[dict[str, Any]] = []
    timeout_s = max(5, int(load_env(env_file).get("LIVE_TOTAL_BUDGET_S", "20")) + REQ_TIMEOUT_PADDING_S)
    seq = 0
    for row in titles:
        media_type = row["media_type"]
        media_id = row["imdb_id"]
        for rep in range(1, repeats + 1):
            seq += 1
            result = request_stream(manifest_comet, media_type, media_id, timeout_s=timeout_s)
            req_rows.append(
                {
                    "seq": seq,
                    "repeat": rep,
                    "slot": row.get("slot"),
                    "category": row.get("category"),
                    "media_type": media_type,
                    "media_id": media_id,
                    "title_hint": row.get("title_hint", ""),
                    "requested_at_utc": utc_now(),
                    **result,
                }
            )

    comet_log = docker_logs("comet", variant_start)
    jackett_log = docker_logs("jackett", variant_start)
    prowlarr_log = docker_logs("prowlarr", variant_start)

    provider_events = parse_provider_events(comet_log)
    rd_events = parse_rd_events(comet_log)
    error_events = []
    error_events.extend(parse_error_events(comet_log, "comet"))
    error_events.extend(parse_error_events(jackett_log, "jackett"))
    error_events.extend(parse_error_events(prowlarr_log, "prowlarr"))

    write_jsonl(variant_dir / "requests.jsonl", req_rows)
    write_jsonl(variant_dir / "provider_events.jsonl", provider_events)
    write_jsonl(variant_dir / "rd_cache_checks.jsonl", rd_events)
    write_jsonl(variant_dir / "indexer_errors.jsonl", error_events)
    (variant_dir / "comet.log").write_text(comet_log + "\n", encoding="utf-8")
    (variant_dir / "jackett.log").write_text(jackett_log + "\n", encoding="utf-8")
    (variant_dir / "prowlarr.log").write_text(prowlarr_log + "\n", encoding="utf-8")
    (variant_dir / "variant_overrides.json").write_text(
        json.dumps(
            {
                "variant": variant_name,
                "overrides": overrides,
                "jackett_bypass_state": jackett_bypass_state,
                "started_utc": variant_start,
                "finished_utc": utc_now(),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    summary = compute_summary(req_rows, error_events)
    (variant_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def run_torrentio_baseline(
    titles: list[dict[str, str]],
    repeats: int,
    manifest_url: str,
    out_path: Path,
) -> dict[str, Any]:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    req_rows: list[dict[str, Any]] = []
    seq = 0
    for row in titles:
        for rep in range(1, repeats + 1):
            seq += 1
            result = request_stream(manifest_url, row["media_type"], row["imdb_id"], timeout_s=30)
            req_rows.append(
                {
                    "seq": seq,
                    "repeat": rep,
                    "slot": row.get("slot"),
                    "category": row.get("category"),
                    "media_type": row["media_type"],
                    "media_id": row["imdb_id"],
                    "title_hint": row.get("title_hint", ""),
                    "requested_at_utc": utc_now(),
                    **result,
                }
            )
    write_jsonl(out_path, req_rows)
    return compute_summary(req_rows, [])


def write_ranked_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "variant",
        "score",
        "hard_gate_ok",
        "p50_s",
        "p95_s",
        "p99_s",
        "avg_streams",
        "median_streams",
        "success_rate",
        "zero_stream_rate",
        "error_events",
        "rd_cached_streams_norm",
        "latency_norm",
        "success_norm",
        "stability_norm",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fields})


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run phased Comet reliability investigation.")
    p.add_argument("--repo", default="/home/ubuntu/aiostreams")
    p.add_argument("--env-file", default="comet-fresh.env")
    p.add_argument("--titles-file", default="test_plan/titles_30_investigation.csv")
    p.add_argument("--phase", choices=["phase0", "phase1", "phase2", "phase3", "full"], default="phase1")
    p.add_argument("--repeats", type=int, default=1)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--manifest-comet", required=True)
    p.add_argument("--manifest-baseline", required=True)
    p.add_argument("--run-id", default="")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    repo = Path(args.repo).resolve()
    env_file = (repo / args.env_file).resolve()
    titles_file = (repo / args.titles_file).resolve()
    if not env_file.exists():
        raise SystemExit(f"env file not found: {env_file}")
    if not titles_file.exists():
        raise SystemExit(f"titles file not found: {titles_file}")

    run_id = args.run_id or f"investigation_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    out_dir = repo / "test_runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    # Backups for rollback.
    env_backup = out_dir / "comet-fresh.env.backup"
    shutil.copy2(env_file, env_backup)
    jackett_backup = out_dir / "jackett_serverconfig.backup.json"
    jackett_cfg = repo / "jackett/data/Jackett/ServerConfig.json"
    if jackett_cfg.exists():
        shutil.copy2(jackett_cfg, jackett_backup)

    titles = load_titles(titles_file, limit=args.limit)
    base_env = load_env(env_file)

    if args.phase == "phase0":
        variants = {"phase0_baseline_current": {}}
    else:
        variants = build_variant_matrix(base_env, args.phase)

    summary_by_variant: dict[str, dict[str, Any]] = {}
    try:
        baseline_summary = run_torrentio_baseline(
            titles=titles,
            repeats=args.repeats,
            manifest_url=args.manifest_baseline,
            out_path=out_dir / "torrentio_baseline_requests.jsonl",
        )
        (out_dir / "torrentio_baseline_summary.json").write_text(
            json.dumps(baseline_summary, indent=2) + "\n", encoding="utf-8"
        )

        for variant_name, overrides in variants.items():
            print(f"== running {variant_name} ==")
            summary_by_variant[variant_name] = run_variant(
                repo=repo,
                variant_name=variant_name,
                overrides=overrides,
                env_file=env_file,
                titles=titles,
                repeats=args.repeats,
                manifest_comet=args.manifest_comet,
                out_dir=out_dir,
            )

        ranked = rank_variants(summary_by_variant)
        write_ranked_csv(out_dir / "ranked_variants.csv", ranked)

        recommendation = []
        recommendation.append("# Reliability Investigation Recommendation")
        recommendation.append("")
        recommendation.append(f"Run ID: `{run_id}`")
        recommendation.append(f"Generated: `{utc_now()}`")
        recommendation.append("")
        recommendation.append("Caveat:")
        recommendation.append("- Runner toggles Jackett FlareSolverr route for Byparr modes.")
        recommendation.append("- Prowlarr Byparr effect depends on existing Prowlarr indexer-proxy/tag setup.")
        recommendation.append("")
        if ranked:
            best = ranked[0]
            recommendation.append(f"Best variant: `{best['variant']}`")
            recommendation.append(f"- score: {best['score']}")
            recommendation.append(f"- hard_gate_ok: {best['hard_gate_ok']}")
            recommendation.append(f"- p95_s: {best['p95_s']}")
            recommendation.append(f"- median_streams: {best['median_streams']}")
            recommendation.append("")
            recommendation.append("Top 5 variants:")
            for row in ranked[:5]:
                recommendation.append(
                    f"- {row['variant']}: score={row['score']} gate={row['hard_gate_ok']} p95={row['p95_s']} median_streams={row['median_streams']}"
                )
        (out_dir / "recommendation.md").write_text("\n".join(recommendation) + "\n", encoding="utf-8")

        gap_lines = []
        gap_lines.append("# Torrentio Gap Report")
        gap_lines.append("")
        gap_lines.append(f"Baseline summary: `{json.dumps(baseline_summary, ensure_ascii=True)}`")
        gap_lines.append("")
        for row in ranked[:5]:
            ratio = (
                (float(row["p95_s"]) / float(baseline_summary.get("p95_s", 1.0)))
                if baseline_summary.get("p95_s", 0.0) > 0
                else None
            )
            gap_lines.append(
                f"- {row['variant']}: p95_ratio_vs_baseline={ratio:.2f}x, median_streams={row['median_streams']}, zero_stream_rate={row['zero_stream_rate']}"
            )
        gap_lines.append("")
        gap_lines.append("Likely gap drivers:")
        gap_lines.append("- indexer coverage/health and challenge behavior")
        gap_lines.append("- provider timeout pressure near budget")
        gap_lines.append("- RD cache availability variance by title category")
        gap_lines.append("- filtering strictness and metadata quality")
        (out_dir / "torrentio_gap_report.md").write_text("\n".join(gap_lines) + "\n", encoding="utf-8")

        (out_dir / "summary_by_variant.json").write_text(
            json.dumps(summary_by_variant, indent=2) + "\n", encoding="utf-8"
        )
        print(f"run_dir={out_dir}")
        return 0
    finally:
        # Rollback to original env/config regardless of run outcome.
        shutil.copy2(env_backup, env_file)
        if jackett_backup.exists():
            run(["sudo", "cp", str(jackett_backup), str(jackett_cfg)], check=False)
        try:
            docker_compose(repo, "up", "-d", "comet", "jackett")
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
