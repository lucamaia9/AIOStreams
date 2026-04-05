#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import shutil
import statistics
import subprocess
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

REQ_TIMEOUT_PADDING_S = 15


def run(
    cmd: list[str], cwd: Path | None = None, check: bool = True
) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=check,
    )


def utc_now() -> str:
    return (
        dt.datetime.now(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


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


def docker_compose(repo: Path, *args: str) -> subprocess.CompletedProcess:
    return run(["sudo", "docker", "compose", *args], cwd=repo)


def docker_logs(container: str, since: str) -> str:
    cp = run(["sudo", "docker", "logs", "--since", since, container], check=False)
    return f"{cp.stdout}\n{cp.stderr}".strip()


def parse_manifest_base_b64(manifest_url: str) -> tuple[str, str]:
    u = urlparse(manifest_url)
    parts = [p for p in u.path.split("/") if p]
    if len(parts) < 2 or parts[-1] != "manifest.json":
        raise ValueError(f"Invalid manifest url: {manifest_url}")
    return f"{u.scheme}://{u.netloc}", parts[-2]


def request_stream(
    manifest_url: str, media_type: str, media_id: str, timeout_s: float
) -> dict[str, Any]:
    base, b64 = parse_manifest_base_b64(manifest_url)
    url = f"{base}/{b64}/stream/{media_type}/{media_id}.json"
    t0 = time.perf_counter()
    error = ""
    status = 0
    streams = 0
    try:
        r = requests.get(url, timeout=timeout_s)
        status = r.status_code
        payload = (
            r.json() if "application/json" in r.headers.get("content-type", "") else {}
        )
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


def health_check(timeout_s: int = 180) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        cp = run(
            [
                "sudo",
                "docker",
                "exec",
                "comet",
                "sh",
                "-lc",
                "wget -qO- http://127.0.0.1:8000/health",
            ],
            check=False,
        )
        if cp.returncode == 0 and "ok" in cp.stdout.lower():
            return
        time.sleep(2)
    raise RuntimeError("Comet health check failed")


def wait_public_manifest(manifest_url: str, timeout_s: int = 120) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            r = requests.get(manifest_url, timeout=8)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError(f"Manifest not ready: {manifest_url}")


def probe_proxy_lane(container: str) -> dict[str, Any]:
    ip_cmd = [
        "sudo",
        "docker",
        "exec",
        container,
        "sh",
        "-lc",
        "curl -fsSL --max-time 15 --proxy http://localhost:8888 https://ifconfig.me",
    ]
    tr_cmd = [
        "sudo",
        "docker",
        "exec",
        container,
        "sh",
        "-lc",
        "curl -I -L -sS --max-time 20 --proxy http://localhost:8888 https://torrentio.strem.fun/configure | head -n 1",
    ]
    ip = run(ip_cmd, check=False)
    tr = run(tr_cmd, check=False)
    return {
        "container": container,
        "ok": ip.returncode == 0,
        "egress_ip": ip.stdout.strip(),
        "torrentio_head": tr.stdout.strip(),
        "ip_err": ip.stderr.strip(),
        "torrentio_err": tr.stderr.strip(),
        "ts": utc_now(),
    }


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")


def parse_error_events(text: str, source: str) -> list[dict[str, Any]]:
    needles = (
        "timeout",
        "403",
        "429",
        "forbidden",
        "cooldown",
        "cookies provided by flaresolverr are not valid",
    )
    out = []
    for line in text.splitlines():
        low = line.lower()
        if any(n in low for n in needles):
            out.append({"source": source, "line": line})
    return out


def compute_summary(
    request_rows: list[dict[str, Any]],
    errors: list[dict[str, Any]],
    lane_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    ok = [r for r in request_rows if r["status"] == 200 and not r["error"]]
    lat = [float(r["elapsed_s"]) for r in ok]
    streams = [int(r["streams"]) for r in ok]
    success = len([s for s in streams if s > 0])
    zero = len([s for s in streams if s == 0])
    lane_ok = len([r for r in lane_rows if r.get("ok")])
    lane_total = len(lane_rows)
    return {
        "total_requests": len(request_rows),
        "ok_requests": len(ok),
        "p50_s": round(statistics.median(lat), 3) if lat else 0.0,
        "p95_s": round(percentile(lat, 0.95), 3) if lat else 0.0,
        "p99_s": round(percentile(lat, 0.99), 3) if lat else 0.0,
        "avg_streams": round(statistics.mean(streams), 3) if streams else 0.0,
        "median_streams": statistics.median(streams) if streams else 0,
        "success_rate": round((success / len(ok)), 4) if ok else 0.0,
        "zero_stream_rate": round((zero / len(ok)), 4) if ok else 1.0,
        "error_events": len(errors),
        "lane_ok_ratio": round((lane_ok / lane_total), 4) if lane_total else 0.0,
    }


def norm(v: float, lo: float, hi: float, invert: bool = False) -> float:
    if hi <= lo:
        return 1.0
    x = (v - lo) / (hi - lo)
    x = max(0.0, min(1.0, x))
    return 1.0 - x if invert else x


def rank_variants(summaries: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rel = [float(v.get("error_events", 0)) for v in summaries.values()]
    p95s = [float(v.get("p95_s", 0)) for v in summaries.values()]
    med = [float(v.get("median_streams", 0)) for v in summaries.values()]
    stab = [float(v.get("lane_ok_ratio", 0)) for v in summaries.values()]
    min_rel, max_rel = min(rel), max(rel)
    min_p95, max_p95 = min(p95s), max(p95s)
    min_med, max_med = min(med), max(med)
    min_st, max_st = min(stab), max(stab)

    for name, s in summaries.items():
        reliability = norm(float(s["error_events"]), min_rel, max_rel, invert=True)
        latency = norm(float(s["p95_s"]), min_p95, max_p95, invert=True)
        stream_yield = norm(float(s["median_streams"]), min_med, max_med)
        stability = norm(float(s["lane_ok_ratio"]), min_st, max_st)
        score = (
            (0.45 * reliability)
            + (0.25 * latency)
            + (0.20 * stream_yield)
            + (0.10 * stability)
        )
        hard_gate_ok = (
            float(s["p95_s"]) <= 20.0 and float(s["zero_stream_rate"]) <= 0.75
        )
        rows.append(
            {
                "variant": name,
                "score": round(score, 4),
                "hard_gate_ok": hard_gate_ok,
                "reliability_norm": round(reliability, 4),
                "latency_norm": round(latency, 4),
                "stream_yield_norm": round(stream_yield, 4),
                "stability_norm": round(stability, 4),
                **s,
            }
        )
    rows.sort(key=lambda r: (r["hard_gate_ok"], r["score"]), reverse=True)
    return rows


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
        "lane_ok_ratio",
        "reliability_norm",
        "latency_norm",
        "stream_yield_norm",
        "stability_norm",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fields})


def build_variants(phase: str) -> dict[str, dict[str, str]]:
    base = {"v1_split_defaults": {}}
    if phase in {"phase2", "phase3", "phase4", "full"}:
        base["v2_strict_throttle"] = {
            "PROWLARR_GROUP_SIZE": "1",
            "INDEXER_MANAGER_TIMEOUT": "14",
            "INDEXER_REQUEST_JITTER_MAX_MS": "180",
        }
        base["v3_dup_suppress_tight"] = {
            "LIVE_QUERY_DEDUP_TTL_S": "60",
            "PROWLARR_GROUP_SIZE": "1",
            "INDEXER_REQUEST_JITTER_MAX_MS": "220",
        }
        base["v4_cooldown_aggressive"] = {
            "INDEXER_FAIL_STREAK_THRESHOLD": "2",
            "INDEXER_COOLDOWN_SECONDS": "1200",
            "PROWLARR_GROUP_SIZE": "1",
        }
    if phase in {"phase3", "phase4", "full"}:
        base["v5_byparr_jackett"] = {"BYPARR_MODE": "jackett"}
        base["v6_byparr_prowlarr"] = {"BYPARR_MODE": "prowlarr"}
    return base


def run_variant(
    repo: Path,
    env_file: Path,
    variant: str,
    overrides: dict[str, str],
    manifest: str,
    titles: list[dict[str, str]],
    repeats: int,
    out_dir: Path,
) -> dict[str, Any]:
    vdir = out_dir / variant
    vdir.mkdir(parents=True, exist_ok=True)
    started = utc_now()

    set_env_values(env_file, overrides)
    docker_compose(
        repo,
        "up",
        "-d",
        "--build",
        "comet",
        "aiostreams",
        "jackett",
        "prowlarr",
        "flaresolverr",
        "flaresolverr-jackett",
        "adguard-proxy-helsinki",
        "adguard-proxy-jackett",
        "adguard-proxy-london",
    )
    health_check(180)
    wait_public_manifest(manifest, 120)

    lane_rows = [
        probe_proxy_lane("adguard-proxy-helsinki"),
        probe_proxy_lane("adguard-proxy-jackett"),
        probe_proxy_lane("adguard-proxy-london"),
    ]

    req_rows: list[dict[str, Any]] = []
    timeout_s = max(
        5,
        int(load_env(env_file).get("LIVE_TOTAL_BUDGET_S", "20"))
        + REQ_TIMEOUT_PADDING_S,
    )
    seq = 0
    for row in titles:
        for rep in range(1, repeats + 1):
            seq += 1
            result = request_stream(
                manifest, row["media_type"], row["imdb_id"], timeout_s=timeout_s
            )
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

    comet_log = docker_logs("comet", started)
    jackett_log = docker_logs("jackett", started)
    prowlarr_log = docker_logs("prowlarr", started)

    errors = []
    errors.extend(parse_error_events(comet_log, "comet"))
    errors.extend(parse_error_events(jackett_log, "jackett"))
    errors.extend(parse_error_events(prowlarr_log, "prowlarr"))

    write_jsonl(vdir / "requests.jsonl", req_rows)
    write_jsonl(vdir / "provider_events.jsonl", [])
    write_jsonl(vdir / "rd_cache_checks.jsonl", [])
    write_jsonl(vdir / "indexer_errors.jsonl", errors)
    write_jsonl(vdir / "proxy_lane_health.jsonl", lane_rows)
    (vdir / "comet.log").write_text(comet_log + "\n", encoding="utf-8")
    (vdir / "jackett.log").write_text(jackett_log + "\n", encoding="utf-8")
    (vdir / "prowlarr.log").write_text(prowlarr_log + "\n", encoding="utf-8")

    summary = compute_summary(req_rows, errors, lane_rows)
    (vdir / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    (vdir / "variant_overrides.json").write_text(
        json.dumps({"variant": variant, "overrides": overrides}, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="3-lane anti-bot phased investigation")
    p.add_argument("--repo", default="/home/ubuntu/aiostreams")
    p.add_argument("--env-file", default="comet-fresh.env")
    p.add_argument("--titles-file", default="test_plan/titles_20_antibot.csv")
    p.add_argument("--manifest-comet", required=True)
    p.add_argument(
        "--phase",
        choices=["phase0", "phase1", "phase2", "phase3", "phase4", "full"],
        default="phase1",
    )
    p.add_argument("--repeats", type=int, default=1)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--run-id", default="")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    repo = Path(args.repo).resolve()
    env_file = repo / args.env_file
    titles_file = repo / args.titles_file

    run_id = (
        args.run_id
        or f"antibot_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    )
    out_dir = repo / "test_runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    # snapshots
    snap_dir = out_dir / "snapshots"
    snap_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(repo / "compose.yaml", snap_dir / "compose.yaml")
    shutil.copy2(env_file, snap_dir / env_file.name)
    for f in [
        "adguard-proxy-helsinki.env",
        "adguard-proxy-jackett.env",
        "adguard-proxy-london.env",
        "aiostreams.env",
    ]:
        p = repo / f
        if p.exists():
            shutil.copy2(p, snap_dir / f)
    for p in [
        repo / "jackett/data/Jackett/ServerConfig.json",
        repo / "prowlarr/config/prowlarr.db",
    ]:
        if p.exists():
            shutil.copy2(p, snap_dir / p.name)

    rollback = []
    rollback.append("# Rollback instructions")
    rollback.append("cd /home/ubuntu/aiostreams")
    rollback.append(f"cp {snap_dir / 'compose.yaml'} compose.yaml")
    rollback.append(f"cp {snap_dir / env_file.name} {env_file.name}")
    rollback.append(
        "sudo docker compose up -d --build comet aiostreams jackett prowlarr flaresolverr flaresolverr-jackett adguard-proxy-helsinki"
    )
    (out_dir / "rollback_instructions.txt").write_text(
        "\n".join(rollback) + "\n", encoding="utf-8"
    )

    titles = load_titles(titles_file, args.limit)
    if args.phase == "phase0":
        variants = {"phase0_baseline": {}}
    else:
        variants = build_variants(args.phase)

    summaries: dict[str, dict[str, Any]] = {}

    try:
        for name, overrides in variants.items():
            print(f"== running {name} ==")
            summaries[name] = run_variant(
                repo=repo,
                env_file=env_file,
                variant=name,
                overrides=overrides,
                manifest=args.manifest_comet,
                titles=titles,
                repeats=args.repeats,
                out_dir=out_dir,
            )

        ranked = rank_variants(summaries)
        write_ranked_csv(out_dir / "ranked_variants.csv", ranked)
        (out_dir / "summary.json").write_text(
            json.dumps(
                {"run_id": run_id, "phase": args.phase, "variants": summaries}, indent=2
            )
            + "\n",
            encoding="utf-8",
        )

        rec = []
        rec.append("# Anti-Bot Recommendation")
        rec.append("")
        rec.append(f"Run: `{run_id}`")
        rec.append(f"Generated: `{utc_now()}`")
        rec.append("")
        if ranked:
            best = ranked[0]
            rec.append(f"Best variant: `{best['variant']}`")
            rec.append(f"- score: {best['score']}")
            rec.append(f"- p95_s: {best['p95_s']}")
            rec.append(f"- median_streams: {best['median_streams']}")
            rec.append(f"- error_events: {best['error_events']}")
        rec.append("")
        rec.append("Top variants:")
        for r in ranked[:5]:
            rec.append(
                f"- {r['variant']}: score={r['score']} gate={r['hard_gate_ok']} p95={r['p95_s']} median_streams={r['median_streams']} errors={r['error_events']}"
            )
        (out_dir / "recommendation.md").write_text(
            "\n".join(rec) + "\n", encoding="utf-8"
        )
    finally:
        # Always restore original runtime config after sweeps.
        shutil.copy2(snap_dir / env_file.name, env_file)
        docker_compose(
            repo,
            "up",
            "-d",
            "comet",
            "aiostreams",
            "jackett",
            "prowlarr",
            "flaresolverr",
            "flaresolverr-jackett",
            "adguard-proxy-helsinki",
            "adguard-proxy-jackett",
            "adguard-proxy-london",
        )

    print(f"run_dir={out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
