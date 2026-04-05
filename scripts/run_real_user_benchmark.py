#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import re
import statistics
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Benchmark the real Comet Stremio user path using an explicit manifest URL."
    )
    p.add_argument(
        "--manifest-url",
        default=os.environ.get("REAL_USER_MANIFEST_URL", ""),
        help="Full Comet manifest URL. Defaults to REAL_USER_MANIFEST_URL.",
    )
    p.add_argument(
        "--titles-file",
        default="test_plan/titles_20_antibot.csv",
        help="CSV corpus with columns slot,category,media_type,imdb_id,title_hint.",
    )
    p.add_argument("--repeats", type=int, default=1)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--out-dir", default="")
    p.add_argument("--tail", type=int, default=400)
    p.add_argument("--timeout", type=float, default=25.0)
    p.add_argument("--compare-summary", default="")
    return p.parse_args()


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_z(ts: dt.datetime) -> str:
    return ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def manifest_fingerprint(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
    return f"{parsed.scheme}://{parsed.netloc}#{digest}"


def fetch_json(url: str, timeout: float) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Comet-RealUser-Benchmark/1.0",
            "Accept": "application/json",
        },
        method="GET",
    )
    started = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            elapsed = time.time() - started
            return {
                "ok": True,
                "status": resp.status,
                "elapsed_s": elapsed,
                "body": body,
                "json": json.loads(body),
            }
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        elapsed = time.time() - started
        return {
            "ok": False,
            "status": exc.code,
            "elapsed_s": elapsed,
            "body": body,
            "error": f"HTTPError: {exc}",
        }
    except Exception as exc:  # noqa: BLE001
        elapsed = time.time() - started
        return {
            "ok": False,
            "status": None,
            "elapsed_s": elapsed,
            "body": "",
            "error": f"Request failed: {exc}",
        }


def build_stream_url(manifest_url: str, media_type: str, imdb_id: str) -> str:
    if not manifest_url.endswith("/manifest.json"):
        raise ValueError("Manifest URL must end with /manifest.json")
    base = manifest_url[: -len("/manifest.json")]
    if media_type == "movie":
        return f"{base}/stream/movie/{urllib.parse.quote(imdb_id)}.json"
    return f"{base}/stream/series/{urllib.parse.quote(imdb_id)}.json"


def read_titles(path: Path, limit: int) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if limit > 0:
        return rows[:limit]
    return rows


def run_cmd(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        check=False,
        text=True,
        capture_output=True,
    )


def fetch_container_logs(
    repo_dir: Path,
    service: str,
    since_ts: dt.datetime,
    tail: int,
) -> str:
    cp = run_cmd(
        [
            "docker",
            "logs",
            f"--since={iso_z(since_ts)}",
            f"--tail={tail}",
            service,
        ],
        repo_dir,
    )
    return cp.stdout + cp.stderr


def parse_request_metrics(
    comet_log: str,
    media_type: str,
    imdb_id: str,
) -> dict[str, Any]:
    lines = comet_log.splitlines()
    start_idx = None
    for i, line in enumerate(lines):
        if f"Starting search for ({imdb_id})" in line:
            start_idx = i
        elif media_type == "series" and f"Starting search for ({imdb_id})" in line:
            start_idx = i
    if start_idx is None:
        for i, line in enumerate(lines):
            if f"/stream/{media_type}/{imdb_id}.json" in line:
                start_idx = max(0, i - 120)
                break
    if start_idx is None:
        return {}

    window = lines[start_idx:]
    metrics: dict[str, Any] = {
        "jackett_raw": None,
        "prowlarr_raw": None,
        "post_filter_candidates": None,
        "rd_cached_hits": None,
        "rd_cached_total": None,
        "budget_exhausted": False,
        "early_return": False,
        "providers": [],
        "completion_reason": "",
        "summary_candidates": None,
        "summary_cached_verified": None,
        "summary_returned_streams": None,
        "request_summary": "",
    }
    providers = set()
    for line in window:
        m = re.search(r"Scraper Jackett(?: #\d+)? found (\d+) torrents", line)
        if m:
            metrics["jackett_raw"] = int(m.group(1))
            providers.add("jackett")
        m = re.search(r"Scraper Prowlarr(?: #\d+)? found (\d+) torrents", line)
        if m:
            metrics["prowlarr_raw"] = int(m.group(1))
            providers.add("prowlarr")
        m = re.search(r"Torrents after global RTN filtering: (\d+)", line)
        if m:
            metrics["post_filter_candidates"] = int(m.group(1))
        m = re.search(r"Available cached torrents on realdebrid: (\d+)/(\d+)", line)
        if m:
            metrics["rd_cached_hits"] = int(m.group(1))
            metrics["rd_cached_total"] = int(m.group(2))
        if "Live scrape budget exhausted" in line:
            metrics["budget_exhausted"] = True
        if "Live scrape returned early" in line or "Cached-only fast return:" in line:
            metrics["early_return"] = True
        if "Live request summary for " in line:
            metrics["request_summary"] = line
            summary_match = re.search(
                r"reason=(\w+)\s+providers=\[([^\]]*)\]\s+candidates=(\d+)\s+cached_verified=(\d+)\s+returned_streams=(\d+)",
                line,
            )
            if summary_match:
                metrics["completion_reason"] = summary_match.group(1)
                provider_tokens = [
                    token.strip()
                    for token in summary_match.group(2).split(",")
                    if token.strip() and token.strip() != "none"
                ]
                metrics["providers"] = sorted(set(provider_tokens))
                metrics["summary_candidates"] = int(summary_match.group(3))
                metrics["summary_cached_verified"] = int(summary_match.group(4))
                metrics["summary_returned_streams"] = int(summary_match.group(5))
        if f"/stream/{media_type}/{imdb_id}.json" in line and " - 200 - " in line:
            break
    if not metrics["providers"]:
        metrics["providers"] = sorted(providers)
    return metrics


def count_cached_streams(streams: list[dict[str, Any]]) -> int:
    count = 0
    for stream in streams:
        name = str(stream.get("name", ""))
        if "⚡" in name or "[RD" in name or "[AD" in name or "[PM" in name:
            count += 1
    return count


def summarize_requests(rows: list[dict[str, Any]]) -> dict[str, Any]:
    completed = [r for r in rows if r.get("stream_status") == 200]
    latencies = [r["stream_elapsed_s"] for r in completed]
    rd_rows = [r for r in completed if (r.get("rd_cached_hits") or 0) > 0]
    provider_mix: dict[str, int] = {}
    for row in completed:
        providers = sorted(set(row.get("providers") or []))
        key = "_".join(providers) if providers else "none"
        provider_mix[key] = provider_mix.get(key, 0) + 1
    summary = {
        "requests_total": len(rows),
        "requests_completed": len(completed),
        "median_latency_s": round(statistics.median(latencies), 3) if latencies else None,
        "p95_latency_s": round(sorted(latencies)[max(0, int(len(latencies) * 0.95) - 1)], 3) if latencies else None,
        "budget_exhausted_count": sum(1 for r in completed if r.get("budget_exhausted")),
        "early_return_count": sum(1 for r in completed if r.get("early_return")),
        "rd_cached_positive_count": len(rd_rows),
        "median_rd_cached_hits": statistics.median(
            [r.get("rd_cached_hits", 0) for r in rd_rows]
        ) if rd_rows else None,
        "median_stream_count": statistics.median(
            [r.get("streams_count", 0) for r in completed]
        ) if completed else None,
        "provider_mix": provider_mix,
    }
    return summary


def build_comparison(current: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    def delta(key: str):
        left = current.get(key)
        right = baseline.get(key)
        if left is None or right is None:
            return None
        return round(left - right, 3)

    return {
        "baseline_requests_completed": baseline.get("requests_completed"),
        "delta_median_latency_s": delta("median_latency_s"),
        "delta_p95_latency_s": delta("p95_latency_s"),
        "delta_budget_exhausted_count": delta("budget_exhausted_count"),
        "delta_early_return_count": delta("early_return_count"),
        "delta_rd_cached_positive_count": delta("rd_cached_positive_count"),
        "delta_median_stream_count": delta("median_stream_count"),
    }


def main() -> int:
    args = parse_args()
    if not args.manifest_url:
        raise SystemExit(
            "Manifest URL required. Pass --manifest-url or set REAL_USER_MANIFEST_URL."
        )
    repo_dir = Path("/home/ubuntu/aiostreams")
    titles_path = repo_dir / args.titles_file
    if not titles_path.exists():
        raise SystemExit(f"Titles file not found: {titles_path}")

    run_id = f"realuser_{utc_now().strftime('%Y%m%d_%H%M%S')}"
    out_dir = Path(args.out_dir) if args.out_dir else repo_dir / "test_runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    req_dir = out_dir / "requests"
    req_dir.mkdir(parents=True, exist_ok=True)

    titles = read_titles(titles_path, args.limit)
    manifest_result = fetch_json(args.manifest_url, args.timeout)
    if not manifest_result["ok"]:
        raise SystemExit(f"Manifest fetch failed: {manifest_result.get('error')}")

    (out_dir / "run_meta.txt").write_text(
        "\n".join(
            [
                f"run_id={run_id}",
                f"started_utc={iso_z(utc_now())}",
                f"manifest={manifest_fingerprint(args.manifest_url)}",
                f"titles_file={args.titles_file}",
                f"repeats={args.repeats}",
                f"limit={args.limit}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest_result["json"], ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    run_cmd(["docker", "compose", "ps"], repo_dir)

    rows: list[dict[str, Any]] = []
    seq = 0
    for title in titles:
        for rep in range(1, args.repeats + 1):
            seq += 1
            imdb_id = title["imdb_id"].strip()
            media_type = title["media_type"].strip()
            stream_url = build_stream_url(args.manifest_url, media_type, imdb_id)
            since_ts = utc_now() - dt.timedelta(seconds=2)
            stream_result = fetch_json(stream_url, args.timeout)
            payload = stream_result.get("json", {}) if stream_result.get("ok") else {}
            streams = payload.get("streams", []) if isinstance(payload, dict) else []

            request_dir = req_dir / f"{seq:03d}_{imdb_id.replace(':', '_')}"
            request_dir.mkdir(parents=True, exist_ok=True)
            (request_dir / "stream_url.txt").write_text(stream_url + "\n", encoding="utf-8")
            if payload:
                (request_dir / "response.json").write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8",
                )
            elif stream_result.get("body"):
                (request_dir / "response.txt").write_text(
                    stream_result["body"], encoding="utf-8"
                )

            service_logs = {}
            for service in [
                "comet",
                "jackett",
                "prowlarr",
                "flaresolverr",
                "flaresolverr-jackett",
            ]:
                log_text = fetch_container_logs(repo_dir, service, since_ts, args.tail)
                service_logs[service] = log_text
                (request_dir / f"{service}.log").write_text(log_text, encoding="utf-8")

            metrics = parse_request_metrics(service_logs["comet"], media_type, imdb_id)
            row = {
                "seq": seq,
                "repeat": rep,
                "slot": title.get("slot", ""),
                "category": title.get("category", ""),
                "media_type": media_type,
                "imdb_id": imdb_id,
                "title_hint": title.get("title_hint", ""),
                "stream_status": stream_result.get("status"),
                "stream_elapsed_s": round(stream_result["elapsed_s"], 3),
                "streams_count": len(streams) if isinstance(streams, list) else 0,
                "cached_streams_count": count_cached_streams(streams) if isinstance(streams, list) else 0,
                **metrics,
            }
            rows.append(row)

    with (out_dir / "requests.jsonl").open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")

    summary = summarize_requests(rows)
    if args.compare_summary:
        compare_path = Path(args.compare_summary)
        if compare_path.exists():
            summary["compared_to"] = str(compare_path)
            summary["baseline"] = json.loads(compare_path.read_text(encoding="utf-8"))
            summary["comparison"] = build_comparison(summary, summary["baseline"])
    (out_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )

    report = [
        "# Real User Benchmark Report",
        "",
        f"Run: `{run_id}`",
        f"Manifest: `{manifest_fingerprint(args.manifest_url)}`",
        f"Corpus: `{args.titles_file}`",
        "",
        "## Summary",
        f"- requests_total: {summary['requests_total']}",
        f"- requests_completed: {summary['requests_completed']}",
        f"- median_latency_s: {summary['median_latency_s']}",
        f"- p95_latency_s: {summary['p95_latency_s']}",
        f"- budget_exhausted_count: {summary['budget_exhausted_count']}",
        f"- early_return_count: {summary['early_return_count']}",
        f"- rd_cached_positive_count: {summary['rd_cached_positive_count']}",
        f"- median_rd_cached_hits: {summary['median_rd_cached_hits']}",
        f"- median_stream_count: {summary['median_stream_count']}",
        "",
        "## Provider Mix",
    ]
    for provider_key, count in sorted(summary["provider_mix"].items()):
        report.append(f"- {provider_key}: {count}")
    report.append("")
    completed = [r for r in rows if r.get("stream_status") == 200]
    report.extend([
        "## Completion Reasons",
        f"- completed: {sum(1 for r in completed if r.get('completion_reason') == 'completed')}",
        f"- early_return: {sum(1 for r in completed if r.get('completion_reason') == 'early_return')}",
        f"- budget_exhausted: {sum(1 for r in completed if r.get('completion_reason') == 'budget_exhausted')}",
        "",
    ])
    if summary.get("comparison"):
        report.extend(
            [
                "## Comparison",
                f"- baseline: `{summary['compared_to']}`",
                f"- delta_median_latency_s: {summary['comparison']['delta_median_latency_s']}",
                f"- delta_p95_latency_s: {summary['comparison']['delta_p95_latency_s']}",
                f"- delta_budget_exhausted_count: {summary['comparison']['delta_budget_exhausted_count']}",
                f"- delta_early_return_count: {summary['comparison']['delta_early_return_count']}",
                f"- delta_rd_cached_positive_count: {summary['comparison']['delta_rd_cached_positive_count']}",
                f"- delta_median_stream_count: {summary['comparison']['delta_median_stream_count']}",
                "",
            ]
        )
    report.append("## Slowest Completed Requests")
    for row in sorted(completed, key=lambda r: r["stream_elapsed_s"], reverse=True)[:10]:
        report.append(
            f"- {row['imdb_id']} `{row['title_hint']}`: {row['stream_elapsed_s']}s, "
            f"streams={row['streams_count']}, rd_cached_hits={row.get('rd_cached_hits')}, "
            f"budget_exhausted={row.get('budget_exhausted')}, reason={row.get('completion_reason') or 'unknown'}, "
            f"providers={','.join(row.get('providers', [])) or 'none'}"
        )
    (out_dir / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")

    print(f"out_dir={out_dir}")
    print(f"median_latency_s={summary['median_latency_s']}")
    print(f"p95_latency_s={summary['p95_latency_s']}")
    print(f"budget_exhausted_count={summary['budget_exhausted_count']}")
    print(f"early_return_count={summary['early_return_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
