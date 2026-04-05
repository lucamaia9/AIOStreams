#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path

SUCCESS_W = 0.40
LAT_W = 0.30
AVG_W = 0.20
PENALTY_W = 0.10

THRESH_SUCCESS = 0.70
THRESH_P95 = 6.0
THRESH_ERR = 0.20
THRESH_SCORE = 0.55


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build ranked allowlists from overnight run artifacts.")
    p.add_argument("--run-dir", required=True, help="Path like test_runs/overnight_YYYYmmdd_HHMMSS")
    return p.parse_args()


def load_jsonl(path: Path) -> list[dict]:
    rows = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


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


def build_stats(rows: list[dict], provider: str) -> list[dict]:
    grouped = defaultdict(list)
    for r in rows:
        if r.get("provider") != provider:
            continue
        idx = str(r.get("indexer", "")).strip()
        if not idx:
            continue
        grouped[idx].append(r)

    if not grouped:
        return []

    baseline_p95 = {}
    baseline_avg = {}

    out = []
    for idx, items in grouped.items():
        n = len(items)
        oks = [r for r in items if int(r.get("http_status", 0)) == 200 and not str(r.get("error", "")).strip()]
        success_rate = len(oks) / n if n else 0.0
        err_rate = 1.0 - success_rate
        lat = [float(r.get("elapsed_ms", 0) or 0) / 1000.0 for r in items]
        p95 = percentile(lat, 0.95)
        avg_items = statistics.mean([float(r.get("items", 0) or 0) for r in items]) if items else 0.0

        baseline_p95[idx] = p95
        baseline_avg[idx] = avg_items

        out.append({
            "provider": provider,
            "indexer": idx,
            "queries": n,
            "success_rate": success_rate,
            "p95_latency_s": p95,
            "avg_items": avg_items,
            "error_rate": err_rate,
            "score": 0.0,
            "recommendation": "exclude",
        })

    p95_min = min(baseline_p95.values())
    p95_max = max(baseline_p95.values())
    avg_cap = percentile(list(baseline_avg.values()), 0.90) if baseline_avg else 1.0
    if avg_cap <= 0:
        avg_cap = 1.0

    for r in out:
        success_norm = max(0.0, min(1.0, r["success_rate"]))
        if p95_max == p95_min:
            lat_norm = 1.0
        else:
            lat_norm = 1.0 - ((r["p95_latency_s"] - p95_min) / (p95_max - p95_min))
            lat_norm = max(0.0, min(1.0, lat_norm))
        avg_norm = max(0.0, min(1.0, r["avg_items"] / avg_cap))
        penalty = max(0.0, min(1.0, r["error_rate"]))

        score = (SUCCESS_W * success_norm) + (LAT_W * lat_norm) + (AVG_W * avg_norm) - (PENALTY_W * penalty)
        score = max(0.0, min(1.0, score))
        r["score"] = score

        if (
            r["success_rate"] >= THRESH_SUCCESS
            and r["p95_latency_s"] <= THRESH_P95
            and r["error_rate"] <= THRESH_ERR
            and r["score"] >= THRESH_SCORE
        ):
            r["recommendation"] = "include"

    out.sort(key=lambda x: (x["score"], x["success_rate"], -x["p95_latency_s"], x["avg_items"]), reverse=True)
    return out


def write_ranked_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["provider", "indexer", "queries", "success_rate", "p95_latency_s", "avg_items", "error_rate", "score", "recommendation"],
        )
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "provider": r["provider"],
                    "indexer": r["indexer"],
                    "queries": r["queries"],
                    "success_rate": f"{r['success_rate']:.4f}",
                    "p95_latency_s": f"{r['p95_latency_s']:.3f}",
                    "avg_items": f"{r['avg_items']:.3f}",
                    "error_rate": f"{r['error_rate']:.4f}",
                    "score": f"{r['score']:.4f}",
                    "recommendation": r["recommendation"],
                }
            )


def write_allowlist(path: Path, provider: str, ranked: list[dict]) -> list[str]:
    indexers = [r["indexer"] for r in ranked if r["recommendation"] == "include"]
    payload = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "provider": provider,
        "indexers": indexers,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return indexers


def load_runs_index(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def summarize_protocol(run_dir: Path, runs_index: list[dict]) -> dict:
    totals = {"runs": len(runs_index), "go": 0, "investigate": 0, "fail": 0, "streams": 0}
    for row in runs_index:
        run_id = row.get("protocol_run_id", "")
        if not run_id:
            continue
        summary = run_dir.parent / run_id / "summary.txt"
        if not summary.exists():
            continue
        kv = {}
        for line in summary.read_text(encoding="utf-8", errors="replace").splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                kv[k.strip()] = v.strip()
        score = kv.get("score", "").lower()
        if score == "go":
            totals["go"] += 1
        elif score == "fail":
            totals["fail"] += 1
        else:
            totals["investigate"] += 1
        try:
            totals["streams"] += int(kv.get("normal_streams", "0"))
        except Exception:
            pass
    return totals


def main() -> int:
    args = parse_args()
    run_dir = Path(args.run_dir)
    ranked_dir = run_dir / "ranked"
    ranked_dir.mkdir(parents=True, exist_ok=True)

    runs_index = load_runs_index(run_dir / "runs_index.csv")
    jackett_rows = load_jsonl(run_dir / "raw" / "jackett_probe.jsonl")
    prowlarr_rows = load_jsonl(run_dir / "raw" / "prowlarr_probe.jsonl")

    jackett_ranked = build_stats(jackett_rows, "jackett")
    prowlarr_ranked = build_stats(prowlarr_rows, "prowlarr")

    write_ranked_csv(ranked_dir / "jackett_ranked.csv", jackett_ranked)
    write_ranked_csv(ranked_dir / "prowlarr_ranked.csv", prowlarr_ranked)

    jackett_allow = write_allowlist(ranked_dir / "jackett_allowlist.json", "jackett", jackett_ranked)
    prowlarr_allow = write_allowlist(ranked_dir / "prowlarr_allowlist.json", "prowlarr", prowlarr_ranked)

    env_snippets = [
        "# Comet env snippets generated from overnight ranking",
        f"JACKETT_INDEXERS={json.dumps(jackett_allow, ensure_ascii=True)}",
        f"PROWLARR_INDEXERS={json.dumps(prowlarr_allow, ensure_ascii=True)}",
    ]
    (ranked_dir / "allowlists_env_snippets.txt").write_text("\n".join(env_snippets) + "\n", encoding="utf-8")

    proto = summarize_protocol(run_dir, runs_index)
    report = [
        "# Overnight Ranking Report",
        "",
        f"Run directory: `{run_dir}`",
        f"Generated: `{dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')}`",
        "",
        "## Protocol Summary",
        f"- runs: {proto['runs']}",
        f"- go: {proto['go']}",
        f"- investigate: {proto['investigate']}",
        f"- fail: {proto['fail']}",
        f"- total normal streams: {proto['streams']}",
        "",
        "## Jackett",
        f"- ranked indexers: {len(jackett_ranked)}",
        f"- included indexers: {len(jackett_allow)}",
        "",
        "## Prowlarr",
        f"- ranked indexers: {len(prowlarr_ranked)}",
        f"- included indexers: {len(prowlarr_allow)}",
        "",
        "## Top 10 Jackett",
    ]
    report.extend([f"- {r['indexer']} (score={r['score']:.3f}, success={r['success_rate']:.2f}, p95={r['p95_latency_s']:.2f}s)" for r in jackett_ranked[:10]])
    report.append("")
    report.append("## Top 10 Prowlarr")
    report.extend([f"- {r['indexer']} (score={r['score']:.3f}, success={r['success_rate']:.2f}, p95={r['p95_latency_s']:.2f}s)" for r in prowlarr_ranked[:10]])

    (ranked_dir / "ranking_report.md").write_text("\n".join(report) + "\n", encoding="utf-8")

    print(f"ranked_dir={ranked_dir}")
    print(f"jackett_included={len(jackett_allow)}")
    print(f"prowlarr_included={len(prowlarr_allow)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
