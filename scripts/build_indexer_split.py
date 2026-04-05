#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import re
import statistics
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


DEFAULT_RUNTIME_ARTIFACT = "data/comet-fresh/indexer_split/current.json"


def utc_now() -> str:
    return (
        dt.datetime.now(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build canonical provider split (providers + movies/series/anime) from overnight artifacts."
    )
    p.add_argument("--run-dir", required=True, help="Path like test_runs/overnight_YYYYmmdd_HHMMSS")
    p.add_argument("--runtime-artifact", default=DEFAULT_RUNTIME_ARTIFACT)
    p.add_argument("--owner-change-threshold", type=float, default=0.05)
    p.add_argument("--apply", action="store_true", help="Also publish runtime artifact")
    p.add_argument(
        "--backup-runtime",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="When --apply is used, write a timestamped backup of the previous runtime artifact if it exists",
    )
    p.add_argument(
        "--online-evidence",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use lightweight web evidence sampling for category hints",
    )
    return p.parse_args()


def norm(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def canonical_provider_key(name: str) -> str:
    n = norm(name)
    alias_map = {
        "thepiratebay": "piratebay",
        "kickasstorrentsto": "kickasstorrents",
        "kickasstorrentsws": "kickasstorrents",
        "torrentgalaxyclone": "torrentgalaxy",
        "torrentdownloads": "torrentdownload",
        "extratorrentst": "extratorrent",
    }
    return alias_map.get(n, n)


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


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


def load_previous_runtime(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def maybe_fetch_online_hints(provider_label: str) -> dict[str, float]:
    hints = {"movies": 0.0, "series": 0.0, "anime": 0.0}
    query = f"{provider_label} torrent indexer movies tv anime"
    url = "https://duckduckgo.com/html/?" + urllib.parse.urlencode({"q": query})
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; IndexerSplitBot/1.0)"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=6) as resp:
            body = resp.read().decode("utf-8", errors="replace").lower()[:16000]
    except Exception:
        return hints

    movie_keys = ["movie", "movies", "bluray", "webdl", "yts"]
    series_keys = ["tv", "series", "episode", "s01e", "eztv"]
    anime_keys = ["anime", "nyaa", "subsplease", "bangumi", "tokyo toshokan"]
    if any(k in body for k in movie_keys):
        hints["movies"] += 0.2
    if any(k in body for k in series_keys):
        hints["series"] += 0.2
    if any(k in body for k in anime_keys):
        hints["anime"] += 0.2
    return hints


def local_name_hints(provider_label: str) -> dict[str, float]:
    raw = provider_label.lower()
    hints = {"movies": 0.0, "series": 0.0, "anime": 0.0}
    if any(k in raw for k in ["anime", "nyaa", "subsplease", "bangumi", "tokyo", "dmhy", "mikan", "shana"]):
        hints["anime"] += 0.55
    if any(k in raw for k in ["yts", "movie", "dvdr", "bluray", "4k"]):
        hints["movies"] += 0.25
    if any(k in raw for k in ["eztv", "series", "tv", "episode"]):
        hints["series"] += 0.25
    return hints


def group_by_provider(records: list[dict[str, Any]], provider: str) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in records:
        if row.get("provider") != provider:
            continue
        idx = str(row.get("indexer", "")).strip()
        if not idx:
            continue
        pkey = canonical_provider_key(idx)
        out[pkey].append(row)
    return out


def score_service(rows: list[dict[str, Any]]) -> dict[str, float]:
    if not rows:
        return {
            "queries": 0.0,
            "success_rate": 0.0,
            "non_empty_rate": 0.0,
            "error_rate": 1.0,
            "timeout_rate": 1.0,
            "stability_score": 0.0,
            "repeat_non_empty_stddev": 1.0,
            "repeat_success_stddev": 1.0,
            "repeat_groups": 0.0,
            "p50_latency_s": 999.0,
            "p95_latency_s": 999.0,
        }
    queries = len(rows)
    ok_rows = [
        r
        for r in rows
        if int(r.get("http_status", 0)) == 200 and not str(r.get("error", "")).strip()
    ]
    success_rate = len(ok_rows) / queries
    non_empty_rate = (
        sum(1 for r in ok_rows if float(r.get("items", 0) or 0) > 0.0) / queries
    )
    error_rate = 1.0 - success_rate
    timeout_rows = [
        r
        for r in rows
        if "timeout" in str(r.get("error", "")).strip().lower()
    ]
    timeout_rate = len(timeout_rows) / queries
    lat = [float(r.get("elapsed_ms", 0) or 0) / 1000.0 for r in rows]
    p50 = percentile(lat, 0.50)
    p95 = percentile(lat, 0.95)
    per_repeat_success: dict[str, list[int]] = defaultdict(list)
    per_repeat_non_empty: dict[str, list[int]] = defaultdict(list)
    for r in rows:
        repeat_id = str(r.get("repeat_id", "")).strip() or "default"
        is_success = int(
            int(r.get("http_status", 0)) == 200 and not str(r.get("error", "")).strip()
        )
        is_non_empty = int(is_success and float(r.get("items", 0) or 0) > 0.0)
        per_repeat_success[repeat_id].append(is_success)
        per_repeat_non_empty[repeat_id].append(is_non_empty)

    repeat_success_rates: list[float] = []
    repeat_non_empty_rates: list[float] = []
    for repeat_id in sorted(per_repeat_success):
        success_samples = per_repeat_success[repeat_id]
        non_empty_samples = per_repeat_non_empty[repeat_id]
        if not success_samples or not non_empty_samples:
            continue
        repeat_success_rates.append(sum(success_samples) / len(success_samples))
        repeat_non_empty_rates.append(sum(non_empty_samples) / len(non_empty_samples))

    success_stddev = statistics.pstdev(repeat_success_rates) if len(repeat_success_rates) > 1 else 0.0
    non_empty_stddev = statistics.pstdev(repeat_non_empty_rates) if len(repeat_non_empty_rates) > 1 else 0.0
    repeat_groups = len(repeat_success_rates)
    stability_score = max(0.0, 1.0 - ((0.6 * non_empty_stddev) + (0.4 * success_stddev)))
    return {
        "queries": float(queries),
        "success_rate": success_rate,
        "non_empty_rate": non_empty_rate,
        "error_rate": error_rate,
        "timeout_rate": timeout_rate,
        "stability_score": stability_score,
        "repeat_non_empty_stddev": non_empty_stddev,
        "repeat_success_stddev": success_stddev,
        "repeat_groups": float(repeat_groups),
        "p50_latency_s": p50,
        "p95_latency_s": p95,
    }


def choose_owner(
    jackett_stats: dict[str, float],
    prowlarr_stats: dict[str, float],
    previous_owner: str | None,
    owner_change_threshold: float,
) -> tuple[str, float, dict[str, float]]:
    p95_values = [jackett_stats["p95_latency_s"], prowlarr_stats["p95_latency_s"]]
    p95_min = min(p95_values)
    p95_max = max(p95_values)

    def score(stats: dict[str, float]) -> float:
        if p95_max == p95_min:
            lat_norm = 1.0
        else:
            lat_norm = 1.0 - ((stats["p95_latency_s"] - p95_min) / (p95_max - p95_min))
            lat_norm = max(0.0, min(1.0, lat_norm))
        return (
            (0.35 * stats["success_rate"])
            + (0.25 * stats["non_empty_rate"])
            + (0.15 * lat_norm)
            + (0.15 * stats.get("stability_score", 0.0))
            + (0.10 * (1.0 - stats.get("timeout_rate", 1.0)))
        )

    j_score = score(jackett_stats)
    p_score = score(prowlarr_stats)
    winner = "jackett" if j_score >= p_score else "prowlarr"
    loser = "prowlarr" if winner == "jackett" else "jackett"
    margin = abs(j_score - p_score)

    if previous_owner in {"jackett", "prowlarr"} and margin < owner_change_threshold:
        return previous_owner, max(j_score, p_score), {
            "jackett_score": j_score,
            "prowlarr_score": p_score,
            "winner": previous_owner,
            "margin": margin,
            "sticky": 1.0,
        }
    return winner, max(j_score, p_score), {
        "jackett_score": j_score,
        "prowlarr_score": p_score,
        "winner": winner,
        "margin": margin,
        "sticky": 0.0,
        "loser": loser,
    }


def main() -> int:
    args = parse_args()
    run_dir = Path(args.run_dir)
    raw_dir = run_dir / "raw"
    split_dir = run_dir / "split"
    split_dir.mkdir(parents=True, exist_ok=True)

    jackett_rows = load_jsonl(raw_dir / "jackett_probe.jsonl")
    prowlarr_rows = load_jsonl(raw_dir / "prowlarr_probe.jsonl")

    if not jackett_rows and not prowlarr_rows:
        raise SystemExit(f"No probe artifacts found under {raw_dir}")

    prev_payload = load_previous_runtime(Path(args.runtime_artifact))
    prev_owner_map: dict[str, str] = {}
    for row in prev_payload.get("providers", []):
        if not isinstance(row, dict):
            continue
        key = canonical_provider_key(str(row.get("provider_key", "")))
        owner = str(row.get("owner", "")).strip().lower()
        if key and owner in {"jackett", "prowlarr"}:
            prev_owner_map[key] = owner

    grouped_j = group_by_provider(jackett_rows, "jackett")
    grouped_p = group_by_provider(prowlarr_rows, "prowlarr")
    provider_keys = sorted(set(grouped_j) | set(grouped_p))

    providers_out: list[dict[str, Any]] = []
    movies_out: list[dict[str, Any]] = []
    series_out: list[dict[str, Any]] = []
    anime_out: list[dict[str, Any]] = []
    evidence_rows: list[dict[str, Any]] = []
    owner_decisions: list[dict[str, Any]] = []

    for pkey in provider_keys:
        j_rows = grouped_j.get(pkey, [])
        p_rows = grouped_p.get(pkey, [])
        j_stats = score_service(j_rows)
        p_stats = score_service(p_rows)

        j_names = Counter(str(r.get("indexer", "")).strip() for r in j_rows if r.get("indexer"))
        p_names = Counter(str(r.get("indexer", "")).strip() for r in p_rows if r.get("indexer"))
        j_name = j_names.most_common(1)[0][0] if j_names else ""
        p_name = p_names.most_common(1)[0][0] if p_names else ""
        display_name = j_name or p_name or pkey

        owner, owner_score, owner_meta = choose_owner(
            j_stats,
            p_stats,
            prev_owner_map.get(pkey),
            args.owner_change_threshold,
        )

        status = "working"
        if max(j_stats["success_rate"], p_stats["success_rate"]) < 0.5:
            status = "degraded"
        if max(j_stats["non_empty_rate"], p_stats["non_empty_rate"]) == 0.0:
            status = "empty"

        content_conf = {"movies": 0.0, "series": 0.0, "anime": 0.0}
        content_reasons: dict[str, list[str]] = {"movies": [], "series": [], "anime": []}
        for rows in (j_rows, p_rows):
            by_media: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for r in rows:
                by_media[str(r.get("media_type", "")).strip().lower()].append(r)
            for media, media_rows in by_media.items():
                if media not in {"movie", "movies", "series", "anime"}:
                    continue
                mapped = "movies" if media in {"movie", "movies"} else media
                ok_rows = [
                    r
                    for r in media_rows
                    if int(r.get("http_status", 0)) == 200
                    and not str(r.get("error", "")).strip()
                    and float(r.get("items", 0) or 0) > 0.0
                ]
                media_hit_rate = (len(ok_rows) / len(media_rows)) if media_rows else 0.0
                if media_rows and media_hit_rate >= 0.05:
                    content_conf[mapped] += 0.45
                    content_reasons[mapped].append(
                        f"probe_hit_rate[{mapped}]={media_hit_rate:.3f} (>=0.05)"
                    )
                else:
                    content_reasons[mapped].append(
                        f"probe_hit_rate[{mapped}]={media_hit_rate:.3f} (<0.05)"
                    )

        online_hints = (
            maybe_fetch_online_hints(display_name) if args.online_evidence else {"movies": 0.0, "series": 0.0, "anime": 0.0}
        )
        local_hints = local_name_hints(display_name)
        for c in ("movies", "series", "anime"):
            content_conf[c] += online_hints[c]
            content_conf[c] += local_hints[c]
            if online_hints[c] > 0:
                content_reasons[c].append(f"online_hint[{c}]={online_hints[c]:.2f}")
            if local_hints[c] > 0:
                content_reasons[c].append(f"name_hint[{c}]={local_hints[c]:.2f}")

        supports = {c for c, v in content_conf.items() if v >= 0.40}
        excluded = sorted(set(content_conf.keys()) - set(supports))
        if not supports:
            supports = {"movies", "series"}
            excluded = ["anime"]
            content_reasons["movies"].append("fallback_default_support")
            content_reasons["series"].append("fallback_default_support")
            content_reasons["anime"].append("excluded_by_default_fallback")

        provider_row = {
            "provider_key": pkey,
            "provider": display_name,
            "jackett_name": j_name or None,
            "prowlarr_name": p_name or None,
            "owner": owner,
            "owner_score": round(owner_score, 4),
            "status": status,
            "supports": sorted(supports),
            "stats": {
                "jackett": j_stats,
                "prowlarr": p_stats,
            },
            "decision_meta": {
                **owner_meta,
                "content_reasons": content_reasons,
                "excluded_content": excluded,
            },
        }
        providers_out.append(provider_row)
        owner_decisions.append(
            {
                "provider_key": pkey,
                "provider": display_name,
                "owner": owner,
                "owner_score": round(owner_score, 4),
                "margin": round(float(owner_meta.get("margin", 0.0)), 4),
                "sticky": int(owner_meta.get("sticky", 0.0) > 0),
                "jackett_score": round(float(owner_meta.get("jackett_score", 0.0)), 4),
                "prowlarr_score": round(float(owner_meta.get("prowlarr_score", 0.0)), 4),
                "status": status,
                "supports": ",".join(sorted(supports)),
                "excluded": ",".join(excluded),
            }
        )

        for content in sorted(supports):
            row = {
                "provider_key": pkey,
                "provider": display_name,
                "owner": owner,
                "jackett_name": j_name or None,
                "prowlarr_name": p_name or None,
                "confidence": round(content_conf[content], 4),
            }
            if content == "movies":
                movies_out.append(row)
            elif content == "series":
                series_out.append(row)
            elif content == "anime":
                anime_out.append(row)

        evidence_rows.append(
            {
                "provider_key": pkey,
                "provider": display_name,
                "online_hints": online_hints,
                "local_hints": local_hints,
                "content_confidence": content_conf,
                "owner_meta": owner_meta,
                "content_reasons": content_reasons,
                "excluded_content": excluded,
            }
        )

    providers_out.sort(key=lambda r: r["provider_key"])
    movies_out.sort(key=lambda r: (r["provider_key"], -r["confidence"]))
    series_out.sort(key=lambda r: (r["provider_key"], -r["confidence"]))
    anime_out.sort(key=lambda r: (r["provider_key"], -r["confidence"]))

    artifact = {
        "generated_at_utc": utc_now(),
        "run_id": run_dir.name,
        "scoring_version": "indexer-split-v2",
        "providers": providers_out,
        "movies": movies_out,
        "series": series_out,
        "anime": anime_out,
    }

    (split_dir / "current.json").write_text(
        json.dumps(artifact, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    (split_dir / "evidence_snapshot.json").write_text(
        json.dumps(evidence_rows, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )

    def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for row in rows:
                w.writerow({k: row.get(k, "") for k in fields})

    write_csv(
        split_dir / "providers.csv",
        providers_out,
        ["provider_key", "provider", "jackett_name", "prowlarr_name", "owner", "owner_score", "status", "supports"],
    )
    write_csv(
        split_dir / "owner_decisions.csv",
        owner_decisions,
        ["provider_key", "provider", "owner", "owner_score", "margin", "sticky", "jackett_score", "prowlarr_score", "status", "supports", "excluded"],
    )
    write_csv(
        split_dir / "movies.csv",
        movies_out,
        ["provider_key", "provider", "owner", "jackett_name", "prowlarr_name", "confidence"],
    )
    write_csv(
        split_dir / "series.csv",
        series_out,
        ["provider_key", "provider", "owner", "jackett_name", "prowlarr_name", "confidence"],
    )
    write_csv(
        split_dir / "anime.csv",
        anime_out,
        ["provider_key", "provider", "owner", "jackett_name", "prowlarr_name", "confidence"],
    )

    env_lists: dict[str, list[str]] = {
        "JACKETT_MOVIE_INDEXERS": [],
        "JACKETT_SERIES_INDEXERS": [],
        "JACKETT_ANIME_INDEXERS": [],
        "PROWLARR_MOVIE_INDEXERS": [],
        "PROWLARR_SERIES_INDEXERS": [],
        "PROWLARR_ANIME_INDEXERS": [],
    }

    provider_by_key = {row["provider_key"]: row for row in providers_out}

    def add_env(content_rows: list[dict[str, Any]], content_name: str) -> None:
        for row in content_rows:
            prow = provider_by_key.get(row["provider_key"], {})
            owner = str(prow.get("owner", "")).strip().lower()
            if owner == "jackett":
                exact = str(prow.get("jackett_name") or "").strip()
                if exact:
                    env_lists[f"JACKETT_{content_name}_INDEXERS"].append(exact)
            elif owner == "prowlarr":
                exact = str(prow.get("prowlarr_name") or "").strip()
                if exact:
                    env_lists[f"PROWLARR_{content_name}_INDEXERS"].append(exact)

    add_env(movies_out, "MOVIE")
    add_env(series_out, "SERIES")
    add_env(anime_out, "ANIME")

    lines = [
        "# Generated by build_indexer_split.py",
        f"# run_id={run_dir.name}",
    ]
    for key in sorted(env_lists.keys()):
        unique_vals = sorted(set(env_lists[key]), key=lambda x: x.lower())
        lines.append(f"{key}={json.dumps(unique_vals, ensure_ascii=True)}")
    (split_dir / "content_env_snippets.txt").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )

    changed_owners: list[str] = []
    for row in providers_out:
        old_owner = prev_owner_map.get(row["provider_key"])
        if old_owner and old_owner != row["owner"]:
            changed_owners.append(
                f"{row['provider_key']}: {old_owner} -> {row['owner']}"
            )
    report_lines = [
        "# Indexer Split Decision Report",
        "",
        f"run_dir: `{run_dir}`",
        f"generated_at_utc: `{artifact['generated_at_utc']}`",
        f"scoring_version: `{artifact['scoring_version']}`",
        f"providers_total: `{len(providers_out)}`",
        f"movies_total: `{len(movies_out)}`",
        f"series_total: `{len(series_out)}`",
        f"anime_total: `{len(anime_out)}`",
        "",
        "## Owner Changes",
    ]
    if changed_owners:
        report_lines.extend([f"- {line}" for line in changed_owners])
    else:
        report_lines.append("- none")

    report_lines.extend(
        [
            "",
            "## Health Summary",
            f"- working: {sum(1 for r in providers_out if r['status'] == 'working')}",
            f"- degraded: {sum(1 for r in providers_out if r['status'] == 'degraded')}",
            f"- empty: {sum(1 for r in providers_out if r['status'] == 'empty')}",
            "",
            "## Failure Signals",
            f"- jackett_timeout_rate_avg: {statistics.mean(r['stats']['jackett']['timeout_rate'] for r in providers_out):.3f}",
            f"- prowlarr_timeout_rate_avg: {statistics.mean(r['stats']['prowlarr']['timeout_rate'] for r in providers_out):.3f}",
            f"- jackett_success_rate_avg: {statistics.mean(r['stats']['jackett']['success_rate'] for r in providers_out):.3f}",
            f"- prowlarr_success_rate_avg: {statistics.mean(r['stats']['prowlarr']['success_rate'] for r in providers_out):.3f}",
            "",
            "## Low-Stability Providers",
        ]
    )
    low_stability_rows = sorted(
        providers_out,
        key=lambda r: min(
            float(r["stats"]["jackett"].get("stability_score", 0.0)),
            float(r["stats"]["prowlarr"].get("stability_score", 0.0)),
        ),
    )
    low_stability_any = False
    for row in low_stability_rows:
        j_stability = float(row["stats"]["jackett"].get("stability_score", 0.0))
        p_stability = float(row["stats"]["prowlarr"].get("stability_score", 0.0))
        worst = min(j_stability, p_stability)
        if worst >= 0.75:
            continue
        low_stability_any = True
        report_lines.append(
            f"- {row['provider_key']}: owner={row['owner']} stability(j={j_stability:.3f}, p={p_stability:.3f})"
        )
    if not low_stability_any:
        report_lines.append("- none")

    report_lines.extend(["", "## Content Exclusions"])
    has_exclusions = False
    for row in sorted(providers_out, key=lambda r: r["provider_key"]):
        excluded = row.get("decision_meta", {}).get("excluded_content", [])
        if not excluded:
            continue
        has_exclusions = True
        reasons = row.get("decision_meta", {}).get("content_reasons", {})
        detail = []
        for content in excluded:
            vals = reasons.get(content, [])
            detail.append(f"{content}: {'; '.join(vals) if vals else 'no signal'}")
        report_lines.append(f"- {row['provider_key']} -> {', '.join(excluded)} ({' | '.join(detail)})")
    if not has_exclusions:
        report_lines.append("- none")

    (split_dir / "decision_report.md").write_text(
        "\n".join(report_lines) + "\n", encoding="utf-8"
    )

    owner_groups: dict[str, list[str]] = {"jackett": [], "prowlarr": []}
    for row in providers_out:
        owner = str(row.get("owner", "")).strip().lower()
        if owner not in owner_groups:
            continue
        owner_groups[owner].append(str(row.get("provider_key", "")).strip())
    for key in owner_groups:
        owner_groups[key] = sorted([v for v in owner_groups[key] if v])

    def content_keys(rows: list[dict[str, Any]], owner: str) -> list[str]:
        out: list[str] = []
        for row in rows:
            if str(row.get("owner", "")).strip().lower() != owner:
                continue
            key = str(row.get("provider_key", "")).strip()
            if key:
                out.append(key)
        return sorted(set(out))

    split_summary_lines = [
        "# Indexer Split Summary",
        "",
        f"- run_id: `{run_dir.name}`",
        f"- generated_at_utc: `{artifact['generated_at_utc']}`",
        f"- providers_total: `{len(providers_out)}`",
        "",
        "## Owner Lanes",
        f"- jackett ({len(owner_groups['jackett'])}): {', '.join(owner_groups['jackett'])}",
        f"- prowlarr ({len(owner_groups['prowlarr'])}): {', '.join(owner_groups['prowlarr'])}",
        "",
        "## Content Lanes",
        f"- movies/jackett ({len(content_keys(movies_out, 'jackett'))}): {', '.join(content_keys(movies_out, 'jackett'))}",
        f"- movies/prowlarr ({len(content_keys(movies_out, 'prowlarr'))}): {', '.join(content_keys(movies_out, 'prowlarr'))}",
        f"- series/jackett ({len(content_keys(series_out, 'jackett'))}): {', '.join(content_keys(series_out, 'jackett'))}",
        f"- series/prowlarr ({len(content_keys(series_out, 'prowlarr'))}): {', '.join(content_keys(series_out, 'prowlarr'))}",
        f"- anime/jackett ({len(content_keys(anime_out, 'jackett'))}): {', '.join(content_keys(anime_out, 'jackett'))}",
        f"- anime/prowlarr ({len(content_keys(anime_out, 'prowlarr'))}): {', '.join(content_keys(anime_out, 'prowlarr'))}",
    ]
    (split_dir / "split_summary.md").write_text(
        "\n".join(split_summary_lines) + "\n",
        encoding="utf-8",
    )

    if args.apply:
        runtime_path = Path(args.runtime_artifact)
        runtime_path.parent.mkdir(parents=True, exist_ok=True)
        if args.backup_runtime and runtime_path.exists():
            backup_name = f"{runtime_path.stem}.{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S')}.bak{runtime_path.suffix}"
            backup_path = runtime_path.with_name(backup_name)
            backup_path.write_text(runtime_path.read_text(encoding="utf-8", errors="replace"), encoding="utf-8")
            print(f"runtime_backup={backup_path}")
        runtime_path.write_text(
            json.dumps(artifact, indent=2, ensure_ascii=True) + "\n",
            encoding="utf-8",
        )
        print(f"runtime_artifact={runtime_path}")

    print(f"split_dir={split_dir}")
    print(f"providers={len(providers_out)} movies={len(movies_out)} series={len(series_out)} anime={len(anime_out)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
