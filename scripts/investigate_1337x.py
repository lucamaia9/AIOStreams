#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Probe 1337x via Jackett and FlareSolverr.")
    p.add_argument("--query", action="append", required=True, help="Search query. Repeat for multiple queries.")
    p.add_argument("--jackett-url", default="http://127.0.0.1:9117")
    p.add_argument("--jackett-api-key", required=True)
    p.add_argument("--flaresolverr-url", default="http://127.0.0.1:8191/v1")
    p.add_argument("--out-dir", default="")
    p.add_argument("--timeout", type=float, default=45.0)
    return p.parse_args()


def fetch_json(url: str, timeout: float, headers: dict[str, str] | None = None) -> dict:
    req = urllib.request.Request(url, headers=headers or {}, method="GET")
    started = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        return {
            "status": resp.status,
            "elapsed_s": round(time.time() - started, 3),
            "json": json.loads(body),
        }


def post_json(url: str, payload: dict, timeout: float) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    started = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        return {
            "status": resp.status,
            "elapsed_s": round(time.time() - started, 3),
            "json": json.loads(body),
        }


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir) if args.out_dir else Path(
        f"/home/ubuntu/aiostreams/test_runs/1337x_{int(time.time())}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for query in args.query:
        row = {"query": query}
        encoded = urllib.parse.quote(query)
        jackett_url = (
            f"{args.jackett_url.rstrip('/')}/api/v2.0/indexers/1337x/results"
            f"?apikey={args.jackett_api_key}&Query={encoded}"
        )
        try:
            jackett_result = fetch_json(jackett_url, args.timeout)
            row["jackett_status"] = jackett_result["status"]
            row["jackett_elapsed_s"] = jackett_result["elapsed_s"]
            row["jackett_result_count"] = len(jackett_result["json"].get("Results", []))
            (out_dir / f"jackett_{encoded}.json").write_text(
                json.dumps(jackett_result["json"], ensure_ascii=True, indent=2) + "\n",
                encoding="utf-8",
            )
        except Exception as exc:  # noqa: BLE001
            row["jackett_error"] = str(exc)

        fs_payload = {
            "cmd": "request.get",
            "url": f"https://1337x.to/sort-search/{query}/seeders/desc/1/",
            "maxTimeout": int(args.timeout * 1000),
        }
        try:
            fs_result = post_json(args.flaresolverr_url, fs_payload, args.timeout + 10)
            row["flaresolverr_status"] = fs_result["status"]
            row["flaresolverr_elapsed_s"] = fs_result["elapsed_s"]
            row["flaresolverr_solution_status"] = fs_result["json"].get("solution", {}).get("status")
            (out_dir / f"flaresolverr_{encoded}.json").write_text(
                json.dumps(fs_result["json"], ensure_ascii=True, indent=2) + "\n",
                encoding="utf-8",
            )
        except Exception as exc:  # noqa: BLE001
            row["flaresolverr_error"] = str(exc)

        rows.append(row)

    (out_dir / "summary.json").write_text(
        json.dumps(rows, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"out_dir={out_dir}")
    print(json.dumps(rows, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
