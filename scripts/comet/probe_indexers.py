#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Probe Jackett or Prowlarr indexers for one title.")
    p.add_argument("--provider", choices=["jackett", "prowlarr"], required=True)
    p.add_argument("--media-type", choices=["movie", "series", "anime"], required=True)
    p.add_argument("--imdb-id", required=True)
    p.add_argument("--title-hint", required=True)
    p.add_argument("--timeout", type=float, default=8.0)
    p.add_argument("--concurrency", type=int, default=2)
    p.add_argument("--output", required=True)
    p.add_argument("--slot", default="")
    p.add_argument("--category", default="")
    p.add_argument("--repeat-id", default="")

    p.add_argument("--jackett-url", default="http://jackett:9117")
    p.add_argument("--jackett-api-key", default="")
    p.add_argument("--jackett-indexers-dir", default="jackett/data/Jackett/Indexers")

    p.add_argument("--prowlarr-url", default="http://prowlarr:9696")
    p.add_argument("--prowlarr-api-key", default="")
    return p.parse_args()


def parse_series(imdb_id: str) -> tuple[str, str, str]:
    if ":" not in imdb_id:
        return imdb_id, "", ""
    parts = imdb_id.split(":")
    if len(parts) != 3:
        return imdb_id, "", ""
    return parts[0], parts[1], parts[2]


def http_get(url: str, timeout: float, headers: dict | None = None) -> tuple[int, str, str]:
    req = urllib.request.Request(url, headers=headers or {}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace"), ""
    except urllib.error.HTTPError as ex:
        body = ex.read().decode("utf-8", errors="replace")
        return ex.code, body, f"HTTPError:{ex.code}"
    except Exception as ex:  # noqa: BLE001
        return 0, "", f"{type(ex).__name__}:{ex}"


def load_jackett_indexers(indexers_dir: str) -> list[str]:
    out: list[str] = []
    base = Path(indexers_dir)
    if not base.exists():
        return out
    for path in sorted(base.glob("*.json")):
        if path.name.endswith(".bak"):
            continue
        try:
            json.loads(path.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            continue
        out.append(path.stem)
    return out


def probe_jackett(indexer: str, args: argparse.Namespace) -> dict:
    import time

    imdb_base, season, episode = parse_series(args.imdb_id)
    mode = (
        "movie"
        if args.media_type == "movie"
        else ("tvsearch" if args.media_type == "series" else "search")
    )
    params = {
        "apikey": args.jackett_api_key,
        "t": mode,
        "q": args.title_hint,
        "limit": "80",
        "extended": "1",
        "imdbid": imdb_base,
    }
    if season:
        params["season"] = season
    if episode:
        params["ep"] = episode

    url = f"{args.jackett_url.rstrip('/')}/api/v2.0/indexers/{urllib.parse.quote(indexer)}/results/torznab/api?{urllib.parse.urlencode(params)}"
    t0 = time.time()
    status, body, err = http_get(url, timeout=args.timeout)

    items = 0
    if status == 200 and not err:
        try:
            root = ET.fromstring(body)
            items = sum(1 for x in root.iter() if x.tag.lower().endswith("item"))
        except Exception as ex:  # noqa: BLE001
            err = f"xml_parse_error:{type(ex).__name__}"

    return {
        "provider": "jackett",
        "indexer": indexer,
        "imdb_id": args.imdb_id,
        "media_type": args.media_type,
        "slot": args.slot,
        "category": args.category,
        "repeat_id": args.repeat_id,
        "http_status": status,
        "items": items,
        "elapsed_ms": int((time.time() - t0) * 1000),
        "error": err,
        "timestamp_utc": utc_now(),
    }


def load_prowlarr_indexers(args: argparse.Namespace) -> list[dict]:
    url = f"{args.prowlarr_url.rstrip('/')}/api/v1/indexer"
    headers = {"X-Api-Key": args.prowlarr_api_key}
    status, body, err = http_get(url, timeout=args.timeout, headers=headers)
    if status != 200 or err:
        return []
    try:
        data = json.loads(body)
    except Exception:
        return []

    out: list[dict] = []
    for row in data:
        if row.get("enable"):
            out.append({"id": row.get("id"), "name": row.get("name", str(row.get("id")))})
    return out


def probe_prowlarr(row: dict, args: argparse.Namespace) -> dict:
    import time

    idx_id = row.get("id")
    idx_name = row.get("name", str(idx_id))
    imdb_base, season, episode = parse_series(args.imdb_id)

    mode = (
        "movie"
        if args.media_type == "movie"
        else ("tvsearch" if args.media_type == "series" else "search")
    )
    params = {
        "query": args.title_hint,
        "type": mode,
        "indexerIds": str(idx_id),
        "limit": "80",
        "imdbId": imdb_base,
    }
    if season:
        params["season"] = season
    if episode:
        params["episode"] = episode

    url = f"{args.prowlarr_url.rstrip('/')}/api/v1/search?{urllib.parse.urlencode(params)}"
    headers = {"X-Api-Key": args.prowlarr_api_key}

    t0 = time.time()
    status, body, err = http_get(url, timeout=args.timeout, headers=headers)
    items = 0
    if status == 200 and not err:
        try:
            data = json.loads(body)
            if isinstance(data, list):
                items = len(data)
            else:
                err = "invalid_json_shape"
        except Exception as ex:  # noqa: BLE001
            err = f"json_parse_error:{type(ex).__name__}"
    elif not err:
        err = f"http_{status}"

    return {
        "provider": "prowlarr",
        "indexer": idx_name,
        "indexer_id": idx_id,
        "imdb_id": args.imdb_id,
        "media_type": args.media_type,
        "slot": args.slot,
        "category": args.category,
        "repeat_id": args.repeat_id,
        "http_status": status,
        "items": items,
        "elapsed_ms": int((time.time() - t0) * 1000),
        "error": err,
        "timestamp_utc": utc_now(),
    }


def main() -> int:
    args = parse_args()
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)

    if args.provider == "jackett":
        units = load_jackett_indexers(args.jackett_indexers_dir)
        fn = lambda u: probe_jackett(u, args)
    else:
        units = load_prowlarr_indexers(args)
        fn = lambda u: probe_prowlarr(u, args)

    records: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as ex:
        futs = [ex.submit(fn, u) for u in units]
        for fut in concurrent.futures.as_completed(futs):
            try:
                records.append(fut.result())
            except Exception as ex2:  # noqa: BLE001
                records.append(
                    {
                        "provider": args.provider,
                        "indexer": "unknown",
                        "imdb_id": args.imdb_id,
                        "media_type": args.media_type,
                        "slot": args.slot,
                        "category": args.category,
                        "repeat_id": args.repeat_id,
                        "http_status": 0,
                        "items": 0,
                        "elapsed_ms": 0,
                        "error": f"executor_error:{type(ex2).__name__}:{ex2}",
                        "timestamp_utc": utc_now(),
                    }
                )

    with open(args.output, "a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=True) + "\n")

    ok = sum(1 for r in records if int(r.get("http_status", 0)) == 200 and not str(r.get("error", "")).strip())
    print(f"provider={args.provider} total={len(records)} ok={ok} output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
