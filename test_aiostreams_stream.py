#!/usr/bin/env python3
import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List


def fetch_json(url: str, timeout: float) -> Dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "AIOStreams-TestHarness/1.0",
            "Accept": "application/json",
        },
        method="GET",
    )

    start = time.time()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            elapsed_ms = int((time.time() - start) * 1000)
            return {
                "ok": True,
                "status": response.status,
                "elapsed_ms": elapsed_ms,
                "body": body,
                "json": json.loads(body),
            }
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        elapsed_ms = int((time.time() - start) * 1000)
        return {
            "ok": False,
            "status": error.code,
            "elapsed_ms": elapsed_ms,
            "body": body,
            "error": f"HTTPError: {error}",
        }
    except Exception as error:  # noqa: BLE001
        elapsed_ms = int((time.time() - start) * 1000)
        return {
            "ok": False,
            "status": None,
            "elapsed_ms": elapsed_ms,
            "body": "",
            "error": f"Request failed: {error}",
        }


def build_stream_url(manifest_url: str, media_type: str, imdb_id: str) -> str:
    if not manifest_url.endswith("/manifest.json"):
        raise ValueError("Manifest URL must end with /manifest.json")

    base = manifest_url[: -len("/manifest.json")]
    imdb_id = imdb_id.strip()

    if media_type == "movie":
        return f"{base}/stream/movie/{urllib.parse.quote(imdb_id)}.json"

    return f"{base}/stream/series/{urllib.parse.quote(imdb_id)}.json"


def short(value: Any, max_len: int = 120) -> str:
    text = str(value).replace("\n", " ").strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def print_streams(streams: List[Dict[str, Any]], max_items: int) -> None:
    print(f"streams_count={len(streams)}")
    for index, stream in enumerate(streams[:max_items], start=1):
        name = short(stream.get("name", ""), 80)
        title = short(stream.get("title", ""), 80)
        description = short(stream.get("description", ""), 100)
        url = stream.get("url") or stream.get("externalUrl") or ""
        print(f"[{index}] name={name}")
        if title:
            print(f"    title={title}")
        if description:
            print(f"    description={description}")
        if url:
            print(f"    url={short(url, 120)}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Quick regression test for AIOStreams Stremio stream responses."
    )
    parser.add_argument(
        "--manifest-url",
        default="https://mrdouglas2.duckdns.org/stremio/77299939-7aad-4662-9701-84a379347cb8/eyJpIjoiKzZyZ2lrWkdqaFh3dVRTOUJxM0pWUT09IiwiZSI6InlxbGNnRy9IZXRlVXFMV2pvL2FNUzQxQlRUc3JxekZqRkFKRjY0Zk1aRWs9IiwidCI6ImEifQ/manifest.json",
        help="Configured Stremio manifest URL",
    )
    parser.add_argument(
        "--imdb-id",
        default="tt0133093",
        help="IMDb id (e.g. tt0133093)",
    )
    parser.add_argument(
        "--media-type",
        choices=["movie", "series"],
        default="movie",
        help="Stream media type to query",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=25,
        help="Request timeout in seconds",
    )
    parser.add_argument(
        "--max-streams",
        type=int,
        default=10,
        help="Maximum number of streams to print",
    )
    parser.add_argument(
        "--save-json",
        default="",
        help="Optional path to save raw stream JSON response",
    )

    args = parser.parse_args()

    print("=== Manifest Check ===")
    manifest_result = fetch_json(args.manifest_url, args.timeout)
    print(
        f"manifest_status={manifest_result['status']} elapsed_ms={manifest_result['elapsed_ms']}"
    )

    if not manifest_result["ok"]:
        print(manifest_result.get("error", "manifest request failed"))
        if manifest_result.get("body"):
            print(short(manifest_result["body"], 220))
        return 1

    manifest = manifest_result["json"]
    addon_name = manifest.get("name", "<missing>")
    addon_version = manifest.get("version", "<missing>")
    print(f"addon_name={addon_name}")
    print(f"addon_version={addon_version}")

    stream_url = build_stream_url(args.manifest_url, args.media_type, args.imdb_id)
    print("\n=== Stream Check ===")
    print(f"stream_url={stream_url}")

    stream_result = fetch_json(stream_url, args.timeout)
    print(f"stream_status={stream_result['status']} elapsed_ms={stream_result['elapsed_ms']}")

    if not stream_result["ok"]:
        print(stream_result.get("error", "stream request failed"))
        if stream_result.get("body"):
            print(short(stream_result["body"], 220))
        return 2

    payload = stream_result["json"]
    streams = payload.get("streams", [])
    if not isinstance(streams, list):
        print("Invalid response: 'streams' is not a list")
        return 3

    if args.save_json:
        with open(args.save_json, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
        print(f"saved_json={args.save_json}")

    print_streams(streams, args.max_streams)

    return 0


if __name__ == "__main__":
    sys.exit(main())
