#!/usr/bin/env python3
import argparse
import json
import statistics
import time
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urlparse

import requests


def parse_b64_from_manifest(manifest_url: str) -> Tuple[str, str]:
    u = urlparse(manifest_url)
    parts = [p for p in u.path.split('/') if p]
    if len(parts) >= 2 and parts[-1] == 'manifest.json':
        b64 = parts[-2]
        base = f"{u.scheme}://{u.netloc}"
        return base, b64
    raise ValueError('Manifest URL must end with /<b64>/manifest.json')


def load_ids(ids_file: Path) -> List[str]:
    ids = []
    for line in ids_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        ids.append(line)
    return ids


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    idx = int(round((len(values) - 1) * p))
    return values[idx]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--manifest-url', required=True)
    ap.add_argument('--ids-file', required=True)
    ap.add_argument('--media-type', default='movie', choices=['movie', 'series'])
    ap.add_argument('--timeout', type=float, default=40.0)
    ap.add_argument('--out-jsonl', default='test_runs/live_reliability_benchmark.jsonl')
    args = ap.parse_args()

    base, b64 = parse_b64_from_manifest(args.manifest_url)
    ids = load_ids(Path(args.ids_file))
    out_path = Path(args.out_jsonl)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    with out_path.open('w') as out:
        for media_id in ids:
            url = f"{base}/{b64}/stream/{args.media_type}/{media_id}.json"
            t0 = time.perf_counter()
            err = ''
            streams_count = 0
            status = 0
            try:
                resp = requests.get(url, timeout=args.timeout)
                status = resp.status_code
                payload = resp.json() if resp.headers.get('content-type', '').startswith('application/json') else {}
                streams_count = len(payload.get('streams', [])) if isinstance(payload, dict) else 0
            except Exception as e:
                err = str(e)
            elapsed = time.perf_counter() - t0

            row = {
                'media_id': media_id,
                'status': status,
                'elapsed_s': round(elapsed, 3),
                'streams': streams_count,
                'error': err,
            }
            rows.append(row)
            out.write(json.dumps(row) + '\n')
            print(f"{media_id}: {status} {elapsed:.2f}s streams={streams_count}{' err='+err if err else ''}")

    ok = [r for r in rows if r['status'] == 200 and not r['error']]
    lat = [r['elapsed_s'] for r in ok]
    zero = [r for r in ok if r['streams'] == 0]

    summary = {
        'total': len(rows),
        'ok': len(ok),
        'p50_s': round(statistics.median(lat), 3) if lat else 0.0,
        'p95_s': round(percentile(lat, 0.95), 3) if lat else 0.0,
        'p99_s': round(percentile(lat, 0.99), 3) if lat else 0.0,
        'zero_stream_rate': round((len(zero) / len(ok)), 4) if ok else 1.0,
        'output_jsonl': str(out_path),
    }
    print('\nSummary:')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
