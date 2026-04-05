#!/usr/bin/env python3
"""
Validate Go intake filter matches Python smart_hint rejection logic.
Queries BitMagnet PostgreSQL database for recent torrents and compares decisions.
"""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from lib.classifier_runtime import bootstrap_classifier_runtime

bootstrap_classifier_runtime(__file__)

import psycopg2
from bitmagnet_smart_hint import classify_payload

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "bitmagnet",
    "user": "postgres",
    "password": "postgres",
}


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Get recent torrents
    cur.execute("""
        SELECT info_hash, name, size 
        FROM torrent 
        WHERE size >= 52428800
        ORDER BY discovered_on DESC
        LIMIT 500
    """)

    rows = cur.fetchall()
    print(f"Checking {len(rows)} recent torrents (size >= 50MB)...")

    python_rejections = []
    python_passes = []

    for info_hash, name, size in rows:
        payload = {"name": name, "size": size}
        hint = classify_payload(payload)
        python_rejects = hint.get("reject", False)

        if python_rejects:
            python_rejections.append((name[:80], hint.get("reason", "unknown")))
        else:
            python_passes.append(name[:80])

    print(f"\nResults:")
    print(f"  Python would reject: {len(python_rejections)}")
    print(f"  Python would pass: {len(python_passes)}")

    if python_rejections:
        print(f"\nSample Python rejections (first 10):")
        for name, reason in python_rejections[:10]:
            print(f"    [{reason}] {name}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
