#!/usr/bin/env python3
"""
Build a local TMDB series index for fast title matching.

Downloads TMDB daily TV series export and enriches each series with:
- title, original_title, first_air_date (year)
- imdb_id, tvdb_id
- alternative titles (all countries)
- translations

Uses Radarr's CleanMovieTitle() for normalization (same as movies).

Usage:
    python build_tmdb_series_index.py [--limit N] [--concurrency N]

One-off task: Run once to build the index.
"""

import argparse
import asyncio
import gzip
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiohttp

sys.path.insert(0, str(Path(__file__).parent))
from lib.radarr_parser import clean_movie_title

TMDB_EXPORT_URL = (
    "https://files.tmdb.org/p/exports/tv_series_ids_{month:02d}_{day:02d}_{year}.json.gz"
)
TMDB_API_BASE = "https://api.themoviedb.org/3"

DEFAULT_TMDB_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJlNTkxMmVmOWFhM2IxNzg2Zjk3ZTE1NWY1YmQ3ZjY1MSIsInN1YiI6IjY1M2NjNWUyZTg5NGE2MDBmZjE2N2FmYyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.xrIXsMFJpI1o1j5g2QpQcFP1X3AfRjFA5FlBFO5Naw8"

INDEX_PATH = Path(__file__).parent.parent / "data" / "tmdb" / "series_index.sqlite3"

BATCH_SIZE = 100
PROGRESS_INTERVAL = 1000


@dataclass
class SeriesData:
    tmdb_id: int
    imdb_id: Optional[str] = None
    tvdb_id: Optional[int] = None
    title: str = ""
    original_title: Optional[str] = None
    clean_title: str = ""
    clean_original_title: str = ""
    year: Optional[int] = None
    popularity: float = 0.0
    alt_titles: list = field(default_factory=list)
    translations: list = field(default_factory=list)

    def __post_init__(self):
        if self.alt_titles is None:
            self.alt_titles = []
        if self.translations is None:
            self.translations = []


class TMDBSeriesIndexBuilder:
    def __init__(self, tmdb_token: str, concurrency: int = 40, limit: int = 0):
        self.tmdb_token = tmdb_token
        self.concurrency = concurrency
        self.limit = limit
        self.db_path = INDEX_PATH
        self.conn: Optional[sqlite3.Connection] = None
        self.stats = {
            "total": 0,
            "processed": 0,
            "success": 0,
            "skipped_adult": 0,
            "errors": 0,
        }

    def init_db(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS tmdb_series_index (
                tmdb_id INTEGER PRIMARY KEY,
                imdb_id TEXT,
                tvdb_id INTEGER,
                title TEXT NOT NULL,
                original_title TEXT,
                clean_title TEXT NOT NULL,
                clean_original_title TEXT,
                year INTEGER,
                popularity REAL DEFAULT 0,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS tmdb_series_alt_titles (
                tmdb_id INTEGER NOT NULL,
                clean_title TEXT NOT NULL,
                source TEXT DEFAULT 'alternative',
                PRIMARY KEY (tmdb_id, clean_title)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS tmdb_series_translations (
                tmdb_id INTEGER NOT NULL,
                clean_title TEXT NOT NULL,
                language TEXT NOT NULL,
                PRIMARY KEY (tmdb_id, clean_title, language)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS tmdb_series_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_series_clean_year ON tmdb_series_index(clean_title, year);
            CREATE INDEX IF NOT EXISTS idx_series_clean ON tmdb_series_index(clean_title);
            CREATE INDEX IF NOT EXISTS idx_series_imdb ON tmdb_series_index(imdb_id);
            CREATE INDEX IF NOT EXISTS idx_series_alt_clean ON tmdb_series_alt_titles(clean_title);
            CREATE INDEX IF NOT EXISTS idx_series_trans_clean ON tmdb_series_translations(clean_title);
        """)
        self.conn.commit()

    def save_meta(self, key: str, value: str):
        self.conn.execute(
            "INSERT OR REPLACE INTO tmdb_series_meta (key, value) VALUES (?, ?)",
            (key, value),
        )
        self.conn.commit()

    def load_meta(self, key: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT value FROM tmdb_series_meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def insert_series(self, series: SeriesData):
        now = datetime.utcnow().isoformat() + "Z"
        self.conn.execute(
            """
            INSERT OR REPLACE INTO tmdb_series_index
            (tmdb_id, imdb_id, tvdb_id, title, original_title, clean_title, clean_original_title, year, popularity, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                series.tmdb_id,
                series.imdb_id,
                series.tvdb_id,
                series.title,
                series.original_title,
                series.clean_title,
                series.clean_original_title,
                series.year,
                series.popularity,
                now,
            ),
        )

        for alt_clean in set(series.alt_titles):
            if alt_clean and alt_clean != series.clean_title:
                try:
                    self.conn.execute(
                        "INSERT OR IGNORE INTO tmdb_series_alt_titles (tmdb_id, clean_title, source) VALUES (?, ?, ?)",
                        (series.tmdb_id, alt_clean, "alternative"),
                    )
                except sqlite3.IntegrityError:
                    pass

        for trans_clean, lang in series.translations:
            if trans_clean and trans_clean != series.clean_title:
                try:
                    self.conn.execute(
                        "INSERT OR IGNORE INTO tmdb_series_translations (tmdb_id, clean_title, language) VALUES (?, ?, ?)",
                        (series.tmdb_id, trans_clean, lang),
                    )
                except sqlite3.IntegrityError:
                    pass

    async def fetch_series_details(
        self, session: aiohttp.ClientSession, tmdb_id: int
    ) -> Optional[SeriesData]:
        url = f"{TMDB_API_BASE}/tv/{tmdb_id}"
        params = {
            "append_to_response": "alternative_titles,translations,external_ids"
        }
        headers = {
            "Authorization": f"Bearer {self.tmdb_token}",
            "Content-Type": "application/json",
        }

        try:
            async with session.get(
                url,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return self.parse_series_data(data)
                elif resp.status == 404:
                    return None
                else:
                    text = await resp.text()
                    print(f"  Error {resp.status} for series {tmdb_id}: {text[:100]}")
                    return None
        except asyncio.TimeoutError:
            print(f"  Timeout for series {tmdb_id}")
            return None
        except Exception as e:
            print(f"  Exception for series {tmdb_id}: {e}")
            return None

    def parse_series_data(self, data: dict) -> SeriesData:
        series = SeriesData(tmdb_id=data["id"])

        series.title = data.get("name", "") or ""
        series.original_title = data.get("original_name")

        if data.get("first_air_date"):
            try:
                series.year = int(data["first_air_date"][:4])
            except (ValueError, TypeError):
                pass

        series.popularity = float(data.get("popularity", 0) or 0)

        external_ids = data.get("external_ids", {})
        series.imdb_id = external_ids.get("imdb_id")
        if external_ids.get("tvdb_id"):
            series.tvdb_id = int(external_ids["tvdb_id"])

        # Use same normalization as movies (Radarr's clean_movie_title)
        series.clean_title = clean_movie_title(series.title)
        if series.original_title:
            series.clean_original_title = clean_movie_title(series.original_title)

        alt_titles = data.get("alternative_titles", {}).get("results", [])
        for alt in alt_titles:
            alt_title = alt.get("title", "")
            if alt_title:
                clean = clean_movie_title(alt_title)
                if clean:
                    series.alt_titles.append(clean)

        translations = data.get("translations", {}).get("translations", [])
        for trans in translations:
            lang = trans.get("iso_639_1", "") or trans.get("language", "")
            trans_data = trans.get("data", {})
            trans_title = (
                trans_data.get("name", "") if isinstance(trans_data, dict) else ""
            )
            if trans_title:
                clean = clean_movie_title(trans_title)
                if clean:
                    series.translations.append((clean, lang))

        return series

    def stream_series_ids(self, export_url: str):
        import urllib.request

        print(f"Downloading: {export_url}")
        with urllib.request.urlopen(export_url, timeout=300) as response:
            with gzip.GzipFile(fileobj=response) as gz:
                for line in gz:
                    try:
                        data = json.loads(line.decode("utf-8"))
                        yield data
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue

    async def process_batch(
        self, session: aiohttp.ClientSession, ids: list[int]
    ) -> int:
        tasks = [self.fetch_series_details(session, tmdb_id) for tmdb_id in ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success = 0
        for result in results:
            if isinstance(result, SeriesData):
                self.insert_series(result)
                success += 1
            elif isinstance(result, Exception):
                self.stats["errors"] += 1

        self.conn.commit()
        return success

    async def run(self):
        self.init_db()

        now = datetime.utcnow()
        export_url = TMDB_EXPORT_URL.format(month=now.month, day=now.day, year=now.year)

        print(f"Building TMDB series index...")
        print(f"  Database: {self.db_path}")
        print(f"  Concurrency: {self.concurrency}")
        print(f"  Limit: {self.limit if self.limit else 'none'}")
        print()

        series_ids = []
        for data in self.stream_series_ids(export_url):
            if data.get("adult", False):
                self.stats["skipped_adult"] += 1
                continue

            series_ids.append(data["id"])
            self.stats["total"] += 1

            if self.limit and len(series_ids) >= self.limit:
                break

        print(f"Found {len(series_ids)} series to process")
        print(f"  Skipped (adult): {self.stats['skipped_adult']}")
        print()

        timeout = aiohttp.ClientTimeout(total=60)
        connector = aiohttp.TCPConnector(limit=self.concurrency)

        async with aiohttp.ClientSession(
            timeout=timeout, connector=connector
        ) as session:
            batch = []
            start_time = time.time()

            for i, tmdb_id in enumerate(series_ids, 1):
                batch.append(tmdb_id)

                if len(batch) >= BATCH_SIZE:
                    success = await self.process_batch(session, batch)
                    self.stats["processed"] += len(batch)
                    self.stats["success"] += success
                    batch = []

                    if self.stats["processed"] % PROGRESS_INTERVAL == 0:
                        elapsed = time.time() - start_time
                        rate = self.stats["processed"] / elapsed if elapsed > 0 else 0
                        eta = (
                            (len(series_ids) - self.stats["processed"]) / rate
                            if rate > 0
                            else 0
                        )
                        print(
                            f"  Progress: {self.stats['processed']}/{len(series_ids)} "
                            f"({100 * self.stats['processed'] / len(series_ids):.1f}%) "
                            f"rate={rate:.1f}/s eta={eta / 3600:.1f}h"
                        )

            if batch:
                success = await self.process_batch(session, batch)
                self.stats["processed"] += len(batch)
                self.stats["success"] += success

        elapsed = time.time() - start_time
        print()
        print("Build complete!")
        print(f"  Total processed: {self.stats['processed']}")
        print(f"  Successful: {self.stats['success']}")
        print(f"  Errors: {self.stats['errors']}")
        print(f"  Time: {elapsed / 60:.1f} minutes")
        print(f"  Rate: {self.stats['processed'] / elapsed:.1f} series/sec")

        self.save_meta("build_completed", datetime.utcnow().isoformat())
        self.save_meta("build_stats", json.dumps(self.stats))

        count_rows = self.conn.execute(
            "SELECT COUNT(*) FROM tmdb_series_index"
        ).fetchone()[0]
        count_alts = self.conn.execute(
            "SELECT COUNT(*) FROM tmdb_series_alt_titles"
        ).fetchone()[0]
        count_trans = self.conn.execute(
            "SELECT COUNT(*) FROM tmdb_series_translations"
        ).fetchone()[0]
        print(
            f"  Index size: {count_rows} series, {count_alts} alt titles, {count_trans} translations"
        )

        self.conn.close()


def main():
    parser = argparse.ArgumentParser(description="Build TMDB series index")
    parser.add_argument(
        "--limit", type=int, default=0, help="Limit number of series (0 = all)"
    )
    parser.add_argument(
        "--concurrency", type=int, default=40, help="API concurrency limit"
    )
    parser.add_argument("--token", type=str, default=None, help="TMDB API token")
    args = parser.parse_args()

    token = args.token or os.environ.get("TMDB_READ_ACCESS_TOKEN") or DEFAULT_TMDB_TOKEN

    builder = TMDBSeriesIndexBuilder(
        tmdb_token=token, concurrency=args.concurrency, limit=args.limit
    )

    asyncio.run(builder.run())


if __name__ == "__main__":
    main()
