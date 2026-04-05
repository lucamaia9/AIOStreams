#!/usr/bin/env python3
"""
Build a local TMDB movie index for fast title matching.

Downloads TMDB daily export and enriches each movie with:
- title, original_title, year
- imdb_id
- alternative titles (all countries)
- translations

Uses Radarr's CleanMovieTitle() for normalization.

Usage:
    python build_tmdb_movie_index.py [--limit N] [--concurrency N]

One-off task: Run once to build the index, takes ~8 hours for full build.
"""

import argparse
import asyncio
import gzip
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiohttp

sys.path.insert(0, str(Path(__file__).parent))
from lib.radarr_parser import clean_movie_title

TMDB_EXPORT_URL = (
    "https://files.tmdb.org/p/exports/movie_ids_{month:02d}_{day:02d}_{year}.json.gz"
)
TMDB_API_BASE = "https://api.themoviedb.org/3"

DEFAULT_TMDB_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJlNTkxMmVmOWFhM2IxNzg2Zjk3ZTE1NWY1YmQ3ZjY1MSIsInN1YiI6IjY1M2NjNWUyZTg5NGE2MDBmZjE2N2FmYyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.xrIXsMFJpI1o1j5g2QpQcFP1X3AfRjFA5FlBFO5Naw8"

INDEX_PATH = Path(__file__).parent.parent / "data" / "tmdb" / "movie_index.sqlite3"
META_PATH = Path(__file__).parent.parent / "data" / "tmdb" / "index_meta.json"

BATCH_SIZE = 100
PROGRESS_INTERVAL = 1000


@dataclass
class MovieData:
    tmdb_id: int
    imdb_id: Optional[str] = None
    title: str = ""
    original_title: Optional[str] = None
    clean_title: str = ""
    clean_original_title: str = ""
    year: Optional[int] = None
    secondary_year: Optional[int] = None
    popularity: float = 0.0
    alt_titles: list = None
    translations: list = None

    def __post_init__(self):
        if self.alt_titles is None:
            self.alt_titles = []
        if self.translations is None:
            self.translations = []


class TMDBIndexBuilder:
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
            "skipped_video": 0,
            "errors": 0,
        }

    def init_db(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS tmdb_movie_index (
                tmdb_id INTEGER PRIMARY KEY,
                imdb_id TEXT,
                title TEXT NOT NULL,
                original_title TEXT,
                clean_title TEXT NOT NULL,
                clean_original_title TEXT,
                year INTEGER,
                secondary_year INTEGER,
                popularity REAL DEFAULT 0,
                adult INTEGER DEFAULT 0,
                updated_at TEXT
            );
            
            CREATE TABLE IF NOT EXISTS tmdb_alt_titles (
                tmdb_id INTEGER NOT NULL,
                clean_title TEXT NOT NULL,
                source TEXT DEFAULT 'alternative',
                PRIMARY KEY (tmdb_id, clean_title)
            ) WITHOUT ROWID;
            
            CREATE TABLE IF NOT EXISTS tmdb_translations (
                tmdb_id INTEGER NOT NULL,
                clean_title TEXT NOT NULL,
                language TEXT NOT NULL,
                PRIMARY KEY (tmdb_id, clean_title, language)
            ) WITHOUT ROWID;
            
            CREATE TABLE IF NOT EXISTS tmdb_index_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            
            CREATE INDEX IF NOT EXISTS idx_movie_clean_year ON tmdb_movie_index(clean_title, year);
            CREATE INDEX IF NOT EXISTS idx_movie_clean ON tmdb_movie_index(clean_title);
            CREATE INDEX IF NOT EXISTS idx_movie_imdb ON tmdb_movie_index(imdb_id);
            CREATE INDEX IF NOT EXISTS idx_alt_clean ON tmdb_alt_titles(clean_title);
            CREATE INDEX IF NOT EXISTS idx_trans_clean ON tmdb_translations(clean_title);
        """)
        self.conn.commit()

    def save_meta(self, key: str, value: str):
        self.conn.execute(
            "INSERT OR REPLACE INTO tmdb_index_meta (key, value) VALUES (?, ?)",
            (key, value),
        )
        self.conn.commit()

    def load_meta(self, key: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT value FROM tmdb_index_meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def insert_movie(self, movie: MovieData):
        now = datetime.utcnow().isoformat() + "Z"
        self.conn.execute(
            """
            INSERT OR REPLACE INTO tmdb_movie_index 
            (tmdb_id, imdb_id, title, original_title, clean_title, clean_original_title, year, secondary_year, popularity, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                movie.tmdb_id,
                movie.imdb_id,
                movie.title,
                movie.original_title,
                movie.clean_title,
                movie.clean_original_title,
                movie.year,
                movie.secondary_year,
                movie.popularity,
                now,
            ),
        )

        for alt_clean in set(movie.alt_titles):
            if alt_clean and alt_clean != movie.clean_title:
                try:
                    self.conn.execute(
                        "INSERT OR IGNORE INTO tmdb_alt_titles (tmdb_id, clean_title, source) VALUES (?, ?, ?)",
                        (movie.tmdb_id, alt_clean, "alternative"),
                    )
                except sqlite3.IntegrityError:
                    pass

        for trans_clean, lang in movie.translations:
            if trans_clean and trans_clean != movie.clean_title:
                try:
                    self.conn.execute(
                        "INSERT OR IGNORE INTO tmdb_translations (tmdb_id, clean_title, language) VALUES (?, ?, ?)",
                        (movie.tmdb_id, trans_clean, lang),
                    )
                except sqlite3.IntegrityError:
                    pass

    async def fetch_movie_details(
        self, session: aiohttp.ClientSession, tmdb_id: int
    ) -> Optional[MovieData]:
        url = f"{TMDB_API_BASE}/movie/{tmdb_id}"
        params = {
            "append_to_response": "alternative_titles,translations,external_ids,release_dates"
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
                    return self.parse_movie_data(data)
                elif resp.status == 404:
                    return None
                else:
                    text = await resp.text()
                    print(f"  Error {resp.status} for movie {tmdb_id}: {text[:100]}")
                    return None
        except asyncio.TimeoutError:
            print(f"  Timeout for movie {tmdb_id}")
            return None
        except Exception as e:
            print(f"  Exception for movie {tmdb_id}: {e}")
            return None

    def parse_movie_data(self, data: dict) -> MovieData:
        movie = MovieData(tmdb_id=data["id"])

        movie.title = data.get("title", "") or ""
        movie.original_title = data.get("original_title")

        if data.get("release_date"):
            try:
                movie.year = int(data["release_date"][:4])
            except (ValueError, TypeError):
                pass

        movie.popularity = float(data.get("popularity", 0) or 0)

        external_ids = data.get("external_ids", {})
        movie.imdb_id = external_ids.get("imdb_id")

        movie.clean_title = clean_movie_title(movie.title)
        if movie.original_title:
            movie.clean_original_title = clean_movie_title(movie.original_title)

        release_dates = data.get("release_dates", {}).get("results", [])
        years = set()
        if movie.year:
            years.add(movie.year)
        for rd in release_dates:
            for release in rd.get("release_dates", []):
                if release.get("release_date"):
                    try:
                        y = int(release["release_date"][:4])
                        if 1800 <= y <= 2100:
                            years.add(y)
                    except (ValueError, TypeError):
                        pass
        years.discard(movie.year)
        if years:
            movie.secondary_year = min(years)

        alt_titles = data.get("alternative_titles", {}).get("titles", [])
        for alt in alt_titles:
            alt_title = alt.get("title", "")
            if alt_title:
                clean = clean_movie_title(alt_title)
                if clean:
                    movie.alt_titles.append(clean)

        translations = data.get("translations", {}).get("translations", [])
        for trans in translations:
            lang = trans.get("iso_639_1", "") or trans.get("language", "")
            trans_data = trans.get("data", {})
            trans_title = (
                trans_data.get("title", "") if isinstance(trans_data, dict) else ""
            )
            if trans_title:
                clean = clean_movie_title(trans_title)
                if clean:
                    movie.translations.append((clean, lang))

        return movie

    def stream_movie_ids(self, export_url: str):
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
        tasks = [self.fetch_movie_details(session, tmdb_id) for tmdb_id in ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success = 0
        for result in results:
            if isinstance(result, MovieData):
                self.insert_movie(result)
                success += 1
            elif isinstance(result, Exception):
                self.stats["errors"] += 1

        self.conn.commit()
        return success

    async def run(self):
        self.init_db()

        now = datetime.utcnow()
        export_url = TMDB_EXPORT_URL.format(month=now.month, day=now.day, year=now.year)

        print(f"Building TMDB movie index...")
        print(f"  Database: {self.db_path}")
        print(f"  Concurrency: {self.concurrency}")
        print(f"  Limit: {self.limit if self.limit else 'none'}")
        print()

        movie_ids = []
        for data in self.stream_movie_ids(export_url):
            if data.get("adult", False):
                self.stats["skipped_adult"] += 1
                continue

            if data.get("video", False):
                self.stats["skipped_video"] += 1
                continue

            movie_ids.append(data["id"])
            self.stats["total"] += 1

            if self.limit and len(movie_ids) >= self.limit:
                break

        print(f"Found {len(movie_ids)} movies to process")
        print(f"  Skipped (adult): {self.stats['skipped_adult']}")
        print(f"  Skipped (video): {self.stats['skipped_video']}")
        print()

        timeout = aiohttp.ClientTimeout(total=60)
        connector = aiohttp.TCPConnector(limit=self.concurrency)

        async with aiohttp.ClientSession(
            timeout=timeout, connector=connector
        ) as session:
            batch = []
            start_time = time.time()

            for i, tmdb_id in enumerate(movie_ids, 1):
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
                            (len(movie_ids) - self.stats["processed"]) / rate
                            if rate > 0
                            else 0
                        )
                        print(
                            f"  Progress: {self.stats['processed']}/{len(movie_ids)} "
                            f"({100 * self.stats['processed'] / len(movie_ids):.1f}%) "
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
        print(f"  Rate: {self.stats['processed'] / elapsed:.1f} movies/sec")

        self.save_meta("build_completed", datetime.utcnow().isoformat())
        self.save_meta("build_stats", json.dumps(self.stats))

        count_rows = self.conn.execute(
            "SELECT COUNT(*) FROM tmdb_movie_index"
        ).fetchone()[0]
        count_alts = self.conn.execute(
            "SELECT COUNT(*) FROM tmdb_alt_titles"
        ).fetchone()[0]
        count_trans = self.conn.execute(
            "SELECT COUNT(*) FROM tmdb_translations"
        ).fetchone()[0]
        print(
            f"  Index size: {count_rows} movies, {count_alts} alt titles, {count_trans} translations"
        )

        self.conn.close()


def main():
    parser = argparse.ArgumentParser(description="Build TMDB movie index")
    parser.add_argument(
        "--limit", type=int, default=0, help="Limit number of movies (0 = all)"
    )
    parser.add_argument(
        "--concurrency", type=int, default=40, help="API concurrency limit"
    )
    parser.add_argument("--token", type=str, default=None, help="TMDB API token")
    args = parser.parse_args()

    token = args.token or os.environ.get("TMDB_READ_ACCESS_TOKEN") or DEFAULT_TMDB_TOKEN

    builder = TMDBIndexBuilder(
        tmdb_token=token, concurrency=args.concurrency, limit=args.limit
    )

    asyncio.run(builder.run())


if __name__ == "__main__":
    main()
