#!/usr/bin/env python3
"""
Radarr-style movie ID backfill for magnetico database.

Uses local TMDB index for fast matching, with API fallback.
Ports Radarr's exact title parsing and matching logic.

Usage:
    python resolve_movies_v2.py [--batch-size N] [--dry-run]
"""

import argparse
import asyncio
import json
import os
import re
import sqlite3
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aiohttp

sys.path.insert(0, str(Path(__file__).parent))
from lib.radarr_parser import (
    ParsedMovieInfo,
    clean_movie_title,
    generate_title_variants,
    parse_movie_title,
    parse_year_from_string,
    title_similarity,
)

DB_PATH = (
    Path(__file__).parent.parent
    / "data"
    / "comet-fresh"
    / "magnetico"
    / "active.search.sqlite3"
)
TMDB_INDEX_PATH = Path(__file__).parent.parent / "data" / "tmdb" / "movie_index.sqlite3"

TMDB_API_BASE = "https://api.themoviedb.org/3"
DEFAULT_TMDB_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJlNTkxMmVmOWFhM2IxNzg2Zjk3ZTE1NWY1YmQ3ZjY1MSIsInN1YiI6IjY1M2NjNWUyZTg5NGE2MDBmZjE2N2FmYyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.xrIXsMFJpI1o1j5g2QpQcFP1X3AfRjFA5FlBFO5Naw8"

HISTORICAL_FILTER = "1=1"

SERIES_PATTERNS = [
    re.compile(r"\b[Ss]\d{1,2}\b"),
    re.compile(r"\b[Ee]\d{1,3}\b"),
    re.compile(r"\b[Ss]eason\s*\d+", re.I),
    re.compile(r"\b[Ee][Pp]\d{1,3}\b", re.I),
    re.compile(r"\b\d{1,2}[xX]\d{1,3}\b"),
]

ANIME_PATTERNS = [
    re.compile(r"\[.*?(?:subs?|anime).*?\]", re.I),
    re.compile(r"\b(?:anidb|mal|kitsu|anilist)\b", re.I),
]


@dataclass
class MatchResult:
    status: str
    tmdb_id: Optional[str] = None
    imdb_id: Optional[str] = None
    confidence: str = "weak"
    reason: Optional[str] = None
    match_source: str = "local_index"


class TMDBIndex:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None

    def open(self):
        if not self.db_path.exists():
            raise FileNotFoundError(
                f"TMDB index not found: {self.db_path}. Run build_tmdb_movie_index.py first."
            )
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        if self.conn:
            self.conn.close()

    def find_movie(self, parsed: ParsedMovieInfo) -> MatchResult:
        """
        Find movie using Radarr's matching logic:
        1. Main title + year
        2. Original title + year
        3. Main title (no year, use popularity)
        4. Alternative titles
        """
        year = parsed.year
        candidates = []

        lookup_titles = set()
        for title in parsed.titles[:5]:
            clean = clean_movie_title(title)
            if clean:
                variants = generate_title_variants(clean)
                lookup_titles.update(variants)

        lookup_titles = list(lookup_titles)

        if year:
            for variant in lookup_titles:
                row = self.conn.execute(
                    "SELECT * FROM tmdb_movie_index WHERE clean_title = ? AND year = ? LIMIT 1",
                    (variant, year),
                ).fetchone()
                if row:
                    return MatchResult(
                        status="resolved",
                        tmdb_id=str(row["tmdb_id"]),
                        imdb_id=row["imdb_id"],
                        confidence="exact",
                        match_source="local_index",
                    )

        if year:
            for variant in lookup_titles:
                row = self.conn.execute(
                    "SELECT * FROM tmdb_movie_index WHERE clean_original_title = ? AND year = ? LIMIT 1",
                    (variant, year),
                ).fetchone()
                if row:
                    return MatchResult(
                        status="resolved",
                        tmdb_id=str(row["tmdb_id"]),
                        imdb_id=row["imdb_id"],
                        confidence="exact",
                        match_source="local_index",
                    )

        for variant in lookup_titles:
            rows = self.conn.execute(
                "SELECT * FROM tmdb_movie_index WHERE clean_title = ? ORDER BY popularity DESC LIMIT 5",
                (variant,),
            ).fetchall()
            candidates.extend(rows)

        for variant in lookup_titles:
            rows = self.conn.execute(
                "SELECT * FROM tmdb_movie_index WHERE clean_original_title = ? ORDER BY popularity DESC LIMIT 5",
                (variant,),
            ).fetchall()
            candidates.extend(rows)

        for variant in lookup_titles:
            rows = self.conn.execute(
                "SELECT m.* FROM tmdb_movie_index m "
                "JOIN tmdb_alt_titles a ON m.tmdb_id = a.tmdb_id "
                "WHERE a.clean_title = ? "
                "ORDER BY m.popularity DESC LIMIT 5",
                (variant,),
            ).fetchall()
            candidates.extend(rows)

        by_id = {r["tmdb_id"]: r for r in candidates}

        if not by_id:
            return MatchResult(status="miss", reason="no_match_in_index")

        if len(by_id) == 1:
            r = next(iter(by_id.values()))
            return MatchResult(
                status="resolved",
                tmdb_id=str(r["tmdb_id"]),
                imdb_id=r["imdb_id"],
                confidence="strong",
                match_source="local_index",
            )

        sorted_by_pop = sorted(
            by_id.values(), key=lambda r: r["popularity"] or 0, reverse=True
        )

        if year:
            year_matches = [r for r in sorted_by_pop if r["year"] == year]
            if len(year_matches) == 1:
                return MatchResult(
                    status="resolved",
                    tmdb_id=str(year_matches[0]["tmdb_id"]),
                    imdb_id=year_matches[0]["imdb_id"],
                    confidence="strong",
                    match_source="local_index",
                )

        best = sorted_by_pop[0]
        return MatchResult(
            status="resolved",
            tmdb_id=str(best["tmdb_id"]),
            imdb_id=best["imdb_id"],
            confidence="weak",
            match_source="local_index",
        )


class TMDBApi:
    def __init__(self, session: aiohttp.ClientSession, token: str):
        self.session = session
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    async def search_movie(self, query: str, year: Optional[int] = None) -> list:
        params = {"query": query}
        if year:
            params["year"] = year

        url = f"{TMDB_API_BASE}/search/movie"

        try:
            async with self.session.get(
                url,
                params=params,
                headers=self.headers,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("results", [])
        except Exception as e:
            print(f"    API search error: {e}")

        return []

    async def get_imdb_id(self, tmdb_id: int) -> Optional[str]:
        url = f"{TMDB_API_BASE}/movie/{tmdb_id}/external_ids"

        try:
            async with self.session.get(
                url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("imdb_id")
        except Exception:
            pass

        return None


def detect_misclassified(movie_key: str) -> Optional[str]:
    for pattern in SERIES_PATTERNS:
        if pattern.search(movie_key):
            return "series"

    for pattern in ANIME_PATTERNS:
        if pattern.search(movie_key):
            return "anime"

    return None


NOISE_PATTERNS = [
    re.compile(r"\[[^\]]*\]"),
    re.compile(
        r"\([^)]*(?:rus|bg|lat|dub|sub|hddvd|bluray|dvdrip|webrip|web-dl|hdtv|hdrip|audio)[^)]*\)",
        re.I,
    ),
    re.compile(
        r"\b(xvid|divx|x264|x265|h264|h265|hevc|aac|dd[\W_]?5[\W_]?1|dd5\.?1|5\.1|7\.1|lpcm|dts|ac3|mp3)\b",
        re.I,
    ),
    re.compile(
        r"\b(1080p|720p|480p|2160p|4k|uhd|hdrip|hdtv|webrip|web-dl|bluray|dvdrip|hddvd|open[\W_]?matte)\b",
        re.I,
    ),
    re.compile(r"\b\d{2,4}x\d{3,4}\b"),
    re.compile(r"\b\d{2,4}mb\b", re.I),
    re.compile(r"\b(t\d+|e\d+|s\d+)\b", re.I),
    re.compile(
        r"\b(exkinoray|exki|cinecalidad|megapeer|blitzcrieg|satanas|fgt|next|lord|hellywood|l|d)\b",
        re.I,
    ),
    re.compile(r"\b(bg[\W_]?audio|rus[\W_]?audio|lat[\W_]?audio)\b", re.I),
]

NOISE_SUFFIX = re.compile(r"\s+(d|l|to|rus|lat|bg|audio|dd[\W_]?5[\W_]?1)$", re.I)


def clean_movie_key_title(title: str) -> str:
    result = title
    for pattern in NOISE_PATTERNS:
        result = pattern.sub(" ", result)
    result = re.sub(r"[\._]", " ", result)
    result = re.sub(r"\s+", " ", result)
    result = NOISE_SUFFIX.sub("", result)
    return result.strip()


def parse_movie_key(movie_key: str) -> ParsedMovieInfo:
    if "|" in movie_key:
        parts = movie_key.split("|")
        raw_title = parts[0].strip()
        year = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
        title = clean_movie_key_title(raw_title)
        return ParsedMovieInfo(title=title, year=year, titles=[title])

    parsed = parse_movie_title(movie_key)
    if parsed:
        return parsed

    year = parse_year_from_string(movie_key)
    title = clean_movie_key_title(movie_key)
    title = re.sub(r"\b(19|20)\d{2}\b", "", title).strip()

    return ParsedMovieInfo(
        title=title or movie_key, year=year, titles=[title or movie_key]
    )


async def resolve_via_api(parsed: ParsedMovieInfo, tmdb_api: TMDBApi) -> MatchResult:
    titles = parsed.titles[:3]
    year = parsed.year

    all_candidates = []

    for title in titles:
        results = await tmdb_api.search_movie(title, year=year)

        if not results and year:
            results = await tmdb_api.search_movie(title, year=None)

        for result in results[:8]:
            score = 0.0

            candidate_title = result.get("title", "")
            similarity = title_similarity(titles[0], candidate_title)
            score += similarity * 100

            if year:
                result_year = None
                if result.get("release_date"):
                    try:
                        result_year = int(result["release_date"][:4])
                    except (ValueError, TypeError):
                        pass

                if result_year:
                    if result_year == year:
                        score += 50
                    elif abs(result_year - year) == 1:
                        score += 20
                    elif abs(result_year - year) > 1:
                        score -= 30

            score += min(result.get("popularity", 0) or 0, 20)
            all_candidates.append((score, result))

    if not all_candidates:
        return MatchResult(
            status="miss", reason="tmdb_no_results", match_source="tmdb_api"
        )

    all_candidates.sort(key=lambda x: x[0], reverse=True)

    top_score, best = all_candidates[0]

    if top_score < 50:
        return MatchResult(
            status="miss", reason="low_confidence", match_source="tmdb_api"
        )

    imdb_id = await tmdb_api.get_imdb_id(best["id"])

    return MatchResult(
        status="resolved",
        tmdb_id=str(best["id"]),
        imdb_id=imdb_id,
        confidence="strong" if top_score >= 80 else "weak",
        match_source="tmdb_api",
    )


def update_media_index(conn: sqlite3.Connection, movie_key: str, result: MatchResult):
    if result.status != "resolved":
        return

    conn.execute(
        f"""
        UPDATE media_index 
        SET tmdb_id = ?, imdb_id = ?, updated_at = ?
        WHERE movie_key = ? 
          AND {HISTORICAL_FILTER}
          AND (tmdb_id IS NULL OR tmdb_id = '')
        """,
        (
            result.tmdb_id,
            result.imdb_id,
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            movie_key,
        ),
    )


async def process_batch(
    movie_keys: list[str],
    index: TMDBIndex,
    tmdb_api: TMDBApi,
    db_conn: sqlite3.Connection,
    dry_run: bool,
    stats: Counter,
    use_api: bool = True,
):
    for movie_key in movie_keys:
        misclassified = detect_misclassified(movie_key)
        if misclassified:
            stats[f"skipped_{misclassified}"] += 1
            continue

        parsed = parse_movie_key(movie_key)

        result = index.find_movie(parsed)

        if result.status == "resolved":
            stats["resolved_local"] += 1
            if not dry_run:
                update_media_index(db_conn, movie_key, result)
        elif use_api:
            result = await resolve_via_api(parsed, tmdb_api)

            if result.status == "resolved":
                stats["resolved_api"] += 1
                if not dry_run:
                    update_media_index(db_conn, movie_key, result)
            else:
                stats["miss"] += 1
        else:
            stats["miss"] += 1

        stats["processed"] += 1


async def run(
    batch_size: int = 1000,
    dry_run: bool = False,
    limit: int = 0,
    concurrency: int = 40,
    use_api: bool = True,
):
    print("Radarr-style Movie ID Backfill")
    print("=" * 50)
    print(f"  Database: {DB_PATH}")
    print(f"  TMDB Index: {TMDB_INDEX_PATH}")
    print(f"  Batch size: {batch_size}")
    print(f"  Dry run: {dry_run}")
    print(f"  Limit: {limit if limit else 'none'}")
    print(f"  API fallback: {use_api}")
    print()

    if not TMDB_INDEX_PATH.exists():
        print(f"ERROR: TMDB index not found at {TMDB_INDEX_PATH}")
        print("Run build_tmdb_movie_index.py first!")
        return

    index = TMDBIndex(TMDB_INDEX_PATH)
    index.open()

    db_conn = sqlite3.connect(str(DB_PATH))
    db_conn.row_factory = sqlite3.Row

    count_query = f"""
        SELECT COUNT(DISTINCT movie_key) as cnt
        FROM media_index
        WHERE content_class = 'movie'
          AND movie_key IS NOT NULL AND movie_key != ''
          AND (tmdb_id IS NULL OR tmdb_id = '')
          AND {HISTORICAL_FILTER}
    """
    total_count = db_conn.execute(count_query).fetchone()["cnt"]

    print(f"Movies to process: {total_count:,}")
    print()

    query = f"""
        SELECT DISTINCT movie_key
        FROM media_index
        WHERE content_class = 'movie'
          AND movie_key IS NOT NULL AND movie_key != ''
          AND (tmdb_id IS NULL OR tmdb_id = '')
          AND {HISTORICAL_FILTER}
        ORDER BY movie_key
    """

    if limit:
        query += f" LIMIT {limit}"

    cursor = db_conn.execute(query)
    movie_keys = [row["movie_key"] for row in cursor.fetchall()]

    stats = Counter()
    start_time = time.time()

    token = os.environ.get("TMDB_READ_ACCESS_TOKEN", DEFAULT_TMDB_TOKEN)
    timeout = aiohttp.ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(limit=concurrency)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tmdb_api = TMDBApi(session, token)

        batch = []
        for i, movie_key in enumerate(movie_keys, 1):
            batch.append(movie_key)

            if len(batch) >= batch_size:
                await process_batch(
                    batch, index, tmdb_api, db_conn, dry_run, stats, use_api
                )

                if not dry_run:
                    db_conn.commit()

                elapsed = time.time() - start_time
                rate = stats["processed"] / elapsed if elapsed > 0 else 0
                print(
                    f"  Progress: {stats['processed']:,}/{len(movie_keys):,} "
                    f"({100 * stats['processed'] / len(movie_keys):.1f}%) "
                    f"local={stats['resolved_local']:,} api={stats['resolved_api']:,} "
                    f"miss={stats['miss']:,} rate={rate:.1f}/s"
                )

                batch = []

        if batch:
            await process_batch(
                batch, index, tmdb_api, db_conn, dry_run, stats, use_api
            )
            if not dry_run:
                db_conn.commit()

    elapsed = time.time() - start_time

    print()
    print("Backfill complete!")
    print(f"  Processed: {stats['processed']:,}")
    print(f"  Resolved (local): {stats['resolved_local']:,}")
    print(f"  Resolved (API): {stats['resolved_api']:,}")
    print(f"  Miss: {stats['miss']:,}")
    print(f"  Skipped (series): {stats['skipped_series']:,}")
    print(f"  Skipped (anime): {stats['skipped_anime']:,}")
    print(f"  Time: {elapsed / 60:.1f} minutes")
    print(f"  Rate: {stats['processed'] / elapsed:.1f} movies/sec")

    coverage = (
        (stats["resolved_local"] + stats["resolved_api"])
        / max(stats["processed"], 1)
        * 100
    )
    print(f"  Coverage: {coverage:.1f}%")

    index.close()
    db_conn.close()


def main():
    parser = argparse.ArgumentParser(description="Radarr-style movie ID backfill")
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for processing"
    )
    parser.add_argument("--dry-run", action="store_true", help="Don't write changes")
    parser.add_argument(
        "--limit", type=int, default=0, help="Limit number of movies (0 = all)"
    )
    parser.add_argument("--concurrency", type=int, default=40, help="API concurrency")
    parser.add_argument(
        "--no-api", action="store_true", help="Skip API fallback (local index only)"
    )
    args = parser.parse_args()

    asyncio.run(
        run(
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            limit=args.limit,
            concurrency=args.concurrency,
            use_api=not args.no_api,
        )
    )


if __name__ == "__main__":
    main()
