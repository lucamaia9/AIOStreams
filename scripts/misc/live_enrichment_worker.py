#!/usr/bin/env python3
"""
Live Enrichment Worker Daemon

This daemon runs alongside BitMagnet and enriches torrent_contents
with TMDB/IMDB IDs that were not resolved during classification.

Architecture:
- Polls Postgres for unenriched torrent_contents
- Checks canonical layer first (instant, no API)
- Batches remaining titles for TMDB resolution
- Updates Postgres torrent_contents and SQLite canonical layer

Usage:
    python3 live_enrichment_worker.py [--options]

Environment:
    SEARCH_DB_PATH: Path to SQLite search database
    POSTGRES_HOST: Postgres host (default: bitmagnet-postgres)
    TMDB_API_KEY: TMDB API key for resolution
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sqlite3
import sys
import time
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
LIB_DIR = SCRIPT_DIR / "lib"
if str(LIB_DIR) not in sys.path:
    sys.path.insert(0, str(LIB_DIR))

try:
    from canonical_resolver import (
        CanonicalLayer,
        DEFAULT_SEARCH_DB,
        normalize_title,
        extract_year,
        season_bucket_for,
    )
except ImportError:
    print(
        "ERROR: Could not import canonical_resolver. Ensure lib/canonical_resolver.py exists."
    )
    sys.exit(1)

DEFAULT_SEARCH_DB_PATH = DEFAULT_SEARCH_DB
DEFAULT_POSTGRES_HOST = "bitmagnet-postgres"
DEFAULT_BATCH_SIZE = 100
DEFAULT_BATCH_WINDOW_SECONDS = 30
DEFAULT_POLL_INTERVAL_SECONDS = 10
DEFAULT_TMDB_RATE_LIMIT_PER_SECOND = 0.5

YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")
WORD_PATTERN = re.compile(r"[^\W_]+", re.UNICODE)


@dataclass
class PendingTorrent:
    info_hash: str
    name: str
    content_type: str
    year: int | None
    season: int | None
    episode_start: int | None
    discovered_at: str


@dataclass
class ResolutionBatch:
    batch_key: str
    media_kind: str
    normalized_title: str
    year_hint: int | None
    season_bucket: str
    torrents: list[PendingTorrent]


@dataclass
class ResolutionResult:
    batch_key: str
    tmdb_id: str | None
    imdb_id: str | None
    canonical_title: str | None
    canonical_year: int | None
    confidence: str
    resolver_source: str
    matched_torrents: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live enrichment worker daemon")
    parser.add_argument("--search-db", default=str(DEFAULT_SEARCH_DB_PATH))
    parser.add_argument("--postgres-host", default=DEFAULT_POSTGRES_HOST)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--batch-window-seconds", type=int, default=DEFAULT_BATCH_WINDOW_SECONDS
    )
    parser.add_argument(
        "--poll-interval", type=int, default=DEFAULT_POLL_INTERVAL_SECONDS
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Don't write changes, just log"
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    parser.add_argument(
        "--max-batches",
        type=int,
        default=0,
        help="Max batches to process (0 = unlimited)",
    )
    return parser.parse_args()


class PostgresConnection:
    """Simple async Postgres connection via subprocess (avoiding asyncpg dependency)."""

    def __init__(self, host: str, container: str = "bitmagnet-postgres"):
        self.host = host
        self.container = container

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        """Execute a query and return results as list of dicts."""
        import subprocess

        cmd = [
            "sudo",
            "docker",
            "exec",
            "-i",
            self.container,
            "psql",
            "-U",
            "postgres",
            "-d",
            "bitmagnet",
            "-qt",
            "-c",
            sql,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Postgres query failed: {result.stderr}")

        rows = []
        lines = result.stdout.strip().split("\n")
        for line in lines:
            if line.strip():
                values = [v.strip() for v in line.split("|")]
                if values and any(v for v in values if v):
                    rows.append(values)

        return rows

    def execute(self, sql: str) -> None:
        """Execute a query without returning results."""
        import subprocess

        cmd = [
            "sudo",
            "docker",
            "exec",
            "-i",
            self.container,
            "psql",
            "-U",
            "postgres",
            "-d",
            "bitmagnet",
            "-c",
            sql,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Postgres execute failed: {result.stderr}")


class TMDBResolver:
    """Rate-limited TMDB API resolver."""

    def __init__(
        self,
        api_key: str | None = None,
        rate_limit: float = DEFAULT_TMDB_RATE_LIMIT_PER_SECOND,
    ):
        self.api_key = api_key or os.environ.get("TMDB_API_KEY", "")
        self.rate_limit = rate_limit
        self._last_request_time = 0.0
        self._session = None

    async def _wait_for_rate_limit(self):
        """Wait before next API call to respect rate limit."""
        elapsed = time.time() - self._last_request_time
        wait_time = (1.0 / self.rate_limit) - elapsed
        if wait_time > 0:
            await asyncio.sleep(wait_time)

    async def search_movie(self, title: str, year: int | None = None) -> dict | None:
        """Search TMDB for a movie."""
        if not self.api_key:
            return None

        await self._wait_for_rate_limit()
        self._last_request_time = time.time()

        import aiohttp

        url = "https://api.themoviedb.org/3/search/movie"
        params = {"api_key": self.api_key, "query": title}
        if year:
            params["year"] = str(year)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, params=params, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    results = data.get("results", [])
                    if not results:
                        return None
                    best = results[0]
                    return {
                        "tmdb_id": str(best.get("id")),
                        "title": best.get("title"),
                        "year": int(best.get("release_date", "0000")[:4])
                        if best.get("release_date")
                        else None,
                        "popularity": float(best.get("popularity", 0)),
                    }
        except Exception:
            return None

    async def search_tv(self, title: str, year: int | None = None) -> dict | None:
        """Search TMDB for a TV show."""
        if not self.api_key:
            return None

        await self._wait_for_rate_limit()
        self._last_request_time = time.time()

        import aiohttp

        url = "https://api.themoviedb.org/3/search/tv"
        params = {"api_key": self.api_key, "query": title}
        if year:
            params["first_air_date_year"] = str(year)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, params=params, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    results = data.get("results", [])
                    if not results:
                        return None
                    best = results[0]
                    return {
                        "tmdb_id": str(best.get("id")),
                        "title": best.get("name"),
                        "year": int(best.get("first_air_date", "0000")[:4])
                        if best.get("first_air_date")
                        else None,
                        "popularity": float(best.get("popularity", 0)),
                    }
        except Exception:
            return None

    async def get_imdb_id(self, media_type: str, tmdb_id: str) -> str | None:
        """Get IMDB ID from TMDB ID."""
        if not self.api_key:
            return None

        await self._wait_for_rate_limit()
        self._last_request_time = time.time()

        import aiohttp

        endpoint = "movie" if media_type == "movie" else "tv"
        url = f"https://api.themoviedb.org/3/{endpoint}/{tmdb_id}/external_ids"
        params = {"api_key": self.api_key}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, params=params, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    return data.get("imdb_id")
        except Exception:
            return None


class EnrichmentWorker:
    """Main enrichment worker class."""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.canonical_layer = CanonicalLayer(args.search_db)
        self.postgres = PostgresConnection(args.postgres_host)
        self.tmdb_resolver = TMDBResolver()
        self.stats = {
            "polls": 0,
            "pending_found": 0,
            "canonical_hits": 0,
            "tmdb_resolutions": 0,
            "postgres_updates": 0,
            "canonical_writes": 0,
        }

    def fetch_pending_torrents(self, limit: int) -> list[PendingTorrent]:
        """Fetch unenriched torrents from Postgres."""
        sql = f"""
            SELECT 
                encode(tc.info_hash, 'hex') as info_hash,
                t.name,
                tc.content_type,
                tc.episodes::text as episodes_json
            FROM torrent_contents tc
            JOIN torrents t ON t.info_hash = tc.info_hash
            WHERE tc.content_type IN ('movie', 'tv_show')
              AND (tc.content_source IS NULL OR tc.content_id IS NULL)
            ORDER BY t.created_at DESC
            LIMIT {limit}
        """

        rows = self.postgres.query(sql)
        pending = []

        for row in rows:
            if len(row) < 4:
                continue

            info_hash = str(row[0]).lower() if row[0] else None
            name = str(row[1]) if row[1] else ""
            content_type = str(row[2]) if row[2] else None
            episodes_json = row[3]

            if not info_hash or not name or not content_type:
                continue

            year = extract_year(name)
            season = None
            episode_start = None

            if episodes_json:
                try:
                    episodes = (
                        json.loads(episodes_json) if episodes_json != "null" else {}
                    )
                    if isinstance(episodes, dict):
                        seasons = [int(s) for s in episodes.keys() if s.isdigit()]
                        if seasons:
                            season = min(seasons)
                            ep_keys = episodes.get(str(season), {})
                            if isinstance(ep_keys, dict):
                                ep_nums = [
                                    int(e) for e in ep_keys.keys() if e.isdigit()
                                ]
                                if ep_nums:
                                    episode_start = min(ep_nums)
                except (json.JSONDecodeError, ValueError):
                    pass

            pending.append(
                PendingTorrent(
                    info_hash=info_hash,
                    name=name,
                    content_type=content_type,
                    year=year,
                    season=season,
                    episode_start=episode_start,
                    discovered_at="",
                )
            )

        return pending

    def try_canonical_layer(
        self, torrents: list[PendingTorrent]
    ) -> tuple[list[ResolutionResult], list[PendingTorrent]]:
        """Try to resolve torrents from canonical layer."""
        results = []
        remaining = []

        for t in torrents:
            content_class = "movie" if t.content_type == "movie" else "episode"
            match = self.canonical_layer.lookup(
                title=t.name,
                year=t.year,
                content_class=content_class,
                season=t.season,
            )

            if match.matched and match.tmdb_id:
                results.append(
                    ResolutionResult(
                        batch_key=f"{t.info_hash}:canonical",
                        tmdb_id=match.tmdb_id,
                        imdb_id=match.imdb_id,
                        canonical_title=match.canonical_title,
                        canonical_year=match.canonical_year,
                        confidence=match.confidence or "exact",
                        resolver_source="canonical_layer",
                        matched_torrents=[t.info_hash],
                    )
                )
            else:
                remaining.append(t)

        return results, remaining

    def create_batches(self, torrents: list[PendingTorrent]) -> list[ResolutionBatch]:
        """Group torrents into batches for TMDB resolution."""
        batches_by_key: dict[str, ResolutionBatch] = {}

        for t in torrents:
            norm = normalize_title(t.name)
            if not norm:
                continue

            media_kind = "movie" if t.content_type == "movie" else "series"
            season_bucket = season_bucket_for(t.season)

            # Create batch key from normalized title + year + season
            batch_key = f"{media_kind}|{norm}|{t.year or ''}|{season_bucket}"

            if batch_key not in batches_by_key:
                batches_by_key[batch_key] = ResolutionBatch(
                    batch_key=batch_key,
                    media_kind=media_kind,
                    normalized_title=norm,
                    year_hint=t.year,
                    season_bucket=season_bucket,
                    torrents=[],
                )

            batches_by_key[batch_key].torrents.append(t)

        return list(batches_by_key.values())

    async def resolve_batch(self, batch: ResolutionBatch) -> ResolutionResult | None:
        """Resolve a batch via TMDB API."""
        if batch.media_kind == "movie":
            result = await self.tmdb_resolver.search_movie(
                batch.normalized_title, batch.year_hint
            )
        else:
            result = await self.tmdb_resolver.search_tv(
                batch.normalized_title, batch.year_hint
            )

        if not result:
            return None

        tmdb_id = result.get("tmdb_id")
        if not tmdb_id:
            return None

        imdb_id = await self.tmdb_resolver.get_imdb_id(batch.media_kind, tmdb_id)

        return ResolutionResult(
            batch_key=batch.batch_key,
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
            canonical_title=result.get("title"),
            canonical_year=result.get("year"),
            confidence="strong" if batch.year_hint == result.get("year") else "weak",
            resolver_source="tmdb_api",
            matched_torrents=[t.info_hash for t in batch.torrents],
        )

    def update_postgres(self, results: list[ResolutionResult]) -> int:
        """Update Postgres torrent_contents with resolved IDs."""
        if self.args.dry_run:
            return len(results)

        updated = 0
        for r in results:
            if not r.tmdb_id:
                continue

            for info_hash in r.matched_torrents:
                try:
                    content_type = (
                        "tv_show" if r.batch_key.startswith("series") else "movie"
                    )

                    sql = f"""
                        INSERT INTO content (type, source, id, title, release_year, created_at, updated_at)
                        VALUES ('{content_type}', 'tmdb', '{r.tmdb_id}', '{(r.canonical_title or "").replace("'", "''")}', {r.canonical_year or "NULL"}, NOW(), NOW())
                        ON CONFLICT (type, source, id) DO NOTHING
                        """
                    self.postgres.execute(sql)

                    if r.imdb_id:
                        sql = f"""
                            INSERT INTO content_attributes (content_type, content_source, content_id, source, key, value, created_at, updated_at)
                            VALUES ('{content_type}', 'tmdb', '{r.tmdb_id}', 'imdb', 'id', '{r.imdb_id}', NOW(), NOW())
                            ON CONFLICT DO NOTHING
                            """
                        self.postgres.execute(sql)

                    sql = f"""
                        UPDATE torrent_contents
                        SET content_source = 'tmdb',
                            content_id = '{r.tmdb_id}'
                        WHERE info_hash = decode('{info_hash}', 'hex')
                        AND (content_source IS NULL OR content_id IS NULL)
                    """
                    self.postgres.execute(sql)
                    updated += 1
                except Exception as e:
                    if self.args.verbose:
                        print(f"Error updating {info_hash}: {e}")

        return updated

    def update_canonical_layer(self, results: list[ResolutionResult]) -> int:
        """Write resolved IDs to canonical layer."""
        if self.args.dry_run:
            return len(results)

        # This would write to canonical_title_family and canonical_title_variant
        # For now, we skip this as the promote script handles it
        return 0

    async def run_cycle(self) -> dict[str, int]:
        """Run one enrichment cycle."""
        cycle_stats = {
            "pending": 0,
            "canonical_hits": 0,
            "resolved": 0,
            "updated": 0,
        }

        pending = self.fetch_pending_torrents(self.args.batch_size)
        cycle_stats["pending"] = len(pending)

        if not pending:
            return cycle_stats

        if self.args.verbose:
            print(f"Found {len(pending)} pending torrents")

        canonical_results, remaining = self.try_canonical_layer(pending)
        cycle_stats["canonical_hits"] = len(canonical_results)

        if self.args.verbose and canonical_results:
            print(f"Resolved {len(canonical_results)} from canonical layer")

        if remaining:
            batches = self.create_batches(remaining)
            if self.args.verbose:
                print(f"Created {len(batches)} batches for TMDB resolution")

            tmdb_results = []
            for batch in batches[:10]:
                result = await self.resolve_batch(batch)
                if result:
                    tmdb_results.append(result)

            cycle_stats["resolved"] = len(tmdb_results)

            if self.args.verbose and tmdb_results:
                print(f"Resolved {len(tmdb_results)} via TMDB API")

            all_results = canonical_results + tmdb_results
        else:
            all_results = canonical_results

        if all_results:
            cycle_stats["updated"] = self.update_postgres(all_results)
            self.update_canonical_layer(all_results)

        return cycle_stats

    async def run(self):
        """Main worker loop."""
        print(f"Starting enrichment worker (dry_run={self.args.dry_run})")
        print(f"Canonical layer stats: {self.canonical_layer.stats()}")

        cycle_count = 0
        while True:
            cycle_count += 1
            self.stats["polls"] += 1

            if self.args.verbose:
                print(f"\n--- Cycle {cycle_count} ---")

            try:
                cycle_stats = await self.run_cycle()
                self.stats["pending_found"] += cycle_stats["pending"]
                self.stats["canonical_hits"] += cycle_stats["canonical_hits"]
                self.stats["tmdb_resolutions"] += cycle_stats["resolved"]
                self.stats["postgres_updates"] += cycle_stats["updated"]

                if self.args.verbose:
                    print(f"Cycle stats: {cycle_stats}")
                    print(f"Total stats: {self.stats}")

            except Exception as e:
                print(f"Error in cycle {cycle_count}: {e}")
                import traceback

                traceback.print_exc()

            if self.args.once:
                break

            if self.args.max_batches > 0 and cycle_count >= self.args.max_batches:
                print(f"Reached max batches ({self.args.max_batches}), stopping")
                break

            await asyncio.sleep(self.args.poll_interval)

        print(f"\nFinal stats: {self.stats}")
        self.canonical_layer.close()


def main():
    args = parse_args()
    worker = EnrichmentWorker(args)
    asyncio.run(worker.run())


if __name__ == "__main__":
    main()
