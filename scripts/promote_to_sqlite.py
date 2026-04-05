#!/usr/bin/env python3
"""
Legacy compatibility bridge for promoting already-accepted BitMagnet content into
SQLite `media_index`.

Called by the Go processor with full TorrentContent data via stdin.
Inserts into media_index and updates FTS.

Authoritative workflow note:
- Python owns accept/reject, categorization, canonical identity, and the
  structured fields that should survive into search.
- This script must not become a second classifier or rebuild authority.
- The raw `torrents` + `torrent_files` -> Python ->
  `promote_bitmagnet_live_to_compact.py` path is the authoritative live/rebuild
  sync path.

Usage:
    python3 promote_to_sqlite.py --stdin < payload.json
    python3 promote_to_sqlite.py --dry-run --stdin < payload.json

Input JSON format:
{
    "contents": [
        {
            "info_hash": "abc123...",
            "name": "Movie.Title.2024.1080p.mkv",
            "size": 1234567890,
            "created_at": "2024-03-30T12:00:00Z",
            "content_type": "movie",
            "content_source": "tmdb",
            "content_id": "12345",
            "episodes": {"1": {"5": {}}},
            "video_resolution": "V1080p",
            "video_source": "bluray",
            "video_codec": "x265",
            "release_year": 2024
        }
    ],
    "search_db": "/path/to/search.sqlite3"
}
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from lib.classifier_runtime import bootstrap_classifier_runtime

RUNTIME_PATHS = bootstrap_classifier_runtime(__file__)

import lean_media_contract as lean_contract
from shared_title_normalizer import normalize_for_lookup

REPO_ROOT = RUNTIME_PATHS.repo_root


def _get_default_search_db() -> Path:
    """Get default search DB path from environment or repo."""
    env_path = os.environ.get("SEARCH_DB_PATH") or os.environ.get("MAGNETICO_SEARCH_DB")
    if env_path:
        return Path(env_path)
    return REPO_ROOT / "data" / "comet-fresh" / "magnetico" / "active.search.sqlite3"


DEFAULT_SEARCH_DB = str(_get_default_search_db())

WORD_PATTERN = re.compile(r"[^\W_]+", re.UNICODE)
YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")

ANIME_PATTERNS = [
    re.compile(
        r"\b(?:horriblesubs|subsplease|erai-raws|commie|dmonhiro|asenshi|judgment|puyero|fff|doki|koikoi|reinforce|ohys-raws|leopard-raws|ohys)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\[.*?\].*?(?:1080p|720p|480p)", re.IGNORECASE),
    re.compile(r"\b(?:cr|funimation|hidive)\b.*?(?:web|dl|rip)", re.IGNORECASE),
]


def normalize_title(text: str) -> str:
    """Normalize a title for matching. Delegates to shared normalizer."""
    return normalize_for_lookup(text)


def title_key_for(text: str, normalized: str | None = None) -> str:
    """Generate a title key (first 12 tokens)."""
    norm = normalized if normalized else normalize_title(text)
    tokens = [t for t in norm.split() if t]
    return " ".join(tokens[:12])


def movie_key_for(title: str, year: int | None = None) -> str:
    """Generate a movie key for deduplication."""
    tk = title_key_for(title)
    if year is None:
        return tk
    return f"{tk}|{int(year)}"


def series_key_for(title: str) -> str:
    """Generate a series key for deduplication."""
    return title_key_for(title)


def exact_episode_key_for(
    title: str, season: int | None, episode: int | None
) -> str | None:
    """Generate an exact episode key."""
    if season is None or episode is None:
        return None
    return f"{series_key_for(title)}|s{int(season):02d}e{int(episode):03d}"


def season_pack_key_for(title: str, season: int | None) -> str | None:
    """Generate a season pack key."""
    if season is None:
        return None
    return f"{series_key_for(title)}|s{int(season):02d}"


def detect_anime(name: str) -> int:
    """Detect if content is anime based on name patterns."""
    name_lower = name.lower()
    for pattern in ANIME_PATTERNS:
        if pattern.search(name):
            return 1
    return 0


def parse_episodes(
    episodes: dict | None,
) -> tuple[int | None, int | None, int | None, int]:
    """
    Parse episodes JSONB to (season, episode_start, episode_end, episode_count).

    Input examples:
        {"1": {"5": {}}} -> season=1, episode_start=5, episode_end=5
        {"1": {"1": {}, "2": {}, "3": {}}} -> season=1, episode_start=1, episode_end=3
    """
    if not episodes:
        return None, None, None, 0

    seasons = list(episodes.keys())
    if len(seasons) != 1:
        return None, None, None, 0

    try:
        season = int(seasons[0])
    except (ValueError, TypeError):
        return None, None, None, 0

    episode_nums = []
    for k in episodes[seasons[0]].keys():
        try:
            episode_nums.append(int(k))
        except (ValueError, TypeError):
            continue

    if not episode_nums:
        return season, None, None, 0

    episode_start = min(episode_nums)
    episode_end = max(episode_nums)
    episode_count = len(episode_nums)

    return season, episode_start, episode_end, episode_count


def derive_content_class(content_type: str, episodes: dict | None) -> str:
    """Derive SQLite content_class from BitMagnet content_type and episodes."""
    if content_type == "movie":
        return "movie"

    if content_type == "tv_show":
        if not episodes:
            return "unknown_video"

        seasons = list(episodes.keys())
        if len(seasons) != 1:
            return "season_pack"

        ep_count = len(episodes[seasons[0]])
        if ep_count == 1:
            return "episode"
        elif ep_count <= 10:
            return "multi_episode"
        else:
            return "season_pack"

    return "unknown_video"


def compute_media_score(size: int, resolution: str | None, content_class: str) -> float:
    """Compute a media quality score (0-1)."""
    score = 0.5

    if resolution:
        res = resolution.lower().lstrip("v")
        res_scores = {
            "4320p": 1.0,
            "2160p": 0.95,
            "1440p": 0.8,
            "1080p": 0.7,
            "720p": 0.5,
            "576p": 0.4,
            "480p": 0.3,
            "360p": 0.2,
        }
        score = max(score, res_scores.get(res, 0.5))

    if content_class == "movie":
        score *= 1.0
    elif content_class in ("episode", "anime_episode"):
        score *= 0.95
    elif content_class in ("multi_episode", "season_pack"):
        score *= 0.9

    return min(1.0, max(0.1, score))


def parse_timestamp(ts: str) -> int:
    """Parse ISO timestamp to Unix epoch."""
    if not ts:
        return int(datetime.now().timestamp())

    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(ts, fmt)
            return int(dt.timestamp())
        except ValueError:
            continue

    return int(datetime.now().timestamp())


def promote_batch(
    db_path: str, contents: list[dict], dry_run: bool = False
) -> dict[str, Any]:
    """
    Promote a batch of contents to SQLite media_index.

    Returns dict with 'inserted', 'skipped', 'errors' counts.
    """
    result = {"inserted": 0, "skipped": 0, "errors": 0, "error_details": []}

    if not contents:
        return result

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception as e:
        result["errors"] = len(contents)
        result["error_details"].append(f"Failed to connect to database: {e}")
        return result

    try:
        max_id_row = conn.execute(
            "SELECT COALESCE(MAX(torrent_id), 0) FROM media_index"
        ).fetchone()
        next_id = int(max_id_row[0]) + 1 if max_id_row else 1

        for tc in contents:
            try:
                info_hash = tc.get("info_hash", "").lower()
                if not info_hash:
                    result["skipped"] += 1
                    continue

                existing = conn.execute(
                    "SELECT 1 FROM media_index WHERE info_hash = ?", (info_hash,)
                ).fetchone()
                if existing:
                    result["skipped"] += 1
                    continue

                name = tc.get("name", "")
                size = int(tc.get("size", 0) or 0)
                created_at = tc.get("created_at", tc.get("published_at", ""))
                discovered_on = parse_timestamp(created_at)

                content_type = tc.get("content_type", "")
                episodes = tc.get("episodes")
                season, ep_start, ep_end, ep_count = parse_episodes(episodes)
                content_class = derive_content_class(content_type, episodes)

                year = tc.get("release_year")
                if year:
                    year = int(year)

                content_source = tc.get("content_source", "")
                content_id = tc.get("content_id", "")

                tmdb_id = content_id if content_source == "tmdb" else None
                imdb_id = content_id if content_source == "imdb" else None

                normalized = normalize_title(name)
                title_key = title_key_for(name, normalized)

                is_anime = detect_anime(name)

                resolution = tc.get("video_resolution", "")
                if resolution:
                    resolution = resolution.lstrip("V")

                source_hint = tc.get("video_source", "")
                codec_hint = tc.get("video_codec", "")

                has_exact_episode = 1 if ep_start and ep_start == ep_end else 0
                is_multi_episode = (
                    1
                    if ep_start and ep_end and ep_start != ep_end and ep_count <= 10
                    else 0
                )
                is_season_pack = 1 if ep_count > 10 else 0

                media_score = compute_media_score(size, resolution, content_class)

                canonical_title_key = ""
                movie_key = (
                    movie_key_for(name, year) if content_class == "movie" else ""
                )
                series_key = (
                    series_key_for(name)
                    if content_class
                    in (
                        "episode",
                        "multi_episode",
                        "season_pack",
                        "anime_episode",
                        "anime_pack",
                    )
                    else ""
                )
                exact_episode_key = (
                    exact_episode_key_for(name, season, ep_start)
                    if content_class in ("episode", "anime_episode")
                    else None
                )
                season_pack_key = (
                    season_pack_key_for(name, season)
                    if content_class in ("season_pack", "anime_pack")
                    else None
                )

                torrent_id = next_id
                next_id += 1

                if dry_run:
                    result["inserted"] += 1
                    continue

                row_payload = {
                    "norm_title": normalized,
                    "name": name,
                    "canonical_title_key": canonical_title_key,
                    "movie_key": movie_key,
                    "series_key": series_key,
                    "aliases_text": "",
                    "imdb_id": imdb_id,
                    "tmdb_id": tmdb_id,
                    "year": year,
                    "season": season,
                    "episode_start": ep_start,
                    "episode_end": ep_end,
                    "exact_episode_key": exact_episode_key,
                    "season_pack_key": season_pack_key,
                }
                search_text = lean_contract.search_text_for(row_payload)

                conn.execute(
                    """
                    INSERT INTO media_index (
                        torrent_id, info_hash, name, total_size, discovered_on,
                        norm_title, title_key, canonical_title_key, movie_key, series_key,
                        aliases_text, imdb_id, tmdb_id, year, content_class,
                        is_anime, confidence, season, episode_start, episode_end,
                        exact_episode_key, season_pack_key, episode_count, has_exact_episode,
                        is_multi_episode, is_season_pack, adult_score, software_score,
                        media_score, resolution, source_hint, codec_hint
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        torrent_id,
                        info_hash,
                        name,
                        size,
                        discovered_on,
                        normalized,
                        title_key,
                        canonical_title_key,
                        movie_key,
                        series_key,
                        "",
                        imdb_id,
                        tmdb_id,
                        year,
                        content_class,
                        is_anime,
                        0.5,
                        season,
                        ep_start,
                        ep_end,
                        exact_episode_key,
                        season_pack_key,
                        ep_count,
                        has_exact_episode,
                        is_multi_episode,
                        is_season_pack,
                        0,
                        0,
                        media_score,
                        resolution,
                        source_hint,
                        codec_hint,
                    ),
                )

                conn.execute(
                    "INSERT INTO media_fts(rowid, search_text) VALUES (?, ?)",
                    (
                        torrent_id,
                        search_text,
                    ),
                )

                result["inserted"] += 1

            except Exception as e:
                result["errors"] += 1
                result["error_details"].append(
                    f"Error processing {tc.get('info_hash', 'unknown')}: {e}"
                )

        if not dry_run and result["inserted"] > 0:
            conn.commit()

    finally:
        conn.close()

    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Promote enriched content to SQLite media_index"
    )
    parser.add_argument("--stdin", action="store_true", help="Read payload from stdin")
    parser.add_argument(
        "--search-db", default=DEFAULT_SEARCH_DB, help="Path to SQLite search database"
    )
    parser.add_argument("--dry-run", action="store_true", help="Don't write changes")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    if not args.stdin:
        parser.error("--stdin is required")

    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        print(
            json.dumps(
                {
                    "error": f"Invalid JSON: {e}",
                    "inserted": 0,
                    "skipped": 0,
                    "errors": 1,
                }
            )
        )
        sys.exit(1)

    contents = payload.get("contents", [])
    search_db = payload.get("search_db", args.search_db)

    if args.verbose:
        print(
            f"Processing {len(contents)} contents to {search_db} (dry_run={args.dry_run})",
            file=sys.stderr,
        )

    result = promote_batch(search_db, contents, dry_run=args.dry_run)

    if args.verbose:
        print(
            f"Result: inserted={result['inserted']}, skipped={result['skipped']}, errors={result['errors']}",
            file=sys.stderr,
        )

    print(json.dumps(result))

    if result["errors"] > 0 and result["inserted"] == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
