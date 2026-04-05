#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import sqlite3
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import aiohttp
from guessit import GuessItApi

ROOT_DIR = Path("/home/ubuntu/aiostreams")
COMET_SRC = ROOT_DIR / "cometouglas"

# Add classifier to path for shared normalizer
sys.path.insert(0, str(ROOT_DIR / "bitmagnet-media" / "classifier"))
from shared_title_normalizer import normalize_for_lookup  # noqa: E402

if str(COMET_SRC) not in sys.path:
    sys.path.insert(0, str(COMET_SRC))

from comet.core.models import settings  # noqa: E402
from comet.metadata.tmdb import TMDBApi  # noqa: E402

SEARCH_DB_DEFAULT = ROOT_DIR / "data/comet-fresh/magnetico/active.search.sqlite3"
STATE_PATH_DEFAULT = ROOT_DIR / "data/comet-fresh/magnetico/lean-id-backfill.state.json"
ANIME_CACHE_DIR_DEFAULT = ROOT_DIR / "data/comet-fresh/magnetico/anime-backfill-cache"

PHASES = (
    "propagate_series",
    "resolve_series",
    "propagate_anime",
    "resolve_anime",
    "propagate_movies",
    "resolve_movies",
    "complete",
)
SERIES_CLASSES = ("episode", "multi_episode", "season_pack")
ANIME_CLASSES = ("anime_episode", "anime_pack")
MOVIE_CLASSES = ("movie", "unknown_video")
HISTORICAL_FILTER = "torrent_id NOT IN (SELECT torrent_id FROM live_promotions)"
DEFAULT_GROUP_BATCH = 40
DEFAULT_NETWORK_CONCURRENCY = 4
DEFAULT_HTTP_TIMEOUT = 30
DEFAULT_CACHE_TTL_SECONDS = 7 * 86400
DEFAULT_ANALYZE_EVERY_BATCHES = 10
RESOLVE_SERIES_STRATEGY_VERSION = "cluster-v3"
SERIES_REBUILD_BATCH_SIZE = 5000
BACKFILL_LOG_PATH = ROOT_DIR / "logs/lean-id-backfill.log"
_GUESSIT_EPISODE_API: GuessItApi | None = None

WORD_PATTERN = re.compile(r"[^\W_]+", re.UNICODE)
YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")
SEASON_PATTERN = re.compile(r"\bs(?:eason\s*)?(\d{1,2})\b", re.IGNORECASE)
SEASON_X_PATTERN = re.compile(r"\b(\d{1,2})x\d{1,3}\b", re.IGNORECASE)
LEADING_ENUMERATOR_PATTERN = re.compile(
    r"^(?:\d{4,6}|\d{3}(?!\b(?:007|911)\b))\s+", re.IGNORECASE
)
SERIES_TITLE_DROP_TOKENS = {
    "baibako",
    "coldfilm",
    "complete",
    "c4bbag3",
    "dimension",
    "dlrip",
    "eng",
    "fox",
    "generalfilm",
    "hd",
    "hdtvrip",
    "lostfilm",
    "lol",
    "ma",
    "newstudio",
    "qman",
    "rus",
    "season",
    "series",
    "teewee",
    "tv",
    "webmux",
}
ANIME_PROVIDER_PATTERNS = (
    ("anilist.co/anime/", "anilist_id"),
    ("myanimelist.net/anime/", "mal_id"),
    ("kitsu.app/anime/", "kitsu_id"),
    ("kitsu.io/anime/", "kitsu_id"),
    ("anidb.net/anime/", "anidb_id"),
    ("anime-planet.com/anime/", "anime_planet_id"),
    ("anisearch.com/anime/", "anisearch_id"),
    ("livechart.me/anime/", "livechart_id"),
    ("animecountdown.com/", "animecountdown_id"),
    ("simkl.com/anime/", "simkl_id"),
    ("imdb.com/title/", "imdb_id"),
)
ANIME_AOD_URL = "https://github.com/manami-project/anime-offline-database/releases/latest/download/anime-offline-database-minified.json"
ANIME_FRIBB_URL = "https://raw.githubusercontent.com/Fribb/anime-lists/refs/heads/master/anime-list-full.json"
ANIME_KITSU_IMDB_URL = "https://raw.githubusercontent.com/TheBeastLT/stremio-kitsu-anime/master/static/data/imdb_mapping.json"


def guessit_episode_parse(value: str) -> dict[str, Any]:
    global _GUESSIT_EPISODE_API
    if _GUESSIT_EPISODE_API is None:
        api = GuessItApi()
        api.configure({"type": "episode"})
        _GUESSIT_EPISODE_API = api
    return dict(_GUESSIT_EPISODE_API.guessit(value))


@dataclass
class GroupCandidate:
    phase: str
    group_key: str
    row_count: int
    member_count: int = 1


@dataclass
class GroupContext:
    phase: str
    media_type: str
    group_key: str
    rows: list[sqlite3.Row]
    representative: sqlite3.Row
    title_candidates: list[str]
    year_hint: int | None
    is_anime: bool
    unit_kind: str = "group"
    member_keys: list[str] | None = None
    member_count: int = 1


@dataclass
class ResolutionResult:
    status: str
    confidence: str | None = None
    tmdb_id: str | None = None
    imdb_id: str | None = None
    resolver_source: str | None = None
    canonical_title: str | None = None
    canonical_year: int | None = None
    reason: str | None = None
    details: dict[str, Any] | None = None


@dataclass
class SeriesSummary:
    group_key: str
    row_count: int
    year_hint: int | None
    season_bucket: str
    representative_title: str


@dataclass
class ParsedSeriesIdentity:
    group_key: str
    parsed_title: str
    parsed_year: int | None
    season_bucket: str
    parser_source: str
    parse_confidence: str
    noise_flags: dict[str, Any]
    row_count: int
    representative_title: str
    group_key_noise_score: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill tmdb_id/imdb_id into historical lean SQLite rows."
    )
    parser.add_argument("--search-db", default=str(SEARCH_DB_DEFAULT))
    parser.add_argument("--state-path", default=str(STATE_PATH_DEFAULT))
    parser.add_argument("--anime-cache-dir", default=str(ANIME_CACHE_DIR_DEFAULT))
    parser.add_argument(
        "--phase",
        choices=PHASES[:-1],
        help="Run only one phase instead of the full sequence.",
    )
    parser.add_argument(
        "--max-groups",
        type=int,
        default=0,
        help="Stop after this many groups in the selected phase(s). 0 = no limit.",
    )
    parser.add_argument("--group-batch-size", type=int, default=DEFAULT_GROUP_BATCH)
    parser.add_argument(
        "--network-concurrency", type=int, default=DEFAULT_NETWORK_CONCURRENCY
    )
    parser.add_argument("--http-timeout", type=int, default=DEFAULT_HTTP_TIMEOUT)
    parser.add_argument(
        "--anime-cache-ttl", type=int, default=DEFAULT_CACHE_TTL_SECONDS
    )
    parser.add_argument(
        "--analyze-every-batches", type=int, default=DEFAULT_ANALYZE_EVERY_BATCHES
    )
    parser.add_argument("--audit-only", action="store_true")
    parser.add_argument(
        "--apply", action="store_true", help="Apply staged updates to the lean DB."
    )
    parser.add_argument(
        "--allow-live-search-db",
        action="store_true",
        help=(
            "Override the safety block and allow --apply against the live "
            "active.search.sqlite3 database."
        ),
    )
    parser.add_argument(
        "--reset-phase",
        action="store_true",
        help="Reset the stored cursor for the selected phase before starting.",
    )
    parser.add_argument(
        "--reset-all",
        action="store_true",
        help="Reset all stored phase cursors and done markers before starting.",
    )
    parser.add_argument("--print-summary", action="store_true")
    return parser.parse_args()


def resolve_search_db_target(
    db_path: Path,
    *,
    apply_changes: bool,
    allow_live_search_db: bool,
) -> Path:
    resolved = db_path.expanduser().resolve()
    live_target = SEARCH_DB_DEFAULT.expanduser().resolve()
    if apply_changes and resolved == live_target and not allow_live_search_db:
        raise SystemExit(
            "--apply against the live active.search.sqlite3 is blocked on this host. "
            "Copy the database to an offline stage DB, run backfill there, then "
            "deploy with scripts/rebuild_live_sqlite.py --historical-seed-db "
            "<stage-db> --cutover. Pass --allow-live-search-db only if you "
            "explicitly want the unsafe legacy behavior."
        )
    return resolved


def collapse_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_title(text: str) -> str:
    """Normalize a title for matching. Delegates to shared normalizer."""
    return normalize_for_lookup(text)


def normalize_lookup_title(text: str) -> str:
    cleaned = normalize_title(text)
    tokens = [token for token in cleaned.split() if len(token) > 1]
    return " ".join(tokens[:12])


def extract_year_hint(text: str) -> int | None:
    match = YEAR_PATTERN.search(str(text or ""))
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def extract_season_bucket(text: str) -> str:
    raw = str(text or "")
    match = SEASON_PATTERN.search(raw)
    if match:
        return f"s{int(match.group(1)):02d}"
    match = SEASON_X_PATTERN.search(raw)
    if match:
        return f"s{int(match.group(1)):02d}"
    return ""


def title_noise_score(text: str) -> int:
    raw = normalize_title(text)
    if not raw:
        return 999
    tokens = raw.split()
    if not tokens:
        return 999
    bad_tokens = {
        "complete",
        "season",
        "series",
        "tv",
        "proper",
        "repack",
        "internal",
        "dubbed",
        "subbed",
        "lostfilm",
        "newstudio",
        "fox",
        "dimension",
        "evo",
        "coldfilm",
        "qman",
        "ani",
        "french",
        "eng",
        "rus",
        "ukr",
        "subs",
        "hdrip",
        "bdremux",
        "webrip",
        "xvid",
        "x264",
        "x265",
    }
    score = 0
    for token in tokens:
        if token in bad_tokens:
            score += 3
        if token.isdigit() and len(token) <= 2:
            score += 1
    return score


def series_cluster_key(
    text: str, year_hint: Any = None, season_bucket: str | None = None
) -> str:
    cleaned = normalize_lookup_title(text)
    if not cleaned:
        return ""
    year_value = ""
    try:
        if year_hint not in (None, "", 0):
            year_value = str(int(year_hint))
    except (TypeError, ValueError):
        year_value = ""
    season_value = collapse_spaces(str(season_bucket or "").lower())
    return "\u241f".join((cleaned, year_value, season_value))


def split_series_cluster_key(cluster_key: str) -> tuple[str, int | None, str]:
    parts = str(cluster_key or "").split("\u241f", 2)
    while len(parts) < 3:
        parts.append("")
    title, raw_year, season_bucket = parts
    try:
        year_hint = int(raw_year) if raw_year else None
    except (TypeError, ValueError):
        year_hint = None
    return title, year_hint, season_bucket


def cleanup_series_candidate(text: str) -> str:
    raw = collapse_spaces(str(text or ""))
    if not raw:
        return ""
    parts = raw.split(" ", 1)
    if len(parts) == 2:
        first_token, remainder = parts
        if first_token.isdigit():
            if len(first_token) >= 4 or (
                len(first_token) == 3 and first_token not in {"007", "911"}
            ):
                raw = remainder
    raw = collapse_spaces(raw)
    return raw


def has_strippable_leading_enumerator(text: str) -> bool:
    raw = collapse_spaces(str(text or ""))
    if not raw:
        return False
    first_token = raw.split(" ", 1)[0]
    if not first_token.isdigit():
        return False
    return len(first_token) >= 4 or (
        len(first_token) == 3 and first_token not in {"007", "911"}
    )


def refine_series_title_candidate(text: str, season_bucket: str = "") -> str:
    raw = cleanup_series_candidate(text)
    if not raw:
        return ""
    tokens = normalize_title(raw).split()
    if not tokens:
        return ""
    cleaned_tokens: list[str] = []
    for token in tokens:
        if re.fullmatch(r"s\d{1,2}", token) or re.fullmatch(r"\d{1,2}x\d{1,3}", token):
            continue
        if token in SERIES_TITLE_DROP_TOKENS:
            continue
        cleaned_tokens.append(token)
    while cleaned_tokens and cleaned_tokens[-1] in SERIES_TITLE_DROP_TOKENS:
        cleaned_tokens.pop()
    if season_bucket:
        while (
            cleaned_tokens
            and cleaned_tokens[-1].isdigit()
            and len(cleaned_tokens[-1]) <= 2
            and any(not token.isdigit() for token in cleaned_tokens[:-1])
        ):
            cleaned_tokens.pop()
    while (
        cleaned_tokens
        and len(cleaned_tokens[-1]) == 1
        and not cleaned_tokens[-1].isdigit()
    ):
        cleaned_tokens.pop()
    refined = " ".join(cleaned_tokens)
    return normalize_lookup_title(refined)


def mapping_key_for_series(text: str, year_hint: int | None, season_bucket: str) -> str:
    return "\u241f".join(
        (
            normalize_lookup_title(cleanup_series_candidate(text)),
            str(int(year_hint)) if year_hint not in (None, 0, "") else "",
            collapse_spaces(str(season_bucket or "").lower()),
        )
    )


def canonical_family_key(
    media_kind: str,
    canonical_title: str,
    canonical_year: int | None,
    season_bucket: str,
    imdb_id: str | None,
    tmdb_id: str | None,
) -> str:
    normalized_title = normalize_lookup_title(canonical_title)
    year_value = str(int(canonical_year)) if canonical_year not in (None, "", 0) else ""
    return "\u241f".join(
        (
            collapse_spaces(str(media_kind or "").lower()),
            str(tmdb_id or "").strip(),
            str(imdb_id or "").strip(),
            normalized_title,
            year_value,
            collapse_spaces(str(season_bucket or "").lower()),
        )
    )


def canonical_variant_key(
    media_kind: str,
    variant_title: str,
    year_hint: int | None,
    season_bucket: str,
) -> str:
    normalized_title = normalize_lookup_title(variant_title)
    year_value = str(int(year_hint)) if year_hint not in (None, "", 0) else ""
    return "\u241f".join(
        (
            collapse_spaces(str(media_kind or "").lower()),
            normalized_title,
            year_value,
            collapse_spaces(str(season_bucket or "").lower()),
        )
    )


def coerce_guessit_year(value: Any) -> int | None:
    if value in (None, ""):
        return None


def series_title_candidate_rank(title: str, source: str) -> tuple[int, int, int, int]:
    tokens = title.split()
    score = title_noise_score(title)
    if tokens and tokens[0].isdigit() and len(tokens) >= 2:
        score -= 1
    if source == "guessit" and len(tokens) <= 1:
        score += 2
    return (
        score,
        0 if source == "guessit" else 1,
        -len(tokens),
        len(title),
    )
    if hasattr(value, "year"):
        try:
            return int(value.year)
        except (TypeError, ValueError):
            return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def coerce_guessit_season_bucket(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        return f"s{int(value):02d}"
    except (TypeError, ValueError):
        return ""


def sql_text_classes(classes: tuple[str, ...]) -> str:
    return ", ".join(f"'{item}'" for item in classes)


def open_db(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute(f"PRAGMA busy_timeout={settings.MAGNETICOLOCAL_BUSY_TIMEOUT_MS}")
    connection.execute("PRAGMA temp_store=MEMORY")
    connection.create_function(
        "series_cluster_key", 3, series_cluster_key, deterministic=True
    )
    connection.create_function(
        "normalize_lookup_title", 1, normalize_lookup_title, deterministic=True
    )
    connection.create_function(
        "extract_year_hint", 1, extract_year_hint, deterministic=True
    )
    connection.create_function(
        "extract_season_bucket", 1, extract_season_bucket, deterministic=True
    )
    connection.create_function(
        "title_noise_score", 1, title_noise_score, deterministic=True
    )
    return connection


def ensure_support_tables(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS id_backfill_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS id_backfill_stage (
            run_phase TEXT NOT NULL,
            group_key TEXT NOT NULL,
            torrent_id INTEGER NOT NULL,
            imdb_id TEXT,
            tmdb_id TEXT,
            confidence TEXT NOT NULL,
            resolver_source TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (run_phase, torrent_id)
        );
        CREATE INDEX IF NOT EXISTS id_backfill_stage_phase_group_idx
            ON id_backfill_stage(run_phase, group_key);

        CREATE TABLE IF NOT EXISTS id_backfill_propagation_source (
            phase TEXT NOT NULL,
            group_key TEXT NOT NULL,
            imdb_id TEXT,
            tmdb_id TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (phase, group_key)
        );

        CREATE TABLE IF NOT EXISTS id_backfill_series_cluster (
            phase TEXT NOT NULL,
            cluster_key TEXT NOT NULL,
            group_key TEXT NOT NULL,
            cleaned_title TEXT NOT NULL,
            year_hint INTEGER,
            season_bucket TEXT NOT NULL,
            noise_score INTEGER NOT NULL,
            row_count INTEGER NOT NULL,
            representative_title TEXT,
            PRIMARY KEY (phase, group_key)
        );
        CREATE INDEX IF NOT EXISTS id_backfill_series_cluster_phase_cluster_idx
            ON id_backfill_series_cluster(phase, cluster_key, row_count DESC, noise_score ASC, group_key ASC);
        CREATE INDEX IF NOT EXISTS id_backfill_series_cluster_phase_group_idx
            ON id_backfill_series_cluster(phase, group_key);

        CREATE TABLE IF NOT EXISTS id_backfill_series_parsed (
            phase TEXT NOT NULL,
            group_key TEXT NOT NULL,
            parsed_title TEXT NOT NULL,
            parsed_year INTEGER,
            season_bucket TEXT NOT NULL,
            parser_source TEXT NOT NULL,
            parse_confidence TEXT NOT NULL,
            noise_flags_json TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            representative_title TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (phase, group_key)
        );
        CREATE INDEX IF NOT EXISTS id_backfill_series_parsed_phase_title_idx
            ON id_backfill_series_parsed(phase, parsed_title, parsed_year, season_bucket);

        CREATE TABLE IF NOT EXISTS id_backfill_series_resolved_group (
            phase TEXT NOT NULL,
            group_key TEXT NOT NULL,
            PRIMARY KEY (phase, group_key)
        );
        CREATE INDEX IF NOT EXISTS id_backfill_series_resolved_group_phase_idx
            ON id_backfill_series_resolved_group(phase, group_key);

        CREATE TABLE IF NOT EXISTS id_backfill_series_mapping (
            mapping_key TEXT PRIMARY KEY,
            canonical_title TEXT NOT NULL,
            canonical_year INTEGER,
            season_bucket TEXT NOT NULL,
            mapping_source TEXT NOT NULL,
            mapping_confidence TEXT NOT NULL,
            imdb_id TEXT,
            tmdb_id TEXT,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS id_backfill_series_mapping_title_idx
            ON id_backfill_series_mapping(canonical_title, canonical_year, season_bucket);

        CREATE TABLE IF NOT EXISTS canonical_title_family (
            family_key TEXT PRIMARY KEY,
            media_kind TEXT NOT NULL,
            canonical_title TEXT NOT NULL,
            canonical_year INTEGER,
            season_bucket TEXT NOT NULL,
            imdb_id TEXT,
            tmdb_id TEXT,
            confidence TEXT NOT NULL,
            resolver_source TEXT NOT NULL,
            evidence_count INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS canonical_title_family_media_idx
            ON canonical_title_family(media_kind, canonical_title, canonical_year, season_bucket);
        CREATE INDEX IF NOT EXISTS canonical_title_family_ids_idx
            ON canonical_title_family(media_kind, tmdb_id, imdb_id);

        CREATE TABLE IF NOT EXISTS canonical_title_variant (
            variant_key TEXT NOT NULL,
            family_key TEXT NOT NULL,
            media_kind TEXT NOT NULL,
            raw_title TEXT NOT NULL,
            normalized_title TEXT NOT NULL,
            year_hint INTEGER,
            season_bucket TEXT NOT NULL,
            confidence TEXT NOT NULL,
            variant_source TEXT NOT NULL,
            evidence_count INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (variant_key, family_key),
            FOREIGN KEY (family_key) REFERENCES canonical_title_family(family_key) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS canonical_title_variant_lookup_idx
            ON canonical_title_variant(media_kind, normalized_title, year_hint, season_bucket, confidence);
        CREATE INDEX IF NOT EXISTS canonical_title_variant_family_idx
            ON canonical_title_variant(family_key, normalized_title);

        CREATE TABLE IF NOT EXISTS id_backfill_resolver_cache (
            cache_key TEXT PRIMARY KEY,
            media_type TEXT NOT NULL,
            is_anime INTEGER NOT NULL,
            status TEXT NOT NULL,
            confidence TEXT,
            imdb_id TEXT,
            tmdb_id TEXT,
            resolver_source TEXT,
            canonical_title TEXT,
            canonical_year INTEGER,
            reason TEXT,
            details_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS id_backfill_audit (
            phase TEXT NOT NULL,
            group_key TEXT NOT NULL,
            status TEXT NOT NULL,
            reason TEXT NOT NULL,
            sample_title TEXT,
            sample_year INTEGER,
            resolver_source TEXT,
            details_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (phase, group_key)
        );
        """
    )
    connection.commit()


def ensure_phase_indexes(connection: sqlite3.Connection) -> None:
    connection.executescript(
        f"""
        CREATE INDEX IF NOT EXISTS media_index_backfill_series_missing_idx
        ON media_index(series_key, torrent_id)
        WHERE content_class IN ({sql_text_classes(SERIES_CLASSES)})
          AND is_anime = 0
          AND series_key IS NOT NULL
          AND series_key <> ''
          AND (imdb_id IS NULL OR imdb_id = '')
          AND (tmdb_id IS NULL OR tmdb_id = '');

        CREATE INDEX IF NOT EXISTS media_index_backfill_series_resolved_idx
        ON media_index(series_key, imdb_id, tmdb_id)
        WHERE content_class IN ({sql_text_classes(SERIES_CLASSES)})
          AND is_anime = 0
          AND series_key IS NOT NULL
          AND series_key <> ''
          AND ((imdb_id IS NOT NULL AND imdb_id <> '') OR (tmdb_id IS NOT NULL AND tmdb_id <> ''));

        CREATE INDEX IF NOT EXISTS media_index_backfill_anime_missing_idx
        ON media_index(series_key, torrent_id)
        WHERE content_class IN ({sql_text_classes(ANIME_CLASSES)})
          AND series_key IS NOT NULL
          AND series_key <> ''
          AND (imdb_id IS NULL OR imdb_id = '')
          AND (tmdb_id IS NULL OR tmdb_id = '');

        CREATE INDEX IF NOT EXISTS media_index_backfill_anime_resolved_idx
        ON media_index(series_key, imdb_id, tmdb_id)
        WHERE content_class IN ({sql_text_classes(ANIME_CLASSES)})
          AND series_key IS NOT NULL
          AND series_key <> ''
          AND ((imdb_id IS NOT NULL AND imdb_id <> '') OR (tmdb_id IS NOT NULL AND tmdb_id <> ''));

        CREATE INDEX IF NOT EXISTS media_index_backfill_movie_missing_idx
        ON media_index(movie_key, torrent_id)
        WHERE content_class IN ({sql_text_classes(MOVIE_CLASSES)})
          AND movie_key IS NOT NULL
          AND movie_key <> ''
          AND (imdb_id IS NULL OR imdb_id = '')
          AND (tmdb_id IS NULL OR tmdb_id = '');

        CREATE INDEX IF NOT EXISTS media_index_backfill_movie_resolved_idx
        ON media_index(movie_key, imdb_id, tmdb_id)
        WHERE content_class IN ({sql_text_classes(MOVIE_CLASSES)})
          AND movie_key IS NOT NULL
          AND movie_key <> ''
          AND ((imdb_id IS NOT NULL AND imdb_id <> '') OR (tmdb_id IS NOT NULL AND tmdb_id <> ''));
        """
    )
    connection.commit()


def get_meta(
    connection: sqlite3.Connection, key: str, default: str | None = None
) -> str | None:
    row = connection.execute(
        "SELECT value FROM id_backfill_meta WHERE key = ?", (key,)
    ).fetchone()
    return row[0] if row is not None else default


def set_meta(connection: sqlite3.Connection, key: str, value: str) -> None:
    connection.execute(
        """
        INSERT INTO id_backfill_meta (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def phase_cursor_key(phase: str) -> str:
    return f"{phase}.cursor"


def phase_counter_key(phase: str, counter: str) -> str:
    return f"{phase}.{counter}"


def phase_total_key(phase: str) -> str:
    return f"{phase}.totalGroups"


def phase_started_at_key(phase: str) -> str:
    return f"{phase}.startedAt"


def phase_completed_at_key(phase: str) -> str:
    return f"{phase}.completedAt"


def int_meta(connection: sqlite3.Connection, key: str) -> int:
    raw = get_meta(connection, key, "0")
    try:
        return int(raw or 0)
    except (TypeError, ValueError):
        return 0


def update_counter(
    connection: sqlite3.Connection, phase: str, counter: str, delta: int
) -> None:
    current = int_meta(connection, phase_counter_key(phase, counter))
    set_meta(connection, phase_counter_key(phase, counter), str(current + delta))


def float_meta(connection: sqlite3.Connection, key: str) -> float | None:
    raw = get_meta(connection, key)
    if raw in (None, ""):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def phase_class_filter(phase: str) -> str:
    if phase.endswith("movies"):
        return f"content_class IN ({sql_text_classes(MOVIE_CLASSES)})"
    if phase.endswith("anime"):
        return f"content_class IN ({sql_text_classes(ANIME_CLASSES)})"
    return f"content_class IN ({sql_text_classes(SERIES_CLASSES)}) AND is_anime = 0"


def phase_group_column(phase: str) -> str:
    return "movie_key" if phase.endswith("movies") else "series_key"


def phase_media_type(phase: str) -> str:
    return "movie" if phase.endswith("movies") else "series"


def phase_missing_expr() -> str:
    return "((imdb_id IS NULL OR imdb_id = '') AND (tmdb_id IS NULL OR tmdb_id = ''))"


def phase_resolved_expr() -> str:
    return "((imdb_id IS NOT NULL AND imdb_id <> '') OR (tmdb_id IS NOT NULL AND tmdb_id <> ''))"


def qualify_expr(expr: str, alias: str) -> str:
    if not alias:
        return expr
    qualified = expr
    for column in ("imdb_id", "tmdb_id"):
        qualified = re.sub(rf"\b{column}\b", f"{alias}.{column}", qualified)
    return qualified


def next_phase(phase: str) -> str:
    try:
        idx = PHASES.index(phase)
    except ValueError:
        return PHASES[0]
    return PHASES[min(idx + 1, len(PHASES) - 1)]


def candidate_sql(phase: str, *, propagation: bool, cursor: str, limit: int) -> str:
    if phase == "resolve_series":
        return """
            SELECT cluster_key AS group_key, count(*) AS row_count, count(*) AS member_count
            FROM id_backfill_series_cluster
            WHERE phase = ?
              AND cluster_key > ?
            GROUP BY cluster_key
            ORDER BY cluster_key
            LIMIT ?
        """
    group_col = phase_group_column(phase)
    class_filter = phase_class_filter(phase)
    base_missing = phase_missing_expr()
    if propagation:
        return f"""
            SELECT m.{group_col} AS group_key, count(*) AS row_count
            FROM media_index m
            JOIN id_backfill_propagation_source s
              ON s.phase = ?
             AND s.group_key = m.{group_col}
            WHERE {HISTORICAL_FILTER}
              AND {class_filter}
              AND {group_col} IS NOT NULL
              AND {group_col} <> ''
              AND {qualify_expr(base_missing, "m")}
              AND m.{group_col} > ?
            GROUP BY m.{group_col}
            ORDER BY m.{group_col}
            LIMIT ?
        """
    else:
        extra = f"""
            NOT EXISTS (
                SELECT 1
                FROM media_index x
                WHERE x.{group_col} = m.{group_col}
                  AND {qualify_expr(phase_resolved_expr(), "x")}
            )
        """
    return f"""
        SELECT m.{group_col} AS group_key, count(*) AS row_count
        FROM media_index m
        WHERE {HISTORICAL_FILTER}
          AND {class_filter}
          AND {group_col} IS NOT NULL
          AND {group_col} <> ''
          AND {base_missing}
          AND m.{group_col} > ?
          AND {extra}
        GROUP BY m.{group_col}
        ORDER BY m.{group_col}
        LIMIT ?
    """


def fetch_group_candidates(
    connection: sqlite3.Connection,
    phase: str,
    *,
    propagation: bool,
    cursor: str,
    limit: int,
) -> list[GroupCandidate]:
    if phase == "resolve_series":
        params = (phase, cursor, limit)
    else:
        params = (phase, cursor, limit) if propagation else (cursor, limit)
    rows = connection.execute(
        candidate_sql(phase, propagation=propagation, cursor=cursor, limit=limit),
        params,
    ).fetchall()
    return [
        GroupCandidate(
            phase=phase,
            group_key=str(row["group_key"]),
            row_count=int(row["row_count"]),
            member_count=int(row["member_count"])
            if "member_count" in row.keys()
            else 1,
        )
        for row in rows
    ]


def count_phase_groups(
    connection: sqlite3.Connection, phase: str, *, propagation: bool
) -> int:
    if phase == "resolve_series":
        row = connection.execute(
            "SELECT count(DISTINCT cluster_key) FROM id_backfill_series_cluster WHERE phase = ?",
            (phase,),
        ).fetchone()
        return int(row[0]) if row else 0
    group_col = phase_group_column(phase)
    class_filter = phase_class_filter(phase)
    base_missing = phase_missing_expr()
    if propagation:
        row = connection.execute(
            f"""
            SELECT count(*) FROM (
                SELECT m.{group_col}
                FROM media_index m
                JOIN id_backfill_propagation_source s
                  ON s.phase = ?
                 AND s.group_key = m.{group_col}
                WHERE {HISTORICAL_FILTER}
                  AND {class_filter}
                  AND {group_col} IS NOT NULL
                  AND {group_col} <> ''
                  AND {qualify_expr(base_missing, "m")}
                GROUP BY m.{group_col}
            )
            """,
            (phase,),
        ).fetchone()
        return int(row[0]) if row else 0
    else:
        extra = f"""
            NOT EXISTS (
                SELECT 1
                FROM media_index x
                WHERE x.{group_col} = m.{group_col}
                  AND {qualify_expr(phase_resolved_expr(), "x")}
            )
        """
    row = connection.execute(
        f"""
        SELECT count(*) FROM (
            SELECT m.{group_col}
            FROM media_index m
            WHERE {HISTORICAL_FILTER}
              AND {class_filter}
              AND {group_col} IS NOT NULL
              AND {group_col} <> ''
              AND {base_missing}
              AND {extra}
            GROUP BY m.{group_col}
        )
        """
    ).fetchone()
    return int(row[0]) if row else 0


def propagation_source_ready_key(phase: str) -> str:
    return f"{phase}.propagationSourceReady"


def phase_strategy_version_key(phase: str) -> str:
    return f"{phase}.strategyVersion"


def series_cluster_ready_key(phase: str) -> str:
    return f"{phase}.seriesClusterReady"


def rebuild_propagation_source(connection: sqlite3.Connection, phase: str) -> int:
    group_col = phase_group_column(phase)
    class_filter = phase_class_filter(phase)
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            "DELETE FROM id_backfill_propagation_source WHERE phase = ?", (phase,)
        )
        connection.execute(
            f"""
            INSERT INTO id_backfill_propagation_source (phase, group_key, imdb_id, tmdb_id, updated_at)
            SELECT ?, src.group_key, src.imdb_id, src.tmdb_id, ?
            FROM (
                SELECT
                    {group_col} AS group_key,
                    MAX(NULLIF(imdb_id, '')) AS imdb_id,
                    MAX(NULLIF(tmdb_id, '')) AS tmdb_id,
                    COUNT(
                        DISTINCT COALESCE(NULLIF(imdb_id, ''), '<null>') || '|' || COALESCE(NULLIF(tmdb_id, ''), '<null>')
                    ) AS pair_count
                FROM media_index
                WHERE {class_filter}
                  AND {group_col} IS NOT NULL
                  AND {group_col} <> ''
                  AND {phase_resolved_expr()}
                GROUP BY {group_col}
            ) AS src
            WHERE src.pair_count = 1
            """,
            (phase, timestamp),
        )
        set_meta(connection, propagation_source_ready_key(phase), "true")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    row = connection.execute(
        "SELECT count(*) FROM id_backfill_propagation_source WHERE phase = ?", (phase,)
    ).fetchone()
    return int(row[0]) if row else 0


def fetch_unresolved_series_summaries(
    connection: sqlite3.Connection, cursor: str, limit: int
) -> list[SeriesSummary]:
    query_start = time.perf_counter()
    rows = connection.execute(
        f"""
        WITH unresolved_groups AS (
            SELECT
                m.series_key AS group_key,
                COUNT(*) AS row_count,
                CASE
                    WHEN COUNT(DISTINCT CASE WHEN m.year IS NOT NULL THEN m.year END) = 1 THEN MAX(m.year)
                    ELSE extract_year_hint(m.series_key)
                END AS year_hint,
                CASE
                    WHEN COUNT(DISTINCT CASE WHEN m.season IS NOT NULL THEN m.season END) = 1
                         AND MAX(m.season) IS NOT NULL
                    THEN printf('s%02d', MAX(m.season))
                    ELSE extract_season_bucket(m.series_key)
                END AS season_bucket,
                COALESCE(
                    MAX(NULLIF(m.canonical_title_key, '')),
                    MAX(NULLIF(m.title_key, '')),
                    MAX(NULLIF(m.norm_title, '')),
                    m.series_key
                ) AS representative_title
            FROM media_index m
            WHERE {HISTORICAL_FILTER}
              AND {phase_class_filter("resolve_series")}
              AND m.series_key IS NOT NULL
              AND m.series_key <> ''
              AND COALESCE(NULLIF(m.imdb_id, ''), NULLIF(m.tmdb_id, '')) IS NULL
              AND m.series_key > ?
              AND NOT EXISTS (
                    SELECT 1
                    FROM id_backfill_series_resolved_group rg
                    WHERE rg.phase = 'resolve_series'
                      AND rg.group_key = m.series_key
              )
            GROUP BY m.series_key
            ORDER BY m.series_key
            LIMIT ?
        )
        SELECT group_key, row_count, year_hint, season_bucket, representative_title
        FROM unresolved_groups
        """,
        (cursor, limit),
    ).fetchall()
    query_elapsed = time.perf_counter() - query_start
    if rows and query_elapsed > 2.0:
        breadcrumb(
            "fetch_unresolved_series_summaries.slow_query",
            cursor_prefix=cursor[:40] if cursor else "",
            elapsed_sec=round(query_elapsed, 2),
            limit=limit,
            rows_fetched=len(rows),
        )
    return [
        SeriesSummary(
            group_key=str(row["group_key"]),
            row_count=int(row["row_count"]),
            year_hint=int(row["year_hint"]) if row["year_hint"] is not None else None,
            season_bucket=str(row["season_bucket"] or ""),
            representative_title=str(row["representative_title"] or ""),
        )
        for row in rows
    ]


def load_series_mappings(connection: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    rows = connection.execute(
        """
        SELECT mapping_key, canonical_title, canonical_year, season_bucket, mapping_source, mapping_confidence, imdb_id, tmdb_id
        FROM id_backfill_series_mapping
        """
    ).fetchall()
    return {str(row["mapping_key"]): row for row in rows}


def parse_series_identity(
    summary: SeriesSummary,
    mappings: dict[str, sqlite3.Row],
    parse_cache: dict[str, dict[str, Any]],
) -> ParsedSeriesIdentity | None:
    season_bucket = summary.season_bucket or ""
    mapping_key = mapping_key_for_series(
        summary.group_key, summary.year_hint, season_bucket
    )
    mapping_row = mappings.get(mapping_key)
    noise_flags = {
        "leading_numeric_enumerator": has_strippable_leading_enumerator(
            summary.group_key or ""
        ),
        "short_title": False,
        "mapping_hit": False,
    }
    if mapping_row is not None:
        noise_flags["mapping_hit"] = True
        canonical_title = normalize_lookup_title(
            str(mapping_row["canonical_title"] or "")
        )
        if canonical_title:
            return ParsedSeriesIdentity(
                group_key=summary.group_key,
                parsed_title=canonical_title,
                parsed_year=int(mapping_row["canonical_year"])
                if mapping_row["canonical_year"] is not None
                else summary.year_hint,
                season_bucket=str(mapping_row["season_bucket"] or season_bucket),
                parser_source=str(mapping_row["mapping_source"] or "series_mapping"),
                parse_confidence=str(mapping_row["mapping_confidence"] or "mapped"),
                noise_flags=noise_flags,
                row_count=summary.row_count,
                representative_title=summary.representative_title,
                group_key_noise_score=title_noise_score(summary.group_key),
            )

    title_candidates: list[tuple[str, int | None, str, str]] = []
    candidates = [summary.representative_title, summary.group_key]
    for candidate in candidates:
        candidate = cleanup_series_candidate(candidate)
        if not candidate:
            continue
        if candidate not in parse_cache:
            try:
                parse_cache[candidate] = guessit_episode_parse(candidate)
            except Exception:
                parse_cache[candidate] = {}
        guessed = parse_cache[candidate]
        title_value = guessed.get("title")
        normalized_title = refine_series_title_candidate(
            str(title_value or ""), summary.season_bucket or ""
        )
        if normalized_title:
            parsed_year = coerce_guessit_year(guessed.get("year")) or summary.year_hint
            guessed_bucket = coerce_guessit_season_bucket(guessed.get("season"))
            title_candidates.append(
                (
                    normalized_title,
                    parsed_year,
                    guessed_bucket or season_bucket,
                    "guessit",
                    "parsed",
                )
            )
        fallback_title = refine_series_title_candidate(
            candidate, summary.season_bucket or ""
        )
        if fallback_title:
            title_candidates.append(
                (
                    fallback_title,
                    summary.year_hint,
                    season_bucket,
                    "fallback_cleanup",
                    "weak",
                )
            )

    if not title_candidates:
        return None

    parsed_title, parsed_year, season_bucket, parser_source, parse_confidence = min(
        title_candidates,
        key=lambda item: series_title_candidate_rank(item[0], item[3]),
    )

    noise_flags["short_title"] = len(parsed_title.split()) <= 2
    return ParsedSeriesIdentity(
        group_key=summary.group_key,
        parsed_title=parsed_title,
        parsed_year=parsed_year,
        season_bucket=season_bucket,
        parser_source=parser_source,
        parse_confidence=parse_confidence,
        noise_flags=noise_flags,
        row_count=summary.row_count,
        representative_title=summary.representative_title,
        group_key_noise_score=title_noise_score(summary.group_key),
    )


def write_series_parsed_batch(
    connection: sqlite3.Connection,
    phase: str,
    parsed_rows: list[ParsedSeriesIdentity],
) -> None:
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    connection.executemany(
        """
        INSERT OR REPLACE INTO id_backfill_series_parsed (
            phase, group_key, parsed_title, parsed_year, season_bucket, parser_source, parse_confidence,
            noise_flags_json, row_count, representative_title, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                phase,
                row.group_key,
                row.parsed_title,
                row.parsed_year,
                row.season_bucket,
                row.parser_source,
                row.parse_confidence,
                json.dumps(row.noise_flags, ensure_ascii=True, sort_keys=True),
                row.row_count,
                row.representative_title,
                timestamp,
            )
            for row in parsed_rows
        ],
    )


def write_series_cluster_batch(
    connection: sqlite3.Connection, phase: str, parsed_rows: list[ParsedSeriesIdentity]
) -> None:
    connection.executemany(
        """
        INSERT OR REPLACE INTO id_backfill_series_cluster (
            phase, cluster_key, group_key, cleaned_title, year_hint, season_bucket, noise_score, row_count, representative_title
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                phase,
                series_cluster_key(
                    row.parsed_title, row.parsed_year, row.season_bucket
                ),
                row.group_key,
                row.parsed_title,
                row.parsed_year,
                row.season_bucket,
                row.group_key_noise_score,
                row.row_count,
                row.representative_title,
            )
            for row in parsed_rows
            if row.parsed_title
        ],
    )


def rebuild_series_clusters(
    connection: sqlite3.Connection, phase: str = "resolve_series"
) -> int:
    if phase != "resolve_series":
        return 0
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    rebuild_start = time.perf_counter()
    breadcrumb("rebuild_series_clusters.start", phase=phase)
    connection.execute("BEGIN IMMEDIATE")
    try:
        seed_start = time.perf_counter()
        connection.execute(
            "DELETE FROM id_backfill_series_cluster WHERE phase = ?", (phase,)
        )
        connection.execute(
            "DELETE FROM id_backfill_series_parsed WHERE phase = ?", (phase,)
        )
        connection.execute(
            "DELETE FROM id_backfill_series_resolved_group WHERE phase = ?", (phase,)
        )
        connection.execute(
            f"""
            INSERT INTO id_backfill_series_resolved_group (phase, group_key)
            SELECT ?, series_key
            FROM media_index
            WHERE {phase_class_filter("resolve_series")}
              AND series_key IS NOT NULL
              AND series_key <> ''
              AND {phase_resolved_expr()}
            GROUP BY series_key
            """,
            (phase,),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    resolved_row = connection.execute(
        "SELECT count(*) FROM id_backfill_series_resolved_group WHERE phase = ?",
        (phase,),
    ).fetchone()
    breadcrumb(
        "rebuild_series_clusters.seed_complete",
        elapsed_sec=round(time.perf_counter() - seed_start, 2),
        phase=phase,
        resolved_groups=int(resolved_row[0]) if resolved_row else 0,
    )

    parse_cache: dict[str, dict[str, Any]] = {}
    mappings = load_series_mappings(connection)
    breadcrumb(
        "rebuild_series_clusters.mappings_loaded",
        elapsed_sec=round(time.perf_counter() - rebuild_start, 2),
        mapping_count=len(mappings),
        phase=phase,
    )
    cursor = ""
    batch_number = 0
    total_summaries = 0
    total_parsed = 0
    while True:
        batch_number += 1
        batch_start = time.perf_counter()
        summaries = fetch_unresolved_series_summaries(
            connection, cursor, SERIES_REBUILD_BATCH_SIZE
        )
        if not summaries:
            break
        fetch_elapsed = time.perf_counter() - batch_start
        parse_start = time.perf_counter()
        parsed_rows = [
            parsed
            for summary in summaries
            if (parsed := parse_series_identity(summary, mappings, parse_cache))
            is not None
        ]
        parse_elapsed = time.perf_counter() - parse_start
        write_start = time.perf_counter()
        connection.execute("BEGIN IMMEDIATE")
        try:
            if parsed_rows:
                write_series_parsed_batch(connection, phase, parsed_rows)
                write_series_cluster_batch(connection, phase, parsed_rows)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        write_elapsed = time.perf_counter() - write_start
        total_summaries += len(summaries)
        total_parsed += len(parsed_rows)
        cursor = summaries[-1].group_key
        breadcrumb(
            "rebuild_series_clusters.batch_complete",
            batch_number=batch_number,
            cursor=cursor,
            elapsed_sec=round(time.perf_counter() - rebuild_start, 2),
            fetch_sec=round(fetch_elapsed, 2),
            parse_cache_size=len(parse_cache),
            parse_sec=round(parse_elapsed, 2),
            parsed_rows=len(parsed_rows),
            phase=phase,
            total_batch_sec=round(time.perf_counter() - batch_start, 2),
            total_parsed=total_parsed,
            total_summaries=total_summaries,
            write_sec=round(write_elapsed, 2),
        )
    row = connection.execute(
        "SELECT count(DISTINCT cluster_key) FROM id_backfill_series_cluster WHERE phase = ?",
        (phase,),
    ).fetchone()
    cluster_count = int(row[0]) if row else 0
    set_meta(connection, series_cluster_ready_key(phase), "true")
    set_meta(connection, phase_total_key(phase), "0")
    set_meta(connection, phase_started_at_key(phase), "")
    set_meta(connection, phase_completed_at_key(phase), "")
    set_meta(connection, "lastSeriesClusterRebuildAt", timestamp)
    connection.commit()
    breadcrumb(
        "rebuild_series_clusters.complete",
        batch_count=max(batch_number - 1, 0),
        distinct_clusters=cluster_count,
        elapsed_sec=round(time.perf_counter() - rebuild_start, 2),
        phase=phase,
        total_parsed=total_parsed,
        total_summaries=total_summaries,
    )
    return cluster_count


def fetch_group_rows(
    connection: sqlite3.Connection, phase: str, group_key: str
) -> list[sqlite3.Row]:
    group_col = phase_group_column(phase)
    class_filter = phase_class_filter(phase)
    return connection.execute(
        f"""
        SELECT torrent_id, info_hash, name, norm_title, title_key, canonical_title_key, movie_key, series_key,
               aliases_text, year, season, episode_start, episode_end, content_class, is_anime,
               confidence, media_score, total_size, imdb_id, tmdb_id
        FROM media_index
        WHERE {HISTORICAL_FILTER}
          AND {class_filter}
          AND {group_col} = ?
        ORDER BY confidence DESC, media_score DESC, total_size DESC, torrent_id ASC
        LIMIT 50
        """,
        (group_key,),
    ).fetchall()


def fetch_series_cluster_members(
    connection: sqlite3.Connection, cluster_key: str
) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT phase, cluster_key, group_key, cleaned_title, year_hint, season_bucket, noise_score, row_count, representative_title
        FROM id_backfill_series_cluster
        WHERE phase = 'resolve_series'
          AND cluster_key = ?
        ORDER BY noise_score ASC, row_count DESC, group_key ASC
        """,
        (cluster_key,),
    ).fetchall()


def fetch_series_parsed_rows(
    connection: sqlite3.Connection, phase: str, group_keys: list[str]
) -> dict[str, sqlite3.Row]:
    if not group_keys:
        return {}
    placeholders = ", ".join("?" for _ in group_keys)
    rows = connection.execute(
        f"""
        SELECT phase, group_key, parsed_title, parsed_year, season_bucket, parser_source, parse_confidence, noise_flags_json, row_count, representative_title
        FROM id_backfill_series_parsed
        WHERE phase = ?
          AND group_key IN ({placeholders})
        """,
        (phase, *group_keys),
    ).fetchall()
    return {str(row["group_key"]): row for row in rows}


def best_year(rows: list[sqlite3.Row]) -> int | None:
    years = [int(row["year"]) for row in rows if row["year"] is not None]
    if not years:
        return None
    return Counter(years).most_common(1)[0][0]


def cleaned_title_candidates(row: sqlite3.Row, media_type: str) -> list[str]:
    raw_candidates = [
        str(row["name"] or ""),
        str(row["norm_title"] or ""),
        str(row["canonical_title_key"] or ""),
        str(row["title_key"] or ""),
        str(
            row["movie_key"] or "" if media_type == "movie" else row["series_key"] or ""
        ),
    ]
    cleaned: list[str] = []
    seen: set[str] = set()
    for candidate in raw_candidates:
        if "|" in candidate:
            candidate = candidate.split("|", 1)[0]
        title = normalize_lookup_title(candidate)
        if not title:
            continue
        if media_type == "series":
            title = re.sub(r"\bs\d{1,2}\b", "", title).strip()
            title = collapse_spaces(title)
        if len(title.split()) < 1:
            continue
        if title in seen:
            continue
        seen.add(title)
        cleaned.append(title)
    return cleaned


def derive_group_titles(rows: list[sqlite3.Row], media_type: str) -> list[str]:
    counter: Counter[str] = Counter()
    for row in rows[:20]:
        for title in cleaned_title_candidates(row, media_type):
            counter[title] += 1
    ordered = [
        title
        for title, _count in sorted(
            counter.items(),
            key=lambda item: (-item[1], len(item[0].split()), len(item[0])),
        )
        if len(title) >= 2
    ]
    return ordered[:5]


def pick_representative(rows: list[sqlite3.Row]) -> sqlite3.Row:
    return rows[0]


def pick_cluster_representative(
    connection: sqlite3.Connection, members: list[sqlite3.Row]
) -> tuple[sqlite3.Row, list[sqlite3.Row]]:
    parsed_by_group = fetch_series_parsed_rows(
        connection, "resolve_series", [str(row["group_key"]) for row in members]
    )
    ranked = sorted(
        members,
        key=lambda row: (
            0
            if parsed_by_group.get(str(row["group_key"]))
            and str(parsed_by_group[str(row["group_key"])]["parse_confidence"])
            in {"mapped", "parsed"}
            else 1,
            int(row["noise_score"]),
            -int(row["row_count"]),
            0 if row["year_hint"] is not None else 1,
            0 if str(row["season_bucket"] or "") else 1,
            len(str(row["cleaned_title"] or "")),
            str(row["group_key"]),
        ),
    )
    representative_member = ranked[0]
    rep_rows = fetch_group_rows(
        connection, "resolve_series", str(representative_member["group_key"])
    )
    if rep_rows:
        return representative_member, rep_rows
    for member in ranked[1:]:
        rep_rows = fetch_group_rows(
            connection, "resolve_series", str(member["group_key"])
        )
        if rep_rows:
            return member, rep_rows
    return representative_member, []


def derive_cluster_titles(
    connection: sqlite3.Connection, members: list[sqlite3.Row], media_type: str
) -> list[str]:
    parsed_by_group = fetch_series_parsed_rows(
        connection, "resolve_series", [str(row["group_key"]) for row in members]
    )
    counter: Counter[str] = Counter()
    for member in members[:8]:
        parsed_row = parsed_by_group.get(str(member["group_key"]))
        if parsed_row is not None:
            parsed_title = normalize_lookup_title(str(parsed_row["parsed_title"] or ""))
            if parsed_title:
                counter[parsed_title] += max(int(member["row_count"]), 1) * 3
        for raw_value in (
            str(member["group_key"] or ""),
            str(member["representative_title"] or ""),
            str(member["cleaned_title"] or ""),
        ):
            title = normalize_lookup_title(raw_value)
            if title:
                counter[title] += max(int(member["row_count"]), 1)
        member_rows = fetch_group_rows(
            connection, "resolve_series", str(member["group_key"])
        )
        for row in member_rows[:5]:
            for title in cleaned_title_candidates(row, media_type):
                counter[title] += 2
    ordered = [
        title
        for title, _count in sorted(
            counter.items(),
            key=lambda item: (-item[1], title_noise_score(item[0]), len(item[0])),
        )
        if title
    ]
    return ordered[:6]


def best_cluster_year(
    members: list[sqlite3.Row], rep_rows: list[sqlite3.Row]
) -> int | None:
    values = [
        int(member["year_hint"])
        for member in members
        if member["year_hint"] is not None
    ]
    values.extend(int(row["year"]) for row in rep_rows if row["year"] is not None)
    if not values:
        return None
    counts = Counter(values)
    best_year, best_count = counts.most_common(1)[0]
    if len(counts) > 2 and best_count <= max(2, len(values) // 2):
        return None
    return int(best_year)


def cluster_low_value_reason(context: GroupContext) -> str | None:
    titles = [
        normalize_lookup_title(title)
        for title in context.title_candidates
        if normalize_lookup_title(title)
    ]
    if not titles:
        return "no_clean_titles"
    if all(
        all(token.isdigit() for token in title.split()) and len(title.split()) <= 2
        for title in titles[:3]
    ):
        return "cluster_numeric_only"
    if all(title_noise_score(title) >= 6 for title in titles[:3]):
        return "cluster_noise_only"
    if context.member_count <= 0:
        return "empty_cluster"
    return None


def build_group_context(
    connection: sqlite3.Connection, phase: str, group_key: str
) -> GroupContext | None:
    if phase == "resolve_series":
        members = fetch_series_cluster_members(connection, group_key)
        if not members:
            return None
        representative_member, rep_rows = pick_cluster_representative(
            connection, members
        )
        if not rep_rows:
            return None
        media_type = phase_media_type(phase)
        return GroupContext(
            phase=phase,
            media_type=media_type,
            group_key=group_key,
            rows=rep_rows,
            representative=pick_representative(rep_rows),
            title_candidates=derive_cluster_titles(connection, members, media_type),
            year_hint=best_cluster_year(members, rep_rows),
            is_anime=False,
            unit_kind="series_cluster",
            member_keys=[str(member["group_key"]) for member in members],
            member_count=len(members),
        )
    rows = fetch_group_rows(connection, phase, group_key)
    if not rows:
        return None
    media_type = phase_media_type(phase)
    return GroupContext(
        phase=phase,
        media_type=media_type,
        group_key=group_key,
        rows=rows,
        representative=pick_representative(rows),
        title_candidates=derive_group_titles(rows, media_type),
        year_hint=best_year(rows),
        is_anime=phase.endswith("anime"),
        unit_kind="group",
        member_keys=[group_key],
        member_count=1,
    )


def unique_canonical_pair(
    rows: list[sqlite3.Row],
) -> tuple[str | None, str | None] | None:
    pairs = {
        (
            (str(row["imdb_id"]).strip() if row["imdb_id"] else None),
            (str(row["tmdb_id"]).strip() if row["tmdb_id"] else None),
        )
        for row in rows
        if (row["imdb_id"] is not None and str(row["imdb_id"]).strip())
        or (row["tmdb_id"] is not None and str(row["tmdb_id"]).strip())
    }
    if len(pairs) != 1:
        return None
    imdb_id, tmdb_id = next(iter(pairs))
    return imdb_id, tmdb_id


def historical_unresolved_ids(
    connection: sqlite3.Connection, phase: str, group_key: str
) -> list[int]:
    group_col = phase_group_column(phase)
    class_filter = phase_class_filter(phase)
    rows = connection.execute(
        f"""
        SELECT torrent_id
        FROM media_index
        WHERE {HISTORICAL_FILTER}
          AND {class_filter}
          AND {group_col} = ?
          AND ((imdb_id IS NULL OR imdb_id = '') AND (tmdb_id IS NULL OR tmdb_id = ''))
        ORDER BY torrent_id
        """,
        (group_key,),
    ).fetchall()
    return [int(row[0]) for row in rows]


def historical_unresolved_ids_for_groups(
    connection: sqlite3.Connection, phase: str, group_keys: list[str]
) -> list[int]:
    if not group_keys:
        return []
    group_col = phase_group_column(phase)
    class_filter = phase_class_filter(phase)
    placeholders = ", ".join("?" for _ in group_keys)
    rows = connection.execute(
        f"""
        SELECT torrent_id
        FROM media_index
        WHERE {HISTORICAL_FILTER}
          AND {class_filter}
          AND {group_col} IN ({placeholders})
          AND ((imdb_id IS NULL OR imdb_id = '') AND (tmdb_id IS NULL OR tmdb_id = ''))
        ORDER BY torrent_id
        """,
        group_keys,
    ).fetchall()
    return [int(row[0]) for row in rows]


def resolver_cache_key_for_context(context: GroupContext) -> str:
    normalized_titles = sorted(
        {
            normalize_lookup_title(title)
            for title in context.title_candidates
            if normalize_lookup_title(title)
        }
    )
    parts = [
        context.media_type,
        context.unit_kind,
        "anime" if context.is_anime else "normal",
        str(context.year_hint or ""),
        "|".join(normalized_titles),
    ]
    return "\u241f".join(parts)


def load_resolver_cache(
    connection: sqlite3.Connection, context: GroupContext
) -> ResolutionResult | None:
    row = connection.execute(
        """
        SELECT status, confidence, tmdb_id, imdb_id, resolver_source, canonical_title, canonical_year, reason, details_json
        FROM id_backfill_resolver_cache
        WHERE cache_key = ?
        """,
        (resolver_cache_key_for_context(context),),
    ).fetchone()
    if row is None:
        return None
    return ResolutionResult(
        status=str(row["status"]),
        confidence=row["confidence"],
        tmdb_id=row["tmdb_id"],
        imdb_id=row["imdb_id"],
        resolver_source=row["resolver_source"],
        canonical_title=row["canonical_title"],
        canonical_year=row["canonical_year"],
        reason=row["reason"],
        details=json.loads(row["details_json"]) if row["details_json"] else None,
    )


def write_resolver_cache(
    connection: sqlite3.Connection, context: GroupContext, result: ResolutionResult
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO id_backfill_resolver_cache (
            cache_key, media_type, is_anime, status, confidence, imdb_id, tmdb_id,
            resolver_source, canonical_title, canonical_year, reason, details_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            resolver_cache_key_for_context(context),
            context.media_type,
            1 if context.is_anime else 0,
            result.status,
            result.confidence,
            result.imdb_id,
            result.tmdb_id,
            result.resolver_source,
            result.canonical_title,
            result.canonical_year,
            result.reason,
            json.dumps(result.details or {}, ensure_ascii=True, sort_keys=True),
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        ),
    )


def stage_updates(
    connection: sqlite3.Connection,
    *,
    phase: str,
    group_key: str,
    torrent_ids: list[int],
    imdb_id: str | None,
    tmdb_id: str | None,
    confidence: str,
    resolver_source: str,
) -> int:
    if not torrent_ids or (imdb_id is None and tmdb_id is None):
        return 0
    updated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    connection.executemany(
        """
        INSERT OR REPLACE INTO id_backfill_stage (
            run_phase, group_key, torrent_id, imdb_id, tmdb_id, confidence, resolver_source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                phase,
                group_key,
                torrent_id,
                imdb_id,
                tmdb_id,
                confidence,
                resolver_source,
                updated_at,
            )
            for torrent_id in torrent_ids
        ],
    )
    return len(torrent_ids)


def write_audit(
    connection: sqlite3.Connection,
    *,
    phase: str,
    group_key: str,
    status: str,
    reason: str,
    sample_title: str | None,
    sample_year: int | None,
    resolver_source: str | None,
    details: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO id_backfill_audit (
            phase, group_key, status, reason, sample_title, sample_year, resolver_source, details_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            phase,
            group_key,
            status,
            reason,
            sample_title,
            sample_year,
            resolver_source,
            json.dumps(details, ensure_ascii=True, sort_keys=True),
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        ),
    )


def merge_stage(connection: sqlite3.Connection, phase: str) -> int:
    row = connection.execute(
        "SELECT count(*) FROM id_backfill_stage WHERE run_phase = ?",
        (phase,),
    ).fetchone()
    staged = int(row[0]) if row else 0
    if staged <= 0:
        return 0
    connection.execute(
        """
        UPDATE media_index
        SET
            imdb_id = COALESCE(imdb_id, (
                SELECT s.imdb_id
                FROM id_backfill_stage s
                WHERE s.run_phase = ?
                  AND s.torrent_id = media_index.torrent_id
                LIMIT 1
            )),
            tmdb_id = COALESCE(tmdb_id, (
                SELECT s.tmdb_id
                FROM id_backfill_stage s
                WHERE s.run_phase = ?
                  AND s.torrent_id = media_index.torrent_id
                LIMIT 1
            ))
        WHERE torrent_id IN (
            SELECT torrent_id FROM id_backfill_stage WHERE run_phase = ?
        )
        """,
        (phase, phase, phase),
    )
    connection.execute("DELETE FROM id_backfill_stage WHERE run_phase = ?", (phase,))
    return staged


class TMDBResolver:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.api = TMDBApi(session)
        self.search_cache: dict[tuple[str, str, int | None], list[dict[str, Any]]] = {}
        self.external_cache: dict[tuple[str, str], str | None] = {}

    async def _search(
        self, media_type: str, query: str, year: int | None
    ) -> list[dict[str, Any]]:
        key = (media_type, query, year)
        if key in self.search_cache:
            return self.search_cache[key]
        params = f"query={quote_plus(query)}"
        if year is not None:
            if media_type == "movie":
                params += f"&year={int(year)}"
            else:
                params += f"&first_air_date_year={int(year)}"
        path = f"search/{'movie' if media_type == 'movie' else 'tv'}?{params}"
        payload = await self.api._fetch_tmdb_payload(path)
        results = payload.get("results", []) if isinstance(payload, dict) else []
        self.search_cache[key] = results
        return results

    async def imdb_from_tmdb(self, media_type: str, tmdb_id: str) -> str | None:
        key = (media_type, str(tmdb_id))
        if key not in self.external_cache:
            self.external_cache[key] = await self.api.get_imdb_id_from_tmdb(
                media_type, str(tmdb_id)
            )
        return self.external_cache[key]


class AnimeCatalog:
    def __init__(self, cache_dir: Path, ttl_seconds: int):
        self.cache_dir = cache_dir
        self.ttl_seconds = ttl_seconds
        self.index: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.loaded = False

    def _cache_path(self, name: str) -> Path:
        return self.cache_dir / f"{name}.json"

    def _fresh(self, path: Path) -> bool:
        if not path.exists():
            return False
        return (time.time() - path.stat().st_mtime) <= self.ttl_seconds

    async def _download_json(
        self, session: aiohttp.ClientSession, url: str, path: Path
    ) -> Any:
        if self._fresh(path):
            return json.loads(path.read_text())
        async with session.get(url, timeout=DEFAULT_HTTP_TIMEOUT) as response:
            response.raise_for_status()
            data = json.loads((await response.read()).decode("utf-8"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data))
        return data

    @staticmethod
    def _provider_values(entry: dict[str, Any]) -> dict[str, str]:
        values: dict[str, str] = {}
        for source in entry.get("sources") or []:
            for needle, provider_key in ANIME_PROVIDER_PATTERNS:
                if needle not in source:
                    continue
                provider_id = source.rstrip("/").rsplit("/", 1)[-1]
                if "id=" in source:
                    provider_id = source.split("id=", 1)[1].split("&", 1)[0]
                if provider_id:
                    values[provider_key] = provider_id
        return values

    async def load(self, session: aiohttp.ClientSession) -> None:
        if self.loaded:
            return
        aod = await self._download_json(
            session, ANIME_AOD_URL, self._cache_path("anime-offline-database")
        )
        fribb = await self._download_json(
            session, ANIME_FRIBB_URL, self._cache_path("anime-fribb")
        )
        kitsu = await self._download_json(
            session, ANIME_KITSU_IMDB_URL, self._cache_path("anime-kitsu-imdb")
        )
        fribb_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for item in fribb:
            for provider_key in (
                "anilist_id",
                "mal_id",
                "kitsu_id",
                "anidb_id",
                "anime_planet_id",
                "anisearch_id",
                "livechart_id",
                "animecountdown_id",
                "simkl_id",
            ):
                value = item.get(provider_key)
                if value is not None and str(value):
                    fribb_by_key[(provider_key, str(value))] = item
        kitsu_by_id = {
            str(item.get("kitsu_id")): item for item in kitsu if item.get("kitsu_id")
        }

        for entry in aod.get("data", []):
            provider_values = self._provider_values(entry)
            linked = None
            for provider_key, provider_id in provider_values.items():
                linked = fribb_by_key.get((provider_key, str(provider_id)))
                if linked:
                    break
            imdb_id = None
            tmdb_id = None
            if linked:
                if linked.get("imdb_id"):
                    imdb_id = str(linked["imdb_id"])
                if linked.get("themoviedb_id"):
                    tmdb_id = str(linked["themoviedb_id"])
                kitsu_match = linked.get("kitsu_id")
                if kitsu_match and not imdb_id:
                    kitsu_item = kitsu_by_id.get(str(kitsu_match))
                    if kitsu_item and kitsu_item.get("imdb_id"):
                        imdb_id = str(kitsu_item["imdb_id"])
            elif provider_values.get("kitsu_id"):
                kitsu_item = kitsu_by_id.get(str(provider_values["kitsu_id"]))
                if kitsu_item and kitsu_item.get("imdb_id"):
                    imdb_id = str(kitsu_item["imdb_id"])
            if imdb_id is None and tmdb_id is None:
                continue
            titles = [entry.get("title"), *(entry.get("synonyms") or [])]
            season_year = None
            anime_season = entry.get("animeSeason") or {}
            if isinstance(anime_season, dict) and anime_season.get("year"):
                try:
                    season_year = int(anime_season["year"])
                except (TypeError, ValueError):
                    season_year = None
            payload = {
                "title": entry.get("title"),
                "year": season_year,
                "imdb_id": imdb_id,
                "tmdb_id": tmdb_id,
            }
            for title in titles:
                normalized = normalize_lookup_title(str(title or ""))
                if not normalized:
                    continue
                self.index[normalized].append(payload)
        self.loaded = True

    def resolve(self, titles: list[str], year: int | None) -> ResolutionResult | None:
        candidates: list[dict[str, Any]] = []
        seen: set[tuple[str | None, str | None]] = set()
        for title in titles:
            normalized = normalize_lookup_title(title)
            for entry in self.index.get(normalized, []):
                key = (entry.get("imdb_id"), entry.get("tmdb_id"))
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(entry)
        if not candidates:
            return None
        scored = []
        for entry in candidates:
            score = 0
            if year is not None and entry.get("year") is not None:
                diff = abs(int(entry["year"]) - year)
                score += 100 if diff == 0 else 40 if diff == 1 else -50
            if any(
                normalize_lookup_title(entry["title"]) == normalize_lookup_title(title)
                for title in titles
            ):
                score += 120
            scored.append((score, entry))
        scored.sort(key=lambda item: item[0], reverse=True)
        if len(scored) > 1 and scored[0][0] == scored[1][0]:
            return ResolutionResult(
                status="ambiguous",
                reason="anime_tie",
                details={"titles": titles, "candidates": scored[:5]},
            )
        best = scored[0][1]
        confidence = "exact" if best.get("year") == year or year is None else "strong"
        return ResolutionResult(
            status="resolved",
            confidence=confidence,
            imdb_id=best.get("imdb_id"),
            tmdb_id=best.get("tmdb_id"),
            resolver_source="anime_catalog",
            canonical_title=best.get("title"),
            canonical_year=best.get("year"),
            details={"titles": titles},
        )


def title_similarity(a: str, b: str) -> float:
    ta = normalize_lookup_title(a)
    tb = normalize_lookup_title(b)
    if not ta or not tb:
        return 0.0
    if ta == tb:
        return 1.0
    sa = set(ta.split())
    sb = set(tb.split())
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / max(len(sa | sb), 1)


async def resolve_via_tmdb(
    resolver: TMDBResolver, context: GroupContext
) -> ResolutionResult:
    titles = context.title_candidates
    if not titles:
        return ResolutionResult(
            status="miss",
            reason="no_clean_titles",
            details={"group_key": context.group_key},
        )
    media_type = context.media_type
    all_candidates: list[tuple[float, dict[str, Any], str]] = []
    for title in titles[:3]:
        results = await resolver._search(media_type, title, context.year_hint)
        if not results and context.year_hint is not None:
            results = await resolver._search(media_type, title, None)
        for result in results[:8]:
            candidate_title = (
                result.get("title")
                or result.get("name")
                or result.get("original_title")
                or result.get("original_name")
                or ""
            )
            score = 0.0
            score += 300.0 * max(
                title_similarity(title, candidate_title),
                *(title_similarity(alias, candidate_title) for alias in titles),
            )
            if context.year_hint is not None:
                date_value = (
                    result.get("release_date") or result.get("first_air_date") or ""
                )
                year_match = re.match(r"(\d{4})", date_value or "")
                if year_match:
                    candidate_year = int(year_match.group(1))
                    diff = abs(candidate_year - context.year_hint)
                    score += 120.0 if diff == 0 else 40.0 if diff == 1 else -70.0
            popularity = float(result.get("popularity") or 0.0)
            score += min(popularity, 50.0)
            all_candidates.append((score, result, title))
    if not all_candidates:
        return ResolutionResult(
            status="miss",
            reason="tmdb_no_candidates",
            details={"titles": titles, "year": context.year_hint},
        )
    all_candidates.sort(key=lambda item: item[0], reverse=True)
    top_score, best, query_title = all_candidates[0]
    top_id = str(best.get("id"))
    competing = [
        item for item in all_candidates[1:] if str(item[1].get("id")) != top_id
    ]
    if competing and competing[0][0] >= top_score - 25.0:
        return ResolutionResult(
            status="ambiguous",
            reason="tmdb_competing_candidates",
            details={
                "titles": titles,
                "top": {
                    "title": best.get("title") or best.get("name"),
                    "id": top_id,
                    "score": top_score,
                },
                "next": {
                    "title": competing[0][1].get("title")
                    or competing[0][1].get("name"),
                    "id": competing[0][1].get("id"),
                    "score": competing[0][0],
                },
            },
        )
    best_title = str(best.get("title") or best.get("name") or "")
    best_year = None
    date_value = best.get("release_date") or best.get("first_air_date") or ""
    match = re.match(r"(\d{4})", str(date_value))
    if match:
        best_year = int(match.group(1))
    similarity = max(
        title_similarity(query_title, best_title),
        *(title_similarity(title, best_title) for title in titles),
    )
    if similarity < 0.74:
        return ResolutionResult(
            status="ambiguous",
            reason="tmdb_similarity_too_low",
            details={
                "titles": titles,
                "best_title": best_title,
                "score": round(similarity, 3),
            },
        )
    confidence = "exact"
    if context.year_hint is not None and best_year is not None:
        diff = abs(best_year - context.year_hint)
        if diff == 0:
            confidence = "exact"
        elif diff == 1 and similarity >= 0.92:
            confidence = "strong"
        else:
            return ResolutionResult(
                status="ambiguous",
                reason="tmdb_year_mismatch",
                details={
                    "titles": titles,
                    "best_title": best_title,
                    "best_year": best_year,
                    "group_year": context.year_hint,
                },
            )
    elif similarity < 0.92:
        confidence = "strong"
    imdb_id = await resolver.imdb_from_tmdb(media_type, top_id)
    return ResolutionResult(
        status="resolved",
        confidence=confidence,
        tmdb_id=top_id,
        imdb_id=imdb_id,
        resolver_source=f"tmdb_search_{media_type}",
        canonical_title=best_title,
        canonical_year=best_year,
        details={"query_title": query_title, "titles": titles},
    )


async def resolve_group(
    resolver: TMDBResolver,
    anime_catalog: AnimeCatalog,
    connection: sqlite3.Connection,
    context: GroupContext,
) -> ResolutionResult:
    cached = load_resolver_cache(connection, context)
    if cached is not None:
        return cached
    if context.is_anime:
        anime_result = anime_catalog.resolve(
            context.title_candidates, context.year_hint
        )
        if anime_result is not None:
            if anime_result.tmdb_id is None and anime_result.imdb_id:
                anime_result.tmdb_id = await resolver.api.get_tmdb_id_from_imdb(
                    anime_result.imdb_id
                )
            if anime_result.imdb_id is None and anime_result.tmdb_id:
                anime_result.imdb_id = await resolver.imdb_from_tmdb(
                    "series", anime_result.tmdb_id
                )
            if anime_result.tmdb_id or anime_result.imdb_id:
                return anime_result
        tmdb_result = await resolve_via_tmdb(resolver, context)
        if tmdb_result.status == "resolved":
            tmdb_result.resolver_source = "anime_" + str(tmdb_result.resolver_source)
        return tmdb_result
    return await resolve_via_tmdb(resolver, context)


def cluster_resolved_pair(
    connection: sqlite3.Connection, group_keys: list[str]
) -> tuple[str | None, str | None] | None:
    if not group_keys:
        return None
    placeholders = ", ".join("?" for _ in group_keys)
    row = connection.execute(
        f"""
        SELECT
            MAX(NULLIF(imdb_id, '')) AS imdb_id,
            MAX(NULLIF(tmdb_id, '')) AS tmdb_id,
            COUNT(DISTINCT COALESCE(NULLIF(imdb_id, ''), '<null>') || '|' || COALESCE(NULLIF(tmdb_id, ''), '<null>')) AS pair_count
        FROM media_index
        WHERE {HISTORICAL_FILTER}
          AND {phase_class_filter("resolve_series")}
          AND series_key IN ({placeholders})
          AND {phase_resolved_expr()}
        """,
        group_keys,
    ).fetchone()
    if row is None or int(row["pair_count"] or 0) != 1:
        return None
    return (
        str(row["imdb_id"]).strip() if row["imdb_id"] else None,
        str(row["tmdb_id"]).strip() if row["tmdb_id"] else None,
    )


def write_series_mapping(
    connection: sqlite3.Connection,
    *,
    group_key: str,
    canonical_title: str | None,
    canonical_year: int | None,
    season_bucket: str,
    mapping_source: str,
    mapping_confidence: str,
    imdb_id: str | None,
    tmdb_id: str | None,
) -> None:
    canonical = normalize_lookup_title(canonical_title or "")
    if not canonical:
        return
    connection.execute(
        """
        INSERT OR REPLACE INTO id_backfill_series_mapping (
            mapping_key, canonical_title, canonical_year, season_bucket, mapping_source, mapping_confidence, imdb_id, tmdb_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            mapping_key_for_series(group_key, canonical_year, season_bucket),
            canonical,
            canonical_year,
            season_bucket,
            mapping_source,
            mapping_confidence,
            imdb_id,
            tmdb_id,
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        ),
    )


def write_canonical_family(
    connection: sqlite3.Connection,
    *,
    media_kind: str,
    canonical_title: str | None,
    canonical_year: int | None,
    season_bucket: str,
    confidence: str,
    resolver_source: str,
    imdb_id: str | None,
    tmdb_id: str | None,
) -> str | None:
    canonical = normalize_lookup_title(canonical_title or "")
    if not canonical:
        return None
    family_key = canonical_family_key(
        media_kind,
        canonical,
        canonical_year,
        season_bucket,
        imdb_id,
        tmdb_id,
    )
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    connection.execute(
        """
        INSERT INTO canonical_title_family (
            family_key, media_kind, canonical_title, canonical_year, season_bucket,
            imdb_id, tmdb_id, confidence, resolver_source, evidence_count, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
        ON CONFLICT(family_key) DO UPDATE SET
            canonical_title = excluded.canonical_title,
            canonical_year = excluded.canonical_year,
            season_bucket = excluded.season_bucket,
            imdb_id = COALESCE(excluded.imdb_id, canonical_title_family.imdb_id),
            tmdb_id = COALESCE(excluded.tmdb_id, canonical_title_family.tmdb_id),
            confidence = excluded.confidence,
            resolver_source = excluded.resolver_source,
            evidence_count = canonical_title_family.evidence_count + 1,
            updated_at = excluded.updated_at
        """,
        (
            family_key,
            media_kind,
            canonical,
            canonical_year,
            season_bucket,
            imdb_id,
            tmdb_id,
            confidence,
            resolver_source,
            now,
        ),
    )
    return family_key


def write_canonical_variants(
    connection: sqlite3.Connection,
    *,
    family_key: str,
    media_kind: str,
    season_bucket: str,
    confidence: str,
    variant_source: str,
    titles: list[str],
    year_hint: int | None,
) -> None:
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    seen: set[str] = set()
    for title in titles:
        raw_title = collapse_spaces(str(title or ""))
        normalized_title = normalize_lookup_title(raw_title)
        if not normalized_title or normalized_title in seen:
            continue
        seen.add(normalized_title)
        connection.execute(
            """
            INSERT INTO canonical_title_variant (
                variant_key, family_key, media_kind, raw_title, normalized_title,
                year_hint, season_bucket, confidence, variant_source, evidence_count, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(variant_key, family_key) DO UPDATE SET
                raw_title = excluded.raw_title,
                confidence = excluded.confidence,
                variant_source = excluded.variant_source,
                evidence_count = canonical_title_variant.evidence_count + 1,
                updated_at = excluded.updated_at
            """,
            (
                canonical_variant_key(
                    media_kind, normalized_title, year_hint, season_bucket
                ),
                family_key,
                media_kind,
                raw_title,
                normalized_title,
                year_hint,
                season_bucket,
                confidence,
                variant_source,
                now,
            ),
        )


def sync_series_mappings_to_canonical_layer(connection: sqlite3.Connection) -> None:
    if get_meta(connection, "canonicalLayer.seriesMappingSync") == "true":
        return
    rows = connection.execute(
        """
        SELECT mapping_key, canonical_title, canonical_year, season_bucket,
               mapping_source, mapping_confidence, imdb_id, tmdb_id
        FROM id_backfill_series_mapping
        """
    ).fetchall()
    for row in rows:
        family_key = write_canonical_family(
            connection,
            media_kind="series",
            canonical_title=str(row["canonical_title"] or ""),
            canonical_year=int(row["canonical_year"])
            if row["canonical_year"] is not None
            else None,
            season_bucket=str(row["season_bucket"] or ""),
            confidence=str(row["mapping_confidence"] or "exact"),
            resolver_source=str(row["mapping_source"] or "series_mapping"),
            imdb_id=str(row["imdb_id"]).strip() if row["imdb_id"] else None,
            tmdb_id=str(row["tmdb_id"]).strip() if row["tmdb_id"] else None,
        )
        if not family_key:
            continue
        raw_variant = str(row["mapping_key"] or "").split("\u241f", 1)[0]
        write_canonical_variants(
            connection,
            family_key=family_key,
            media_kind="series",
            season_bucket=str(row["season_bucket"] or ""),
            confidence=str(row["mapping_confidence"] or "exact"),
            variant_source=str(row["mapping_source"] or "series_mapping"),
            titles=[raw_variant, str(row["canonical_title"] or "")],
            year_hint=int(row["canonical_year"])
            if row["canonical_year"] is not None
            else None,
        )
    set_meta(connection, "canonicalLayer.seriesMappingSync", "true")
    connection.commit()


def compatible_cluster_members(
    connection: sqlite3.Connection, context: GroupContext, result: ResolutionResult
) -> list[str]:
    if context.unit_kind != "series_cluster":
        return [context.group_key]
    members = fetch_series_cluster_members(connection, context.group_key)
    _cluster_title, cluster_year, cluster_bucket = split_series_cluster_key(
        context.group_key
    )
    compatible: list[str] = []
    canonical_year = result.canonical_year or context.year_hint
    for member in members:
        member_year = (
            int(member["year_hint"]) if member["year_hint"] is not None else None
        )
        if (
            cluster_year is not None
            and member_year is not None
            and abs(member_year - cluster_year) > 1
        ):
            continue
        if (
            canonical_year is not None
            and member_year is not None
            and abs(member_year - canonical_year) > 1
        ):
            continue
        member_bucket = str(member["season_bucket"] or "")
        if cluster_bucket and member_bucket and member_bucket != cluster_bucket:
            continue
        compatible.append(str(member["group_key"]))
    return compatible or (context.member_keys or [context.group_key])


def canonical_media_kind_for_context(context: GroupContext) -> str:
    if context.is_anime:
        return "anime"
    if context.media_type == "movie":
        return "movie"
    return "series"


def canonical_variant_titles_for_context(
    context: GroupContext,
    compatible_groups: list[str],
    result: ResolutionResult,
) -> list[str]:
    values: list[str] = []
    if context.unit_kind == "series_cluster":
        values.extend(compatible_groups)
    else:
        values.append(context.group_key)
    values.extend(context.title_candidates)
    if result.canonical_title:
        values.append(result.canonical_title)
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        cleaned = collapse_spaces(str(value or ""))
        normalized = normalize_lookup_title(cleaned)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(cleaned)
    return ordered


def season_bucket_for_context(
    phase: str, context: GroupContext, candidate_group_key: str
) -> str:
    if phase == "resolve_series":
        return split_series_cluster_key(candidate_group_key)[2]
    if context.media_type != "series":
        return ""
    if context.representative["season"] is not None:
        try:
            return f"s{int(context.representative['season']):02d}"
        except (TypeError, ValueError):
            return ""
    return ""


async def resolve_leftover_raw_groups(
    resolver: TMDBResolver,
    anime_catalog: AnimeCatalog,
    connection: sqlite3.Connection,
    context: GroupContext,
    *,
    limit: int = 3,
) -> tuple[ResolutionResult | None, list[str]]:
    if context.unit_kind != "series_cluster" or not context.member_keys:
        return None, []
    members = fetch_series_cluster_members(connection, context.group_key)
    ranked = sorted(
        members,
        key=lambda row: (
            int(row["noise_score"]),
            -int(row["row_count"]),
            0 if row["year_hint"] is not None else 1,
            len(str(row["cleaned_title"] or "")),
            str(row["group_key"]),
        ),
    )
    for member in ranked[:limit]:
        member_key = str(member["group_key"])
        raw_context = build_group_context(
            connection, "resolve_series_raw_fallback", member_key
        )
        if raw_context is None:
            continue
        raw_context.phase = "resolve_series"
        raw_context.unit_kind = "raw_fallback_group"
        raw_context.member_keys = [member_key]
        result = await resolve_group(resolver, anime_catalog, connection, raw_context)
        if result.status == "resolved" and result.confidence in {"exact", "strong"}:
            compatible = []
            target_title = normalize_lookup_title(
                result.canonical_title or raw_context.title_candidates[0] or ""
            )
            for sibling in members:
                sibling_title = normalize_lookup_title(
                    str(
                        sibling["cleaned_title"]
                        or sibling["representative_title"]
                        or sibling["group_key"]
                    )
                )
                if sibling_title != target_title:
                    continue
                compatible.append(str(sibling["group_key"]))
            if not compatible:
                compatible = [member_key]
            return result, compatible
    return None, []


async def process_resolution_batch(
    connection: sqlite3.Connection,
    *,
    phase: str,
    candidates: list[GroupCandidate],
    network_concurrency: int,
    anime_catalog: AnimeCatalog,
    http_timeout: int,
    apply_changes: bool,
) -> dict[str, int]:
    stats = Counter()
    breadcrumb(
        "process_resolution_batch.start",
        phase=phase,
        candidate_count=len(candidates),
        apply_changes=apply_changes,
    )
    timeout = aiohttp.ClientTimeout(total=http_timeout)
    connector = aiohttp.TCPConnector(limit=max(network_concurrency, 1))
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        resolver = TMDBResolver(session)
        if phase.endswith("anime"):
            await anime_catalog.load(session)
        semaphore = asyncio.Semaphore(max(network_concurrency, 1))

        async def handle(
            index: int, candidate: GroupCandidate
        ) -> tuple[GroupCandidate, GroupContext | None, ResolutionResult | None]:
            async with semaphore:
                if index <= 3:
                    breadcrumb(
                        "handle.start",
                        phase=phase,
                        index=index,
                        group_key=candidate.group_key,
                    )
                context = build_group_context(connection, phase, candidate.group_key)
                if index <= 3:
                    breadcrumb(
                        "handle.after_build_context",
                        phase=phase,
                        index=index,
                        group_key=candidate.group_key,
                        has_context=context is not None,
                    )
                if context is None:
                    return candidate, None, None
                local_pair = (
                    cluster_resolved_pair(connection, context.member_keys or [])
                    if phase == "resolve_series"
                    else None
                )
                if index <= 3:
                    breadcrumb(
                        "handle.after_local_pair",
                        phase=phase,
                        index=index,
                        group_key=candidate.group_key,
                        has_local_pair=local_pair is not None,
                    )
                if local_pair is not None:
                    imdb_id, tmdb_id = local_pair
                    result = ResolutionResult(
                        status="resolved",
                        confidence="exact",
                        imdb_id=imdb_id,
                        tmdb_id=tmdb_id,
                        resolver_source="local_cluster_propagation",
                        canonical_title=context.title_candidates[0]
                        if context.title_candidates
                        else None,
                        canonical_year=context.year_hint,
                        details={"titles": context.title_candidates},
                    )
                else:
                    low_value_reason = (
                        cluster_low_value_reason(context)
                        if phase == "resolve_series"
                        else None
                    )
                    if index <= 3:
                        breadcrumb(
                            "handle.after_low_value",
                            phase=phase,
                            index=index,
                            group_key=candidate.group_key,
                            low_value_reason=low_value_reason,
                        )
                    if low_value_reason:
                        result = ResolutionResult(
                            status="miss",
                            reason=low_value_reason,
                            details={"titles": context.title_candidates},
                        )
                    else:
                        if index <= 3:
                            breadcrumb(
                                "handle.before_resolve_group",
                                phase=phase,
                                index=index,
                                group_key=candidate.group_key,
                            )
                        result = await resolve_group(
                            resolver, anime_catalog, connection, context
                        )
                        if index <= 3:
                            breadcrumb(
                                "handle.after_resolve_group",
                                phase=phase,
                                index=index,
                                group_key=candidate.group_key,
                                result_status=result.status,
                                result_confidence=result.confidence,
                                result_reason=result.reason,
                            )
                        if phase == "resolve_series" and (
                            result.status != "resolved"
                            or result.confidence not in {"exact", "strong"}
                        ):
                            if index <= 3:
                                breadcrumb(
                                    "handle.before_raw_fallback",
                                    phase=phase,
                                    index=index,
                                    group_key=candidate.group_key,
                                )
                            (
                                fallback_result,
                                fallback_groups,
                            ) = await resolve_leftover_raw_groups(
                                resolver,
                                anime_catalog,
                                connection,
                                context,
                            )
                            if index <= 3:
                                breadcrumb(
                                    "handle.after_raw_fallback",
                                    phase=phase,
                                    index=index,
                                    group_key=candidate.group_key,
                                    fallback_status=None
                                    if fallback_result is None
                                    else fallback_result.status,
                                    fallback_confidence=None
                                    if fallback_result is None
                                    else fallback_result.confidence,
                                    fallback_reason=None
                                    if fallback_result is None
                                    else fallback_result.reason,
                                    fallback_group_count=len(fallback_groups),
                                )
                            if fallback_result is not None:
                                if fallback_result.details is None:
                                    fallback_result.details = {}
                                fallback_result.details["fallbackGroups"] = (
                                    fallback_groups
                                )
                                fallback_result.resolver_source = f"{fallback_result.resolver_source or 'resolver'}_raw_fallback"
                                result = fallback_result
                if index <= 3:
                    breadcrumb(
                        "handle.return",
                        phase=phase,
                        index=index,
                        group_key=candidate.group_key,
                        result_status=None if result is None else result.status,
                    )
                return candidate, context, result

        results = await asyncio.gather(
            *(
                handle(index, candidate)
                for index, candidate in enumerate(candidates, start=1)
            )
        )
    breadcrumb(
        "process_resolution_batch.after_gather", phase=phase, result_count=len(results)
    )

    breadcrumb("process_resolution_batch.before_begin_immediate", phase=phase)
    connection.execute("BEGIN IMMEDIATE")
    breadcrumb("process_resolution_batch.after_begin_immediate", phase=phase)
    try:
        for index, (candidate, context, result) in enumerate(results, start=1):
            stats["groupsScanned"] += 1
            if index == 1:
                breadcrumb(
                    "process_resolution_batch.first_result",
                    phase=phase,
                    candidate_group=candidate.group_key,
                    has_context=context is not None,
                    result_status=None if result is None else result.status,
                    result_confidence=None if result is None else result.confidence,
                    result_reason=None if result is None else result.reason,
                )
            if context is None or result is None:
                stats["groupsMissed"] += 1
                continue
            compatible_groups = context.member_keys or [candidate.group_key]
            write_resolver_cache(connection, context, result)
            sample_title = (
                context.title_candidates[0]
                if context.title_candidates
                else context.group_key
            )
            if result.status != "resolved" or result.confidence not in {
                "exact",
                "strong",
            }:
                stats["groupsSkipped"] += 1
                if phase == "resolve_series" and result.reason in {
                    "cluster_noise_only",
                    "cluster_numeric_only",
                    "no_clean_titles",
                }:
                    stats["lowValueSkipped"] += 1
                write_audit(
                    connection,
                    phase=phase,
                    group_key=candidate.group_key,
                    status=result.status,
                    reason=result.reason or "unresolved",
                    sample_title=sample_title,
                    sample_year=context.year_hint,
                    resolver_source=result.resolver_source,
                    details=result.details or {},
                )
                continue
            if phase == "resolve_series":
                compatible_groups = (
                    result.details.get("fallbackGroups") if result.details else None
                )
                if compatible_groups:
                    compatible_groups = [str(item) for item in compatible_groups]
                    stats["leftoverRawResolved"] += 1
                elif (result.resolver_source or "").startswith(
                    "local_cluster_propagation"
                ):
                    compatible_groups = compatible_cluster_members(
                        connection, context, result
                    )
                    stats["siblingGroupsPropagated"] += max(
                        len(compatible_groups) - 1, 0
                    )
                else:
                    compatible_groups = compatible_cluster_members(
                        connection, context, result
                    )
                    stats["clustersResolved"] += 1
                unresolved_ids = historical_unresolved_ids_for_groups(
                    connection, phase, compatible_groups
                )
                sibling_count = max(len(compatible_groups) - 1, 0)
                if sibling_count > 0 and not (result.resolver_source or "").startswith(
                    "local_cluster_propagation"
                ):
                    stats["siblingGroupsPropagated"] += sibling_count
                stage_group_key = candidate.group_key
            else:
                unresolved_ids = historical_unresolved_ids(
                    connection, phase, candidate.group_key
                )
                stage_group_key = candidate.group_key
            if not unresolved_ids:
                stats["groupsNoop"] += 1
                continue
            staged = stage_updates(
                connection,
                phase=phase,
                group_key=stage_group_key,
                torrent_ids=unresolved_ids,
                imdb_id=result.imdb_id,
                tmdb_id=result.tmdb_id,
                confidence=result.confidence,
                resolver_source=result.resolver_source or "resolver",
            )
            stats["groupsResolved"] += 1
            stats["rowsStaged"] += staged
            write_audit(
                connection,
                phase=phase,
                group_key=candidate.group_key,
                status="resolved",
                reason=result.confidence,
                sample_title=sample_title,
                sample_year=context.year_hint,
                resolver_source=result.resolver_source,
                details={
                    "canonical_title": result.canonical_title,
                    "canonical_year": result.canonical_year,
                    "imdb_id": result.imdb_id,
                    "tmdb_id": result.tmdb_id,
                    "memberKeys": context.member_keys
                    if phase == "resolve_series"
                    else None,
                    **(result.details or {}),
                },
            )
            media_kind = canonical_media_kind_for_context(context)
            season_bucket = season_bucket_for_context(
                phase, context, candidate.group_key
            )
            family_key = write_canonical_family(
                connection,
                media_kind=media_kind,
                canonical_title=result.canonical_title or sample_title,
                canonical_year=result.canonical_year or context.year_hint,
                season_bucket=season_bucket,
                confidence=result.confidence,
                resolver_source=result.resolver_source or "resolver",
                imdb_id=result.imdb_id,
                tmdb_id=result.tmdb_id,
            )
            if family_key:
                write_canonical_variants(
                    connection,
                    family_key=family_key,
                    media_kind=media_kind,
                    season_bucket=season_bucket,
                    confidence=result.confidence,
                    variant_source=result.resolver_source or "resolver",
                    titles=canonical_variant_titles_for_context(
                        context, compatible_groups, result
                    ),
                    year_hint=result.canonical_year or context.year_hint,
                )
            if phase == "resolve_series":
                for member_key in compatible_groups:
                    write_series_mapping(
                        connection,
                        group_key=str(member_key),
                        canonical_title=result.canonical_title or sample_title,
                        canonical_year=result.canonical_year or context.year_hint,
                        season_bucket=season_bucket,
                        mapping_source=result.resolver_source or "resolver",
                        mapping_confidence=result.confidence,
                        imdb_id=result.imdb_id,
                        tmdb_id=result.tmdb_id,
                    )
            if index == len(results):
                breadcrumb(
                    "process_resolution_batch.after_loop",
                    phase=phase,
                    groups_scanned=stats["groupsScanned"],
                    groups_resolved=stats["groupsResolved"],
                    groups_skipped=stats["groupsSkipped"],
                    rows_staged=stats["rowsStaged"],
                )
        merged = 0
        if apply_changes:
            breadcrumb("process_resolution_batch.before_merge_stage", phase=phase)
            merged = merge_stage(connection, phase)
            stats["rowsMerged"] = merged
            breadcrumb(
                "process_resolution_batch.after_merge_stage", phase=phase, merged=merged
            )
        breadcrumb("process_resolution_batch.before_commit", phase=phase)
        connection.commit()
        breadcrumb("process_resolution_batch.after_commit", phase=phase)
    except Exception:
        breadcrumb("process_resolution_batch.exception", phase=phase)
        connection.rollback()
        raise
    return dict(stats)


def process_propagation_batch(
    connection: sqlite3.Connection,
    *,
    phase: str,
    candidates: list[GroupCandidate],
    apply_changes: bool,
) -> dict[str, int]:
    stats = Counter()
    if not candidates:
        return dict(stats)
    group_keys = [candidate.group_key for candidate in candidates]
    placeholders = ", ".join("?" for _ in group_keys)
    group_col = phase_group_column(phase)
    class_filter = phase_class_filter(phase)
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            "DELETE FROM id_backfill_stage WHERE run_phase = ?", (phase,)
        )
        connection.execute(
            f"""
            INSERT OR REPLACE INTO id_backfill_stage (
                run_phase, group_key, torrent_id, imdb_id, tmdb_id, confidence, resolver_source, updated_at
            )
            SELECT
                ?, u.{group_col}, u.torrent_id, s.imdb_id, s.tmdb_id, 'exact', 'local_propagation', ?
            FROM media_index u
            JOIN id_backfill_propagation_source s
              ON s.phase = ?
             AND s.group_key = u.{group_col}
            WHERE {HISTORICAL_FILTER}
              AND {class_filter}
              AND u.{group_col} IN ({placeholders})
              AND {qualify_expr(phase_missing_expr(), "u")}
            """,
            [phase, timestamp, phase, *group_keys],
        )
        resolved_group_rows = connection.execute(
            "SELECT DISTINCT group_key FROM id_backfill_stage WHERE run_phase = ?",
            (phase,),
        ).fetchall()
        resolved_groups = {str(row[0]) for row in resolved_group_rows}
        stats["groupsScanned"] = len(candidates)
        stats["groupsResolved"] = len(resolved_groups)
        stats["groupsSkipped"] = max(len(candidates) - len(resolved_groups), 0)
        stats["rowsStaged"] = int(
            connection.execute(
                "SELECT count(*) FROM id_backfill_stage WHERE run_phase = ?", (phase,)
            ).fetchone()[0]
        )
        for candidate in candidates:
            if candidate.group_key in resolved_groups:
                continue
            write_audit(
                connection,
                phase=phase,
                group_key=candidate.group_key,
                status="skipped",
                reason="no_unresolved_rows",
                sample_title=candidate.group_key,
                sample_year=None,
                resolver_source="local_propagation",
                details={},
            )
        merged = 0
        if apply_changes:
            merged = merge_stage(connection, phase)
            stats["rowsMerged"] = merged
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return dict(stats)


def write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def breadcrumb(event: str, **fields: Any) -> None:
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    details = " ".join(f"{key}={fields[key]!r}" for key in sorted(fields))
    line = f"[{timestamp}] breadcrumb {event}"
    if details:
        line += f" {details}"
    print(line, flush=True)
    try:
        BACKFILL_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with BACKFILL_LOG_PATH.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    except OSError:
        pass


def phase_done(connection: sqlite3.Connection, phase: str) -> bool:
    return get_meta(connection, f"{phase}.done") == "true"


def set_phase_done(connection: sqlite3.Connection, phase: str) -> None:
    set_meta(connection, f"{phase}.done", "true")


def analyze_and_optimize(connection: sqlite3.Connection) -> None:
    connection.execute("ANALYZE")
    connection.execute("PRAGMA optimize")
    connection.commit()


def build_phase_progress_payload(
    connection: sqlite3.Connection, phase: str, cursor: str
) -> dict[str, Any]:
    total_groups = int_meta(connection, phase_total_key(phase))
    groups_scanned = int_meta(connection, phase_counter_key(phase, "groupsScanned"))
    groups_resolved = int_meta(connection, phase_counter_key(phase, "groupsResolved"))
    groups_skipped = int_meta(connection, phase_counter_key(phase, "groupsSkipped"))
    groups_noop = int_meta(connection, phase_counter_key(phase, "groupsNoop"))
    groups_missed = int_meta(connection, phase_counter_key(phase, "groupsMissed"))
    rows_merged = int_meta(connection, phase_counter_key(phase, "rowsMerged"))
    rows_staged = int_meta(connection, phase_counter_key(phase, "rowsStaged"))
    clusters_resolved = int_meta(
        connection, phase_counter_key(phase, "clustersResolved")
    )
    sibling_groups_propagated = int_meta(
        connection, phase_counter_key(phase, "siblingGroupsPropagated")
    )
    leftover_raw_resolved = int_meta(
        connection, phase_counter_key(phase, "leftoverRawResolved")
    )
    low_value_skipped = int_meta(
        connection, phase_counter_key(phase, "lowValueSkipped")
    )
    started_at_ts = float_meta(connection, phase_started_at_key(phase))
    now_ts = time.time()
    elapsed_seconds = (
        max(now_ts - started_at_ts, 0.0) if started_at_ts is not None else None
    )
    groups_per_second = (
        (groups_scanned / elapsed_seconds)
        if elapsed_seconds and elapsed_seconds > 0
        else None
    )
    remaining_groups = (
        max(total_groups - groups_scanned, 0) if total_groups > 0 else None
    )
    eta_seconds = (
        (remaining_groups / groups_per_second)
        if remaining_groups is not None and groups_per_second and groups_per_second > 0
        else None
    )
    progress_percent = (
        ((groups_scanned / total_groups) * 100.0) if total_groups > 0 else None
    )
    return {
        "phase": phase,
        "phaseUnit": "cluster" if phase == "resolve_series" else "group",
        "cursor": cursor,
        "processedGroups": groups_scanned,
        "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "initializingTotals": total_groups <= 0,
        "phaseTotals": {
            "totalGroups": total_groups,
            "remainingGroups": remaining_groups,
            "progressPercent": round(progress_percent, 3)
            if progress_percent is not None
            else None,
        },
        "rates": {
            "elapsedSeconds": round(elapsed_seconds, 3)
            if elapsed_seconds is not None
            else None,
            "groupsPerSecond": round(groups_per_second, 4)
            if groups_per_second is not None
            else None,
            "etaSeconds": round(eta_seconds, 1) if eta_seconds is not None else None,
        },
        "stats": {
            "clustersResolved": clusters_resolved,
            "groupsMissed": groups_missed,
            "groupsNoop": groups_noop,
            "groupsResolved": groups_resolved,
            "groupsScanned": groups_scanned,
            "groupsSkipped": groups_skipped,
            "leftoverRawResolved": leftover_raw_resolved,
            "lowValueSkipped": low_value_skipped,
            "rowsMerged": rows_merged,
            "rowsStaged": rows_staged,
            "siblingGroupsPropagated": sibling_groups_propagated,
        },
    }


def build_initializing_payload(
    phase: str, *, message: str, cursor: str = ""
) -> dict[str, Any]:
    return {
        "phase": phase,
        "phaseUnit": "cluster" if phase == "resolve_series" else "group",
        "cursor": cursor,
        "processedGroups": 0,
        "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "initializingTotals": True,
        "status": message,
        "phaseTotals": {
            "totalGroups": 0,
            "remainingGroups": None,
            "progressPercent": None,
        },
        "rates": {
            "elapsedSeconds": None,
            "groupsPerSecond": None,
            "etaSeconds": None,
        },
        "stats": {
            "clustersResolved": 0,
            "groupsMissed": 0,
            "groupsNoop": 0,
            "groupsResolved": 0,
            "groupsScanned": 0,
            "groupsSkipped": 0,
            "leftoverRawResolved": 0,
            "lowValueSkipped": 0,
            "rowsMerged": 0,
            "rowsStaged": 0,
            "siblingGroupsPropagated": 0,
        },
    }


async def run_phase(
    connection: sqlite3.Connection,
    *,
    phase: str,
    max_groups: int,
    group_batch_size: int,
    network_concurrency: int,
    anime_catalog: AnimeCatalog,
    http_timeout: int,
    apply_changes: bool,
    state_path: Path,
    analyze_every_batches: int,
) -> None:
    cursor_key = phase_cursor_key(phase)
    cursor = get_meta(connection, cursor_key, "") or ""
    batches = 0
    processed_groups = 0
    propagation = phase.startswith("propagate_")
    breadcrumb(
        "run_phase.start",
        phase=phase,
        cursor=cursor,
        propagation=propagation,
        max_groups=max_groups,
        batch_size=group_batch_size,
    )
    if not get_meta(connection, phase_started_at_key(phase)):
        set_meta(connection, phase_started_at_key(phase), str(time.time()))
        set_meta(connection, "currentPhase", phase)
        set_meta(
            connection, "lastRunAt", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        )
        connection.commit()
        write_state(state_path, build_phase_progress_payload(connection, phase, cursor))
    if int_meta(connection, phase_total_key(phase)) <= 0:
        breadcrumb("run_phase.before_count_totals", phase=phase)
        set_meta(
            connection,
            phase_total_key(phase),
            str(count_phase_groups(connection, phase, propagation=propagation)),
        )
        connection.commit()
        breadcrumb(
            "run_phase.after_count_totals",
            phase=phase,
            total_groups=int_meta(connection, phase_total_key(phase)),
        )
        write_state(state_path, build_phase_progress_payload(connection, phase, cursor))
    while True:
        remaining = (
            max_groups - processed_groups if max_groups > 0 else group_batch_size
        )
        if max_groups > 0 and remaining <= 0:
            break
        limit = min(group_batch_size, remaining) if max_groups > 0 else group_batch_size
        breadcrumb(
            "run_phase.before_fetch_candidates",
            phase=phase,
            cursor=cursor,
            limit=limit,
            batch_number=batches + 1,
        )
        candidates = fetch_group_candidates(
            connection, phase, propagation=propagation, cursor=cursor, limit=limit
        )
        breadcrumb(
            "run_phase.after_fetch_candidates",
            phase=phase,
            fetched=len(candidates),
            first_group=None if not candidates else candidates[0].group_key,
            batch_number=batches + 1,
        )
        if not candidates:
            set_phase_done(connection, phase)
            set_meta(connection, cursor_key, cursor)
            set_meta(
                connection,
                phase_completed_at_key(phase),
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            )
            connection.commit()
            write_state(
                state_path, build_phase_progress_payload(connection, phase, cursor)
            )
            break
        if propagation:
            stats = process_propagation_batch(
                connection,
                phase=phase,
                candidates=candidates,
                apply_changes=apply_changes,
            )
        else:
            breadcrumb(
                "run_phase.before_process_resolution_batch",
                phase=phase,
                candidate_count=len(candidates),
                batch_number=batches + 1,
            )
            stats = await process_resolution_batch(
                connection,
                phase=phase,
                candidates=candidates,
                network_concurrency=network_concurrency,
                anime_catalog=anime_catalog,
                http_timeout=http_timeout,
                apply_changes=apply_changes,
            )
            breadcrumb(
                "run_phase.after_process_resolution_batch",
                phase=phase,
                batch_number=batches + 1,
                stats=dict(stats),
            )
        processed_groups += len(candidates)
        cursor = candidates[-1].group_key
        set_meta(connection, cursor_key, cursor)
        set_meta(connection, "currentPhase", phase)
        set_meta(
            connection, "lastRunAt", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        )
        for key, value in stats.items():
            update_counter(connection, phase, key, int(value))
        breadcrumb(
            "run_phase.before_commit_batch_stats",
            phase=phase,
            batch_number=batches + 1,
            cursor=cursor,
        )
        connection.commit()
        breadcrumb(
            "run_phase.after_commit_batch_stats",
            phase=phase,
            batch_number=batches + 1,
            cursor=cursor,
        )
        batches += 1
        if analyze_every_batches > 0 and batches % analyze_every_batches == 0:
            analyze_and_optimize(connection)
        write_state(state_path, build_phase_progress_payload(connection, phase, cursor))
        if len(candidates) < limit:
            set_phase_done(connection, phase)
            set_meta(
                connection,
                phase_completed_at_key(phase),
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            )
            connection.commit()
            write_state(
                state_path, build_phase_progress_payload(connection, phase, cursor)
            )
            break


def selected_phases(args: argparse.Namespace) -> list[str]:
    if args.phase:
        return [args.phase]
    return list(PHASES[:-1])


def summary_snapshot(connection: sqlite3.Connection) -> dict[str, Any]:
    snapshot = {
        "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "coverage": {},
        "phases": {},
    }
    for label, query in {
        "totalRows": "SELECT count(*) FROM media_index",
        "historicalRows": f"SELECT count(*) FROM media_index WHERE {HISTORICAL_FILTER}",
        "rowsWithImdb": "SELECT count(*) FROM media_index WHERE imdb_id IS NOT NULL AND imdb_id <> ''",
        "rowsWithTmdb": "SELECT count(*) FROM media_index WHERE tmdb_id IS NOT NULL AND tmdb_id <> ''",
        "historicalWithImdb": f"SELECT count(*) FROM media_index WHERE {HISTORICAL_FILTER} AND imdb_id IS NOT NULL AND imdb_id <> ''",
        "historicalWithTmdb": f"SELECT count(*) FROM media_index WHERE {HISTORICAL_FILTER} AND tmdb_id IS NOT NULL AND tmdb_id <> ''",
    }.items():
        snapshot["coverage"][label] = int(connection.execute(query).fetchone()[0])
    for phase in PHASES[:-1]:
        snapshot["phases"][phase] = {
            "done": phase_done(connection, phase),
            "cursor": get_meta(connection, phase_cursor_key(phase), ""),
            "totalGroups": int_meta(connection, phase_total_key(phase)),
            "completedAt": get_meta(connection, phase_completed_at_key(phase), ""),
            "clustersResolved": int_meta(
                connection, phase_counter_key(phase, "clustersResolved")
            ),
            "groupsResolved": int_meta(
                connection, phase_counter_key(phase, "groupsResolved")
            ),
            "groupsScanned": int_meta(
                connection, phase_counter_key(phase, "groupsScanned")
            ),
            "groupsSkipped": int_meta(
                connection, phase_counter_key(phase, "groupsSkipped")
            ),
            "leftoverRawResolved": int_meta(
                connection, phase_counter_key(phase, "leftoverRawResolved")
            ),
            "rowsMerged": int_meta(connection, phase_counter_key(phase, "rowsMerged")),
            "siblingGroupsPropagated": int_meta(
                connection, phase_counter_key(phase, "siblingGroupsPropagated")
            ),
        }
    return snapshot


def reset_phase_state(connection: sqlite3.Connection, phase: str) -> None:
    keys = [
        phase_cursor_key(phase),
        f"{phase}.done",
        phase_total_key(phase),
        phase_started_at_key(phase),
        phase_completed_at_key(phase),
        propagation_source_ready_key(phase),
        series_cluster_ready_key(phase),
    ]
    for key in keys:
        set_meta(connection, key, "false" if key.endswith(".done") else "")
    for counter in (
        "groupsScanned",
        "groupsResolved",
        "groupsSkipped",
        "groupsNoop",
        "groupsMissed",
        "rowsMerged",
        "rowsStaged",
        "clustersResolved",
        "siblingGroupsPropagated",
        "leftoverRawResolved",
        "lowValueSkipped",
    ):
        set_meta(connection, phase_counter_key(phase, counter), "0")


def ensure_resolve_series_strategy(connection: sqlite3.Connection) -> None:
    current = get_meta(connection, phase_strategy_version_key("resolve_series"))
    if current == RESOLVE_SERIES_STRATEGY_VERSION:
        return
    reset_phase_state(connection, "resolve_series")
    connection.execute("DELETE FROM id_backfill_audit WHERE phase = 'resolve_series'")
    connection.execute(
        "DELETE FROM id_backfill_stage WHERE run_phase = 'resolve_series'"
    )
    connection.execute(
        "DELETE FROM id_backfill_series_cluster WHERE phase = 'resolve_series'"
    )
    connection.execute(
        "DELETE FROM id_backfill_series_parsed WHERE phase = 'resolve_series'"
    )
    connection.execute(
        "DELETE FROM id_backfill_series_resolved_group WHERE phase = 'resolve_series'"
    )
    set_meta(
        connection,
        phase_strategy_version_key("resolve_series"),
        RESOLVE_SERIES_STRATEGY_VERSION,
    )
    connection.commit()


async def main() -> int:
    args = parse_args()
    if args.audit_only and args.apply:
        raise SystemExit("--audit-only and --apply are mutually exclusive")
    apply_changes = bool(args.apply and not args.audit_only)
    db_path = resolve_search_db_target(
        Path(args.search_db),
        apply_changes=apply_changes,
        allow_live_search_db=bool(args.allow_live_search_db),
    )
    state_path = Path(args.state_path)
    anime_cache_dir = Path(args.anime_cache_dir)
    connection = open_db(db_path)
    ensure_support_tables(connection)
    ensure_phase_indexes(connection)
    ensure_resolve_series_strategy(connection)
    sync_series_mappings_to_canonical_layer(connection)

    phases = selected_phases(args)
    if args.reset_all:
        for phase in PHASES[:-1]:
            reset_phase_state(connection, phase)
        set_meta(
            connection,
            phase_strategy_version_key("resolve_series"),
            RESOLVE_SERIES_STRATEGY_VERSION,
        )
        connection.commit()
    if args.reset_phase:
        for phase in phases:
            reset_phase_state(connection, phase)
            if phase == "resolve_series":
                set_meta(
                    connection,
                    phase_strategy_version_key("resolve_series"),
                    RESOLVE_SERIES_STRATEGY_VERSION,
                )
        connection.commit()

    anime_catalog = AnimeCatalog(anime_cache_dir, args.anime_cache_ttl)
    for phase in phases:
        if phase_done(connection, phase):
            continue
        if (
            phase.startswith("propagate_")
            and get_meta(connection, propagation_source_ready_key(phase)) != "true"
        ):
            rebuild_propagation_source(connection, phase)
        if (
            phase == "resolve_series"
            and get_meta(connection, series_cluster_ready_key(phase)) != "true"
        ):
            set_meta(connection, "currentPhase", phase)
            set_meta(
                connection,
                "lastRunAt",
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            )
            connection.commit()
            write_state(
                state_path,
                build_initializing_payload(phase, message="rebuilding_series_clusters"),
            )
            rebuild_series_clusters(connection, phase)
        await run_phase(
            connection,
            phase=phase,
            max_groups=args.max_groups,
            group_batch_size=args.group_batch_size,
            network_concurrency=args.network_concurrency,
            anime_catalog=anime_catalog,
            http_timeout=args.http_timeout,
            apply_changes=apply_changes,
            state_path=state_path,
            analyze_every_batches=args.analyze_every_batches,
        )
    snapshot = summary_snapshot(connection)
    write_state(state_path, snapshot)
    if args.print_summary or True:
        print(json.dumps(snapshot, indent=2, sort_keys=True))
    connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
