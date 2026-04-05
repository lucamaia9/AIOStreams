#!/usr/bin/env python3
"""
Shared canonical resolution logic for live DHT enrichment.

This module provides:
1. Canonical layer lookups (instant, no API)
2. Title normalization for matching
3. Batch resolution via TMDB API
4. Anime catalog integration

Used by:
- bitmagnet_smart_hint.py (pre-classification lookup)
- live_enrichment_worker.py (post-classification enrichment)
- promote_bitmagnet_live_to_compact.py (fallback enrichment)
"""

from __future__ import annotations

import re
import sqlite3
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_SEARCH_DB = Path(
    "/home/ubuntu/aiostreams/data/comet-fresh/magnetico/active.search.sqlite3"
)

WORD_PATTERN = re.compile(r"[^\W_]+", re.UNICODE)
MULTISPACE = re.compile(r"\s+")
YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")

TITLE_CLEAN_PATTERNS = [
    re.compile(r"\[[^\]]*\]"),
    re.compile(r"\([^\)]*\)"),
    re.compile(r"\{[^\}]*\}"),
    re.compile(
        r"\b(?:2160p|1080p|1080i|720p|576p|540p|480p|360p|4k|8k|uhd|hdr|webrip|web[- .]?dl|bluray|brrip|bdrip|hdtv|dvdrip|x264|x265|hevc|avc|aac|dts|truehd|atmos|10bit|remux|proper|repack|internal|multi|dual)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\b(?:mkv|mp4|avi|wmv|mov|webm|ts|m4v|mpeg|mpg)\b", re.IGNORECASE),
    re.compile(
        r"\b(?:s\d{1,2}e\d{1,3}(?:[ ._\-]*e?\d{1,3})*|\d{1,2}x\d{1,3}(?:[ ._\-]*(?:x|e)?\d{1,3})*|season\s*\d{1,2}|episode\s*\d{1,3}|ep\s*\d{1,3}|complete\s*(?:season|series))\b",
        re.IGNORECASE,
    ),
    YEAR_PATTERN,
]


@dataclass
class CanonicalMatch:
    """Result of a canonical layer lookup."""

    matched: bool
    tmdb_id: str | None = None
    imdb_id: str | None = None
    canonical_title: str | None = None
    canonical_year: int | None = None
    confidence: str | None = None
    family_key: str | None = None


@dataclass
class ResolutionRequest:
    """A request to resolve a title to IDs."""

    raw_title: str
    normalized_title: str
    year_hint: int | None
    content_class: str
    info_hash: str
    season_bucket: str = ""


def normalize_title(text: str) -> str:
    """Normalize a title for matching."""
    cleaned = unicodedata.normalize("NFKC", str(text or ""))
    cleaned = (
        cleaned.replace("_", " ").replace(".", " ").replace("/", " ").replace("\\", " ")
    )
    for pattern in TITLE_CLEAN_PATTERNS:
        cleaned = pattern.sub(" ", cleaned)
    cleaned = re.sub(r"[^\w\s-]", " ", cleaned, flags=re.UNICODE)
    return MULTISPACE.sub(" ", cleaned).strip().lower()


def title_key_for(text: str, normalized: str | None = None) -> str:
    """Generate a title key (first 12 tokens)."""
    tokens = WORD_PATTERN.findall(normalized if normalized else normalize_title(text))
    return " ".join(tokens[:12])


def extract_year(text: str) -> int | None:
    """Extract a year from text."""
    years = [int(m.group(1)) for m in YEAR_PATTERN.finditer(str(text or ""))]
    if not years:
        return None
    import datetime

    max_year = datetime.datetime.now().year + 1
    for year in years:
        if 1900 <= year <= max_year:
            return year
    return None


def season_bucket_for(season: int | None) -> str:
    """Generate a season bucket string."""
    if season is None:
        return ""
    return f"s{int(season):02d}"


class CanonicalLayer:
    """
    Read-only interface to the canonical title layer.

    The canonical layer stores learned title->ID mappings from historical backfill.
    This provides instant lookups without TMDB API calls.
    """

    def __init__(self, db_path: Path | str | None = None):
        self.db_path = Path(db_path) if db_path else DEFAULT_SEARCH_DB
        self._conn: sqlite3.Connection | None = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def lookup(
        self,
        title: str,
        year: int | None = None,
        content_class: str = "movie",
        season: int | None = None,
    ) -> CanonicalMatch:
        """
        Look up a title in the canonical layer.

        Args:
            title: Raw torrent title
            year: Optional year hint
            content_class: movie, episode, anime_episode, etc.
            season: Optional season number for series

        Returns:
            CanonicalMatch with resolved IDs if found
        """
        normalized = normalize_title(title)
        if not normalized:
            return CanonicalMatch(matched=False)

        media_kind = self._content_class_to_media_kind(content_class)
        season_bucket = season_bucket_for(season)

        # Try exact match first
        match = self._lookup_exact(normalized, year, media_kind, season_bucket)
        if match.matched:
            return match

        # Try without year (for titles with wrong year)
        if year:
            match = self._lookup_exact(normalized, None, media_kind, season_bucket)
            if match.matched:
                return match

        # Try fuzzy prefix match for series
        if media_kind == "series":
            match = self._lookup_prefix(normalized, media_kind, season_bucket)
            if match.matched:
                return match

        return CanonicalMatch(matched=False)

    def _content_class_to_media_kind(self, content_class: str) -> str:
        """Map content_class to media_kind used in canonical layer."""
        if content_class in ("episode", "multi_episode", "season_pack"):
            return "series"
        elif content_class in ("anime_episode", "anime_pack"):
            return "series"
        elif content_class == "movie":
            return "movie"
        elif content_class == "unknown_video":
            return "movie"
        return "movie"

    def _lookup_exact(
        self,
        normalized: str,
        year: int | None,
        media_kind: str,
        season_bucket: str,
    ) -> CanonicalMatch:
        """Try exact normalized title match."""
        query = """
            SELECT
                f.tmdb_id,
                f.imdb_id,
                f.canonical_title,
                f.canonical_year,
                f.confidence,
                v.family_key
            FROM canonical_title_variant v
            JOIN canonical_title_family f ON f.family_key = v.family_key
            WHERE v.media_kind = ?
              AND v.normalized_title = ?
              AND v.season_bucket = ?
        """
        params: list[Any] = [media_kind, normalized, season_bucket]

        if year:
            query += " AND (v.year_hint = ? OR v.year_hint IS NULL)"
            params.append(year)

        query += " ORDER BY v.evidence_count DESC, f.evidence_count DESC LIMIT 1"

        row = self.conn.execute(query, params).fetchone()
        if row:
            return CanonicalMatch(
                matched=True,
                tmdb_id=row["tmdb_id"],
                imdb_id=row["imdb_id"],
                canonical_title=row["canonical_title"],
                canonical_year=row["canonical_year"],
                confidence=row["confidence"],
                family_key=row["family_key"],
            )
        return CanonicalMatch(matched=False)

    def _lookup_prefix(
        self,
        normalized: str,
        media_kind: str,
        season_bucket: str,
    ) -> CanonicalMatch:
        """Try prefix match for series (handles partial titles)."""
        tokens = normalized.split()
        if len(tokens) < 2:
            return CanonicalMatch(matched=False)

        # Try progressively shorter prefixes
        for i in range(len(tokens) - 1, max(0, len(tokens) - 4), -1):
            prefix = " ".join(tokens[:i])
            if len(prefix) < 3:
                continue

            row = self.conn.execute(
                """
                SELECT
                    f.tmdb_id,
                    f.imdb_id,
                    f.canonical_title,
                    f.canonical_year,
                    f.confidence,
                    v.family_key
                FROM canonical_title_variant v
                JOIN canonical_title_family f ON f.family_key = v.family_key
                WHERE v.media_kind = ?
                  AND v.normalized_title LIKE ? || '%'
                  AND v.season_bucket = ?
                  AND f.tmdb_id IS NOT NULL
                ORDER BY v.evidence_count DESC, f.evidence_count DESC
                LIMIT 1
                """,
                [media_kind, prefix, season_bucket],
            ).fetchone()

            if row:
                return CanonicalMatch(
                    matched=True,
                    tmdb_id=row["tmdb_id"],
                    imdb_id=row["imdb_id"],
                    canonical_title=row["canonical_title"],
                    canonical_year=row["canonical_year"],
                    confidence=row["confidence"],
                    family_key=row["family_key"],
                )
        return CanonicalMatch(matched=False)

    def batch_lookup(
        self,
        requests: list[ResolutionRequest],
    ) -> dict[str, CanonicalMatch]:
        """
        Batch lookup multiple titles.

        Args:
            requests: List of resolution requests

        Returns:
            Dict mapping info_hash to CanonicalMatch
        """
        results = {}
        for req in requests:
            results[req.info_hash] = self.lookup(
                title=req.raw_title,
                year=req.year_hint,
                content_class=req.content_class,
                season=self._parse_season_from_season_bucket(req.season_bucket),
            )
        return results

    def _parse_season_from_season_bucket(self, bucket: str) -> int | None:
        """Parse season number from season bucket string like 's01'."""
        if not bucket:
            return None
        match = re.match(r"s(\d+)", bucket)
        if match:
            return int(match.group(1))
        return None

    def stats(self) -> dict[str, int]:
        """Return canonical layer statistics."""
        return {
            "families": self.conn.execute(
                "SELECT COUNT(*) FROM canonical_title_family"
            ).fetchone()[0],
            "variants": self.conn.execute(
                "SELECT COUNT(*) FROM canonical_title_variant"
            ).fetchone()[0],
            "with_tmdb": self.conn.execute(
                "SELECT COUNT(*) FROM canonical_title_family WHERE tmdb_id IS NOT NULL"
            ).fetchone()[0],
            "with_imdb": self.conn.execute(
                "SELECT COUNT(*) FROM canonical_title_family WHERE imdb_id IS NOT NULL"
            ).fetchone()[0],
        }


def canonical_layer_from_env() -> CanonicalLayer:
    """Create CanonicalLayer from environment or default path."""
    import os

    db_path = os.environ.get("SEARCH_DB_PATH") or os.environ.get("MAGNETICO_SEARCH_DB")
    return CanonicalLayer(db_path or DEFAULT_SEARCH_DB)
