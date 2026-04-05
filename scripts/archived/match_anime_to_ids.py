#!/usr/bin/env python3
"""
Match historical anime torrents against unified anime mapping data.

Uses three data sources:
1. Fribb/anime-lists: 42K entries with AniDB->IMDB/TMDB/MAL/AniList mappings
2. Anime Offline Database (manami): 40K entries with titles, synonyms, types
3. Kitsu-IMDB mapping: 6K entries with Kitsu->IMDB mappings

Workflow:
    1. Load and merge all three sources into unified index
    2. Index by: title, synonyms (all variants)
    3. Query SQLite for unique anime entries without IDs
    4. Match each entry against the index
    5. Batch update SQLite

Usage:
    python match_anime_to_ids.py [--dry-run] [--batch-size N]
"""

import argparse
import json
import re
import sqlite3
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ANIME_DIR = Path(__file__).parent.parent / "data" / "anime-database"
SQLITE_DB = (
    Path(__file__).parent.parent
    / "data"
    / "comet-fresh"
    / "magnetico"
    / "active.search.sqlite3"
)

BATCH_SIZE = 5000
PROGRESS_INTERVAL = 10000

# ---------------------------------------------------------------------------
# Data Loaders
# ---------------------------------------------------------------------------


def load_fribb_mappings() -> dict:
    """Load Fribb/anime-lists mappings (AniDB -> IMDB/TMDB/MAL/AniList).

    Returns: {anidb_id: {imdb_id, tmdb_id, mal_id, anilist_id, ...}}
    """
    path = ANIME_DIR / "fribb-mappings.json"
    with open(path) as f:
        entries = json.load(f)

    mappings = {}
    for entry in entries:
        anidb_id = entry.get("anidb_id")
        if anidb_id:
            mappings[anidb_id] = {
                "imdb_id": entry.get("imdb_id"),
                "tmdb_id": entry.get("themoviedb_id"),
                "tvdb_id": entry.get("tvdb_id"),
                "mal_id": entry.get("mal_id"),
                "anilist_id": entry.get("anilist_id"),
                "kitsu_id": entry.get("kitsu_id"),
                "simkl_id": entry.get("simkl_id"),
            }

    print(f"  Fribb: {len(mappings):,} entries with ID mappings")
    return mappings


def load_manami_db() -> list:
    """Load Anime Offline Database (titles, synonyms, types).

    Returns: list of {title, synonyms, type, year, mal_id, anilist_id, kitsu_id, anidb_id}
    """
    path = ANIME_DIR / "manami-db.json"
    with open(path) as f:
        data = json.load(f)

    entries = []
    for item in data.get("data", []):
        # Extract provider IDs from sources URLs
        sources = item.get("sources", [])
        ids = {}
        for url in sources:
            if "myanimelist.net" in url:
                ids["mal_id"] = int(url.split("/anime/")[1].split("/")[0])
            elif "anilist.co" in url:
                ids["anilist_id"] = int(url.split("/anime/")[1].split("/")[0])
            elif "kitsu.app" in url:
                ids["kitsu_id"] = int(url.split("/anime/")[1].split("/")[0])
            elif "anidb.net" in url:
                ids["anidb_id"] = int(url.split("/anime/")[1].split("/")[0])

        season = item.get("animeSeason", {})
        entries.append(
            {
                "title": item.get("title", ""),
                "synonyms": item.get("synonyms", []),
                "type": item.get("type", "UNKNOWN"),
                "year": season.get("year"),
                **ids,
            }
        )

    print(f"  Manami: {len(entries):,} entries with titles/synonyms")
    return entries


def load_kitsu_imdb() -> dict:
    """Load Kitsu-IMDB mapping.

    Returns: {kitsu_id: imdb_id}
    """
    path = ANIME_DIR / "kitsu-imdb-mapping.json"
    with open(path) as f:
        entries = json.load(f)

    mappings = {}
    for entry in entries:
        kitsu_id = entry.get("kitsu_id")
        imdb_id = entry.get("imdb_id")
        if kitsu_id and imdb_id:
            mappings[kitsu_id] = imdb_id

    print(f"  Kitsu-IMDB: {len(mappings):,} entries")
    return mappings


# ---------------------------------------------------------------------------
# Unified Index Builder
# ---------------------------------------------------------------------------


def build_unified_anime_index() -> dict:
    """
    Build unified anime index by merging all three sources.

    Strategy:
    1. Load Fribb (ID mappings by AniDB ID)
    2. Load Manami (titles/synonyms by MAL/AniList/Kitsu/AniDB IDs)
    3. Cross-reference: for each Manami entry, look up AniDB ID -> get IMDB/TMDB from Fribb
    4. Also use Kitsu-IMDB as fallback

    Returns: {normalized_title: {imdb_id, tmdb_id, mal_id, anilist_id, ...}}
    """
    print("  Loading data sources...")
    fribb = load_fribb_mappings()
    manami = load_manami_db()
    kitsu_imdb = load_kitsu_imdb()

    # Build lookup: mal_id -> fribb entry, anilist_id -> fribb entry, etc.
    mal_to_fribb = {}
    anilist_to_fribb = {}
    kitsu_to_fribb = {}
    anidb_to_fribb = {}

    for anidb_id, mapping in fribb.items():
        if mapping.get("mal_id"):
            mal_to_fribb[mapping["mal_id"]] = mapping
        if mapping.get("anilist_id"):
            anilist_to_fribb[mapping["anilist_id"]] = mapping
        if mapping.get("kitsu_id"):
            kitsu_to_fribb[mapping["kitsu_id"]] = mapping
        anidb_to_fribb[anidb_id] = mapping

    # Build unified index: title -> IDs
    index = {}
    title_clean = re.compile(r"[^\w\s-]")
    multispace = re.compile(r"\s+")

    def normalize_title(text: str) -> str:
        text = title_clean.sub(" ", text)
        text = multispace.sub(" ", text).strip().lower()
        return text

    matched_entries = 0
    for entry in manami:
        # Find IDs via cross-reference
        ids = {}

        # Try AniDB -> Fribb
        if entry.get("anidb_id") and entry["anidb_id"] in anidb_to_fribb:
            fribb_entry = anidb_to_fribb[entry["anidb_id"]]
            ids.update({k: v for k, v in fribb_entry.items() if v})

        # Try MAL -> Fribb
        if (
            not ids.get("imdb_id")
            and entry.get("mal_id")
            and entry["mal_id"] in mal_to_fribb
        ):
            fribb_entry = mal_to_fribb[entry["mal_id"]]
            ids.update({k: v for k, v in fribb_entry.items() if v})

        # Try AniList -> Fribb
        if (
            not ids.get("imdb_id")
            and entry.get("anilist_id")
            and entry["anilist_id"] in anilist_to_fribb
        ):
            fribb_entry = anilist_to_fribb[entry["anilist_id"]]
            ids.update({k: v for k, v in fribb_entry.items() if v})

        # Try Kitsu -> Fribb
        if (
            not ids.get("imdb_id")
            and entry.get("kitsu_id")
            and entry["kitsu_id"] in kitsu_to_fribb
        ):
            fribb_entry = kitsu_to_fribb[entry["kitsu_id"]]
            ids.update({k: v for k, v in fribb_entry.items() if v})

        # Fallback: Kitsu -> IMDB direct
        if (
            not ids.get("imdb_id")
            and entry.get("kitsu_id")
            and entry["kitsu_id"] in kitsu_imdb
        ):
            ids["imdb_id"] = kitsu_imdb[entry["kitsu_id"]]

        if not ids.get("imdb_id") and not ids.get("tmdb_id"):
            continue  # Skip entries without any ID

        matched_entries += 1

        # Index by title and synonyms
        titles_to_index = [entry["title"]] + entry.get("synonyms", [])

        for title in titles_to_index:
            clean = normalize_title(title)
            if not clean or len(clean) < 2:
                continue

            if clean not in index:
                index[clean] = {**ids, "titles": []}
            index[clean]["titles"].append(title)

    print(
        f"  Unified index: {len(index):,} titles from {matched_entries:,} anime entries"
    )
    return index


# ---------------------------------------------------------------------------
# SQLite Matching
# ---------------------------------------------------------------------------


def get_unresolved_anime(conn: sqlite3.Connection) -> list[tuple[str, str, int]]:
    """Get unique anime names without IDs, ordered by row count (highest first).

    Returns: [(name, series_key, count), ...]
    """
    return conn.execute(
        """
        SELECT name, series_key, COUNT(*) as cnt
        FROM media_index
        WHERE content_class IN ('anime_episode', 'anime_pack')
          AND name IS NOT NULL
          AND (imdb_id IS NULL OR imdb_id = '')
          AND (tmdb_id IS NULL OR tmdb_id = '')
        GROUP BY name
        ORDER BY cnt DESC
        """
    ).fetchall()


# Compiled once at module level
_TITLE_CLEAN = re.compile(
    r"\[[^\]]*\]|\([^\)]*\)|\{[^\}]*\}|"
    r"\b(?:2160p|1440p|1080p|1080i|720p|576p|480p|360p|"
    r"4k|8k|uhd|hdr|bluray|blu[- ]?ray|webrip|web[- ]?dl|hdtv|dvdrip|"
    r"x264|x265|h264|h265|hevc|avc|aac|dts|mkv|mp4|avi|"
    r"proper|repack|internal|dual|dubbed|subbed|"
    r"at[- ]?x|bs11|mx|tx|tv)"
    r"\b",
    re.IGNORECASE,
)
_EPISODE_PATTERN = re.compile(r"\s+-\s+\d{1,4}\b")
_MULTISPACE = re.compile(r"\s+")
_NON_WORD = re.compile(r"[^\w\s-]")
_TRAILING_NUM = re.compile(r"\s+\d+$")


# Pre-compiled regex patterns
_TITLE_CLEAN = re.compile(
    r"\[[^\]]*\]|\([^\)]*\)|\{[^\}]*\}|"
    r"\b(?:2160p|1440p|1080p|1080i|720p|576p|480p|360p|"
    r"4k|8k|uhd|hdr|bluray|blu[- ]?ray|webrip|web[- ]?dl|hdtv|dvdrip|"
    r"x264|x265|h264|h265|hevc|avc|aac|dts|mkv|mp4|avi|"
    r"proper|repack|internal|dual|dubbed|subbed|"
    r"at[- ]?x|bs11|mx|tx|tv)"
    r"\b",
    re.IGNORECASE,
)
_EPISODE_PATTERN = re.compile(r"\s+-\s+\d{1,4}\b")
_MULTISPACE = re.compile(r"\s+")
_NON_WORD = re.compile(r"[^\w\s-]")
_TRAILING_NUM = re.compile(r"\s+\d+$")


def match_anime_name(
    name: str,
    anime_index: dict,
) -> dict | None:
    """Match an anime torrent name against the unified index."""
    cleaned = _TITLE_CLEAN.sub(" ", name)
    cleaned = _EPISODE_PATTERN.sub(" ", cleaned)
    cleaned = _NON_WORD.sub(" ", cleaned)
    cleaned = _MULTISPACE.sub(" ", cleaned).strip().lower()

    if not cleaned:
        return None

    # Try exact match
    if cleaned in anime_index:
        return anime_index[cleaned]

    # Try without trailing numbers (season/episode artifacts)
    cleaned_no_num = _TRAILING_NUM.sub("", cleaned)
    if cleaned_no_num != cleaned and cleaned_no_num in anime_index:
        return anime_index[cleaned_no_num]

    return None

    # Try exact match
    if cleaned in anime_index:
        return anime_index[cleaned]

    # Try without trailing numbers (season/episode artifacts)
    cleaned_no_num = _TRAILING_NUM.sub("", cleaned)
    if cleaned_no_num != cleaned and cleaned_no_num in anime_index:
        return anime_index[cleaned_no_num]

    return None


def batch_update_sqlite(
    conn: sqlite3.Connection,
    updates: list[tuple[str, str | None, str | None]],
    dry_run: bool = False,
) -> int:
    """Batch update SQLite with matched anime IDs using series_key (indexed)."""
    if not updates:
        return 0

    if dry_run:
        return len(updates)

    cursor = conn.cursor()
    total_updated = 0

    for series_key, imdb_id, tmdb_id in updates:
        cursor.execute(
            """
            UPDATE media_index
            SET imdb_id = ?, tmdb_id = ?
            WHERE series_key = ?
              AND (imdb_id IS NULL OR imdb_id = '')
              AND (tmdb_id IS NULL OR tmdb_id = '')
            """,
            (imdb_id, tmdb_id, series_key),
        )
        total_updated += cursor.rowcount

    return total_updated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Match anime against unified ID index")
    parser.add_argument("--dry-run", action="store_true", help="Don't update SQLite")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help="Batch size for SQLite updates",
    )
    args = parser.parse_args()

    start_time = time.time()
    print("=" * 60)
    print("Anime ID Matching")
    print("=" * 60)

    # Step 1: Build unified index
    print("\n[1/3] Building unified anime index...")
    anime_index = build_unified_anime_index()

    # Step 2: Connect to SQLite
    print("\n[2/3] Connecting to SQLite...")
    conn = sqlite3.connect(str(SQLITE_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Step 3: Match and update
    print("\n[3/3] Matching anime names...")
    anime_names = get_unresolved_anime(conn)
    print(f"  Found {len(anime_names):,} unique anime names without IDs")

    matched = 0
    unmatched = 0
    updates = []

    for i, (name, series_key, count) in enumerate(anime_names, 1):
        result = match_anime_name(name, anime_index)
        if result:
            imdb_id = result.get("imdb_id")
            tmdb_id = result.get("tmdb_id")
            if imdb_id or tmdb_id:
                updates.append((series_key, imdb_id, tmdb_id))
                matched += 1
        else:
            unmatched += 1

        if i % PROGRESS_INTERVAL == 0:
            print(
                f"  Progress: {i:,}/{len(anime_names):,} "
                f"({matched:,} matched, {unmatched:,} unmatched, "
                f"{100.0 * matched / max(i, 1):.1f}%)"
            )

        # Batch update
        if len(updates) >= args.batch_size:
            updated = batch_update_sqlite(conn, updates, args.dry_run)
            if not args.dry_run:
                conn.commit()
            print(f"  Batch update: {updated:,} rows updated")
            updates = []

    # Final batch
    if updates:
        updated = batch_update_sqlite(conn, updates, args.dry_run)
        if not args.dry_run:
            conn.commit()
        print(f"  Final batch update: {updated:,} rows updated")

    print(f"\n  Results: {matched:,} matched, {unmatched:,} unmatched")
    print(f"  Match rate: {100.0 * matched / max(matched + unmatched, 1):.1f}%")

    conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"Completed in {elapsed:.0f}s ({elapsed / 60:.1f} minutes)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
