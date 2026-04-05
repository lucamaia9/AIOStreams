#!/usr/bin/env python3
import sqlite3
import time
import re
from pathlib import Path
from lib.radarr_parser import (
    clean_movie_title as radarr_clean_title,
    generate_title_variants,
)

DB_PATH = (
    Path(__file__).parent.parent
    / "data"
    / "comet-fresh"
    / "magnetico"
    / "active.search.sqlite3"
)
INDEX_PATH = Path(__file__).parent.parent / "data" / "tmdb" / "movie_index.sqlite3"

NOISE_PATTERNS = [
    re.compile(r"\[[^\]]*\]"),
    re.compile(
        r"\([^)]*(?:rus|bg|lat|dub|sub|hddvd|bluray|dvdrip|webrip|web-dl|hdtv|hdrip|audio)[^)]*\)",
        re.I,
    ),
    re.compile(
        r"\b(xvid|divx|x264|x265|h264|h265|hevc|aac|dd5\.?\s?1|5\.1|7\.1|lpcm|dts|ac3|mp3)\b",
        re.I,
    ),
    re.compile(
        r"\b(1080p|720p|480p|2160p|4k|uhd|hdrip|hdtv|webrip|web-dl|bluray|dvdrip|hddvd|open[\W_]?matte|bdremux|hc)\b",
        re.I,
    ),
    re.compile(r"\b\d{2,4}x\d{3,4}\b"),
    re.compile(r"\b\d+[,\.]?\d*\s*(?:mb|gb)\b", re.I),
    re.compile(r"\b(t\d+|e\d+|s\d+)\b", re.I),
    re.compile(
        r"\b(exkinoray|exki|cinecalidad|megapeer|blitzcrieg|satanas|fgt|next|lord|hellywood|galaxyrg|evo|rpg|shadow|hqc|atlas31|kinozal)\b",
        re.I,
    ),
    re.compile(r"\b(bg[\W_]?audio|rus[\W_]?audio|lat[\W_]?audio|mvo|dub)\b", re.I),
]
NOISE_SUFFIX = re.compile(r"\s+(d|l|to|rus|lat|bg|audio)$", re.I)
SERIES_PATTERN = re.compile(
    r"\b[Ss]\d{1,2}\b|\b[Ee]\d{1,3}\b|\bseason\b|\bepisode\b", re.I
)


def clean_movie_key(raw_title: str) -> str:
    result = raw_title
    for pattern in NOISE_PATTERNS:
        result = pattern.sub(" ", result)
    result = re.sub(r"[\._]", " ", result)
    result = re.sub(r"\s+", " ", result)
    result = NOISE_SUFFIX.sub("", result)
    return result.strip()


def find_movie(index_conn, clean, year):
    if year:
        match = index_conn.execute(
            "SELECT * FROM tmdb_movie_index WHERE clean_title = ? AND year = ? LIMIT 1",
            (clean, year),
        ).fetchone()
        if match:
            return match
    match = index_conn.execute(
        "SELECT * FROM tmdb_movie_index WHERE clean_title = ? ORDER BY popularity DESC LIMIT 1",
        (clean,),
    ).fetchone()
    if match:
        return match
    match = index_conn.execute(
        "SELECT * FROM tmdb_movie_index WHERE clean_original_title = ? ORDER BY popularity DESC LIMIT 1",
        (clean,),
    ).fetchone()
    if match:
        return match
    match = index_conn.execute(
        "SELECT m.* FROM tmdb_movie_index m "
        "JOIN tmdb_alt_titles a ON m.tmdb_id = a.tmdb_id "
        "WHERE a.clean_title = ? ORDER BY m.popularity DESC LIMIT 1",
        (clean,),
    ).fetchone()
    return match


index_conn = sqlite3.connect(str(INDEX_PATH))
index_conn.row_factory = sqlite3.Row

db_conn = sqlite3.connect(str(DB_PATH), timeout=120)
db_conn.row_factory = sqlite3.Row

query = """
    SELECT torrent_id, movie_key
    FROM media_index
    WHERE content_class = 'movie'
      AND movie_key IS NOT NULL AND movie_key != ''
      AND (tmdb_id IS NULL OR tmdb_id = '')
    LIMIT 100
"""
rows = db_conn.execute(query).fetchall()
print(f"Processing {len(rows)} rows...", flush=True)

t0 = time.time()
resolved = 0
for row in rows:
    key = row["movie_key"]

    if SERIES_PATTERN.search(key):
        continue

    if "|" in key:
        parts = key.split("|")
        raw_title = parts[0].strip()
        year = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
    else:
        raw_title = key
        year_match = re.search(r"\b((?:19|20)\d{2})\b", key)
        year = int(year_match.group(1)) if year_match else None

    cleaned_title = clean_movie_key(raw_title)
    clean = radarr_clean_title(cleaned_title)

    if not clean or len(clean) < 2:
        continue

    variants = generate_title_variants(clean)
    for variant in variants:
        match = find_movie(index_conn, variant, year)
        if match:
            resolved += 1
            break

print(f"Done: {resolved}/{len(rows)} resolved in {time.time() - t0:.2f}s")
index_conn.close()
db_conn.close()
