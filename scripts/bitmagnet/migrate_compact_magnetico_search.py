#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
TOOLS_DIR = SCRIPT_DIR.parent / "bitmagnet" / "classifier-tools"
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

from shared_classifier_contract import AUTHORITATIVE_CONTRACT_VERSION, infer_confidence_tier


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate the compact Magnetico SQLite DB to the structured search projection."
    )
    parser.add_argument(
        "--search-db",
        default="/home/ubuntu/aiostreams/data/comet-fresh/magnetico/active.search.sqlite3",
    )
    return parser.parse_args()


def movie_key_for(title_key: str, year: int | None) -> str:
    if year is None:
        return title_key
    return f"{title_key}|{int(year)}"


def exact_episode_key_for(series_key: str, season: int | None, episode: int | None) -> str | None:
    if season is None or episode is None:
        return None
    return f"{series_key}|s{int(season):02d}e{int(episode):03d}"


def season_pack_key_for(series_key: str, season: int | None) -> str | None:
    if season is None:
        return None
    return f"{series_key}|s{int(season):02d}"


def rebuild_search_text(row: sqlite3.Row) -> str:
    parts = [
        row["norm_title"],
        row["name"],
        row["canonical_title_key"],
        row["movie_key"],
        row["series_key"],
        row["aliases_text"],
        row["imdb_id"],
        row["tmdb_id"],
    ]
    if row["year"] is not None:
        parts.append(str(row["year"]))
    if row["season"] is not None:
        parts.extend((f"season {row['season']}", f"s{int(row['season']):02d}"))
    if row["episode_start"] is not None:
        parts.extend((f"episode {row['episode_start']}", f"e{int(row['episode_start']):02d}"))
    if row["episode_end"] is not None and row["episode_end"] != row["episode_start"]:
        parts.extend((f"episode {row['episode_end']}", f"e{int(row['episode_end']):02d}"))
    if row["exact_episode_key"]:
        parts.append(row["exact_episode_key"])
    if row["season_pack_key"]:
        parts.append(row["season_pack_key"])
    return " ".join(str(part) for part in parts if part)


def main() -> None:
    args = parse_args()
    db_path = Path(args.search_db)
    if not db_path.exists():
        raise SystemExit(f"search db not found: {db_path}")

    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA busy_timeout=30000")

    existing_columns = {
        row[1] for row in connection.execute("PRAGMA table_info(media_index)").fetchall()
    }
    for name, sql_type, default in [
        ("canonical_title_key", "TEXT", "''"),
        ("movie_key", "TEXT", "''"),
        ("series_key", "TEXT", "''"),
        ("aliases_text", "TEXT", "''"),
        ("imdb_id", "TEXT", "NULL"),
        ("tmdb_id", "TEXT", "NULL"),
        ("exact_episode_key", "TEXT", "NULL"),
        ("season_pack_key", "TEXT", "NULL"),
        ("confidence_tier", "TEXT", "'none'"),
        ("reject_reason", "TEXT", "NULL"),
        ("reason_codes", "TEXT", "''"),
        ("match_source", "TEXT", "NULL"),
        ("contract_version", "TEXT", f"'{AUTHORITATIVE_CONTRACT_VERSION}'"),
    ]:
        if name not in existing_columns:
            connection.execute(
                f"ALTER TABLE media_index ADD COLUMN {name} {sql_type} DEFAULT {default}"
            )

    connection.execute("BEGIN IMMEDIATE")
    try:
        select_rows = connection.execute(
            """
            SELECT torrent_id, title_key, year, season, episode_start, has_exact_episode,
                   canonical_title_key, movie_key, series_key, aliases_text,
                   imdb_id, tmdb_id, exact_episode_key, season_pack_key,
                   norm_title, name, episode_end, content_class, confidence,
                   confidence_tier, contract_version
            FROM media_index
            """
        )
        while True:
            rows = select_rows.fetchmany(50000)
            if not rows:
                break
            for row in rows:
                title_key = row["title_key"] or ""
                season = row["season"]
                episode_start = row["episode_start"]
                has_exact_episode = int(row["has_exact_episode"] or 0)
                payload = {
                    "torrent_id": row["torrent_id"],
                    "canonical_title_key": row["canonical_title_key"] or title_key,
                    "movie_key": row["movie_key"] or movie_key_for(title_key, row["year"]),
                    "series_key": row["series_key"] or title_key,
                    "aliases_text": row["aliases_text"] or "",
                    "exact_episode_key": row["exact_episode_key"]
                    or exact_episode_key_for(
                        title_key,
                        season,
                        episode_start if has_exact_episode else None,
                    ),
                    "season_pack_key": row["season_pack_key"] or season_pack_key_for(title_key, season),
                    "confidence_tier": row["confidence_tier"]
                    or infer_confidence_tier(
                        content_class=str(row["content_class"] or ""),
                        confidence=float(row["confidence"] or 0.0),
                    ),
                    "contract_version": row["contract_version"] or AUTHORITATIVE_CONTRACT_VERSION,
                    "torrent_id_match": row["torrent_id"],
                }
                connection.execute(
                    """
                    UPDATE media_index
                    SET canonical_title_key = :canonical_title_key,
                        movie_key = :movie_key,
                        series_key = :series_key,
                        aliases_text = :aliases_text,
                        exact_episode_key = :exact_episode_key,
                        season_pack_key = :season_pack_key,
                        confidence_tier = :confidence_tier,
                        contract_version = :contract_version
                    WHERE torrent_id = :torrent_id_match
                    """,
                    payload,
                )

        connection.execute("INSERT INTO media_fts(media_fts) VALUES ('delete-all')")
        select_rows = connection.execute(
            """
            SELECT torrent_id, norm_title, name, canonical_title_key, movie_key, series_key,
                   aliases_text, imdb_id, tmdb_id, year, season, episode_start, episode_end,
                   exact_episode_key, season_pack_key
            FROM media_index
            """
        )
        while True:
            rows = select_rows.fetchmany(50000)
            if not rows:
                break
            connection.executemany(
                "INSERT INTO media_fts(rowid, search_text) VALUES (?, ?)",
                [(row["torrent_id"], rebuild_search_text(row)) for row in rows],
            )
        connection.executescript(
            """
            CREATE INDEX IF NOT EXISTS media_index_canonical_title_key_idx ON media_index(canonical_title_key);
            CREATE INDEX IF NOT EXISTS media_index_movie_key_idx ON media_index(movie_key);
            CREATE INDEX IF NOT EXISTS media_index_series_key_idx ON media_index(series_key);
            CREATE INDEX IF NOT EXISTS media_index_imdb_id_idx ON media_index(imdb_id);
            CREATE INDEX IF NOT EXISTS media_index_tmdb_id_idx ON media_index(tmdb_id);
            CREATE INDEX IF NOT EXISTS media_index_exact_episode_key_idx ON media_index(exact_episode_key);
            CREATE INDEX IF NOT EXISTS media_index_season_pack_key_idx ON media_index(season_pack_key);
            """
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    connection.execute("INSERT INTO media_fts(media_fts) VALUES ('optimize')")
    connection.execute("ANALYZE")
    connection.commit()
    connection.close()
    print(f"migrated compact search db: {db_path}")


if __name__ == "__main__":
    main()
