#!/usr/bin/env python3
"""
Regression tests for the stage-only movie exact recovery helper.

These cases use real unmatched-name patterns observed on this host so the
representative-name guard stays conservative when the movie bucket contains
broadcast or episodic noise.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from match_movies_to_tmdb import (
    iter_clean_movie_lookup_titles,
    match_movie_key_with_source,
    parse_movie_title,
    representative_name_is_safe_for_movie_match,
)


class FakeTMDBIndex:
    def __init__(self, rows: dict[tuple[str, int | None], tuple[int, str | None, int | None]]):
        self.rows = rows
        self.movie_data = {
            tmdb_id: (imdb_id, matched_year, None, 1.0)
            for (tmdb_id, imdb_id, matched_year) in rows.values()
        }

    def find(
        self, clean: str, year: int | None
    ) -> tuple[int, str | None, int | None] | None:
        return self.rows.get((clean, year))


class MatchMoviesToTmdbTests(unittest.TestCase):
    def test_iter_clean_movie_lookup_titles_filters_numeric_artifacts(self) -> None:
        parsed = parse_movie_title(
            "60 минут (Вечерний выпуск. Эфир от 18.08.2021) HDTVRip (720p) by h4ck.mp4"
        )
        titles = iter_clean_movie_lookup_titles(parsed)
        self.assertTrue(titles)
        self.assertNotIn("1", titles)
        self.assertNotIn("2", titles)
        self.assertTrue(all(not title.isdigit() for title in titles))
        self.assertTrue(all(len(title) >= 2 for title in titles))

    def test_representative_name_guard_rejects_broadcast_and_episode_noise(self) -> None:
        self.assertFalse(
            representative_name_is_safe_for_movie_match(
                "60 минут (Вечерний выпуск. Эфир от 18.08.2021) HDTVRip (720p) by h4ck.mp4"
            )
        )
        self.assertFalse(
            representative_name_is_safe_for_movie_match(
                "www.TamilBlasters.mx - Survivor (2021) [Tamil - S01-EP18 - 1080p UNCUT HD AVC - UNTOUCHED - AAC - x264 - 2.1GB].mkv"
            )
        )
        self.assertFalse(
            representative_name_is_safe_for_movie_match(
                "Tour de France 2017 - Stage 09 [LAST 25 KM].mp4"
            )
        )
        self.assertTrue(
            representative_name_is_safe_for_movie_match(
                "Avengers Endgame (2019) [BluRay] [1080p] English"
            )
        )

    def test_match_movie_key_uses_representative_name_for_clean_movie_release(self) -> None:
        representative_name = "Avengers Endgame (2019) [BluRay] [1080p] English"
        clean_title = iter_clean_movie_lookup_titles(parse_movie_title(representative_name))[0]
        fake_index = FakeTMDBIndex(
            {(clean_title, 2019): (299534, "tt4154796", 2019)}
        )

        result = match_movie_key_with_source(
            "foo english rip|2019",
            fake_index,
            representative_name=representative_name,
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.source, "representative_name")
        self.assertEqual(result.tmdb_id, 299534)
        self.assertEqual(result.imdb_id, "tt4154796")
        self.assertEqual(result.year, 2019)

    def test_match_movie_key_skips_broadcast_representative_names(self) -> None:
        representative_name = (
            "60 минут (Вечерний выпуск. Эфир от 18.08.2021) HDTVRip (720p) by h4ck.mp4"
        )
        clean_title = iter_clean_movie_lookup_titles(parse_movie_title(representative_name))[0]
        fake_index = FakeTMDBIndex({(clean_title, 2021): (1308060, "tt6803782", 2022)})

        result = match_movie_key_with_source(
            "60 минут hdtvrip by h4ck|2021",
            fake_index,
            representative_name=representative_name,
        )

        self.assertIsNone(result)

    def test_match_movie_key_skips_episodic_representative_names(self) -> None:
        representative_name = (
            "www.TamilBlasters.mx - Survivor (2021) [Tamil - S01-EP18 - 1080p UNCUT HD AVC - UNTOUCHED - AAC - x264 - 2.1GB].mkv"
        )
        clean_title = iter_clean_movie_lookup_titles(parse_movie_title(representative_name))[0]
        fake_index = FakeTMDBIndex({(clean_title, 2021): (606870, "tt9242528", 2022)})

        result = match_movie_key_with_source(
            "foo episodic rip|2021",
            fake_index,
            representative_name=representative_name,
        )

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
