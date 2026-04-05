import sys
import unittest
from pathlib import Path


CLASSIFIER_DIR = (
    Path(__file__).resolve().parents[2] / "bitmagnet-media" / "classifier"
)
if str(CLASSIFIER_DIR) not in sys.path:
    sys.path.insert(0, str(CLASSIFIER_DIR))

from bitmagnet_smart_hint import (  # noqa: E402
    ParsedMovieInfo,
    _match_movie_from_parsed_titles,
)
from tmdb_series_index import TMDBSeriesIndex  # noqa: E402


class TMDBSeriesIndexVariantTests(unittest.TestCase):
    def _make_index(self) -> TMDBSeriesIndex:
        index = TMDBSeriesIndex.__new__(TMDBSeriesIndex)
        index.series_by_clean = {}
        index.series_by_orig = {}
        index.alt_titles = {}
        index.translations = {}
        index.series_data = {}
        index.fuzzy_matcher = None
        return index

    def test_find_uses_variants_across_all_lookup_phases(self) -> None:
        tmdb_id = 4
        imdb_id = "tt0075148"

        for field_name in (
            "series_by_clean",
            "series_by_orig",
            "alt_titles",
            "translations",
        ):
            with self.subTest(field=field_name):
                index = self._make_index()
                index.series_data[tmdb_id] = (imdb_id, None, 1976, 100.0)

                if field_name in {"series_by_clean", "series_by_orig"}:
                    getattr(index, field_name)["rockyiv"] = [
                        (tmdb_id, 1976, 100.0, imdb_id)
                    ]
                else:
                    getattr(index, field_name)["rockyiv"] = [tmdb_id]

                self.assertEqual(index.find("rocky4", 1976), (tmdb_id, imdb_id))


class MovieFastPathAkaTests(unittest.TestCase):
    class _FakeIndex:
        def __init__(self) -> None:
            self.calls: list[tuple[str, int | None]] = []
            self.movie_data = {123: ("tt1234567", 2001, None, 50.0)}

        def find(self, clean: str, year: int | None) -> tuple[int, str] | None:
            self.calls.append((clean, year))
            if clean == "secondarytitle":
                return (123, "tt1234567")
            return None

    def test_match_movie_from_parsed_titles_uses_secondary_titles(self) -> None:
        parsed = ParsedMovieInfo(
            title="Primary Title",
            year=2001,
            titles=[
                "Primary Title",
                "Secondary Title",
            ],
        )
        index = self._FakeIndex()

        self.assertEqual(
            _match_movie_from_parsed_titles(parsed, None, index),
            (123, "tt1234567", 2001),
        )
        self.assertEqual(
            index.calls,
            [
                ("primarytitle", 2001),
                ("secondarytitle", 2001),
            ],
        )


if __name__ == "__main__":
    unittest.main()
