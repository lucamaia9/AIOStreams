import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_runner


class TestRunnerBridgeTests(unittest.TestCase):
    def test_load_samples_reads_jsonl_fixture(self) -> None:
        samples_path = Path(__file__).parent / "samples" / "bitmagnet_bridge_fixture.jsonl"
        samples = test_runner.load_samples(samples_path)
        self.assertEqual(len(samples), 3)
        self.assertEqual(samples[0]["expected"]["contentType"], "movie")

    def test_normalize_explicit_expected_block(self) -> None:
        normalized = test_runner.normalize_sample(
            {
                "infoHash": "abcd" * 10,
                "name": "The.Matrix.1999.1080p.BluRay.x264-GRP",
                "files": [{"path": "The.Matrix.1999.1080p.BluRay.x264-GRP.mkv", "size": 734003200}],
                "expected": {
                    "contentType": "movie",
                    "contentSource": "tmdb",
                    "contentId": "603",
                    "action": "accept",
                },
            }
        )
        self.assertEqual(normalized.expected_type, "movie")
        self.assertEqual(normalized.expected_content_source, "tmdb")
        self.assertEqual(normalized.expected_content_id, "603")
        self.assertEqual(normalized.expected_tmdb, "603")
        self.assertFalse(normalized.expected_reject)
        self.assertFalse(normalized.expected_needs_tmdb_lookup)

    def test_normalize_local_baseline_record(self) -> None:
        normalized = test_runner.normalize_sample(
            {
                "torrentId": 42,
                "infoHash": "dcba" * 10,
                "name": "Totally.Unknown.Show.S01E01.720p.WEB-DL.x264-GRP",
                "size": 734003200,
                "files": [{"path": "Totally.Unknown.Show.S01E01.720p.WEB-DL.x264-GRP.mkv", "size": 734003200}],
                "localBaseline": {
                    "contentClass": "episode",
                    "rejectReason": "",
                    "importHints": {
                        "contentType": "tv_show",
                    },
                },
            }
        )
        self.assertEqual(normalized.expected_type, "tv_show")
        self.assertFalse(normalized.expected_reject)
        self.assertTrue(normalized.expected_needs_tmdb_lookup)
        self.assertEqual(normalized.torrent_id, 42)

    def test_normalize_live_bitmagnet_row(self) -> None:
        normalized = test_runner.normalize_sample(
            {
                "info_hash": "abcd" * 10,
                "torrent_name": "Komediya.o.Lisistrate_1989.SATRip.avi",
                "total_size": 1564301312,
                "source_created_at": "2026-04-01T12:05:08.000000+00:00",
                "files": [],
                "expected": {
                    "contentType": "movie",
                    "contentSource": "tmdb",
                    "contentId": "409731",
                    "action": "accept",
                },
            }
        )
        self.assertEqual(normalized.name, "Komediya.o.Lisistrate_1989.SATRip.avi")
        self.assertEqual(normalized.files[0]["path"], "Komediya.o.Lisistrate_1989.SATRip.avi")
        self.assertEqual(normalized.published_at, "2026-04-01T12:05:08.000000+00:00")
        self.assertEqual(normalized.expected_tmdb, "409731")

    def test_run_single_test_reports_normalization_error(self) -> None:
        result = test_runner.run_single_test({"infoHash": "abcd" * 10, "size": 123})
        self.assertEqual(result.name, "<unnamed sample>")
        self.assertIn("missing a name/title", result.error)


if __name__ == "__main__":
    unittest.main()
