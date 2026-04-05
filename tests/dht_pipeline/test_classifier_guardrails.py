import sys
import unittest
from pathlib import Path


CLASSIFIER_TOOLS = Path(__file__).resolve().parents[2] / "bitmagnet" / "classifier-tools"
if str(CLASSIFIER_TOOLS) not in sys.path:
    sys.path.insert(0, str(CLASSIFIER_TOOLS))

from compact_media_search import classify_compact_row
from magnetico_media_probe import SampledTorrent


STRICT_MIN_SIZE = 50 * 1024 * 1024
BROAD_MIN_SIZE = 25 * 1024 * 1024


def classify_case(name: str, files: list[dict[str, int | str]], *, size: int | None = None):
    total_size = size if size is not None else sum(int(file_row["size"]) for file_row in files)
    torrent = SampledTorrent(
        id=1,
        info_hash="1" * 40,
        name=name,
        total_size=total_size,
        discovered_at="",
    )
    return classify_compact_row(torrent, files, STRICT_MIN_SIZE, BROAD_MIN_SIZE)


class ClassifierGuardrailTests(unittest.TestCase):
    def test_rejects_courseware_excel_stack_as_software(self) -> None:
        result = classify_case(
            "Microsoft Excel 2019-2016. Уровень 8. Углублённое изучение DAX и Excel PowerPivot",
            [
                {"path": "020. Постановка задачи.wmv", "size": 200_000_000},
                {"path": "030. Лабораторный стенд.wmv", "size": 180_000_000},
                {"path": "100. Знакомство с PowerPivot.wmv", "size": 220_000_000},
                {"path": "module/lesson01.wmv", "size": 210_000_000},
            ],
            size=8_000_000_000,
        )

        self.assertEqual(result.content_class, "reject")
        self.assertEqual(result.reject_reason, "software")
        self.assertEqual(result.software_score, 1)
        self.assertIn("signal:software", result.reason_codes)

    def test_rejects_formula_one_event_dump(self) -> None:
        result = classify_case(
            "F1.2012.13.Italy",
            [
                {"path": "F1.2012.13.Italy.Race.SkySportsF1HD.HDTVRemux.ts", "size": 3_000_000_000},
                {"path": "F1.2012.13.Italy.Qualifying.SkySportsF1HD.HDTVRemux.ts", "size": 2_000_000_000},
                {"path": "F1.2012.13.Italy.FP1.SkySportsF1HD.HDTVRemux.ts", "size": 1_500_000_000},
            ],
            size=12_000_000_000,
        )

        self.assertEqual(result.content_class, "reject")
        self.assertEqual(result.reject_reason, "other")
        self.assertIn("signal:sports_event", result.reason_codes)

    def test_rejects_stage_race_event_dump(self) -> None:
        result = classify_case(
            "Tour de France 2012 SD",
            [
                {"path": "Tour de France 2012 Stage 16 (Eurosport HD).avi", "size": 900_000_000},
                {"path": "Tour de France 2012 Stage 17 (Eurosport HD).avi", "size": 950_000_000},
                {"path": "Tour de France 2012 Stage 18 (Eurosport HD).avi", "size": 980_000_000},
            ],
            size=20_000_000_000,
        )

        self.assertEqual(result.content_class, "reject")
        self.assertIn("signal:sports_event", result.reason_codes)

    def test_rejects_preview_junk_release(self) -> None:
        result = classify_case(
            "24GB LEAKED PORNOS Keira Knightley Fake Face Sitting Catfight Javier Des Leon Anime Femboy Neko",
            [
                {"path": "Preview Video.mp4", "size": 120_000_000},
                {"path": "Links.txt", "size": 1_000},
            ],
        )

        self.assertEqual(result.content_class, "reject")
        self.assertIn("signal:junk", result.reason_codes)

    def test_rejects_known_adult_performer_release(self) -> None:
        result = classify_case(
            "Ivy Wolfe & Danni Rivers - Caught At The Kissing Booth 29.03.2019_2160p.mp4",
            [
                {
                    "path": "Ivy Wolfe & Danni Rivers - Caught At The Kissing Booth 29.03.2019_2160p.mp4",
                    "size": 3_000_000_000,
                }
            ],
        )

        self.assertEqual(result.content_class, "reject")
        self.assertEqual(result.reject_reason, "adult")
        self.assertIn("signal:adult", result.reason_codes)

    def test_rejects_known_adult_code_release(self) -> None:
        result = classify_case(
            "madoubt.com 693392.xyz WAAA-492",
            [{"path": "madoubt.com 693392.xyz WAAA-492.mp4", "size": 1_000_000_000}],
        )

        self.assertEqual(result.content_class, "reject")
        self.assertEqual(result.reject_reason, "adult")

    def test_rejects_targeted_cjk_adult_release(self) -> None:
        result = classify_case(
            "推特大神【SEVEN】高级VIP群，388人民币福利，极品人妻3P，大白臀好评推荐",
            [{"path": "推特大神【SEVEN】高级VIP群，388人民币福利，极品人妻3P，大白臀好评推荐.mp4", "size": 1_000_000_000}],
        )

        self.assertEqual(result.content_class, "reject")
        self.assertEqual(result.reject_reason, "adult")

    def test_classifies_anime_volume_batch_as_anime_pack(self) -> None:
        result = classify_case(
            "[sandoe41] Arifureta Shokugyou de Sekai Saikyou Vol 01&03 (BDRip 720p x265 Hi10 FLAC)",
            [
                {
                    "path": "[sandoe41] Arifureta Shokugyou De Sekai Saikyou 01 (BDrip 720P X265 Hi10 Flac).mkv",
                    "size": 700_000_000,
                },
                {
                    "path": "[sandoe41] Arifureta Shokugyou De Sekai Saikyou 02 (BDrip 720P X265 Hi10 Flac).mkv",
                    "size": 710_000_000,
                },
                {
                    "path": "[sandoe41] Arifureta Shokugyou De Sekai Saikyou 03 (BDrip 720P X265 Hi10 Flac).mkv",
                    "size": 705_000_000,
                },
            ],
            size=14_000_000_000,
        )

        self.assertEqual(result.content_class, "anime_pack")
        self.assertTrue(result.is_anime)
        self.assertIn("tag:anime", result.reason_codes)

    def test_keeps_legit_movie_safe(self) -> None:
        result = classify_case(
            "The.Matrix.1999.1080p.BluRay.x264-GRP",
            [{"path": "The.Matrix.1999.1080p.BluRay.x264-GRP.mkv", "size": 734_003_200}],
        )

        self.assertEqual(result.content_class, "movie")
        self.assertIsNone(result.reject_reason)

    def test_keeps_legit_episode_safe(self) -> None:
        result = classify_case(
            "Monk.S07E01.Mr.Monk.Buys.a.House.1080p.BluRay.x264-OFT",
            [{"path": "Monk.S07E01.Mr.Monk.Buys.a.House.1080p.BluRay.x264-OFT.mkv", "size": 734_003_200}],
        )

        self.assertEqual(result.content_class, "episode")
        self.assertIsNone(result.reject_reason)


if __name__ == "__main__":
    unittest.main()
