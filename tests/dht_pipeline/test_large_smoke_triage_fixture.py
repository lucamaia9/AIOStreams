import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_runner


SAMPLES_DIR = Path(__file__).parent / "samples"
TAXONOMY_PATH = SAMPLES_DIR / "large_smoke_triage_taxonomy.json"
CASES_PATH = SAMPLES_DIR / "large_smoke_regression_cases.jsonl"


def load_taxonomy() -> dict:
    return json.loads(TAXONOMY_PATH.read_text(encoding="utf-8"))


class LargeSmokeTriageFixtureTests(unittest.TestCase):
    def test_taxonomy_has_expected_priority_buckets(self) -> None:
        taxonomy = load_taxonomy()
        self.assertEqual(
            taxonomy["highestPriorityFixBuckets"],
            [
                "canonical-title-normalization-gap",
                "movie-title-collision-and-year-drift",
                "guardrail-noncanonical-acceptance",
                "anime-tv-structure-routed-as-movie",
            ],
        )

    def test_regression_cases_load_and_cover_tracks(self) -> None:
        samples = test_runner.load_samples(CASES_PATH)
        self.assertGreaterEqual(len(samples), 10)
        tracks = {sample["triage"]["track"] for sample in samples}
        self.assertEqual(tracks, {"canonical-matching", "guardrail"})

    def test_regression_cases_reference_known_root_cause_groups(self) -> None:
        taxonomy = load_taxonomy()
        known_groups = {group["id"] for group in taxonomy["rootCauseGroups"]}
        samples = test_runner.load_samples(CASES_PATH)
        for sample in samples:
            triage = sample["triage"]
            self.assertIn(triage["rootCauseGroup"], known_groups)
            self.assertIn(triage["priority"], {"P0", "P1", "P2", "P3"})
            self.assertTrue(sample["expected"])

    def test_baseline_only_examples_stay_out_of_regression_fixture(self) -> None:
        samples = test_runner.load_samples(CASES_PATH)
        baseline_tracks = {sample["triage"]["track"] for sample in samples if sample["triage"]["track"] == "baseline-data"}
        self.assertEqual(baseline_tracks, set())


if __name__ == "__main__":
    unittest.main()
