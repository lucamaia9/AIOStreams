#!/usr/bin/env python3
"""
DHT Pipeline Test Runner

Runs tests for the live DHT scraping workflow including:
- Series matching
- Movie matching
- Episode parsing
- Rejection handling
- Fast-path verification

Supported sample inputs:
- legacy JSON arrays with flat expected_* keys
- JSON/JSONL BitMagnet-derived samples with an explicit expected block
- JSON/JSONL classifier trial samples with localBaseline/importHints metadata
"""

import json
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional

# Add classifier-tools to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "bitmagnet" / "classifier-tools"))

from bitmagnet_smart_hint import classify_payload

JSONL_SUFFIXES = {".jsonl", ".ndjson"}
DEFAULT_SAMPLE_CANDIDATES = (
    "test_samples.json",
    "test_samples.jsonl",
    "bitmagnet_bridge_fixture.jsonl",
)
COMMON_FILE_EXTENSIONS = {
    ".avi",
    ".flv",
    ".m2ts",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".ts",
    ".webm",
    ".wmv",
}
REJECT_CONTENT_CLASSES = {"reject", "unknown_video"}
TV_CONTENT_CLASSES = {
    "episode",
    "multi_episode",
    "season_pack",
    "anime_episode",
    "anime_pack",
}
ACCEPT_ACTIONS = {"accept", "accepted", "matched", "pass", "keep", "kept"}
REJECT_ACTIONS = {"reject", "rejected"}
LOOKUP_ACTIONS = {"manual_review", "manual-review", "needs_tmdb_lookup", "needs-tmdb-lookup", "lookup"}


@dataclass
class NormalizedSample:
    name: str
    info_hash: str
    size: int
    files: list[dict[str, Any]]
    expected_tmdb: Optional[str] = None
    expected_type: Optional[str] = None
    expected_content_source: Optional[str] = None
    expected_content_id: Optional[str] = None
    expected_reject: Optional[bool] = None
    expected_needs_tmdb_lookup: Optional[bool] = None
    torrent_id: Optional[int] = None
    published_at: Optional[str] = None


@dataclass
class TestResult:
    name: str
    expected_tmdb: Optional[str]
    expected_type: Optional[str]
    expected_content_source: Optional[str] = None
    expected_content_id: Optional[str] = None
    expected_reject: Optional[bool] = None
    expected_needs_tmdb_lookup: Optional[bool] = None
    actual_tmdb: Optional[str] = None
    actual_imdb: Optional[str] = None
    actual_type: Optional[str] = None
    tmdb_match: bool = True
    content_id_match: bool = True
    type_match: bool = True
    reject_match: bool = True
    lookup_match: bool = True
    fast_path: bool = False
    needs_tmdb_lookup: bool = False
    rejected: bool = False
    reject_reason: str = ""
    content_class: str = ""
    episodes: Optional[dict[str, Any]] = None
    error: str = ""


def first_value(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def str_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def bool_or_none(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
        return None
    return bool(value)


def dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def normalize_content_source(value: Any) -> Optional[str]:
    text = str_or_none(value)
    return text.lower() if text else None


def sample_display_name(sample: Any) -> str:
    if not isinstance(sample, dict):
        return "<invalid sample>"
    return (
        str_or_none(
            first_value(
                sample.get("name"),
                sample.get("title"),
                sample.get("torrent_name"),
                sample.get("torrentName"),
            )
        )
        or "<unnamed sample>"
    )


def synthetic_file_path(name: str) -> str:
    suffix = Path(name).suffix.lower()
    if suffix in COMMON_FILE_EXTENSIONS:
        return name
    return f"{name}.mkv"


def load_samples(samples_path: str | Path) -> list[dict[str, Any]]:
    path = Path(samples_path)
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []

    if path.suffix.lower() in JSONL_SUFFIXES:
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    payload = json.loads(text)
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict) and isinstance(payload.get("samples"), list):
        return [row for row in payload["samples"] if isinstance(row, dict)]
    raise ValueError(f"Unsupported sample payload in {path}; expected JSON array, JSON object with 'samples', or JSONL")


def default_samples_path() -> Path:
    samples_dir = Path(__file__).parent / "samples"
    for candidate in DEFAULT_SAMPLE_CANDIDATES:
        candidate_path = samples_dir / candidate
        if candidate_path.exists():
            return candidate_path
    raise FileNotFoundError(
        f"No sample file found in {samples_dir}. Checked: {', '.join(DEFAULT_SAMPLE_CANDIDATES)}"
    )


def normalize_files(name: str, sample: dict[str, Any]) -> list[dict[str, Any]]:
    raw_files = sample.get("files")
    if not isinstance(raw_files, list) or not raw_files:
        default_size = int(first_value(sample.get("size"), sample.get("total_size"), 1000000000) or 0)
        return [{"path": synthetic_file_path(name), "size": default_size}]

    normalized_files = []
    for raw_file in raw_files:
        if isinstance(raw_file, dict):
            normalized_files.append(
                {
                    "path": str(first_value(raw_file.get("path"), raw_file.get("name"), name) or name),
                    "size": int(raw_file.get("size", 0) or 0),
                }
            )
        else:
            normalized_files.append({"path": str(raw_file), "size": 0})
    return normalized_files


def normalize_sample(sample: dict[str, Any]) -> NormalizedSample:
    if not isinstance(sample, dict):
        raise TypeError(f"Expected sample to be a JSON object, got {type(sample)!r}")

    expected = dict_or_empty(sample.get("expected"))
    local_baseline = dict_or_empty(sample.get("localBaseline"))
    import_hints = dict_or_empty(local_baseline.get("importHints"))

    name = str_or_none(
        first_value(
            sample.get("name"),
            sample.get("title"),
            sample.get("torrent_name"),
            sample.get("torrentName"),
        )
    )
    if not name:
        raise ValueError("Sample is missing a name/title")

    expected_type = str_or_none(
        first_value(
            expected.get("contentType"),
            expected.get("content_type"),
            expected.get("type"),
            sample.get("expected_type"),
            sample.get("contentType"),
            sample.get("content_type"),
            import_hints.get("contentType"),
        )
    )
    expected_tmdb = str_or_none(
        first_value(
            expected.get("tmdbId"),
            expected.get("tmdb_id"),
            sample.get("expected_tmdb"),
            import_hints.get("tmdbId"),
        )
    )
    expected_content_source = normalize_content_source(
        first_value(
            expected.get("contentSource"),
            expected.get("content_source"),
            sample.get("expected_content_source"),
            sample.get("contentSource"),
            sample.get("content_source"),
        )
    )
    expected_content_id = str_or_none(
        first_value(
            expected.get("contentId"),
            expected.get("content_id"),
            sample.get("expected_content_id"),
            sample.get("contentId"),
            sample.get("content_id"),
        )
    )

    outcome = str_or_none(
        first_value(
            expected.get("action"),
            expected.get("outcome"),
            sample.get("expected_action"),
            sample.get("expected_outcome"),
            sample.get("action"),
            sample.get("outcome"),
        )
    )
    normalized_outcome = outcome.lower() if outcome else None

    expected_reject = bool_or_none(
        first_value(
            expected.get("reject"),
            expected.get("rejected"),
            sample.get("expected_reject"),
            sample.get("reject"),
            sample.get("rejected"),
        )
    )
    expected_needs_tmdb_lookup = bool_or_none(
        first_value(
            expected.get("needsTmdbLookup"),
            expected.get("needs_tmdb_lookup"),
            sample.get("expected_needs_tmdb_lookup"),
            sample.get("needsTmdbLookup"),
            sample.get("needs_tmdb_lookup"),
        )
    )

    if normalized_outcome in ACCEPT_ACTIONS:
        expected_reject = False if expected_reject is None else expected_reject
        expected_needs_tmdb_lookup = False if expected_needs_tmdb_lookup is None else expected_needs_tmdb_lookup
    elif normalized_outcome in REJECT_ACTIONS:
        expected_reject = True
        expected_needs_tmdb_lookup = False if expected_needs_tmdb_lookup is None else expected_needs_tmdb_lookup
    elif normalized_outcome in LOOKUP_ACTIONS:
        expected_reject = False if expected_reject is None else expected_reject
        expected_needs_tmdb_lookup = True

    if expected_reject is None and local_baseline:
        expected_reject = str(local_baseline.get("contentClass") or "").strip() in REJECT_CONTENT_CLASSES

    if expected_needs_tmdb_lookup is None and local_baseline and not expected_reject:
        expected_needs_tmdb_lookup = bool(import_hints.get("contentType")) and not bool(
            import_hints.get("tmdbId") or import_hints.get("imdbId")
        )

    if expected_content_source == "tmdb" and expected_content_id and not expected_tmdb:
        expected_tmdb = expected_content_id

    files = normalize_files(name, sample)
    size = int(
        first_value(
            sample.get("size"),
            sample.get("total_size"),
            sum(int(file_row.get("size", 0) or 0) for file_row in files) or 1000000000,
        )
        or 0
    )

    torrent_id = first_value(sample.get("torrentId"), sample.get("torrent_id"))
    return NormalizedSample(
        name=name,
        info_hash=str_or_none(first_value(sample.get("info_hash"), sample.get("infoHash"))) or f"test_{hash(name)}",
        size=size,
        files=files,
        expected_tmdb=expected_tmdb,
        expected_type=expected_type,
        expected_content_source=expected_content_source,
        expected_content_id=expected_content_id,
        expected_reject=expected_reject,
        expected_needs_tmdb_lookup=expected_needs_tmdb_lookup,
        torrent_id=int(torrent_id) if torrent_id is not None else None,
        published_at=str_or_none(
            first_value(
                sample.get("publishedAt"),
                sample.get("discoveredAt"),
                sample.get("sourceCreatedAt"),
                sample.get("source_created_at"),
            )
        ),
    )


def matches_expected_content_id(result: TestResult) -> bool:
    if not result.expected_content_id:
        return True
    if result.expected_content_source == "tmdb":
        return result.actual_tmdb == result.expected_content_id
    if result.expected_content_source == "imdb":
        return result.actual_imdb == result.expected_content_id
    return result.expected_content_id in {result.actual_tmdb, result.actual_imdb}


def type_matches_expected(expected_type: Optional[str], actual_type: Optional[str], content_class: str) -> bool:
    if not expected_type:
        return True
    return actual_type == expected_type or (
        content_class in TV_CONTENT_CLASSES and expected_type == "tv_show"
    )


def run_single_test(sample: dict[str, Any]) -> TestResult:
    """Run a single test case."""
    try:
        normalized = normalize_sample(sample)
    except Exception as exc:
        return TestResult(
            name=sample_display_name(sample),
            expected_tmdb=None,
            expected_type=None,
            error=str(exc),
        )

    result = TestResult(
        name=normalized.name,
        expected_tmdb=normalized.expected_tmdb,
        expected_type=normalized.expected_type,
        expected_content_source=normalized.expected_content_source,
        expected_content_id=normalized.expected_content_id,
        expected_reject=normalized.expected_reject,
        expected_needs_tmdb_lookup=normalized.expected_needs_tmdb_lookup,
    )

    try:
        payload = {
            "infoHash": normalized.info_hash,
            "name": normalized.name,
            "size": normalized.size,
            "files": normalized.files,
        }
        if normalized.torrent_id is not None:
            payload["torrentId"] = normalized.torrent_id
        if normalized.published_at:
            payload["publishedAt"] = normalized.published_at

        classification = classify_payload(payload)

        hints = dict_or_empty(classification.get("importHints"))

        result.actual_tmdb = str_or_none(hints.get("tmdbId"))
        result.actual_imdb = str_or_none(hints.get("imdbId"))
        result.actual_type = str_or_none(hints.get("contentType"))
        result.content_class = str_or_none(classification.get("contentClass")) or ""
        result.fast_path = bool(hints.get("fastPath", False))
        result.needs_tmdb_lookup = bool(classification.get("needsTmdbLookup", False))
        result.rejected = bool(classification.get("reject", False))
        result.reject_reason = str_or_none(classification.get("rejectReason")) or ""
        result.episodes = hints.get("episodes")

        result.tmdb_match = (
            result.actual_tmdb == result.expected_tmdb if result.expected_tmdb else True
        )
        result.content_id_match = matches_expected_content_id(result)
        result.type_match = type_matches_expected(
            result.expected_type,
            result.actual_type,
            result.content_class,
        )
        if result.expected_reject is not None:
            result.reject_match = result.rejected == result.expected_reject
        if result.expected_needs_tmdb_lookup is not None:
            result.lookup_match = result.needs_tmdb_lookup == result.expected_needs_tmdb_lookup

    except Exception as exc:
        result.error = str(exc)

    return result


def run_all_tests(samples: list[dict[str, Any]]) -> list[TestResult]:
    """Run all test samples."""
    results = []
    for index, sample in enumerate(samples):
        result = run_single_test(sample)
        results.append(result)
        if (index + 1) % 20 == 0:
            print(f"  Processed {index + 1}/{len(samples)} samples...")
    return results


def build_summary(results: list[TestResult]) -> dict[str, int]:
    return {
        "total": len(results),
        "tmdb_matches": sum(1 for result in results if result.tmdb_match),
        "content_id_expectations": sum(1 for result in results if result.expected_content_id),
        "content_id_matches": sum(
            1 for result in results if result.expected_content_id and result.content_id_match
        ),
        "type_matches": sum(1 for result in results if result.type_match),
        "reject_expectations": sum(1 for result in results if result.expected_reject is not None),
        "reject_matches": sum(
            1 for result in results if result.expected_reject is not None and result.reject_match
        ),
        "lookup_expectations": sum(
            1 for result in results if result.expected_needs_tmdb_lookup is not None
        ),
        "lookup_matches": sum(
            1
            for result in results
            if result.expected_needs_tmdb_lookup is not None and result.lookup_match
        ),
        "fast_paths": sum(1 for result in results if result.fast_path),
        "rejected": sum(1 for result in results if result.rejected),
        "errors": sum(1 for result in results if result.error),
    }


def percentage(matches: int, total: int) -> str:
    if total <= 0:
        return "n/a"
    return f"{100 * matches / total:.1f}%"


def print_summary(results: list[TestResult]) -> bool:
    """Print test summary."""
    summary = build_summary(results)
    total = summary["total"]

    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Total tests: {total}")
    print(
        f"TMDB matches: {summary['tmdb_matches']}/{total} "
        f"({percentage(summary['tmdb_matches'], total)})"
    )
    print(
        f"Type matches: {summary['type_matches']}/{total} "
        f"({percentage(summary['type_matches'], total)})"
    )
    if summary["content_id_expectations"]:
        print(
            f"Content-ID matches: {summary['content_id_matches']}/{summary['content_id_expectations']} "
            f"({percentage(summary['content_id_matches'], summary['content_id_expectations'])})"
        )
    if summary["reject_expectations"]:
        print(
            f"Reject expectation matches: {summary['reject_matches']}/{summary['reject_expectations']} "
            f"({percentage(summary['reject_matches'], summary['reject_expectations'])})"
        )
    if summary["lookup_expectations"]:
        print(
            f"Lookup expectation matches: {summary['lookup_matches']}/{summary['lookup_expectations']} "
            f"({percentage(summary['lookup_matches'], summary['lookup_expectations'])})"
        )
    print(f"Fast-path hits: {summary['fast_paths']}")
    print(f"Rejected: {summary['rejected']}")
    print(f"Errors: {summary['errors']}")
    print()

    failures = [
        result
        for result in results
        if result.error
        or not result.tmdb_match
        or not result.content_id_match
        or not result.type_match
        or not result.reject_match
        or not result.lookup_match
    ]
    if failures:
        print(f"FAILURES ({len(failures)}):")
        print("-" * 60)
        for result in failures[:20]:
            if result.error:
                print(f"  ERROR: {result.name[:50]} - {result.error[:50]}")
            elif not result.content_id_match:
                print(
                    f"  ID: {result.name[:40]} expected={result.expected_content_source or 'any'}:{result.expected_content_id} "
                    f"got=tmdb:{result.actual_tmdb} imdb:{result.actual_imdb} "
                    f"lookup={result.needs_tmdb_lookup} class={result.content_class}"
                )
            elif not result.tmdb_match:
                print(
                    f"  TMDB: {result.name[:40]} expected={result.expected_tmdb} got={result.actual_tmdb}"
                )
            elif not result.type_match:
                print(
                    f"  TYPE: {result.name[:40]} expected={result.expected_type} got={result.actual_type}"
                )
            elif not result.reject_match:
                print(
                    f"  REJECT: {result.name[:38]} expected={result.expected_reject} got={result.rejected} reason={result.reject_reason}"
                )
            elif not result.lookup_match:
                print(
                    f"  LOOKUP: {result.name[:38]} expected={result.expected_needs_tmdb_lookup} got={result.needs_tmdb_lookup}"
                )
        if len(failures) > 20:
            print(f"  ... and {len(failures) - 20} more failures")

    return not failures


def save_results(results: list[TestResult], output_path: str | Path) -> None:
    """Save results to JSON file."""
    data = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "summary": build_summary(results),
        "results": [asdict(result) for result in results],
    }
    with Path(output_path).open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)
    print(f"\nResults saved to: {output_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run DHT pipeline tests")
    parser.add_argument("--samples", default=None, help="Path to samples JSON/JSONL file")
    parser.add_argument("--output", default=None, help="Path to output results JSON")
    args = parser.parse_args()

    samples_path = Path(args.samples) if args.samples else default_samples_path()
    samples = load_samples(samples_path)

    print(f"Running {len(samples)} tests from {samples_path}...")
    results = run_all_tests(samples)

    passed = print_summary(results)

    output_path = Path(args.output) if args.output else Path(__file__).parent / "test_results.json"
    save_results(results, output_path)

    sys.exit(0 if passed else 1)
