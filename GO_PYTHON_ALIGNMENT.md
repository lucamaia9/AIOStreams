# Go/Python Filter Alignment

## Architecture

Go and Python filters serve **different purposes** in the pipeline:

| Layer | Purpose | Decision Model | When |
|-------|---------|---------------|------|
| **Go IntakeFilter** | Fast pre-persist gate at DHT crawl time | Binary (reject/pass) | Before DB write |
| **Python Smart Hint** | Authoritative media classification | Ternary (keep/review/reject) | After Go passes, before DB persist |
| **Python Full Classifier** | Deep classification with file structure | Full 3-tier pipeline | Batch promotion, export |

Go is a **conservative fast gate**. Python is the **authoritative decision engine**.

## Pattern Sync

Go patterns are **generated from Python** via `scripts/misc/generate_patterns_go.py`.
The output file `internal/dhtcrawler/patterns_gen.go` has the header:
```
// Code generated from Python classifier patterns. DO NOT EDIT.
```

### Pattern Sources

| Pattern Set | Python Source | Go Target | Sync Method |
|-------------|--------------|-----------|-------------|
| Adult brands | `shared_adult_title_classifier.py:ADULT_BRANDS` | `patterns_gen.go:adultBrands` | `generate_patterns_go.py` |
| Adult performers | `ADULT_PERFORMERS` | `adultPerformers` | `generate_patterns_go.py` |
| Adult scenes | `ADULT_SCENE_TITLES` | `adultSceneTitles` | `generate_patterns_go.py` |
| CJK adult terms | `ADULT_CJK_TERMS` | `cjkAdultTerms` | `generate_patterns_go.py` |
| JAV code prefixes | `ADULT_CODE_PREFIXES` | `adultCodePrefixes` | `generate_patterns_go.py` |

### Patterns NOT synced (Go has its own)

| Pattern | Where | Notes |
|---------|-------|-------|
| Explicit vocabulary regex | `intake_filter.go` inline | Go has its own comprehensive regex |
| Embedded adult patterns | `intake_filter.go` inline | 12 patterns, similar to Python |
| Courseware pattern | `intake_filter.go` inline | Same terms as Python |
| Benign context guards | `intake_filter.go:isBenignContext()` | 6 guards (BBC, DP, dick, anal, fuck, cock) |
| XXX + context check | `intake_filter.go` inline | Requires adult context or release format |

## Known Differences

| Aspect | Go | Python | Impact |
|--------|-----|--------|--------|
| Decision model | Binary | Ternary (keep/review/reject) | Go has no "review" middle ground |
| Scoring | First match wins | Weighted scoring (1-3 per signal) | Go cannot distinguish weak vs strong |
| Brand matching | `strings.Contains` | `ExactFamilyMatcher` (multi-index) | Python more precise |
| BBC disambiguation | Basic pattern | 7 sub-patterns + context | Python more nuanced |
| RTN integration | None | Yes | Python has external library detection |
| Supplemental scanning | Inline | File-path-level deep scan | Python catches more edge cases |
| Size threshold | 50MB hard reject | 100MB signal | Go more aggressive |

## Acceptance Criteria

- Go rejection rate: 15-20% of DHT intake (expected)
- Go false positives on known-good media: **zero** (critical)
- Python-only rejects among Go-passed torrents: <5% (acceptable)

## Validation

Run consistency test:
```bash
cd /home/ubuntu/aiostreams
python3 scripts/testing/test_go_python_consistency.py --sample 10000
```

Run Go unit tests:
```bash
cd bitmagnet-media
go test ./internal/dhtcrawler -run TestIntakeFilter -v
```

## Status

**Last verified:** 2026-04-05 (audit during master plan cleanup)

**Findings:**
- Go patterns are generated from Python source lists (brands, performers, scenes, CJK, JAV codes)
- Go has additional inline patterns (explicit regex, embedded, courseware) that mirror Python logic
- Benign context guards exist in both (BBC, DP, dick, anal, fuck, cock)
- Go is intentionally more aggressive on size (50MB hard reject vs Python's 100MB signal)
- The unified_classifier shim is NOT used by production paths (bitmagnet_smart_hint.py, compact_media_search.py, stream_export_bitmagnet.py all import directly from compact_media_search)
