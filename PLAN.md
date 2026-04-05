# BitMagnet/Comet Enrichment Master Plan

## Goal Statement

**Achieve <2% error rate per content category while fully adopting Sonarr/Radarr matching logic. Clear all unmatched content as unusable.**

| Target | Metric |
|--------|--------|
| Error rate | <2% per category (movies, episodes, anime, season packs, multi-episodes) |
| Match coverage | Maximize for English-language content (movies, series, anime) |
| Clear criteria | Foreign language, adult, low confidence, no TMDB match |
| Approach | Adopt Sonarr/Radarr matching techniques not yet implemented |

---

## 1. Current State Analysis

### 1.1 Database Overview

**Main Database:** `/home/ubuntu/aiostreams/data/comet-fresh/magnetico/active.search.sqlite3` (~7GB)

**TMDB Indexes:**
- Series: `/home/ubuntu/aiostreams/data/tmdb/series_index.sqlite3` (218,119 entries)
- Movies: `/home/ubuntu/aiostreams/data/tmdb/movie_index.sqlite3` (1,117,308 entries)

### 1.2 Current Match Rates

| Category | Total | Matched | Unmatched | Match Rate |
|----------|-------|---------|-----------|------------|
| Movies (non-anime) | 6,092,100 | 1,344,820 | 4,747,280 | **22.1%** |
| Episodes (series) | 1,800,815 | 889,702 | 911,113 | **49.4%** |
| Multi-episodes | 596,957 | 344,390 | 252,567 | **57.7%** |
| Season Packs | 197,241 | 123,176 | 74,065 | **62.4%** |
| Anime Episodes | 186,648 | 141,471 | 45,177 | **75.8%** |
| Anime Packs | 11,105 | 7,456 | 3,649 | **67.1%** |
| Unknown Video | 178,508 | 21,704 | 156,804 | **12.2%** |
| **TOTAL** | **9,076,024** | **2,877,241** | **6,198,783** | **31.7%** |

### 1.3 Critical Finding: Parsing Works, Matching Fails

**100% of unmatched rows have `canonical_title_key` populated** - The parser correctly extracts titles. The problem is in the matching pipeline.

**Key insight:** 1.28M high-confidence (>=0.8) series entries are unmatched despite good title quality. This is the low-hanging fruit.

### 1.4 Why Content Fails to Match

| Reason | % of Unmatched | Fixable? |
|--------|---------------|----------|
| Foreign language (Chinese, Korean, Russian, etc.) | ~40% | NO |
| Adult/XXX content misclassified | ~15% | NO |
| Year missing/wrong (especially episodes) | ~47% | PARTIAL |
| TMDB doesn't have the title | ~20% (anime) | NO |
| Title extracted but matching failed | ~5-10% | **YES** |
| Recognition/formatting issues | ~15% | **YES** |

---

## 2. Root Cause Analysis

### 2.1 What We Have (vs. Radarr/Sonarr)

| Feature | Radarr/Sonarr | BitMagnet | Status |
|---------|---------------|-----------|--------|
| Exact cascade (4 phases) | ✅ | ✅ | Implemented |
| Alt titles | ✅ | ✅ | Implemented |
| Translations | ✅ | ✅ | Implemented |
| Year ±1 tolerance | ✅ | ✅ | Implemented |
| Roman/Arabic variants | ✅ | Movies only | **GAP** |
| Secondary year | ✅ | Movies only | **GAP** |
| RapidFuzz fuzzy | ❌ (Levenshtein) | ✅ | Implemented |
| AKA title iteration | ✅ | Live: **NO**, Backfill: YES | **GAP** |
| TVDB ID matching | ✅ | ❌ | Not needed |
| Scene mappings | ✅ | ❌ | Low priority |

### 2.2 Gap Remediation Priorities

| Priority | Gap | Impact | Effort |
|----------|-----|--------|--------|
| **HIGH** | Series Roman/Arabic variants | High | Low |
| **HIGH** | Series alt title variants | High | Low |
| **HIGH** | Live classifier AKA iteration | High | Low |
| **MEDIUM** | Series secondary year | Medium | Medium |
| **MEDIUM** | Fuzzy: alt titles as candidates | Medium | High |
| **LOW** | Go anime absolute episodes | Low | High |
| **LOW** | Anime scene mappings | Low | Very High |

### 2.3 Previous Session Changes (Already Applied)

1. **Embedded episode pattern stripping** - `shared_series_parser.py` now handles `walkingdeads09e14` style patterns
2. **`find_with_fuzzy()` enhancement** - `tmdb_series_index.py` now calls `strip_episode_info()` before fuzzy matching

These changes are already in place but **not yet validated**.

---

## 3. Phase-by-Phase Execution Plan

### Phase 0: Preparation (22:00-22:30)

#### 0.1 Clarification Needed BEFORE Starting

> **The current `batch_fuzzy_update.py` and `match_series_to_ids.py` scripts already exist and have been run partially.**
>
> **Question:** Have these never been run on the current DB, or have they been run and we just want to improve them? Do you want to start fresh (clear existing TMDB IDs) and re-run the full pipeline?
>
> This determines whether we're doing initial enrichment vs. re-enrichment with improved logic.

#### 0.2 Backup
```bash
# Create timestamped backup
cp -a /home/ubuntu/aiostreams/data/comet-fresh/magnetico/active.search.sqlite3 \
  /home/ubuntu/aiostreams/data/comet-fresh/magnetico/backup-pre-enrichment-$(date +%Y%m%d_%H%M%S).sqlite3

# Verify backup integrity
sqlite3 /home/ubuntu/aiostreams/data/comet-fresh/magnetico/backup-pre-enrichment-*.sqlite3 "SELECT COUNT(*) FROM media_index"
```

**Storage:** 64GB available. Backup will need ~15GB. ✅ Feasible.

#### 0.3 Baseline Validation
```bash
cd /home/ubuntu/aiostreams

# Get current match rates
sqlite3 data/comet-fresh/magnetico/active.search.sqlite3 << 'EOF'
SELECT 
  content_class,
  COUNT(*) as total,
  SUM(CASE WHEN tmdb_id IS NOT NULL THEN 1 ELSE 0 END) as matched,
  ROUND(100.0 * SUM(CASE WHEN tmdb_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as match_rate
FROM media_index 
GROUP BY content_class;
EOF

# Run validation on unmatched samples
python3 scripts/validate_unmatched_sample.py --category all --sample 500 --db-path data/comet-fresh/magnetico/active.search.sqlite3
```

#### 0.4 Verify Previous Changes Are In Place
Check these files exist and contain expected changes:
- `bitmagnet-media/classifier/shared_series_parser.py` - `_strip_embedded_episode_patterns()` function
- `bitmagnet-media/classifier/tmdb_series_index.py` - `find_with_fuzzy()` calls `strip_episode_info()`

---

### Phase 1: Gap Remediation (22:30-01:00)

#### 1.1 [HIGH] Series Roman/Arabic Variants

**File:** `bitmagnet-media/classifier/tmdb_series_index.py`

**Problem:** The `find()` method does exact lookup only. Movies use `generate_title_variants()` to handle Roman/Arabic numeral conversion, but series doesn't.

**Current code (approx line 120-130):**
```python
# Phase 1: Main title
candidates = self.series_by_clean.get(clean, [])
```

**Change to:**
```python
# Generate variants (Roman/Arabic) - same logic as movies
from scripts.lib.radarr_parser import generate_title_variants
variants = generate_title_variants(clean)
for variant in variants:
    if variant in self.series_by_clean:
        candidates = self.series_by_clean[variant]
        if candidates:
            break
```

**Verification:**
```bash
cd /home/ubuntu/aiostreams
python3 -c "
from scripts.lib.radarr_parser import generate_title_variants
print(generate_title_variants('godfather'))
print(generate_title_variants('rocky'))
print(generate_title_variants('breakingbad'))
"
# Expected: {'godfather', 'godfather2', 'godfatherii', ...}
```

**Expected Impact:** Recover mismatches like "godfather2" ↔ "godfatheri", "rockyiv" ↔ "rocky4", "starwars4" ↔ "starwarsiv"

---

#### 1.2 [HIGH] Series Alt Title Variants

**File:** `bitmagnet-media/classifier/tmdb_series_index.py`

**Problem:** Series alt titles don't get Roman/Arabic variant treatment. Movies check alt titles WITH variants.

**Current (approx Phase 3):**
```python
# Phase 3: Alternative titles
tmdb_ids = self.alt_titles.get(clean, [])
```

**Change to:**
```python
# Phase 3: Alternative titles (with variants)
tmdb_ids = []
for variant in generate_title_variants(clean):
    tmdb_ids.extend(self.alt_titles.get(variant, []))
```

---

#### 1.3 [HIGH] Live Classifier AKA Iteration

**File:** `bitmagnet-media/classifier/bitmagnet_smart_hint.py`

**Problem:** `find_best_tmdb_id_for_movie()` only uses `parsed.title`, not the AKA variants in `parsed.titles`. Backfill scripts use it but live classifier doesn't.

**Current (approx line 378-393):**
```python
parsed = parse_movie_title(torrent.name)
if parsed and parsed.title:
    clean = clean_movie_title(parsed.title)  # Only uses primary title!
    tmdb_index = get_tmdb_index()
    if tmdb_index and tmdb_index.movie_data:
        match = tmdb_index.find(clean, parsed.year or classification.year)
```

**Change to:**
```python
parsed = parse_movie_title(torrent.name)
if parsed and parsed.titles:
    lookup_titles = set()
    for title in parsed.titles[:5]:  # Max 5 titles
        clean = clean_movie_title(title)
        lookup_titles.update(generate_title_variants(clean))
    
    tmdb_index = get_tmdb_index()
    if tmdb_index and tmdb_index.movie_data:
        for lookup in lookup_titles:
            match = tmdb_index.find(lookup, parsed.year or classification.year)
            if match:
                break
```

**Expected Impact:** Release names like "L'hypothèse du movie volé AKA The Hypothesis" will match on secondary title.

---

#### 1.4 [MEDIUM] Series Secondary Year Support

**File:** `bitmagnet-media/classifier/tmdb_series_index.py`

**Problem:** Movies check `secondary_year` from TMDB (for movies released in different years on different markets). Series only has single `year`.

**Change:** Add `secondary_year` column to series index and apply same `year_matches()` logic.

**Note:** This requires rebuilding the TMDB series index. May defer if time permits.

---

#### 1.5 [MEDIUM] Fuzzy Matcher: Alt Titles as Candidates

**File:** `bitmagnet-media/classifier/fuzzy_series_matcher.py`

**Problem:** Fuzzy matcher only indexes `clean_title`, not alt/translation variants.

**Change:** Build a parallel index of `alt_title → tmdb_id` for fuzzy fallback.

**Note:** This is complex - defer to Phase 2 if time permits.

---

### Phase 2: Test Run on Sample (01:00-03:00)

#### 2.1 Smoke Test - Series Matching
```bash
cd /home/ubuntu/aiostreams

# Dry run on 5000 series keys
python3 scripts/match_series_to_ids.py --dry-run --limit 5000 --batch-size 1000

# Expected output: shows how many would match
```

#### 2.2 Smoke Test - Movie Matching
```bash
# Dry run on 5000 movie keys
python3 scripts/match_movies_to_tmdb.py --dry-run --limit 5000 --batch-size 1000
```

#### 2.3 Smoke Test - Fuzzy Update
```bash
# Dry run on 1000 keys
python3 scripts/batch_fuzzy_update.py --limit 1000 --dry-run --db-path data/comet-fresh/magnetico/active.search.sqlite3
```

#### 2.4 Measure Improvement
```bash
# Compare before/after match rates
sqlite3 data/comet-fresh/magnetico/active.search.sqlite3 << 'EOF'
SELECT 
  content_class,
  COUNT(*) as total,
  SUM(CASE WHEN tmdb_id IS NOT NULL THEN 1 ELSE 0 END) as matched,
  ROUND(100.0 * SUM(CASE WHEN tmdb_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as match_rate
FROM media_index 
GROUP BY content_class;
EOF
```

---

### Phase 3: Full Run + Validation (03:00-05:30)

#### 3.1 Full Series/Anime Enrichment
```bash
cd /home/ubuntu/aiostreams

# Run series matching (4-phase cascade)
python3 scripts/match_series_to_ids.py --batch-size 5000 2>&1 | tee logs/series_match_$(date +%Y%m%d).log

# Run fuzzy matching for remaining unmatched
python3 scripts/batch_fuzzy_update.py --batch-size 1000 2>&1 | tee logs/fuzzy_match_$(date +%Y%m%d).log
```

**Expected runtime:** ~2-3 hours for full run

#### 3.2 Full Movie Enrichment
```bash
cd /home/ubuntu/aiostreams

# Run movie matching
python3 scripts/match_movies_to_tmdb.py --batch-size 5000 2>&1 | tee logs/movie_match_$(date +%Y%m%d).log
```

#### 3.3 Manual Precision Audit (Sample-Based)

For each category, manually review 200 matched results:

```bash
# Get samples for audit - Series
sqlite3 data/comet-fresh/magnetico/active.search.sqlite3 << 'EOF'
.mode csv
.headers on
.output audit_series.csv
SELECT name, tmdb_id, content_class, confidence 
FROM media_index 
WHERE content_class IN ('episode', 'multi_episode', 'season_pack')
  AND tmdb_id IS NOT NULL
  AND confidence >= 0.8
ORDER BY RANDOM()
LIMIT 200;
EOF

# Get samples - Anime
sqlite3 data/comet-fresh/magnetico/active.search.sqlite3 << 'EOF'
.output audit_anime.csv
SELECT name, tmdb_id, content_class, confidence 
FROM media_index 
WHERE is_anime = 1 AND tmdb_id IS NOT NULL
ORDER BY RANDOM()
LIMIT 200;
EOF

# Get samples - Movies
sqlite3 data/comet-fresh/magnetico/active.search.sqlite3 << 'EOF'
.output audit_movies.csv
SELECT name, tmdb_id, content_class, confidence 
FROM media_index 
WHERE content_class = 'movie' AND tmdb_id IS NOT NULL
ORDER BY RANDOM()
LIMIT 200;
EOF
```

**Manual audit procedure:**
1. For each CSV sample, manually verify: Does `tmdb_id` actually correspond to the content in `name`?
2. Use `https://www.themoviedb.org/movie/{tmdb_id}` to verify
3. Count errors per category
4. Calculate error rate = errors / total_audited

**Target:** <2% error per category

---

### Phase 4: Clear Unmatched Content (05:30-06:00)

#### 4.1 Pre-Clear Validation Checklist
- [ ] All enrichment scripts completed without errors
- [ ] Error rate validated <2% per category
- [ ] Manual audit passed (or decisions documented)
- [ ] Backup verified

#### 4.2 Clear Categories

**Movies (clear ALL unmatched):**
```sql
-- Foreign + adult + no TMDB match = clear
DELETE FROM media_index 
WHERE content_class = 'movie' 
  AND tmdb_id IS NULL;
-- Expected: ~4.7M rows deleted
```

**Episodes (confidence <0.8 OR is foreign):**
```sql
-- Clear low confidence + foreign
DELETE FROM media_index 
WHERE content_class IN ('episode', 'multi_episode', 'season_pack')
  AND (tmdb_id IS NULL AND confidence < 0.8);
-- Expected: ~300K rows deleted

-- For high confidence but still unmatched, manually review before clearing
-- These are likely genuinely foreign content
```

**Anime (all unmatched):**
```sql
-- Anime unmatched = clear (VOSTFR, subs, no TMDB coverage)
DELETE FROM media_index 
WHERE is_anime = 1 AND tmdb_id IS NULL;
-- Expected: ~45K rows deleted
```

**Series (high confidence but unmatched):**
```sql
-- These are likely foreign/localized - clear
DELETE FROM media_index 
WHERE content_class IN ('episode', 'multi_episode', 'season_pack')
  AND is_anime = 0
  AND tmdb_id IS NULL
  AND confidence >= 0.8;
-- Expected: ~600K rows deleted (foreign/localized)
```

#### 4.3 Post-Clear Stats
```sql
SELECT 
  content_class,
  COUNT(*) as remaining,
  SUM(CASE WHEN tmdb_id IS NOT NULL THEN 1 ELSE 0 END) as matched,
  ROUND(100.0 * SUM(CASE WHEN tmdb_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as match_rate
FROM media_index 
GROUP BY content_class;
```

---

## 4. Expected Final State

| Category | Match Rate Target | Error Rate Target | Action for Unmatched |
|----------|-----------------|------------------|---------------------|
| Movies | >80% | <2% | Clear |
| Episodes | >70% | <2% | Clear (foreign) |
| Anime | >80% | <2% | Clear |
| Season Packs | >70% | <2% | Clear (foreign) |
| Multi-episodes | >70% | <2% | Clear (foreign) |

---

## 5. Key Files Reference

### Scripts (Enrichment)
| Script | Purpose | Key Parameters |
|--------|---------|---------------|
| `scripts/match_series_to_ids.py` | Match series keys against TMDB series index | `--dry-run`, `--batch-size` |
| `scripts/match_movies_to_tmdb.py` | Match movie keys against TMDB movie index | `--dry-run`, `--batch-size` |
| `scripts/batch_fuzzy_update.py` | RapidFuzz fuzzy matching for remaining unmatched | `--limit`, `--dry-run`, `--batch-size` |
| `scripts/validate_unmatched_sample.py` | Determine % of unmatched that is valid media vs noise | `--category`, `--sample` |

### TMDB Index Building
| Script | Purpose |
|--------|---------|
| `scripts/build_tmdb_series_index.py` | Build series index from TMDB API |
| `scripts/build_tmdb_movie_index.py` | Build movie index from TMDB API |

### Classifier Files (Gap Remediation)
| File | Purpose | Gap |
|------|---------|-----|
| `bitmagnet-media/classifier/tmdb_series_index.py` | Series TMDB matching | Roman variants, alt title variants |
| `bitmagnet-media/classifier/bitmagnet_smart_hint.py` | Live classifier | AKA title iteration |
| `bitmagnet-media/classifier/fuzzy_series_matcher.py` | RapidFuzz fuzzy matching | Alt titles as candidates |
| `scripts/lib/radarr_parser.py` | Title normalization | `generate_title_variants()` function |

### Database
| File | Purpose |
|------|---------|
| `data/comet-fresh/magnetico/active.search.sqlite3` | Main SQLite DB (7GB) |
| `data/tmdb/series_index.sqlite3` | TMDB series index (218K entries) |
| `data/tmdb/movie_index.sqlite3` | TMDB movie index (1.1M entries) |

---

## 6. Risk Mitigation

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| New code introduces regressions | Medium | Keep old code as fallback during testing |
| Error rate >2% after fixes | Medium | Don't clear DB until validated |
| TMDB coverage gap (foreign content) | High | Accept limitation, clear foreign content |
| DB corruption during clearing | Low | Full backup before any DELETE |
| Script runtime too long | Medium | Reduce batch sizes, spread over multiple nights |
| Storage issues | Low | Pause, clean old backups |

---

## 7. Fallback Procedures

| Issue | Action |
|-------|--------|
| Error rate >2% in any category | Stop clearing, investigate specific error patterns |
| Script runtime too long | Reduce batch sizes, spread over multiple nights |
| Storage issues | Pause, clean old backups |
| Regression in matching | Restore backup, revert changes incrementally |

### Restore from Backup
```bash
# If issues occur, restore from backup
cp /home/ubuntu/aiostreams/data/comet-fresh/magnetico/backup-pre-enrichment-*.sqlite3 \
   /home/ubuntu/aiostreams/data/comet-fresh/magnetico/active.search.sqlite3
```

---

## 8. Clarifications Needed Before Starting

1. **Previous runs:** Have `match_series_to_ids.py` and `batch_fuzzy_update.py` been run on the current DB? Should we start fresh or continue from current state?

2. **Foreign content:** Confirm: Clear all Chinese/Korean/Russian/Japanese content? No effort to match these?

3. **Adult content:** Confirm: Clear all adult content in movie category? No effort to match?

4. **Anime VOSTFR:** Confirm: French-subtitled anime (VOSTFR) should be cleared? No effort to match?

---

## 9. Execution Checklist

### Before Starting
- [ ] Clarify questions answered
- [ ] Backup created and verified
- [ ] Baseline validation run
- [ ] Previous session changes verified in place

### After Phase 1 (Gap Remediation)
- [ ] Code changes reviewed
- [ ] Smoke tests pass

### After Phase 2 (Test Run)
- [ ] Match rate improved
- [ ] Error rate acceptable

### After Phase 3 (Full Run)
- [ ] All enrichment complete
- [ ] Manual audit passed (<2% error per category)

### After Phase 4 (Clear)
- [ ] Unmatched content cleared
- [ ] Post-clear stats verified
- [ ] Documentation updated

---

## 10. Timeline Summary

```
22:00-22:30  Phase 0: Preparation + Backup
22:30-01:00  Phase 1: Gap Remediation (1.1, 1.2, 1.3)
01:00-03:00  Phase 2: Test Run on Sample
03:00-05:30  Phase 3: Full Run + Manual Validation
05:30-06:00  Phase 4: Clear Unmatched Content
06:00+       Review Results + Document
```

---

## Appendix: Radarr/Sonarr Matching Reference

### Radarr Title Normalization (`Parser.cs`)
```csharp
// Articles removed: a, an, the (when NOT at start/end)
// Connector words removed: and, or, of (same rules)
// All non-word characters removed
// German umlauts replaced: ö→oe, ä→ae, ü→ue, ß→ss
// Accents removed via Unicode normalization
```

### Sonarr Title Normalization (`Parser.cs`)
```csharp
// Same normalization as Radarr
// Stopword removal via NormalizeRegex
// Accent removal via RemoveAccent()
```

### Radarr/Sonarr Matching Cascade
1. **Exact match** on `CleanTitle`
2. **Exact match** on `CleanOriginalTitle`
3. **Exact match** on `AlternativeTitles[].CleanTitle`
4. **Exact match** on `Translations[].CleanTitle`
5. **Fuzzy match** (Levenshtein distance <= 1) - only for interactive search

### Roman Numeral Handling (Radarr)
```python
# generate_title_variants() in scripts/lib/radarr_parser.py
ROMAN_TO_ARABIC = {
    "i": "1", "ii": "2", "iii": "3", "iv": "4", "v": "5",
    ...
}
# For "ghostbusters2" → generates "ghostbustersii"
# For "godfather" → generates "godfather2", "godfatherii", etc.
```

### AKA Title Parsing (Radarr)
```python
# Split on AKA or /
AKA_REGEX = re.compile(r"[ ]+(?:AKA|\/)[ ]+", re.IGNORECASE)
# Handle "(aka ...)" patterns
BRACKETED_AKA_REGEX = re.compile(r"(.*) \([ ]*AKA[ ]+(.*)\)", ...)
# Multiple titles stored in MovieTitles list
```
