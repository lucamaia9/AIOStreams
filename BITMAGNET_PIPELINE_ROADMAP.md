# BitMagnet Pipeline Improvement Roadmap

> Created: 2026-04-03
> Status: Active — prioritized backlog

## Current Coverage Baseline (2026-04-03)

| Category | Coverage | Target | Status |
|----------|----------|--------|--------|
| Overall | 31.73% | — | — |
| Anime | 72.09% | 50%+ | ✅ Exceeded |
| Movies | 22.12% | 60%+ | ❌ Blocked by misclassified content |
| Episodes | 52.30% | 70%+ | 🟡 In progress |

> **2026-04-03 Update**: Series matching (`resolve_series_direct.py`) ran in 61s, matched 14,954 series keys (2.5% match rate), updated 92,157 rows. Episodes +1.9%, season packs +6.9%. Movie matching is fundamentally broken (0% match rate) due to dirty `movie_key` values from Go classifier.

## Pipeline Health Summary

| Stage | Status | Notes |
|-------|--------|-------|
| DHT Crawling | ✅ Healthy | ~6K torrents/hr |
| Go Intake Filter | ✅ Effective | Blocks 60-70% junk pre-DB |
| Python Smart Hint | ✅ Working | 90-95% rejection of remaining |
| Go Classifier | ✅ Functional | TMDB/IMDB matching + API fallback |
| SQLite Promotion | ✅ Working | 10-20/min inline |
| Comet MagneticoLocal | ✅ Working | 59 streams Matrix, 17 Breaking Bad |
| Historical ID Backfill | 🟡 Stage-only | Run against an offline stage DB; no installed systemd backfill service on this host |
| Daily Pruning | ❌ Disabled | Timer off post-wide-drain |
| Live Promotion Timer | ⚪ Not installed | Inline promotion is active; use `run_bitmagnet_live_promotion.sh` manually or add your own scheduler if needed |

---

## Priority Backlog

### P1 — Run Historical ID Enrichment Safely

**Task**: Prepare an offline stage DB, run `backfill_lean_ids.py` against it, and monitor progress there.

**Why**: Historical ID coverage is still one of the biggest wins, but direct bulk writes to the live `active.search.sqlite3` are unsafe and the old `aiostreams-lean-id-backfill.service` is not installed on this host.

**Commands**:
```bash
cp /home/ubuntu/aiostreams/data/comet-fresh/magnetico/active.search.sqlite3 \
  /home/ubuntu/aiostreams/data/comet-fresh/magnetico/backups/enriched-stage.backup.sqlite3
cp /home/ubuntu/aiostreams/data/comet-fresh/magnetico/backups/enriched-stage.backup.sqlite3 \
  /home/ubuntu/aiostreams/data/comet-fresh/magnetico/enriched-stage.sqlite3

LEAN_ID_BACKFILL_SEARCH_DB=/home/ubuntu/aiostreams/data/comet-fresh/magnetico/enriched-stage.sqlite3 \
LEAN_ID_BACKFILL_STATE=/home/ubuntu/aiostreams/data/comet-fresh/magnetico/enriched-stage.state.json \
LEAN_ID_BACKFILL_LOG=/home/ubuntu/aiostreams/logs/enriched-stage.log \
  /home/ubuntu/aiostreams/scripts/run_lean_id_backfill.sh

LEAN_ID_BACKFILL_STATE=/home/ubuntu/aiostreams/data/comet-fresh/magnetico/enriched-stage.state.json \
LEAN_ID_BACKFILL_LOG=/home/ubuntu/aiostreams/logs/enriched-stage.log \
  /home/ubuntu/aiostreams/scripts/watch_lean_id_backfill.sh
```

**What to look for**:
- Current phase (`propagate_series`, `resolve_series`, `propagate_anime`, `resolve_anime`, `propagate_movies`, `resolve_movies`, `complete`)
- `remainingGroups` count trending down
- `etaSeconds` reasonable (not infinite or growing)
- No repeated failures in the stage-run log

**Cutover**:
```bash
python3 /home/ubuntu/aiostreams/scripts/rebuild_live_sqlite.py \
  --search-db /home/ubuntu/aiostreams/data/comet-fresh/magnetico/active.search.sqlite3 \
  --historical-seed-db /home/ubuntu/aiostreams/data/comet-fresh/magnetico/enriched-stage.sqlite3 \
  --integrity-check full \
  --cutover
```

**If stuck**: Identify which phase is blocked, check for memory pressure, consider manual intervention with `--max-groups` audit runs.

---

### P2 — Build Episode Enrichment Script

**Task**: Create `match_episodes_to_ids.py` following the same pattern as `match_anime_to_ids.py`.

**Why**: Episodes at 49.82% → target 70%. This is the most actionable coverage gap.

**Approach**:
1. Extract unique `series_key` values from unmatched episode rows
2. Build a lightweight series-to-TMDB/IMDB lookup from existing `canonical_title_family` + `canonical_title_variant` tables
3. Fan out resolved series IDs to all episode rows sharing that `series_key`
4. Use set-based `UPDATE` with indexed `series_key` column (same optimization that made anime matching work in 70s)

**Estimated impact**: +20% episode coverage

**Files to create**:
- `aiostreams/scripts/match_episodes_to_ids.py`

**Dependencies**: `canonical_title_family`, `canonical_title_variant` tables already populated by backfill.

---

### P3 — Fix Go/Python Episode Parsing Merge Conflict

**Task**: Ensure Go's simpler regex (~4 patterns) cannot overwrite Python's superior Sonarr parser (109 patterns).

**Why**: This affects the quality of ALL future content entering the pipeline. Every episode parsed by Go instead of Python loses accuracy.

**Root cause**: The `Merge` function in Go checks `len(a.Episodes) == 0` AFTER Go parses, so Python's results are lost if Go finds any episodes.

**Fix location**: `bitmagnet-media/internal/processor/` — likely `action_parse_video_content.go` or `classification/result.go`

**Approach**:
1. Check if Python already provided episodes via `ctx.torrent.Hint.Episodes`
2. If yes, skip Go parsing entirely and use Python's results
3. If no, fall back to Go parsing as before

**Note**: A fix was attempted on 2026-04-02 (see `info.md` entry). Verify it's still in place and working correctly after the repo reorganization to `bitmagnet-media/`.

---

### P4 — Recreate BitMagnet Container for New Paths

**Task**: Run `sudo docker compose --profile bitmagnet up -d --build bitmagnet` to pick up new `bitmagnet-media/` bind mounts.

**Why**: The running container still uses old paths from `bitmagnet.old/classifier-tools`. All new code changes (intake filter, smart hint, promotion) won't apply until the container is recreated.

**Risk**: Brief interruption to DHT crawling (~30s). Queue jobs will resume after restart.

**Pre-flight checks**:
```bash
# Verify compose config is correct
sudo docker compose config | grep -A5 "bitmagnet:"

# Check current bind mounts
sudo docker inspect bitmagnet | grep -A2 "Source.*bitmagnet"
```

**Post-flight verification**:
```bash
# Verify new mounts
sudo docker inspect bitmagnet | grep "bitmagnet-media"

# Verify health
sudo docker exec bitmagnet wget -qO- http://127.0.0.1:3333/status

# Verify smart hint still works
sudo docker compose logs bitmagnet 2>&1 | grep "smart hint" | tail -5
```

---

### P5 — Re-enable Pruning Only; Keep Live Promotion Deliberate

**Task**: Re-enable pruning once the guard allows it, but do not assume a live-promotion timer exists on this host.

**Why**: Postgres will grow unbounded without pruning. Live promotion already has inline coverage, and any recurring catch-up scheduling should be recreated intentionally instead of assuming the old user timer is present.

**Commands**:
```bash
systemctl --user enable --now aiostreams-bitmagnet-prune.timer

# Optional manual catch-up run
/home/ubuntu/aiostreams/scripts/run_bitmagnet_live_promotion.sh --max-batches 1
```

**Note**: Verify `wideDrainCompleted=true` in SQLite metadata before re-enabling pruning. `aiostreams-bitmagnet-live-promotion.timer` is not installed on this host; only recreate a scheduler if inline promotion plus manual catch-up proves insufficient.

---

### P6 — Movie Coverage Improvement (Deferred)

**Task**: Improve movie coverage from 21.75% to 60%+.

**Why**: The unmatched "movie" pool is 95%+ misclassified content (anime, Russian TV, noise, Chinese dramas). Exact matching has hit its ceiling.

**Options** (pick one or more):
1. **Improve classification quality**: Better pre-filtering to reduce anime/Russian TV/noise in the "movie" bucket
2. **TMDB API fallback**: Batch API lookups for top unmatched movie keys (rate-limited, cost consideration)
3. **Fuzzy matching**: Token-level similarity matching for near-miss titles
4. **Accept current state**: Recognize that the 21.75% represents actual movies and the rest is misclassified junk

**Status**: Deferred pending decision on approach. The matching logic works correctly — there simply aren't real movies left to match in the unmatched pool.

---

## Completed Items

- [x] **Phase 0: Cleanup** — Deleted stale backup (393MB freed), archived old timer/service units
- [x] **Phase 1: Indexes** — Dropped corrupted indexes, recreated 4 indexes safely
- [x] **Phase 2: Unified normalizer** — Created `shared_title_normalizer.py`, 18/18 tests pass
- [x] **Phase 4: Anime matching** — Created `match_anime_to_ids.py`, 72.3% match rate, 70s runtime
- [x] **Movie enrichment Phases 1-2** — Rewrote `match_movies_to_tmdb.py` with 4-phase cascade
- [x] **Normalizer migration** — All 4 callers migrated to `shared_title_normalizer`
- [x] **Historical backfill service hardening** — fixed the old unit while it existed; the current host now uses stage-only manual runs instead
- [x] **Coverage monitoring** — Created `check_bitmagnet_id_coverage.py` + daily timer
- [x] **Wide drain** — Completed 2026-03-22, verified by source-frontier-equality
- [x] **Workspace reset** — Postgres reduced from 18.5GB to 824MB logical
- [x] **Repo reorganization** — `bitmagnet/` → `bitmagnet-media/`, all paths updated

---

## Deferred / Future Considerations

| Item | Notes |
|------|-------|
| Sonarr-style scene mappings | External/provider-fed mappings, regex-scoped alias activation, season-number remapping |
| Generic non-anime season remapping | Deferred until post-run audit shows repeated miss patterns |
| Richer per-mapping search-mode policy | Deferred until canonical-family approach shows material gaps |
| Adult content detection at Go level | Defense-in-depth; currently Python-only |
| Anime detection at Go level | Currently relies entirely on Python's `is_anime` flag |
| Self-healing schema drift | Currently requires manual `rebuild_live_sqlite.py` intervention |
