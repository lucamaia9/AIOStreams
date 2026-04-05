# Complete Project Understanding - April 2026

## Executive Summary

This is a sophisticated **self-hosted Stremio scraping stack** with three major subsystems:

1. **Cometouglas v2** - Custom Comet instance with live torrent scraping from multiple indexers
2. **AIOStreams** - Stremio addon service for stream aggregation
3. **IPTV Pipeline** - Automated IPTV aggregation from Telegram sources

All services run on Oracle Cloud infrastructure with multi-region VPN proxy lanes, Traefik reverse proxy, and shared PostgreSQL databases.

---

## 1. AIOStreams Project

**Location**: `/home/ubuntu/aiostreams`

**Purpose**: Self-hosted Stremio addon and scraper stack centered on customized Comet with live torrent retrieval.

### Core Architecture

```
Internet → Traefik (reverse proxy) → Services:
  ├── comet.douglinhas.mywire.org → comet (custom scraper)
  ├── aiostreams.douglinhas.mywire.org → aiostreams (addon)
  ├── jackett.douglinhas.mywire.org → jackett (indexer aggregator)
  ├── prowlarr.douglinhas.mywire.org → prowlarr (indexer manager)
  └── iptv.douglinhas.mywire.org → iptv-addon (IPTV addon)
```

### Key Services

| Service | Type | Purpose |
|---------|------|---------|
| `comet` | Custom scraper | Live scraping from Jackett + Prowlarr + DMM + MagneticoLocal |
| `comet-builder` | DMM ingester | Offline database management population |
| `aiostreams` | Stremio addon | Multi-source stream aggregation (upstream image) |
| `postgres` | Database | Shared PostgreSQL for Comet runtime |
| `bitmagnet-postgres` | Database | Dedicated PostgreSQL for BitMagnet |
| `bitmagnet` | DHT crawler | BitTorrent DHT crawling + smart classification |
| `jackett` | Indexer | Torrent indexer aggregator |
| `prowlarr` | Manager | Torrent indexer manager |

### VPN Proxy Topology (4-Lane)

| Lane | Exit Location | Purpose |
|------|--------------|---------|
| `adguard-proxy-helsinki` | Helsinki (185.77.216.3) | Addon traffic (aiostreams) |
| `adguard-proxy-amsterdam` | Amsterdam (84.17.46.88) | Standby |
| `adguard-proxy-london` | London (154.47.24.154) | Standby |
| `adguard-proxy-nyc` | NYC (79.127.206.186) | Standby |

### Performance Baseline (March 2026)

Cached-only RD (Real-Debrid) benchmark on 20-title corpus:
- **Median latency**: 1.79s
- **P95 latency**: 8.02s
- **RD cached positive**: 18/20 (90%)
- **Median stream count**: 7.0 streams

Live scrape architecture:
- Single-wave parallel search (all indexers fire simultaneously)
- Content-type aware filtering (anime/movie/series)
- Early return on RD cached hits
- RTN (Rank-Torrent-Name) filtering before RD API check

### Custom Modifications

Comet source: `/home/ubuntu/aiostreams/cometouglas`

Modified files (verified):
- `comet/api/endpoints/stream.py` - Unified single-wave parallel search
- `comet/core/models.py` - Live path settings
- `comet/scrapers/jackett.py` - Content-type filtering
- `comet/scrapers/prowlarr.py` - Content-type filtering
- `comet/services/orchestration.py` - Unified scrape entry point
- `comet/services/indexer_health.py` - Circuit breaker (untracked)
- `comet/services/query_dedupe.py` - TTL-based dedup (untracked)

Key features:
- Content-type detection (anime detection via kitsu/anime_mapper)
- Indexer deduplication across Jackett/Prowlarr
- RD probing with chunked precheck + callback
- Cached-only fast return (stops at 10 visible cached hits)

---

## 2. BitMagnet Project

**Location**: `/home/ubuntu/aiostreams/bitmagnet-media`

**Purpose**: BitTorrent DHT crawler with Python smart classification for media enrichment.

### Architecture Decision

**Core Decision**: BitMagnet is **NOT** the source of truth for media classification.

BitMagnet is treated as:
- Temporary raw torrent/file staging store
- Long-running DHT collector
- Torznab/API surface
- Enrichment workspace

**Source of truth** for media filtering: Python smart classifier stack at `classifier/`

### Intended Data Flow

1. Magnetico SQLite database
2. Python smart classification (compact_media_search.py, magnetico_media_probe.py)
3. Lean SQLite media DB beside Comet (`data/comet-fresh/magnetico/active.search.sqlite3`)

For live BitMagnet intake:
1. BitMagnet raw torrents + torrent files
2. Python smart classification (same contract)
3. Same lean SQLite media DB
4. Optional BitMagnet enrichment metadata

### Current State

| Category | Total Entries | Matched | Match Rate |
|----------|--------------|---------|------------|
| Movies | 6.09M | 1.34M | **22.1%** |
| Episodes | 1.80M | 889K | **49.4%** |
| Multi-episodes | 597K | 344K | **57.7%** |
| Season Packs | 197K | 123K | **62.4%** |
| Anime Episodes | 187K | 141K | **75.8%** |
| Anime Packs | 11K | 7K | **67.1%** |
| **Total** | **9.08M** | **2.88M** | **31.7%** |

### Smart Hint Pipeline

Fixed in March 2026:
- Mutex deadlock in processor.go
- Unbounded goroutine concurrency (limited to 4 now)
- `persist()` never wrote to `torrent_hints` table
- Queue job timeout increased to 60 minutes
- `release_year` carried through from Python classifier

### Live Promotion

- Script: `scripts/promote_bitmagnet_live_to_compact.py`
- Inline promotion from BitMagnet PostgreSQL → lean SQLite
- Preserves Python classifier decisions
- Syncs raw provenance + classification + search keys

### Key Scripts

| Script | Purpose |
|--------|---------|
| `promote_bitmagnet_live_to_compact.py` | Live promotion to SQLite |
| `backfill_lean_ids.py` | Offline ID enrichment (blocks live DB writes) |
| `match_movies_to_tmdb.py` | Conservative movie recovery |
| `match_series_to_ids.py` | Series TMDB matching |
| `batch_fuzzy_update.py` | RapidFuzz fuzzy matching |
| `rebuild_live_sqlite.py` | Cutover from stage DB to live |
| `check_bitmagnet_id_coverage.py` | SLO proxy metrics |
| `run_production_gates.py` | Production gate bundle |

---

## 3. Cometouglas Project

**Location**: `/home/ubuntu/aiostreams/cometouglas`

**Branch**: `restore/live-path` (from `pre-restore-baseline` tag)

**Remotes**:
- `origin` → `lucamaia9/cometouglas`
- `upstream` → `g0ldyy/comet`

### Custom Features

**Live Path Architecture (v2 architecture):**
- Single-wave unified parallel search (all scrapers fire simultaneously)
- Content-type detection (anime/movie/series)
- Indexer deduplication across providers
- Early return on RD cached hits
- RTN filtering before RD API check

**MagneticoLocal Historical Search**:
- Compact SQLite at `data/comet-fresh/magnetico/active.search.sqlite3`
- `MagneticoLocalScraper` for historical queries
- Tiered retrieval: exact ID → exact key → canonical family → FTS fallback
- `8.78M` processed rows, `8.66M` searchable rows

**DMM Integration**:
- Public comet: `SCRAPE_DMM=live`, `DMM_INGEST_ENABLED=False`
- Ded builder: `DMM_INGEST_ENABLED=True`, shared PostgreSQL
- Runtime: `data/comet-fresh/runtime/comet-runtime.sqlite3` (staged, not cut over yet)

### Live Path Workflow

```
1. PARSE - Extract media_id, season, episode, resolve aliases
2. DB CACHE CHECK - Load from PostgreSQL, decide if scrape needed
3. CACHEDONLY OVERRIDE - Force live scrape when cached < target
4. CONTENT-TYPE DETECTION - kitsu → anime, movie, series, anime detection
5. UNIFIED SCRAPE (all parallel):
   ├─ magnetico_local (5s timeout)
   ├─ dmm (5s timeout)
   ├─ jackett (10s timeout, content-type filtered)
   └─ prowlarr (10s timeout, content-type filtered)
6. POST-SCRAPE: DB AVAILABILITY LOOKUP
7. RTN FILTER - Filter before RD API (20-50% fewer API calls)
8. NEEDS_DEBRID_CHECK - Only ranked hashes
9. RD API CHECK - Chunks of 500, cache to DB
10. RESPONSE BUILDING - Per-resolution selection
```

### Content-Type Filtering

| Content Type | Jackett Indexers | Prowlarr Indexers |
|-------------|-----------------|-------------------|
| **series** | 13 (all except nyaa, dmhy) | 6-7 |
| **movie** | 15 (all except nyaa, dmhy, eztv) | 7 |
| **anime** | 2-4 (nyaa, dmhy, knaben, bitsearch) | 1 (mikan) |

### Key Settings

Live scrape timing budget:
- `LIVE_SCRAPE_HARD_TIMEOUT=35` - absolute scrape ceiling
- `LIVE_TOTAL_BUDGET_S=35` - total request budget
- `LIVE_SCRAPE_BUDGET_S=30` - scrape-phase budget
- `LIVE_FAST_RETURN_RD_CACHED_RESULTS=10` - cached results before fast return
- `LIVE_EARLY_RETURN_ON_RD_CACHED=True`
- `LIVE_MAX_CANDIDATES_BEFORE_RD_CHECK=400`

Background scraper:
- `BACKGROUND_SCRAPER_ENABLED=True`
- `BACKGROUND_SCRAPER_CONCURRENT_WORKERS=2`
- `BACKGROUND_SCRAPER_MAX_MOVIES_PER_RUN=50`

---

## 4. IPTV Project

**Two sub-projects**:

### A. IPTV Aggregator

**Location**: `/home/ubuntu/aiostreams/iptv-aggregator`

**Purpose**: Aggregate IPTV sources from Telegram scraping, validate channels, export curated catalog.

**Pipeline**:
```
Telegram Scraper → Aggregator → TV Doug Addon
```

**Automated Pipeline**:
- Script: `scripts/run_iptv_pipeline.sh`
- Timer: `iptv-pipeline.timer` (every 12h, 00:00 and 12:00 UTC)
- Optimized pipeline: ~3 min per run

**Latest Results (2026-03-24)**:
- **482 ready channels** (up from 391)
- **0 undercovered** channels
- **2 unavailable** channels
- **2,410 .m3u8 sources** (HLS, all Android TV compatible)
- All streams HLS - no more 10-15s cutouts on Android TV

**Commands**:
```bash
# Optimized maintenance (fast path)
npm run start -- run-optimized-telegram \
  --input sources_clean.json \
  --output production_dir \
  --parallel 6 \
  --backup-target 3

# Discovery (manual)
npm run start -- scan-telegram --input sources_clean.json --output scan_dir

# Inventory (manual)
npm run start -- inventory-telegram --working working_servers.json --output inv_dir
```

**Key Components**:
- `iptv-aggregator/src/aggregator-v2/production.js` - Orchestration
- `iptv-aggregator/src/aggregator-v2/inventory.js` - Channel validation
- `iptv-aggregator/src/aggregator-v2/telegram-scan.js` - Server discovery
- `iptv-addon/personal-addon.js` - TV Doug personal addon

**Android TV Compatibility** (Fixed 2026-03-24):
- All URLs `.m3u8` (HLS) - ExoPlayer handles natively
- No `proxyHeaders`, no `filename` hints - direct URL pass-through
- Verified working on Android TV with no cutouts

### B. Telegram Scraper

**Location**: `/home/ubuntu/aiostreams/telegram-scraper`

**Purpose**: Scrape M3U URLs and Xtream credentials from Telegram channels.

**Status**: Fixed with Telethon 1.40.0, fresh session from 2026-03-19
**Output**: `telegram-scraper/output/sources_clean.json`

---

## 5. Infrastructure

**Location**: Home directory infrastructure docs

### Traefik Reverse Proxy

- Version: v3
- Ports: 80, 443, 8080 (dashboard)
- Domains: `*.douglinhas.mywire.org`
- Auto-redirect HTTP → HTTPS

### Compose Structure

**Networks**:
- `web` - External network for Traefik + routed services
- `vpn-net` - Internal network (`172.22.0.0/16`) for VPN proxy

**Active Services** (default):
- postgres, traefik, aiostreams, comet
- jackett, prowlarr, flaresolverr, flaresolverr-jackett
- adguard-proxy-{helsinki,amsterdam,london,nyc}

**Profile-gated**:
- `bitmagnet` profile: bitmagnet-postgres, bitmagnet
- `builder` profile: NOT behind profile anymore (survives default compose up)

**Removed**:
- byparr services (not needed, BYPARR_MODE=off in Comet)
- aiostreams-custom (commented out)
- comet-prowlarr (commented out)

### Resource Limits

Traefik: 0.5 CPU, 256MB RAM, 256 PIDs
PostgreSQL: 1.0 CPU, 512MB RAM, 256 PIDs
AdGuard proxies: 0.25 CPU, 96MB RAM, 128 PIDs (optimized from actual usage)
BitMagnet: 1.0 CPU, 1.5GB RAM, 512 PIDs

### Automation

**User-level timers**:
- `aiostreams-rebalance.timer` - Daily 03:30 UTC
- `aiostreams-benchmark.timer` - Weekly Sun 02:00 UTC
- `aiostreams-indexer-split.timer` - Weekly Sun 03:00 UTC
- `aiostreams-cleanup.timer` - Weekly Sat 04:00 UTC

**System-level timers**:
- `iptv-pipeline.timer` - Every 12h (00:00, 12:00 UTC)

---

## 6. Data Flow & Storage

### Primary Storage

| Path | Purpose | Size |
|------|---------|------|
| `aiostreams/data/comet-fresh/magnetico/active.search.sqlite3` | Lean SQLite media DB | ~5.6 GB |
| `aiostreams/data/comet-fresh/runtime/comet-runtime.sqlite3` | Staged runtime SQLite | ~2.2 GB |
| `aiostreams/bitmagnet-media/runtime/postgres` | BitMagnet PostgreSQL | ~18.5 GB |
| `aiostreams/cometouglas/data` | Comet work data | Varies |
| `aiostreams/iptv-aggregator/output` | IPTV catalogs | Small |

### Database Relationships

```
BitMagnet PostgreSQL (temporary staging)
    ↓ promote_bitmagnet_live_to_compact.py
Lean SQLite (data/comet-fresh/magnetico/active.search.sqlite3)
    ↓ read by MagneticoLocal scraper
Comet Live Path (reads from SQLite + PostgreSQL)
    ↓ serves to
AIOStreams → Stremio
```

---

## 7. Key Scripts & Tools

### BitMagnet/Classification

| Script | Purpose | Status |
|--------|---------|--------|
| `promote_bitmagnet_live_to_compact.py` | Live promotion to SQLite | ✅ Active |
| `rebuild_live_sqlite.py` | Cutover stage → live | ✅ Active |
| `backfill_lean_ids.py` | Offline ID enrichment | ✅ Blocks live DB |
| `match_movies_to_tmdb.py` | Conservative movie recovery | ✅ Stage-only |
| `match_series_to_ids.py` | Series TMDB matching | ✅ Stage-only |
| `batch_fuzzy_update.py` | Fuzzy matching | ✅ Stage-only |
| `check_bitmagnet_id_coverage.py` | SLO proxy metrics | ✅ Active |
| `run_production_gates.py` | Production gate bundle | ✅ Active |
| `prune_bitmagnet_workspace.py` | PostgreSQL pruning | ⏸️ Blocked until wide drain |

### Comet/Testing

| Script | Purpose |
|--------|---------|
| `run_real_user_benchmark.py` | Real-user benchmark |
| `rebalance_from_live_usage.py` | Indexer rebalance |
| `run_antibot_3lane.py` | Anti-bot investigation |
| `build_indexer_split.py` | Generate indexer split |
| `probe_indexers.py` | Probe indexer health |
| `rebuild_comet_runtime_sqlite.py` | Offline runtime rebuild |

### IPTV

| Script | Purpose |
|--------|---------|
| `run_iptv_pipeline.sh` | Automated pipeline |
| `run_iptv_refresh_pipeline.sh` | Legacy full refresh |
| `run_iptv_health_check.sh` | Health verification |

---

## 8. Current Operational Status

### What's Working (April 2026)

✅ **Comet live path** - Single-wave parallel search operational
✅ **MagneticoLocal** - Compact SQLite with 8.78M entries
✅ **BitMagnet live promotion** - Inline promotion to SQLite healthy
✅ **IPTV pipeline** - Fully automated, 482 ready channels, HLS-only
✅ **Android TV** - No more cutouts, all .m3u8 streams
✅ **DMM integration** - Staged in builder, manual off-peak runs
✅ **RD fast path** - Early return on cached hits working

### Known Issues

⚠️ **Low match rates** - Movies only at 22%, episodes at 50%, target is 60-70%+
⚠️ **Unmatched content** - 6.2M unmatched rows, 77.8% recoverable (don't mass-clear!)
⚠️ **Wide drain not complete** - Parity-correct wide drain still in progress
⚠️ **PostgreSQL pruning blocked** - Blocked until `wideDrainCompleted=true`
⚠️ **Runtime SQLite not cut over** - Still using PostgreSQL for live, SQLite is staging

### Recent Changes

- **2026-04-04**: Staged enrichment cutover, final validated artifact at `rebuild-runs/20260404T061324Z-enriched-stage-cutover`
- **2026-04-04**: Production gate bundle created, currently expected to stay red until SLO improves
- **2026-04-04**: BitMagnet source reorganized from `bitmagnet/` to `bitmagnet-media/`
- **2026-04-04**: `scripts/backfill_lean_ids.py` and `match_movies_to_tmdb.py` now block live DB writes by default
- **2026-04-04**: `scripts/rebuild_live_sqlite.py` now strips old live-overlay rows from `media_fts` using FTS5 delete tokens

---

## 9. Documentation Map

| Doc | Purpose |
|-----|---------|
| `PROJECT_OVERVIEW.md` | Current verified runtime and architecture |
| `AGENTS.md` | Agent entry point: subsystem map, conventions |
| `COMET_OPERATIONS.md` | Comet live path, waves, indexer lanes, DMM |
| `IPTV_OPERATIONS.md` | IPTV aggregator, addon, Telegram workflow |
| `BITMAGNET_OPERATIONS.md` | BitMagnet import, recovery, A/B results |
| `INFRASTRUCTURE.md` | Traefik, VPN proxy, AdGuard topology |
| `BITMAGNET_TARGET_LOGIC.md` | Architecture and ownership boundaries |
| `PLAN.md` | BitMagnet/Comet enrichment master plan |
| `SCRIPTS_CATALOG.md` | All scripts with purpose and usage |
| `TESTING_RUNBOOK.md` | Testing and validation procedures |
| `BITMAGNET_PIPELINE_ROADMAP.md` | Prioritized improvement backlog |

---

## 10. Quick Reference Commands

### Daily Operations

```bash
cd /home/ubuntu/aiostreams

# Check service health
sudo docker compose ps
sudo docker compose logs --tail 20 bitmagnet comet

# Rebuild/restart Comet
sudo docker compose up -d --build comet

# Tail Comet logs
docker logs --since 15m comet
docker logs --since 15m bitmagnet

# Smoke-check endpoints
curl -sS https://comet.douglinhas.mywire.org/manifest.json
curl -sS https://aiostreams.douglinhas.mywire.org/api/v1/status
```

### BitMagnet Operations

```bash
# Check BitMagnet status endpoint
curl http://localhost:3333/status

# Run live promotion manually
python3 scripts/promote_bitmagnet_live_to_compact.py --batch-size 5000

# Check coverage stats
python3 scripts/check_bitmagnet_id_coverage.py --json

# Run production gates
python3 scripts/run_production_gates.py --json

# Rebuild live SQLite from stage
python3 scripts/rebuild_live_sqlite.py \
  --search-db data/comet-fresh/magnetico/active.search.sqlite3 \
  --historical-seed-db data/comet-fresh/magnetico/stage.db \
  --cutover
```

### Testing

```bash
# Real-user benchmark (smoke test, 5 titles)
export REAL_USER_MANIFEST_URL='<direct Comet manifest URL>'
./scripts/run_real_user_benchmark.sh --limit 5

# Full acceptance benchmark (20 titles)
./scripts/run_real_user_benchmark.sh --limit 20

# Rebalance dry run
./scripts/rebalance_from_live_usage.py --dry-run

# Check indexer split
./scripts/run_indexer_split.sh
```

---

## 11. Public Endpoints

All services accessible at:

- **Comet**: `https://comet.douglinhas.mywire.org/manifest.json`
- **AIOStreams**: `https://aiostreams.douglinhas.mywire.org/api/v1/status`
- **Jackett**: `https://jackett.douglinhas.mywire.org`
- **Prowlarr**: `https://prowlarr.douglinhas.mywire.org`
- **IPTV Addon**: `https://iptv.douglinhas.mywire.org/manifest.json` (TV Doug)
- **BitMagnet**: Internal only (not exposed)

---

## 12. Key Technical Decisions

### Why This Architecture?

1. **Lean SQLite-first**: Avoid PostgreSQL overhead for Comet reads, keep BitMagnet as temporary staging
2. **Single-wave parallel search**: Simpler than 3-wave sequential, faster for most titles
3. **Pre-filtered imports**: Don't import everything into BitMagnet hoping it classifies correctly later
4. **4-lane VPN proxy**: Geographic diversity for addon traffic, Helsinki-primary for latency
5. **HLS-only IPTV**: Android TV compatibility, no more streaming cutouts

### What Makes This Stack Unique

1. **Live DHT + Smart Classification**: BitMagnet crawls, Python classifies, SQLite serves
2. **Multi-source scraping**: Jackett + Prowlarr + DMM + MagneticoLocal in parallel
3. **Progressive results**: First-come-first-serve with early return on cached hits
4. **Automated IPTV curation**: Telegram → validation → TV Doug, 12h automated cycle
5. **Custom Comet fork**: Live path optimizations not present in upstream

---

## 13. Next Major Milestones

From `BITMAGNET_PIPELINE_ROADMAP.md`:

**High Priority**:
- Complete parity-correct wide drain from BitMagnet PostgreSQL
- Enable wide-DB pruning once `wideDrainCompleted=true`
- Improve movie match rate from 22% toward 60%+
- Enable fuzzy matching for remaining unmatched series/episodes

**Medium Priority**:
- Cut over runtime SQLite as authoritative Comet backend (validate shared writer pattern)
- Complete unmatched content audit before destructive clears
- Implement Sonarr/Radarr parity for episode parsing (109 patterns vs Go's ~4)

**Low Priority**:
- Anime scene mappings
- Go anime absolute episode handling
- Additional indexer health metrics

---

## Summary

This is a **production-grade, highly customized Stremio scraping stack** with:
- Live multi-source torrent scraping
- BitTorrent DHT crawling with Python smart classification
- Automated IPTV aggregation from Telegram sources
- Multi-region VPN proxy infrastructure
- Comprehensive testing and benchmarking tooling

The stack is operational but has clear improvement targets (match rates, wide drain completion, runtime SQLite cutover) that are well-documented in the roadmap.
