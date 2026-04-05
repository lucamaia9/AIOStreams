# IPTV Infrastructure Analysis - Document Index

## Overview
Complete exploration of the aiostreams IPTV infrastructure (aggregator + addon) for planning purposes only. This index guides you through the analysis documents.

## Quick Start (Pick One)

### 🚀 **For Quick Reference (5 min read)**
→ **File**: `./IPTV_ANALYSIS_INDEX.txt` (238 lines, 11 KB)
- Visual diagram of two-stage pipeline
- Direct answers to all 5 analysis questions
- Key files summary table
- Command reference
- Implementation readiness checklist

### 📖 **For Detailed Analysis (20 min read)**
→ **File**: `./IPTV_OPERATIONS.md` (existing operations guide, comprehensive)
- In-depth explanation of each pipeline stage
- File paths and line numbers for all key code
- JSON structure examples
- Complete configuration details
- Critical gaps analysis

## Five Core Questions Answered

### ❶ What is the current IPTV data flow end-to-end?
**Summary**: Two-stage pipeline
1. **Aggregator** (`/home/ubuntu/aiostreams/iptv-aggregator`) — On-demand Node.js CLI tool that:
   - Discovers Xtream servers via Telegram sources
   - Fetches & parses M3U playlists
   - Validates channels with ffprobe
   - Verifies 3+ backup URLs per channel
   - Exports 401 verified channels to JSON

2. **Addon** (`/home/ubuntu/aiostreams/iptv-addon`) — Always-on Express.js service that:
   - Serves web UI for user configuration
   - Generates Stremio addon manifests
   - Supports Direct M3U and Xtream Codes API providers
   - Caches results (6h TTL)

**Status**: Both implemented ✓ | Not integrated ✗

**Key Files**:
- `/home/ubuntu/aiostreams/iptv-aggregator/bin/iptv-aggregator.js` (28.6 KB, CLI entry)
- `/home/ubuntu/aiostreams/iptv-addon/addon.js` (765 lines)
- `/home/ubuntu/aiostreams/iptv-addon/server.js` (300+ lines)

---

### ❷ What outputs does the aggregator produce for addon consumption?
**Summary**: Primary output is addon_channels.json

**Location**: 
```
/home/ubuntu/aiostreams/iptv-aggregator/output/telegram_production_full_20260313_v5/addon/addon_channels.json
```

**Content**:
- **401 ready channels** with 3+ backup servers each
- **Size**: 570 KB
- **Structure per channel**:
  ```json
  {
    "canonicalId": "amc",              // unique ID
    "displayName": "AMC HD",           // friendly name
    "group": "category:7",             // category
    "primaryUrl": "http://...",        // main stream
    "backups": [
      {
        "order": 1,
        "server": "http://server",
        "url": "http://server/live/...",
        "sourceTier": "proven",
        "firstSeen": "2026-03-11T17:36:13Z",
        "tvgId": "Amc.br",
        "tvgName": "AMC 4K"
      },
      ...  // 2+ more backups
    ]
  }
  ```

**Secondary Outputs**:
- `addon_channels.m3u` — M3U playlist export
- `addon_channels.md` — Markdown report

**Key Code**:
- `/home/ubuntu/aiostreams/iptv-aggregator/src/aggregator-v2/inventory.js` (2419 lines, main validation logic)
- `/home/ubuntu/aiostreams/iptv-aggregator/src/aggregator-v2/exporter.js` (95 lines, JSON export)

---

### ❸ How does the current iptv-addon work and what are its gaps vs desired single personal addon?

**Current Capabilities** ✓:
- Two provider backends: Direct M3U + Xtream Codes API
- Token-based configuration web UI (https://iptv.douglinhas.mywire.org)
- LRU/Redis caching support (6h TTL configurable)
- Stremio-compatible manifest generation
- Optional XMLTV EPG binding

**Current Gaps** ✗:
1. **No aggregator integration** — Doesn't read addon_channels.json (manual workflow)
2. **No backup failover** — Single stream URL per channel, no fallback if primary fails
3. **No automatic refresh** — Requires manual intervention every 12h
4. **No persistent credentials** — Token-based, stateless (user must reconfigure each session)

**Desired Personal Vault Features**:
- Auto-load 401 verified channels from aggregator
- Primary + 3 automatic fallbacks if stream fails
- Automatic 12h refresh from aggregator output
- Persistent personal vault with stored credentials
- Rich catalog with group/category filtering
- TVG/EPG metadata auto-synced

**Key Code**:
- `/home/ubuntu/aiostreams/iptv-addon/addon.js` (765 lines, M3UEPGAddon class)
- `/home/ubuntu/aiostreams/iptv-addon/server.js` (300+ lines, HTTP routes)
- `/home/ubuntu/aiostreams/iptv-addon/src/js/providers/directProvider.js` (~120 lines)
- `/home/ubuntu/aiostreams/iptv-addon/src/js/providers/xtreamProvider.js` (~180 lines)

---

### ❹ Where does automation/workflow live and what already runs every 12h?

**Current Automation Status**:
- ✓ **Implemented**: Maintenance command `run-maintenance-telegram`
- ✗ **NOT scheduled**: No cron, no systemd timer, no GitHub Actions

**What Runs (When Triggered)**:
```bash
npm run start -- run-maintenance-telegram \
  --input ./output/telegram_production_full_20260313_v5/discovery \
  --output ./output/maintenance_YYYYMMDD \
  --inventory ./output/telegram_production_full_20260313_v5/inventory/channel_inventory.json
```

**Workflow Stages**:
1. Revalidate existing channels (parallel=6, timeout tuning)
2. Refill channels below target
3. Rebalance server selection
4. Export addon bundle

**Key Files**:
- `/home/ubuntu/aiostreams/iptv-aggregator/bin/iptv-aggregator.js` (all commands defined)
- `/home/ubuntu/aiostreams/iptv-aggregator/src/aggregator-v2/production.js` (153 lines, orchestration)

**What's Missing for Automation**:
1. No cron job (e.g., `0 */12 * * * ...`)
2. No systemd timer
3. No GitHub Actions workflow
4. No scheduled trigger at all (runs on-demand only)

---

### ❺ What are the key files/docs that should anchor an implementation plan?

**Documentation** (Start Here):
| File | Purpose |
|------|---------|
| `/home/ubuntu/aiostreams/info.md` | Historical notes + IPTV sections (lines 3-250: aggregator, 187-350: addon) |
| `/home/ubuntu/aiostreams/PROJECT_OVERVIEW.md` | Current verified runtime & architecture |
| `./IPTV_ANALYSIS_INDEX.txt` | Quick reference (this analysis) |
| `./IPTV_OPERATIONS.md` | Full analysis with code paths |

**Aggregator Code** (4,320 total lines):
| File | Size | Purpose |
|------|------|---------|
| `bin/iptv-aggregator.js` | 28.6 KB | CLI entry point (all commands) |
| `src/aggregator-v2/production.js` | 153 lines | `runTelegramMaintenance` orchestration |
| `src/aggregator-v2/inventory.js` | 2419 lines | Server discovery → channel validation |
| `src/aggregator-v2/telegram-scan.js` | 945 lines | Xtream server discovery logic |
| `src/aggregator-v2/dedupe.js` | 156 lines | Channel deduplication |
| `src/aggregator-v2/filter-rules.js` | 142 lines | Filtering heuristics |
| `src/aggregator-v2/exporter.js` | 95 lines | JSON/M3U output |

**Addon Code**:
| File | Size | Purpose |
|------|------|---------|
| `addon.js` | 765 lines | M3UEPGAddon class (core logic) |
| `server.js` | 300+ lines | Express HTTP routes |
| `src/js/providers/directProvider.js` | ~120 lines | M3U + XMLTV handler |
| `src/js/providers/xtreamProvider.js` | ~180 lines | Xtream API handler |

**Configuration**:
| File | Purpose |
|------|---------|
| `/home/ubuntu/aiostreams/iptv-addon.env` | PORT=7000, CACHE_TTL_MS=21600000 |
| `/home/ubuntu/aiostreams/iptv-aggregator/config/sources.json` | Input source definitions |
| `/home/ubuntu/aiostreams/compose.yaml` | Docker Compose (lines 285-301: addon, 565-580: aggregator) |

**Current Output** (Production):
| Path | Content |
|------|---------|
| `/home/ubuntu/aiostreams/iptv-aggregator/output/telegram_production_full_20260313_v5/addon/addon_channels.json` | **MAIN**: 401 channels, 3+ backups |
| `/home/ubuntu/aiostreams/iptv-aggregator/output/telegram_production_full_20260313_v5/addon/addon_channels.m3u` | M3U export |

---

## Implementation Readiness Summary

### ✓ READY NOW
- Aggregator core (proven, produces valid output)
- Addon service (running, serving to Stremio)
- Output format (concrete, validated)
- Maintenance workflow logic (12h strategy exists)
- All code accessible and documented

### ✗ MISSING
- Scheduler (cron/systemd/CI-CD for 12h trigger)
- Data pipeline (aggregator JSON → addon sync)
- Backup failover logic (addon needs to try backups)
- Persistent vault (credentials storage)

### ⚠ EFFORT ESTIMATE (PLANNING REFERENCE)
- Scheduling: 1-2 hours
- Addon integration: 4-6 hours
- Testing: 2-3 hours
- **Total**: ~8-12 hours (not including planning/design)

---

## Command Reference

### Run Aggregator (On-Demand)
```bash
cd /home/ubuntu/aiostreams
docker compose --profile iptv-aggregator run --rm iptv-aggregator
```

### Run Maintenance Workflow (12h candidate)
```bash
cd /home/ubuntu/aiostreams/iptv-aggregator
npm run start -- run-maintenance-telegram \
  --input ./output/telegram_production_full_20260313_v5/discovery \
  --output ./output/maintenance_$(date +%Y%m%d_%H%M%S) \
  --inventory ./output/telegram_production_full_20260313_v5/inventory/channel_inventory.json
```

### Export Addon Bundle
```bash
npm run start -- export-addon-bundle \
  --inventory <inventory-path> \
  --output <output-path>/addon
```

### Check Addon Status
```bash
curl -s https://iptv.douglinhas.mywire.org/ | head -20
```

### View Current Output
```bash
cat /home/ubuntu/aiostreams/iptv-aggregator/output/telegram_production_full_20260313_v5/addon/addon_channels.json | jq '.channels[0:2]'
```

---

## Notes

- **All analysis is for planning purposes only** (no implementation)
- **File paths are absolute** (/home/ubuntu/aiostreams base)
- **All code is accessible** and documented
- **Production output is current** (last run: 2026-03-13 21:56 UTC)
- **Integration between aggregator and addon is manual** (needs automation)

---

## Document History

| Date | Version | Notes |
|------|---------|-------|
| 2026-03-14 | 1.0 | Initial exploration complete; 5 questions answered |

---

**Created**: 2026-03-14  
**Scope**: Planning analysis only (no implementation)  
**Status**: Complete ✓
