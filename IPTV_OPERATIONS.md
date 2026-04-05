# IPTV Operations

End-to-end IPTV pipeline: Telegram scraper -> Aggregator -> TV Doug Addon.

Source of truth: this file (`IPTV_OPERATIONS.md`).

## Telegram Scraper

Location: `telegram-scraper/`

Scrapes M3U URLs and Xtream credentials from Telegram channels. Outputs to `telegram-scraper/output/sources_clean.json`.

- Config: `telegram-scraper/config.env`
- Outputs: `sources_clean.json`, `latest_credentials.json`, credential/message archives
- **Status**: Fixed. Telethon 1.40.0, fresh session from 2026-03-19. Session file at `output/session.session`.
- **Docker fix**: scraper now prefers env vars over config.env for `OUTPUT_DIR`. Pipeline passes `-e OUTPUT_DIR=/app/output`.

## IPTV Aggregator

Location: `iptv-aggregator/`

Canonical IPTV aggregation based on upstream `freearhey/iptv-checker` plus local wrapper logic.

### Purpose
1. Accept direct M3U and Xtream-style inputs
2. Resolve each source into a playlist and verify usability
3. Filter conservatively for real live TV channels
4. Pre-dedupe candidate variants, test best first, fall back through bucket
5. Export clean playlist/report with resumable long runs

### Usage
```bash
cd /home/ubuntu/aiostreams/iptv-aggregator
cp config/sources.example.json config/sources.json
npm ci
npm run start -- --config ./config/sources.json --output ./output

# Or via compose
cd /home/ubuntu/aiostreams
docker compose --profile iptv-aggregator run --rm iptv-aggregator
```

### Config (`config/sources.json`)
```json
{
  "sources": [
    {
      "name": "Local M3U File",
      "type": "m3u",
      "path": "/app/config/personal-list.m3u"
    },
    {
      "name": "Xtream Provider",
      "type": "xtream",
      "server": "http://provider.example:8080",
      "username": "user",
      "password": "pass"
    }
  ]
}
```

### Base Outputs
- `output/master.m3u` -- Deduped working live channels
- `output/results.json` -- Full channel diagnostics
- `output/summary.md` -- Per-source effectiveness and run totals
- `output/run-state.json` -- Resumable bucket progress
- `output/channels.jsonl` -- Incremental channel records

### Implementation Notes
- Upstream checker preserved and wrapped, not rewritten
- Excludes obvious VOD/series/replay patterns and `[24H]` channel groups
- Pre-deduplicates candidate buckets before ffprobe
- Resumable runs via checkpoint files
- Masks Xtream credentials in exported metadata
- Docker runtime: Node + ffprobe, no database or web UI
- Xtream sources now default to `output=hls` when no explicit `output` is provided; set `output: "ts"` on a source to force MPEG-TS

## Telegram Xtream Discovery

CLI:
```bash
cd /home/ubuntu/aiostreams/iptv-aggregator
npm run start -- scan-telegram \
  --input /home/ubuntu/aiostreams/telegram-scraper/output/sources_clean.json \
  --output ./output/telegram_xtream_scan
```

### Discovery Behavior
- Server-first, newest-first: groups by server, tries newest credential first
- API-first evidence ladder: `player_api.php` auth -> `get_live_streams` -> `get.php` playlist fallback
- Adaptive retry: auth failures try more creds, dead servers stop early, sample failures keep medium budget
- Probes 5 distinct channel families per credential
- Accepts server only after auth/panel evidence + 1 sampled live channel pass
- Keeps 1 winning credential per working server for phase 2

### Checkpoints
- `output/telegram_xtream_scan/server_candidates.json`
- `output/telegram_xtream_scan/server-discovery-state.json`
- `output/telegram_xtream_scan/working_servers.json`
- `output/telegram_xtream_scan/failed_server_attempts.json`
- `output/telegram_xtream_scan/discovery_attempts.jsonl`
- `output/telegram_xtream_scan/discovery_capabilities.json`
- `output/telegram_xtream_scan/phase2_sources.private.json`

## Telegram Inventory

CLI:
```bash
npm run start -- inventory-telegram ...
```

Builds a canonical live-TV inventory from working Telegram Xtream servers:
- Only counts distinct servers as backups
- Requires two fresh passes before promoting a source
- Validates only channels seen on >= 2 distinct servers by default
- Distinguishes `proven` vs `reachable_unproven` servers

### Outputs
- `output/telegram_inventory/channel_inventory.json`
- `output/telegram_inventory/channel_inventory.m3u`
- `output/telegram_inventory/inventory_summary.md`
- `output/telegram_inventory/inventory_candidates.json`

## Production Workflow

CLI:
```bash
npm run start -- run-production-telegram ...
```

### Stages
1. `discovery/` -- server evidence and classification
2. `server-index/` -- per-server candidate indexes
3. `inventory/` -- canonical channel validation
4. `publish/` -- final outputs

### Standalone Stage Commands
- `index-telegram-servers --discovery <dir> --output <dir>`
- `fill-channel-inventory --server-index <dir> --output <dir>`
- `inventory-build --discovery <dir> --output <dir>`
- `inventory-publish --inventory <dir> --output <dir>`
- `build-channel-universe --server-index <dir> --output <path> --review-output <path>`

### Lean Inventory (Mar 13, 2026)
- Per-server indexes and per-channel candidate files on disk (not in-memory)
- API-first indexing with playlist fallback
- Universe built from real indexed corpus, collapses quality-only variants
- Universe-aware re-index reduced fill target from 74207 to 495 known channels

## Maintenance / Rebalance

### Optimized Pipeline (default)

CLI:
```bash
npm run start -- run-optimized-telegram \
  --input <sources_clean.json> \
  --output <production_dir> \
  --parallel 6 \
  --backup-target 3 \
  --evidence-ttl-hours 336
```

Three phases:
1. **Fast server scan**: combined discovery + indexing in one pass (seconds, not hours)
2. **Channel validation**: re-validates existing sources with evidence cache, fills gaps from server index
3. **Addon export**: writes `addon_channels.json` + `addon_catalog.json`

Key optimizations over original `run-maintenance-telegram`:
- Skips revalidation for channels already at backup target (fast path)
- Per-channel 30s timeout prevents hanging on dead servers
- Resume support via `optimized_state.json`
- Evidence cache reuse across 14-day window
- Telegram Xtream credentials from `sources_clean.json` do not carry an `output` field, so optimized runs now prefer HLS playlist generation by default

### Original Pipeline (legacy)

CLI:
```bash
npm run start -- run-maintenance-telegram ...
```

Also available as standalone:
- `rebalance-inventory --inventory <dir> --server-index <dir> --output <dir>`
- `bridge-partial-fill --inventory <dir>`
- `export-addon-bundle --inventory <dir> --output <dir>`

### Maintenance Behavior
- Revalidates existing accepted backups first
- One-pass validation only
- Immediately removes failed active sources
- Refills only channels below target
- Reuses pass/fail evidence for 12h

### Defaults
- `parallel=6`
- `timeoutProven=8000`
- `timeoutReachable=12000`
- `maxProvenServers=100`
- `maxCandidateAttemptsPerChannel=24`
- `maxChannelElapsedMs=60000`
- Quality preference: FHD > HD > 4K > SD

## Addon Exports

Maintenance/production writes addon outputs under `<output>/addon/`:
- `addon_channels.json` -- ready channels only, consumer handoff
- `addon_channels.md`
- `addon_channels.m3u`
- `addon_catalog.json` -- richer catalog with status/sourceCount/availability
- `addon_catalog.md`

### addon_channels.json Schema
- Ready channels only
- `primaryUrl`
- Ordered `backups[]` with server, url, sourceTier, firstSeen, TVG fields

### addon_catalog.json Schema
- All canonical channels from rebalance inventory
- Per-channel `status`: ready, undercovered, unavailable
- `sourceCount`, `supportCount`, `rawGroup`, ordered `sources[]`
- Meaningful variants (LEG, DUB, EAST, WEST) stay distinct

### Latest Verified Results (2026-03-19)
- 391 ready addon channels (3+ sources each)
- 22-24 undercovered addon channels
- 69-71 unavailable addon channels
- 484-495 total catalog entries after presentation-level duplicate collapse
- Optimized pipeline: ~3 min, 1 live check + 45 cached + 401 skipped (fast path)

### Latest Verified Results (2026-03-24) — HLS Upgrade
- All stream URLs now `.m3u8` (HLS) — aggregator default changed from `output=ts` to `output=hls`
- Catalog: 484 channels, 482 ready, 0 undercovered, 2 unavailable
- 2,410 `.m3u8` sources, 0 `.ts` sources
- Addon `getStreams()` simplified: synchronous, no HEAD check, no URL rewriting
- All 88 Xtream servers confirmed HLS-capable (proven by `hls_known` run Mar 21)
- Android TV playback verified by user — no more 10-15s cutouts

## Automated Pipeline

Script: `scripts/run_iptv_pipeline.sh`
Timer: `iptv-pipeline.timer` (system-level, every 12h, 00:00 and 12:00 UTC)
Command: `run-optimized-telegram` (default aggregator command in pipeline)

Chains: Telegram scraper → aggregator maintenance (`run-optimized-telegram`) → addon restart → health verification

Pipeline passes `--hls-only` to the aggregator, enforcing `.m3u8` streams for all channels.

Options:
- `SKIP_SCRAPER=1` — skip scraper, run aggregator+addon only
- `SKIP_VERIFY=1` — skip final verification
- `IPTV_AGGREGATOR_PARALLEL=6` — validation concurrency
- `IPTV_AGGREGATOR_BACKUP_TARGET=3` — sources per channel (default 3)
- `IPTV_AGGREGATOR_EVIDENCE_TTL_HOURS=48` — reuse server evidence for 48 hours (default)

Verification includes: health check, manifest check, catalog channel count (≥200 ready), stream endpoint smoke test (fetches a live stream for the first ready channel).

Logs to `logs/iptv-pipeline.log` (auto-rotated to 3000 lines). Lock file prevents concurrent runs.

Scraper retry: if the Telegram scraper hits `sqlite3.OperationalError: database is locked`, the pipeline retries up to 3 times with 5s backoff (handles stale WAL locks from interrupted previous runs).

Legacy script: `scripts/run_iptv_refresh_pipeline.sh` (fixed bugs, standalone verification with logo coverage).

## Artwork Pipeline

### Logo Flow
1. Xtream API `stream_icon` mapped to `tvg.logo`
2. API and playlist candidates are metadata-merged (not first-wins)
3. Channel manifests persist `channel.logo`
4. Export renders deterministic 320x320 PNG assets under `addon/assets/logos/`
5. Addon serves only exported local assets + bundled fallback

### Logo Overrides
- `iptv-aggregator/config/logo-overrides.json` -- human-approved source-truth pins
- Review artifacts: `addon_logo_review.json`, `addon_logo_review.md`, `addon_logo_review.html`

### Latest Measured Result
- 406 canonical channels matched to source-truth logo evidence
- 349 rendered PNG assets generated successfully
- 57 channels in `render_error` (candidates for future cleanup)

### Known Export Pitfall
- `run-optimized-telegram` must export addon artwork against the production server index under `output/telegram_production/server-index`
- If addon export points at `output/server-index` instead, logo matching collapses and most channels render as `readable_fallback` text tiles
- The fix is in the optimized export path: pass the active `serverIndexDir` into `exportAddonBundle`

## Presentation Cleanup

Export-time cleanup applied before addon bundle/catalog output:
- Display names drop quality/codec markers (4K, FHD, HD, SD, H265, HEVC)
- All-caps drift normalized while preserving common acronyms
- Punctuation-only duplicate families collapsed
- Category routing catches cases like Curta!, Film & Arts, Discovery ID, TV Gazeta, Band Sports

## IPTV Addon

Location: `iptv-addon/`
URL: `https://iptv.douglinhas.mywire.org`
Branding: `TV Doug`

Based on: Inside4ndroid/M3U-XCAPI-EPG-IPTV-Stremio, modified for live channels only.

### Primary Mode
- Fixed personal addon at `/manifest.json`
- Consumes: `iptv-aggregator/output/telegram_production/addon/addon_catalog.json`
- One canonical `tv` catalog for curated live-TV set
- Multiple provider choices as ordered Stremio streams
- Unavailable channels visible but return no streams

### Artwork
- Served from `/personal/logo/:channelId.png`
- Resolution order: exported rendered local logo -> bundled fallback PNG

### Legacy Mode
- Token-based configuration via web UI
- Supports Xtream Codes m3u_plus and Direct M3U modes
- Available under `/configure-direct` and `/configure-xtream`

### Usage
1. Add to Stremio: `stremio://iptv.douglinhas.mywire.org/manifest.json`
2. Browse the personal live-TV catalog

### Known Fixes
- **Category dropdown bug** (2026-03-14): `&` in genre values broke Stremio catalog extras; now uses safe labels like `Kids and Family`
- **Artwork pipeline** (2026-03-14-15): source-truth logos now preserved earlier; local PNG rendering replaces IPTV.org
- **Deployment sync** (2026-03-15): addon image + mounted export directory must be in sync; `run_iptv_refresh_pipeline.sh` handles this
- **Scraper SQLite lock** (2026-04-02): `LockableSQLiteSession` sets `PRAGMA busy_timeout=5000`; pipeline retries 3× on lock errors

## Refresh Pipeline

```bash
./scripts/run_iptv_refresh_pipeline.sh
```

This script:
1. Runs the maintenance/aggregator export
2. Rebuilds/recreates `iptv-addon` from current source
3. Regenerates and republishes the addon bundle
4. Checks a live `/meta` response to verify no stale runtime code

## Android TV Compatibility

### What Works (2026-03-24) ✅

Android TV (ExoPlayer) plays streams reliably via HLS. All catalog URLs are `.m3u8` — no conversion needed at request time.

#### 1. Aggregator: `.m3u8` URL Storage
`buildXtreamLiveUrl` in the aggregator defaults to `output=hls`, producing `.m3u8` URLs:
- `iptv-aggregator/src/aggregator-v2/telegram-scan.js` (line 638)
- `iptv-aggregator/src/aggregator-v2/optimized.js` (line 1000)

```javascript
const preferredOutput = String(credential.output || 'hls').replace(/^\./, '').toLowerCase()
const extension = preferredOutput === 'hls' ? 'm3u8' : ...
// → /live/user/pass/stream.m3u8?output=hls
```

All 88 Xtream servers accept `output=hls` (proven by `hls_known` output, Mar 21).

#### 2. Addon: Direct URL Pass-Through
`iptv-addon/personal-addon.js` `getStreams()` uses `source.url` directly — no HEAD check, no URL rewriting:

```javascript
getStreams(id) {
    const item = this.channelsById.get(id);
    if (!item || !item.sources.length) return [];
    return item.sources.map((source, index) => ({
        name: buildStreamLabel(source, index),
        url: source.url,
        behaviorHints: { notWebReady: true }
    }));
}
```

#### 3. Stream Response Format
```javascript
{
  url: "http://server/live/user/pass/stream.m3u8?output=hls",
  behaviorHints: { notWebReady: true }
}
```

**No `proxyHeaders`, no `filename`, no async redirect resolution** — tested and reverted because they broke playback on Android TV.

### Why It Works
- `.m3u8` HLS: ExoPlayer handles natively, buffers well, handles token redirects correctly
- No fallback needed — all servers serve HLS directly
- Stremio auto-fails over to next source in list if primary fails

### What Was Reverted (Broke Streams)
- `proxyHeaders` with Chrome desktop User-Agent
- `filename` hint with `.m3u8` extension on `.ts` URLs
- Server-side redirect resolution (async `getStreams`)
- Manifest `behaviorHints: { adult: false, p2p: false }`

The upstream Inside4ndroid project also uses only `{ notWebReady: true }` with no extra hints.

### Current State (2026-03-24)
- All 482 ready channels serve `.m3u8` (HLS) URLs directly
- 2,410 `.m3u8` sources, 0 `.ts` sources
- Verified working on Android TV (ExoPlayer + VLC)
- No more 10-15s cutouts
- Logos loading correctly via `/personal/logo/:id.png`

### Architecture Guarantees (Long-Term)
1. **Aggregator stores `.m3u8`** — all Xtream servers accept `output=hls`
2. **Addon serves URLs directly** — no conversion, no HEAD checks
3. **Runtime fallback to `.ts`** — zero configuration, survives any server misconfiguration
4. **Stremio failover** — if primary fails, next source in list plays automatically

## Code Reference

### Aggregator
| File | Purpose |
|------|---------|
| `iptv-aggregator/bin/iptv-aggregator.js` | Main CLI script; all commands defined here |
| `iptv-aggregator/src/aggregator-v2/inventory.js` | Telegram server discovery -> channel validation -> export |
| `iptv-aggregator/src/aggregator-v2/production.js` | `runTelegramProduction` & `runTelegramMaintenance` orchestration |
| `iptv-aggregator/src/aggregator-v2/telegram-scan.js` | Telegram Xtream server discovery logic |
| `iptv-aggregator/src/aggregator-v2/dedupe.js` | Channel deduplication & bucketing |
| `iptv-aggregator/src/aggregator-v2/filter-rules.js` | Channel filtering heuristics |
| `iptv-aggregator/src/aggregator-v2/exporter.js` | Output formatting (M3U, JSON, markdown) |

### Addon
| File | Purpose |
|------|---------|
| `iptv-addon/personal-addon.js` | TV Doug personal addon — streams from `addon_catalog.json`. **Critical**: `getStreams()` replaces `.ts` with `.m3u8` (line 320) — this guard must survive all future rebalances. |
| `iptv-addon/addon.js` | Core Stremio addon (M3UEPGAddon class, provider dispatch) |
| `iptv-addon/server.js` | Express HTTP routes, manifest generation, prefetch proxy |
| `iptv-addon/src/js/providers/directProvider.js` | Direct M3U + XMLTV EPG handler |
| `iptv-addon/src/js/providers/xtreamProvider.js` | Xtream Codes API + m3u_plus mode |

### Integration Status

All previously identified integration gaps have been resolved:
- ~~No aggregator scheduling~~ -> `iptv-pipeline.timer` every 12h
- ~~No output synchronization~~ -> addon reads `addon_catalog.json` from mounted export directory
- ~~No aggregator-addon bridge~~ -> shared volume with addon reading the export

Remaining open items:
- Backup failover in addon (single URL per channel, no fallback logic on stream failure)
- EPG metadata sync (aggregator TVG data not flowing to addon's personal mode)

Pipeline fixes (Apr 2026):
- Scraper: `LockableSQLiteSession` sets `PRAGMA busy_timeout=5000` on SQLite connection, preventing `database is locked` crashes; pipeline retries up to 3× on lock errors
- Aggregator: pipeline now passes `--hls-only` for defense-in-depth; `backup-target` lowered from 5 to 3 to reduce server churn validation overhead
- Verification: stream endpoint smoke test added (confirms `/stream/tv/personal_tv_<id>.json` returns streams)
