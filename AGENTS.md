# AIOStreams Agent Guide

## Purpose

This file tells coding agents how to work effectively in `/home/ubuntu/aiostreams`.
It replaces the monolithic `docs/info.md` as the primary entry point for project context.

## Subsystem Map

Read the right doc before working on a subsystem:

| Area | Read First | Deep-Dive Docs | Source Code |
|------|-----------|----------------|-------------|
| Comet code, live path, waves, filtering | `docs/COMET_OPERATIONS.md` | `cometouglas/docs/` | `cometouglas/` |
| IPTV aggregator + addon | `docs/IPTV_OPERATIONS.md` | `iptv-aggregator/docs/` | `iptv-addon/`, `iptv-aggregator/`, `telegram-scraper/` |
| BitMagnet / Magnetico search | `docs/BITMAGNET_OPERATIONS.md` | `bitmagnet-media/DOCUMENTATION_INDEX.md` → `bitmagnet-media/docs/` | `bitmagnet-media/internal/`, `bitmagnet-media/classifier/` |
| BitMagnet pruning / backfill / alignment | `docs/BITMAGNET_OPERATIONS.md` | `bitmagnet-media/docs/03-operations/PRUNING.md`, `BACKFILL_PROCEDURE.md`; `bitmagnet-media/docs/04-classifier/GO_PYTHON_ALIGNMENT.md` | `scripts/bitmagnet/` |
| Infrastructure (Traefik, VPN, proxy) | `docs/INFRASTRUCTURE.md` | — | `traefik/`, `vpn-proxy/`, `compose.yaml` |
| Any script in `scripts/` | `scripts/SCRIPTS_CATALOG.md` | `scripts/README.md` | `scripts/` |
| Testing / benchmarks | `scripts/testing/TESTING_RUNBOOK.md` | — | `scripts/testing/`, `bitmagnet-media/tests/` |
| High-level architecture | `docs/PROJECT_OVERVIEW.md` | — | — |
| All documentation | `DOCUMENTATION_INDEX.md` | — | — |
| Historical changelog | `docs/info.md` | — | — |

## Current Runtime Snapshot

- **Comet**: PostgreSQL-backed, live Jackett + Prowlarr scraping, DMM enabled (live), builder for ingestion
- **AIOStreams**: `ghcr.io/viren070/aiostreams:latest` via Traefik
- **Indexers**: Jackett (direct) + Prowlarr (proxy lane) with dedicated FlareSolverr instances
- **IPTV**: Telegram scraper -> aggregator -> TV Doug addon at `iptv.mrdouglas.uk`
- **BitMagnet**: DHT crawler with dedicated PostgreSQL, internal-only, `SCRAPE_BITMAGNET=False` on public comet. Source reorganized to `bitmagnet-media/` (2026-04-03). Running container uses old paths until next restart.
- **Proxy**: 4-lane AdGuard (Helsinki for addons, Amsterdam Prowlarr lane, London standby, NYC standby)

## Key Conventions

### Docker
- All docker commands require `sudo` on this host
- `sudo docker compose ...` for all container lifecycle operations

### Build Source
- `compose.yaml` builds `comet` from `./cometouglas` (local source, not a remote image)
- Do not point it back to `./comet-custom` without explicit request

### Secrets
- API keys/secrets exist in `.env` files and config files
- Never paste secrets into logs, docs, or committed files

### Network
- External network: `web` (Traefik + routed services)
- Internal network: `vpn-net` (`172.22.0.0/16`)
- Prefer internal Docker DNS over host-public endpoints

### Git
- Comet source: `cometouglas/` with `origin` -> `lucamaia9/cometouglas`, `upstream` -> `g0ldyy/comet`
- AIOStreams fork: `lucamaia9/AIOStreams` with upstream `Viren070/AIOStreams`

## Doc Update Rules

When changing something, update the right doc:

- Comet code or config -> `docs/COMET_OPERATIONS.md`
- IPTV pipeline -> `docs/IPTV_OPERATIONS.md`
- Infrastructure or compose -> `docs/INFRASTRUCTURE.md`
- BitMagnet pruning or backfill -> `bitmagnet-media/docs/03-operations/` (PRUNING.md, BACKFILL_PROCEDURE.md)
- BitMagnet Go/Python filter sync -> `bitmagnet-media/docs/04-classifier/GO_PYTHON_ALIGNMENT.md`
- New or changed scripts -> `scripts/SCRIPTS_CATALOG.md`
- New services or endpoints -> `docs/PROJECT_OVERVIEW.md`
- Significant changes -> add a dated entry to `docs/info.md` (changelog)

## Automation Status

- **IPTV pipeline**: `scripts/run_iptv_pipeline.sh` via `iptv-pipeline.timer` (system-level, every 12h, 00:00 and 12:00 UTC)
  - Chains: Telegram scraper → aggregator (`run-optimized-telegram`) → addon restart → health verification
  - Supports `SKIP_SCRAPER=1` to run aggregator only
  - Logs to `logs/iptv-pipeline.log` (auto-rotated, last 3000 lines)
  - Legacy script: `scripts/run_iptv_refresh_pipeline.sh` (fixed bugs, standalone verification with logo coverage)
- **BitMagnet live promotion**: `bitmagnet-live-promotion.timer` (system-level, every 12h) — promotes classified content into the lean search index
- **BitMagnet daily prune**: `bitmagnet-daily-prune.timer` (system-level, 00:00 UTC) — prunes stale BitMagnet records
- **IPTV health**: `iptv-health.timer` (system-level, 18:00 UTC) — checks IPTV pipeline health
- **AIOStreams rebalance**: `aiostreams-rebalance.timer` (user-level, 04:30 UTC daily)
- **AIOStreams lean ID backfill**: `aiostreams-lean-id-backfill.timer` (user-level, 06:00 UTC daily)
- **AIOStreams BitMagnet coverage**: `aiostreams-bitmagnet-coverage.timer` (user-level, 07:00 UTC daily)
- **AIOStreams cleanup**: `aiostreams-cleanup.timer` (user-level, 05:00 UTC weekly on Saturday)
- **AIOStreams benchmark**: `aiostreams-benchmark.timer` (user-level, 03:00 UTC weekly on Sunday)
