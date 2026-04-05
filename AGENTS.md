# AIOStreams Agent Guide

## Purpose

This file tells coding agents how to work effectively in `/home/ubuntu/aiostreams`.
It replaces the monolithic `info.md` as the primary entry point for project context.

## Subsystem Map

Read the right doc before working on a subsystem:

| Area | Read First | Notes |
|------|-----------|-------|
| Comet code, live path, waves, filtering | `COMET_OPERATIONS.md` | Source lives in `cometouglas/` |
| IPTV aggregator + addon | `IPTV_OPERATIONS.md` | Covers aggregator, addon, Telegram workflow |
| BitMagnet / Magnetico search | `BITMAGNET_OPERATIONS.md` | Runtime/ops source of truth. Then read `bitmagnet-media/BITMAGNET_TARGET_LOGIC.md` and `classifier/unified_classifier/WORKFLOW.md`. Use `bitmagnet-media/DOCUMENTATION_INDEX.md` for the complete docs map. Go code at `bitmagnet-media/internal/`, Python at `bitmagnet-media/classifier/`. |
| Infrastructure (Traefik, VPN, proxy) | `INFRASTRUCTURE.md` | Compose, proxy lanes, networking |
| Any script in `scripts/` | `SCRIPTS_CATALOG.md` | Purpose, usage, related env vars |
| Testing / benchmarks | `TESTING_RUNBOOK.md` | Health checks, smoke tests, acceptance runs |
| High-level architecture | `PROJECT_OVERVIEW.md` | Services, endpoints, performance baselines |
| All documentation | `DOCUMENTATION_INDEX.md` | Master index of every doc in the project |
| Historical changelog | `info.md` | Chronological entries only; do not treat as operational truth |

## Current Runtime Snapshot

- **Comet**: PostgreSQL-backed, live Jackett + Prowlarr scraping, DMM enabled (live), builder for ingestion
- **AIOStreams**: `ghcr.io/viren070/aiostreams:latest` via Traefik
- **Indexers**: Jackett (direct) + Prowlarr (proxy lane) with dedicated FlareSolverr instances
- **IPTV**: Telegram scraper -> aggregator -> TV Doug addon at `iptv.douglinhas.mywire.org`
- **BitMagnet**: DHT crawler with dedicated PostgreSQL, internal-only, `SCRAPE_BITMAGNET=False` on public comet. Source reorganized to `bitmagnet-media/` (2026-04-03). Running container uses old paths until next restart.
- **Proxy**: 3-lane AdGuard (Helsinki for addons, Prowlarr lane, Jackett direct)

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

- Comet code or config -> `COMET_OPERATIONS.md`
- IPTV pipeline -> `IPTV_OPERATIONS.md`
- Infrastructure or compose -> `INFRASTRUCTURE.md`
- New or changed scripts -> `SCRIPTS_CATALOG.md`
- New services or endpoints -> `PROJECT_OVERVIEW.md`
- Significant changes -> add a dated entry to `info.md` (changelog)

## Automation Status

- **IPTV pipeline**: `scripts/run_iptv_pipeline.sh` via `iptv-pipeline.timer` (system-level, every 12h, 00:00 and 12:00 UTC)
  - Chains: Telegram scraper → aggregator (`run-optimized-telegram`) → addon restart → health verification
  - Supports `SKIP_SCRAPER=1` to run aggregator only
  - Logs to `logs/iptv-pipeline.log` (auto-rotated, last 3000 lines)
  - Legacy script: `scripts/run_iptv_refresh_pipeline.sh` (fixed bugs, standalone verification with logo coverage)
