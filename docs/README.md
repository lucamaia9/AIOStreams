# AIOStreams Documentation Index

## How to Read This Project

Start here. This index covers every `.md` file in the project, organized by purpose.

## Quick Start

| What you need | Where to look |
|---------------|---------------|
| Project overview | [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md) |
| Infrastructure (Traefik, VPN, compose) | [INFRASTRUCTURE.md](INFRASTRUCTURE.md) |
| How to test things | [TESTING_RUNBOOK.md](TESTING_RUNBOOK.md) |
| What scripts exist | [SCRIPTS_CATALOG.md](SCRIPTS_CATALOG.md) |
| Changelog | [info.md](info.md) |
| Agent instructions | [AGENTS.md](AGENTS.md) |

## Directory Structure

| Directory | Contents |
|-----------|----------|
| `config/` | Environment configuration files (moved 2026-03-23) |
| `certs/` | TLS/SSL certificates for VPN proxies (moved 2026-03-23) |
| `logs/` | Runtime logs and lock files |
| `data/` | Persistent data volumes |
| `_archive-delete/` | Archived legacy directories |

## Subsystem Operations Guides

These are the primary operational references for each subsystem:

| Subsystem | Operations Guide | Source Code |
|-----------|-----------------|-------------|
| Comet (torrent scraping) | [COMET_OPERATIONS.md](COMET_OPERATIONS.md) | `cometouglas/` |
| IPTV (live TV pipeline) | [IPTV_OPERATIONS.md](IPTV_OPERATIONS.md) | `iptv-addon/`, `iptv-aggregator/`, `telegram-scraper/` |
| BitMagnet (DHT crawler) | [BITMAGNET_OPERATIONS.md](BITMAGNET_OPERATIONS.md) | `go/`, `classifier/` |
| Magnetico search layer | [MAGNETICO_SEARCH_MASTER_PLAN.md](MAGNETICO_SEARCH_MASTER_PLAN.md) | `classifier/` |

## Comet

| File | Purpose |
|------|---------|
| [COMET_OPERATIONS.md](COMET_OPERATIONS.md) | Live path, wave orchestration, DMM, indexer lanes, deployment |
| [cometouglas/README.md](cometouglas/README.md) | Upstream Comet project README |
| [cometouglas/CHANGELOG.md](cometouglas/CHANGELOG.md) | Upstream release history |
| [cometouglas/docs/README.md](cometouglas/docs/README.md) | Comet documentation index (beginner + advanced paths) |
| [cometouglas/docs/beginner/01-get-started-docker.md](cometouglas/docs/beginner/01-get-started-docker.md) | Docker setup guide |
| [cometouglas/docs/beginner/02-configure-and-install-stremio.md](cometouglas/docs/beginner/02-configure-and-install-stremio.md) | Stremio configuration |
| [cometouglas/docs/beginner/03-admin-dashboard.md](cometouglas/docs/beginner/03-admin-dashboard.md) | Admin dashboard guide |
| [cometouglas/docs/advanced/01-runtime-architecture.md](cometouglas/docs/advanced/01-runtime-architecture.md) | Runtime process model |
| [cometouglas/docs/advanced/02-configuration-model.md](cometouglas/docs/advanced/02-configuration-model.md) | Config model and env vars |
| [cometouglas/docs/advanced/03-streaming-and-debrid-flow.md](cometouglas/docs/advanced/03-streaming-and-debrid-flow.md) | Streaming and debrid flow |
| [cometouglas/docs/advanced/04-scrapers-background-and-dmm.md](cometouglas/docs/advanced/04-scrapers-background-and-dmm.md) | Scrapers, background scraper, DMM |
| [cometouglas/docs/advanced/05-database-and-operations.md](cometouglas/docs/advanced/05-database-and-operations.md) | Database backends and operations |
| [cometouglas/docs/advanced/06-http-api-reference.md](cometouglas/docs/advanced/06-http-api-reference.md) | HTTP API reference |
| [cometouglas/docs/troubleshooting.md](cometouglas/docs/troubleshooting.md) | Troubleshooting guide |
| [cometouglas/docs/cometnet/README.md](cometouglas/docs/cometnet/README.md) | CometNet P2P documentation index |
| [cometouglas/docs/cometnet/quickstart.md](cometouglas/docs/cometnet/quickstart.md) | CometNet quick start |
| [cometouglas/docs/cometnet/cometnet.md](cometouglas/docs/cometnet/cometnet.md) | CometNet full reference |
| [cometouglas/docs/cometnet/docker.md](cometouglas/docs/cometnet/docker.md) | CometNet Docker deployment |
| [cometouglas/deployment/cloudflare-cache-rules.md](cometouglas/deployment/cloudflare-cache-rules.md) | Cloudflare cache rules |
| [cometouglas/kodi/README.md](cometouglas/kodi/README.md) | Kodi add-on installation |

## IPTV

| File | Purpose |
|------|---------|
| [IPTV_OPERATIONS.md](IPTV_OPERATIONS.md) | End-to-end pipeline: scraper -> aggregator -> addon, artwork, maintenance |
| [iptv-addon/README.md](iptv-addon/README.md) | TV Doug addon: endpoints, config, features, architecture |
| [iptv-aggregator/README.md](iptv-aggregator/README.md) | Local aggregator: discovery, inventory, production, maintenance commands |
| [iptv-aggregator/README.upstream.md](iptv-aggregator/README.upstream.md) | Upstream iptv-checker reference |
| [iptv-aggregator/.readme/errors.md](iptv-aggregator/.readme/errors.md) | Error codes reference |
| [docs/IPTV_ANALYSIS_INDEX.md](docs/IPTV_ANALYSIS_INDEX.md) | IPTV infrastructure analysis index |
| [docs/IPTV_ANALYSIS_INDEX.txt](docs/IPTV_ANALYSIS_INDEX.txt) | IPTV quick reference summary |

## BitMagnet

| File | Purpose |
|------|---------|
| [BITMAGNET_OPERATIONS.md](BITMAGNET_OPERATIONS.md) | Runtime behavior, import, recovery, A/B results |
| [bitmagnet/BITMAGNET_TARGET_LOGIC.md](bitmagnet/BITMAGNET_TARGET_LOGIC.md) | Architecture, ownership boundaries, source of truth |
| [bitmagnet/RECOVERY.md](bitmagnet/RECOVERY.md) | Recovery notes for deployed service |
| [docs/README.md](docs/README.md) | Docs map and navigation |
| **Current docs** | |
| [docs/current/README.md](docs/current/README.md) | Entry points for current docs |
| [docs/current/ARCHITECTURE.md](docs/current/ARCHITECTURE.md) | Implementation shape, source-of-truth boundaries |
| [docs/current/OPERATIONS.md](docs/current/OPERATIONS.md) | Deployment model, operator entrypoints |
| [docs/current/ROLLOUT.md](docs/current/ROLLOUT.md) | Rollout and validation commands |
| [docs/current/LEAN_DB_CONVERGENCE.md](docs/current/LEAN_DB_CONVERGENCE.md) | One-lean-DB architecture, wide drain completion |
| **Classifier stack** | |
| [classifier/docs/README.md](classifier-stack/docs/README.md) | Classifier docs index |
| [classifier/docs/IMPORT_BRIDGE.md](classifier-stack/docs/IMPORT_BRIDGE.md) | Import bridge details |
| [classifier/docs/VALIDATION_AND_BUILDS.md](classifier-stack/docs/VALIDATION_AND_BUILDS.md) | Validation and build workflows |
| **Unified classifier** | |
| [classifier/unified_classifier/WORKFLOW.md](classifier/unified_classifier/WORKFLOW.md) | Classification pipeline workflow (supersedes old WORKFLOW_EXPLAINED.md) |
| **Historical docs** | |
| [docs/historical/README.md](docs/historical/README.md) | Historical docs index |
| [docs/historical/session-2026-03-20.md](docs/historical/session-2026-03-20.md) | Session documentation |
| [docs/historical/code-changes.md](docs/historical/code-changes.md) | Smart-hint integration code changes |
| [docs/historical/optimization-notes.md](docs/historical/optimization-notes.md) | Database optimization notes |
| [docs/historical/deployment-handoff.md](docs/historical/deployment-handoff.md) | Deployment handoff notes |
| [docs/historical/audits.md](docs/historical/audits.md) | Audit summary |
| [docs/historical/versions.md](docs/historical/versions.md) | Version history summary |
| [docs/historical/comet-bitmagnetico-search-plan.md](docs/historical/comet-bitmagnetico-search-plan.md) | Comet + Bitmagnetico search plan |
| [docs/historical/future-bitmagnet-integration.md](docs/historical/future-bitmagnet-integration.md) | Future integration path |
| **Reference** | |
| [docs/reference/README.md](docs/reference/README.md) | Reference docs pointer |

## Scripts and Testing

| File | Purpose |
|------|---------|
| [SCRIPTS_CATALOG.md](SCRIPTS_CATALOG.md) | All scripts: purpose, usage, env vars |
| [TESTING_RUNBOOK.md](TESTING_RUNBOOK.md) | Testing: health checks, smoke tests, acceptance benchmarks |

## Generated Outputs (Not Curated Docs)

These are timestamped run outputs, not hand-curated documentation:

| Path | Content |
|------|---------|
| `test_runs/*/report.md` | Benchmark run reports |
| `test_runs/*/ranked/*_report.md` | Rebalance and ranking reports |
| `iptv-aggregator/output/` | Aggregator pipeline outputs |
