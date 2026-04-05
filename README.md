# AIOStreams - Stremio Scraping Pipeline

Three-layer automated pipeline for Stremio content discovery and delivery.

## Quick Start

```bash
# Start all services
sudo docker compose up -d

# Check pipeline health
sudo docker compose ps

# View logs
sudo docker compose logs -f bitmagnet
```

## Architecture

```
DHT Network → BitMagnet (Go) → PostgreSQL → Python Classifier → SQLite → Cometouglas (FastAPI) → Stremio
   6K/hr         intake filter   2-day retention  109 patterns   9M rows      79% RTN filter     Stream API
```

**Three Layers:**
1. **BitMagnet (Go)** - DHT crawling, intake filtering, PostgreSQL staging
2. **Python Classifier** - Unified media classification, canonical resolution, TMDB matching
3. **Cometouglas (Python/FastAPI)** - Stremio API, RD caching, multi-source scraping

## Key Components

| Service | Purpose | Location |
|---------|---------|----------|
| BitMagnet | DHT crawler | `bitmagnet-media/` |
| Python Classifier | Media classification | `bitmagnet-media/classifier/` |
| Cometouglas | Stremio API | `cometouglas/` |
| VPN Proxy | 4-lane proxy | `vpn-proxy/` |
| IPTV | TV addon | `iptv-addon/`, `iptv-aggregator/` |

## Documentation

- [Architecture](docs/ARCHITECTURE.md) - System design and data flow
- [Workflow](docs/WORKFLOW.md) - End-to-end pipeline stages
- [Ownership](docs/OWNERSHIP.md) - Component responsibilities
- [Database](docs/DATABASE.md) - Storage strategy and indexes
- [Matching](docs/MATCHING.md) - Classification logic and flows
- [Runbook](docs/RUNBOOK.md) - Operations and procedures
- [Testing](docs/TESTING.md) - Benchmarks and validation

## Status

- Database: ~1.9GB PostgreSQL (277K torrents, 2-day retention)
- Search Index: 9M rows in SQLite (6.4GB)
- Throughput: ~6K torrents/hour crawled
- Filter: 15-20% rejection rate at intake (adult + non-media)

## Private Repos

- [lucamaia9/bitmagnet-media](https://github.com/lucamaia9/bitmagnet-media) - DHT crawler fork
- [lucamaia9/cometouglas](https://github.com/lucamaia9/cometouglas) - Stremio server fork
