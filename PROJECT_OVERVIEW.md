# AIOStreams / Comet Project Overview

## Project Overview

**Purpose**: Operate a self-hosted Stremio scraping stack centered on a customized Comet instance (`Cometouglas v2`) with live torrent retrieval from both Jackett and Prowlarr, plus AIOStreams as a separate addon service.

**Primary runtime directory**: `/home/ubuntu/aiostreams`

**Primary Comet source directory**: `/home/ubuntu/aiostreams/cometouglas`

**Current source of truth**:
- Runtime topology: `compose.yaml`
- Active Comet settings: `comet-fresh.env`
- Live Comet code: `/home/ubuntu/aiostreams/cometouglas`

Historical notes in `info.md` remain useful, but older sections should not be treated as the current runtime definition without cross-checking the files above.

## Architecture Map

```text
Internet
  |
  v
Traefik
  |
  +--> aiostreams.douglinhas.mywire.org -> aiostreams
  |
  +--> comet.douglinhas.mywire.org -> comet
  |
  +--> jackett.douglinhas.mywire.org -> jackett
  |
  +--> prowlarr.douglinhas.mywire.org -> prowlarr

Internal runtime
  |
  +--> postgres
  |
  +--> comet
  |     +--> Jackett scraper
  |     +--> Prowlarr scraper
  |     +--> dynamic indexer manager
  |     +--> torrent ranking / filtering / debrid cache checks
  |     +--> shared Postgres reads/writes
  |
  +--> comet-builder (manual/off-peak profile)
  |     +--> DMM ingester
  |     +--> shared Postgres writes
  |
  +--> bitmagnet (manual/off-peak profile)
  |     +--> bitmagnet-postgres
  |     +--> internal Torznab/API only
  |     +--> published BitTorrent/DHT port 3334/tcp+udp
  |
  +--> jackett
  |     +--> flaresolverr-jackett
  |     +--> direct egress
  |
  +--> prowlarr
  |     +--> flaresolverr
  |
  +--> aiostreams
        +--> adguard-proxy-helsinki for upstream addon requests
```

## Active Services

Verified via `docker compose ps` on 2026-03-08:
- `postgres`
- `traefik`
- `aiostreams`
- `comet`
- `jackett`
- `prowlarr`
- `flaresolverr`
- `flaresolverr-jackett`
- `adguard-proxy-helsinki`
- `adguard-proxy-amsterdam`
- `adguard-proxy-london`
- `adguard-proxy-nyc`

Not active in the current compose runtime:
- `comet-builder` is provisioned behind the `builder` profile and is intended for manual/off-peak runs
- `bitmagnet` and `bitmagnet-postgres` are provisioned behind the `bitmagnet` profile and are intended for manual/off-peak rollout and warm-up
- `aiostreams-custom` is commented out in `compose.yaml`
- `comet-prowlarr` is commented out in `compose.yaml`
- `byparr*` services are not present in the active compose file

## Public Endpoints

Verified on 2026-03-08:
- `https://comet.douglinhas.mywire.org/manifest.json`
  - HTTP `200`
  - manifest name: `Cometouglas v2`
  - manifest version: `2.0.0`
- `https://aiostreams.douglinhas.mywire.org/api/v1/status`
  - HTTP `200`

Traefik routes currently configured in `compose.yaml`:
- `aiostreams.douglinhas.mywire.org`
- `comet.douglinhas.mywire.org`
- `jackett.douglinhas.mywire.org`
- `prowlarr.douglinhas.mywire.org`

## Comet Runtime Configuration

Active Comet service:
- Docker service: `comet`
- Build source: `./cometouglas`
- Env file: `comet-fresh.env`
- Data volume: `./data/comet-fresh:/app/data`
- Database: shared PostgreSQL at `postgres:5432`

Provisioned builder service:
- Docker service: `comet-builder`
- Build source: `./cometouglas`
- Env file: `comet-builder.env`
- Data volume: `./data/comet-fresh:/app/data`
- Database: shared PostgreSQL at `postgres:5432`
- Runtime policy: manual/off-peak only, not part of the default `docker compose up -d` path

Provisioned BitMagnet services:
- Docker services:
  - `bitmagnet-postgres`
  - `bitmagnet`
- Compose profile: `bitmagnet`
- Database: dedicated PostgreSQL at `bitmagnet-postgres:5432`
- Runtime policy: internal-only, manual/off-peak only in the current phase
- API base URL: `http://bitmagnet:3333`
- Builder integration policy: staged in `comet-builder.env`, to be applied on the next builder recreate after the current DMM ingest finishes

Verified active scraper settings in `comet-fresh.env`:
- `SCRAPE_JACKETT=both`
- `SCRAPE_PROWLARR=both`
- `JACKETT_URL=http://jackett:9117`
- `PROWLARR_URL=http://prowlarr:9696`
- `BYPARR_MODE=off`

Verified live-performance settings in `comet-fresh.env`:
- progressive provider behavior enabled
- partial provider merge enabled
- live scrape budgets configured
- query dedupe enabled
- indexer jitter enabled
- explicit allowlists for both Jackett and Prowlarr configured
- cached-only direct requests use an RD-first fast path with live availability probing before full scrape fallback
- cached-only live scraping uses an explicit local-first phase:
  - `magneticolocal`
  - `dmm`
  - RD account snapshot when enabled by request config
- `Prowlarr` and `Jackett` are expansion-only providers after the local phase

## Customizations Over Upstream Comet

`/home/ubuntu/aiostreams/cometouglas` is a git checkout of upstream `g0ldyy/comet`, but the working tree has local modifications.

Verified modified/untracked files on 2026-03-08:
- modified:
  - `comet/api/endpoints/stream.py`
  - `comet/core/models.py`
  - `comet/scrapers/jackett.py`
  - `comet/scrapers/manager.py`
  - `comet/scrapers/prowlarr.py`
  - `comet/services/orchestration.py`
- untracked:
  - `comet/services/indexer_health.py`
  - `comet/services/query_dedupe.py`

Verified custom behavior present in the live source:
- Jackett and Prowlarr both participate in live scraping
- scraper results are yielded progressively
- live scrape can return early when enough candidates/providers are available
- cached-only requests can short-circuit from verified RD cache without waiting for a full provider fan-out
- cached-only live requests now skip foreground Jackett waves when Prowlarr already produced a useful candidate set
- Jackett live execution is split into fast and slow indexer lanes (`JACKETT_FAST_INDEXERS`, `JACKETT_SLOW_INDEXERS`)
- cached-only series requests can trigger a fuller RD probe when the initial partial probe finds zero cache hits
- wave3 Jackett escalation is now skipped unless wave2 actually added candidates
- duplicate live indexer queries are suppressed for a TTL window
- small request jitter is added before indexer fanout
- provider/indexer health metrics and suppression logic exist
- selected upstream Comet updates from `origin/main` were merged manually on 2026-03-08:
  - URL-level scrape mode parsing in scraper manager
  - safer Gunicorn import path
  - richer StremThru exception logging

## Jackett and Prowlarr Integration

### Jackett

Verified current runtime:
- service: `jackett`
- URL used by Comet: `http://jackett:9117`
- FlareSolverr URL in Jackett config: `http://flaresolverr-jackett:8191`
- `ProxyType=-1` in Jackett internal config

Operational interpretation:
- Jackett itself is not configured to use its own internal Jackett proxy settings
- the Jackett lane no longer uses container-level `HTTP_PROXY` / `HTTPS_PROXY`
- Jackett is routed alongside a dedicated `flaresolverr-jackett` instance
- on 2026-03-08, disabling the Jackett VPN/proxy lane restored `1337x` and `kickasstorrents.to` searches

### Prowlarr

Verified current runtime:
- service: `prowlarr`
- URL used by Comet: `http://prowlarr:9696`
- API key present in `prowlarr/config/config.xml`
- log level: `debug`
- dedicated `flaresolverr` service is used on the Prowlarr lane
- on 2026-03-08, `1337x` recovered after adding `flaresolverr` to `NO_PROXY` / `no_proxy` for `prowlarr` and clearing stale `IndexerStatus` state for provider `4`

## VPN / Proxy Topology

Verified 4-lane proxy design in `compose.yaml` (location-based naming, 2026-04-04):
- `adguard-proxy-helsinki` -- addon-facing traffic
- `adguard-proxy-amsterdam` -- standby
- `adguard-proxy-london` -- standby (renamed from `adguard-proxy-prowlarr`)
- `adguard-proxy-nyc` -- standby

Verified proxy usage in compose:
- `aiostreams` uses `ADDON_PROXY=http://adguard-proxy-helsinki:8888` via env
- `prowlarr` and `flaresolverr` use direct egress
- `jackett` and `flaresolverr-jackett` run without container-level proxy envs

Current interpretation:
- the prior Jackett proxy lane was removed from the active compose because it broke FlareSolverr cookie validation for CF-protected sites

## Current Performance Baseline

Primary live acceptance benchmark on 2026-03-08 after the PostgreSQL cutover:
- corpus: `test_plan/titles_20_antibot.csv`
- run: `test_runs/realuser_20260308_230636/`
- requests_total: `20`
- requests_completed: `20`
- median_latency_s: `2.188`
- p95_latency_s: `10.746`
- budget_exhausted_count: `0`
- early_return_count: `0`
- rd_cached_positive_count: `18`

Prior RD chunked-probe baseline on SQLite:
- corpus: `test_plan/titles_20_antibot.csv`
- run: `test_runs/realuser_20260308_223744/`
- requests_total: `20`
- requests_completed: `20`
- median_latency_s: `1.727`
- p95_latency_s: `10.977`
- budget_exhausted_count: `0`
- early_return_count: `0`
- rd_cached_positive_count: `18`

Cold-start builder regression observed and not accepted as default runtime:
- run with builder auto-started: `test_runs/realuser_20260308_230223/`
- median_latency_s: `3.079`
- p95_latency_s: `13.565`
- budget_exhausted_count: `0`
- interpretation: first-run builder DMM ingestion/background work stole enough host capacity to regress the live path
- resulting policy: keep `comet-builder` provisioned but manual/off-peak until builder startup can be tuned further

Off-peak DMM preload run with builder stopped before benchmarking:
- run: `test_runs/realuser_20260308_232415/`
- median_latency_s: `1.835`
- p95_latency_s: `12.224`
- budget_exhausted_count: `0`
- rd_cached_positive_count: `18`
- median_stream_count: `6.5`
- interpretation: DMM prewarming into the shared PostgreSQL database is real and improved median latency with no stream-count loss, but it is not yet a clean overall win because p95 regressed versus the PostgreSQL live-only baseline

Observed DMM database state on 2026-03-08:
- `dmm_entries=24915`
- `dmm_ingested_files=0`
- interpretation: a manual off-peak builder run successfully populated DMM-backed rows, but interrupted runs have not yet proven file-level ingest checkpointing

Cached-only live DMM A/B accepted on 2026-03-08:
- run: `test_runs/realuser_20260308_234053/`
- median_latency_s: `1.793`
- p95_latency_s: `8.022`
- budget_exhausted_count: `0`
- early_return_count: `2`
- rd_cached_positive_count: `18`
- median_stream_count: `7.0`
- provider_mix: `dmm=4`, `dmm_prowlarr=1`, `jackett=2`, `jackett_prowlarr=3`, `prowlarr=3`, `none=7`
- interpretation: enabling DMM on the live node for cached-only RD requests improved latency and median stream count versus the PostgreSQL live-only baseline without reducing cache-positive coverage

User-selected cached-hit threshold change on 2026-03-08:
- current runtime setting: `LIVE_FAST_RETURN_RD_CACHED_RESULTS=10`
- benchmark run: `test_runs/realuser_20260308_235556/`
- median_latency_s: `3.911`
- p95_latency_s: `8.787`
- early_return_count: `0`
- rd_cached_positive_count: `18`
- median_stream_count: `7.0`
- interpretation: raising the cached-only early-return threshold from the previously accepted value materially reduced early returns and worsened latency on the 20-title corpus, while leaving cache-positive coverage unchanged

Current interpretation:
- live Comet now runs on PostgreSQL without functional regression
- live Comet now includes DMM as a cached-only live metadata source behind the accepted live gate
- the current live threshold for cached-only early return is user-selected and no longer matches the best measured latency baseline
- easy cached-only titles still return quickly from the RD-first path
- hard zero-cache titles no longer exhaust the live scrape budget on the acceptance corpus
- remaining tail-latency cases are dominated by large candidate sets or hard zero-hit cached-only misses, not provider deadlock
- the current RD policy is latency-first: it cuts RD work earlier on hard titles, which improves median and p95 latency at the cost of slightly fewer median returned streams
- builder-side DMM ingestion is proven to populate the shared DB and can be run manually off-peak, but is not yet proven safe to auto-run on the same host during live traffic
- the benchmark harness now records arbitrary provider combinations, which was required to measure DMM participation correctly
- `prowlarr-clean` has been removed from the compose/runtime because it was no longer part of the accepted path and had no verified operational role

## Indexer Ranking and Rebalance Tooling

Verified scripts present:
- `scripts/run_overnight_indexer_benchmark.sh`
- `scripts/probe_indexers.py`
- `scripts/rank_indexers_from_overnight.py`
- `scripts/build_indexer_split.py`
- `scripts/run_indexer_split.sh`
- `scripts/run_nightly_rebalance.sh`
- `scripts/rebalance_from_live_usage.py`
- `scripts/run_reliability_investigation.sh`
- `scripts/run_antibot_3lane.sh`

Verified artifacts present:
- overnight ranking reports under `test_runs/overnight_*/ranked/`
- canonical provider/content split artifacts under `test_runs/overnight_*/split/`
- rebalance reports under `test_runs/rebalance_*/ranked/`
- anti-bot investigation reports under `test_runs/antibot_*/`
- reliability investigation reports under `test_runs/investigation_*/`

Current runtime split source:
- generated artifact: `data/comet-fresh/indexer_split/current.json`
- Comet fallback config lists:
  - `JACKETT_MOVIE_INDEXERS`, `JACKETT_SERIES_INDEXERS`, `JACKETT_ANIME_INDEXERS`
  - `PROWLARR_MOVIE_INDEXERS`, `PROWLARR_SERIES_INDEXERS`, `PROWLARR_ANIME_INDEXERS`

## Automation Status

IPTV pipeline (fully automated):
- `scripts/run_iptv_pipeline.sh` via `iptv-pipeline.timer` (system-level, every 12h)
- Chains: Telegram scraper → `run-optimized-telegram` → addon restart → health verify
- Optimized pipeline: ~3 min per run, fast path skips already-ready channels

Comet rebalance (user-level timers):
- `aiostreams-rebalance.timer` — daily 03:30 UTC
- `aiostreams-benchmark.timer` — weekly Sun 02:00 UTC
- `aiostreams-indexer-split.timer` — weekly Sun 03:00 UTC
- `aiostreams-cleanup.timer` — weekly Sat 04:00 UTC
- Historical runs exist under `test_runs/`

## Operational Commands

```bash
cd /home/ubuntu/aiostreams

# Current service status
docker compose ps

# Rebuild/restart the active Comet service
docker compose up -d --build comet

# Real-user direct Comet benchmark
export REAL_USER_MANIFEST_URL='<canonical direct Comet manifest URL>'
./scripts/run_real_user_benchmark.sh --limit 5

# Tail current runtime logs
docker logs --since 15m comet
docker logs --since 15m jackett
docker logs --since 15m prowlarr
docker logs --since 15m flaresolverr
docker logs --since 15m flaresolverr-jackett

# Smoke-check public endpoints
curl -sS https://comet.douglinhas.mywire.org/manifest.json
curl -sS https://aiostreams.douglinhas.mywire.org/api/v1/status
```

Latest verified real-user benchmark on 2026-03-08:
- smoke run: `test_runs/realuser_20260308_220638/`
  - requests_completed: `5/5`
  - median_latency_s: `2.526`
  - p95_latency_s: `2.734`
  - budget_exhausted_count: `0`
- full 20-title acceptance run: `test_runs/realuser_20260308_221315/`
  - requests_completed: `20/20`
  - median_latency_s: `4.031`
  - p95_latency_s: `13.542`
  - budget_exhausted_count: `1`
  - early_return_count: `1`
  - rd_cached_positive_count: `18`
  - compared to `test_runs/realuser_20260308_220707/`, this reduced median latency by `6.679s`, reduced budget exhaustion by `13`, and increased median stream count by `3.0`

## Documentation Map

- `AGENTS.md`
  - agent entry point: subsystem map, conventions, runtime snapshot
- `COMET_OPERATIONS.md`
  - Comet live path, waves, indexer lanes, DMM, deployment commands
- `IPTV_OPERATIONS.md`
  - IPTV aggregator, addon (TV Doug), Telegram workflow, artwork pipeline
- `BITMAGNET_OPERATIONS.md`
  - BitMagnet import, recovery, A/B results, Magnetico search layer
- `INFRASTRUCTURE.md`
  - Traefik, VPN proxy, AdGuard 3-lane topology, compose notes
- `SCRIPTS_CATALOG.md`
  - all scripts with purpose, usage, and related env vars
- `TESTING_RUNBOOK.md`
  - current verified testing procedures
- `PROJECT_OVERVIEW.md` (this file)
  - current verified runtime and architecture
- `info.md`
  - chronological changelog of significant changes
- `TESTING_RUNBOOK.md`
  - current testing and validation procedures
