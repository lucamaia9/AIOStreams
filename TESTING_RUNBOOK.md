# AIOStreams / Comet Testing Runbook

## Purpose

This document captures the current verified test procedures for the live stack in `/home/ubuntu/aiostreams`.

It separates:
- current procedures that match the active runtime
- legacy procedures that still exist on disk but target older services/URLs

## Current Runtime Under Test

Verified active services on 2026-03-08:
- `postgres`
- `comet`
- `aiostreams`
- `jackett`
- `prowlarr`
- `flaresolverr`
- `flaresolverr-jackett`

Provisioned but not part of the default runtime:
- `comet-builder` via `docker compose --profile builder up -d comet-builder`
- `bitmagnet-postgres` and `bitmagnet` via `docker compose --profile bitmagnet up -d bitmagnet-postgres bitmagnet`

Current public endpoints:
- `https://comet.douglinhas.mywire.org/manifest.json`
- `https://aiostreams.douglinhas.mywire.org/api/v1/status`

## Quick Health Check

```bash
cd /home/ubuntu/aiostreams

docker compose ps
curl -sS -o /tmp/comet_manifest.json -w 'comet_manifest_http=%{http_code}\n' \
  https://comet.douglinhas.mywire.org/manifest.json
curl -sS -o /tmp/aiostreams_status.json -w 'aiostreams_status_http=%{http_code}\n' \
  https://aiostreams.douglinhas.mywire.org/api/v1/status
```

Pass criteria:
- core containers are `Up`
- Comet manifest returns HTTP `200`
- AIOStreams status returns HTTP `200`

## BitMagnet Smoke Test

Use this only for the self-hosted BitMagnet subsystem. It is internal-only in the current phase.

```bash
cd /home/ubuntu/aiostreams

docker compose --profile bitmagnet up -d bitmagnet-postgres bitmagnet
docker exec bitmagnet wget -qO- http://127.0.0.1:3333/status
docker exec bitmagnet wget -qO- "http://127.0.0.1:3333/torznab/api?t=movie&imdbid=tt0133093"
```

Pass criteria:
- Torznab returns valid RSS/XML
- cold installs may return an empty RSS channel until BitMagnet has warmed up
- `/status` may return `503` while BitMagnet is still cold-starting or waiting for internal subsystems to become ready; do not treat an empty Torznab response as a failure by itself

### DHT anomaly triage fixture

The current large-smoke/manual-audit triage artifacts that drive DHT remediation live in:

- `tests/dht_pipeline/samples/large_smoke_triage_taxonomy.json`
- `tests/dht_pipeline/samples/large_smoke_regression_cases.jsonl`

Use `python3 -m unittest tests/dht_pipeline/test_large_smoke_triage_fixture.py` to validate the curated fixture metadata before working on the later canonical-matching and guardrail todos.

## Comet Smoke Test

The existing `test_aiostreams_stream.py` script is generic enough to test a Stremio manifest URL. For current Comet smoke tests, pass the Comet manifest explicitly.

### Movie test

```bash
cd /home/ubuntu/aiostreams

python3 ./test_aiostreams_stream.py \
  --manifest-url "https://comet.douglinhas.mywire.org/manifest.json" \
  --media-type movie \
  --imdb-id tt0133093 \
  --save-json /tmp/comet_movie_tt0133093.json
```

### Series test

```bash
cd /home/ubuntu/aiostreams

python3 ./test_aiostreams_stream.py \
  --manifest-url "https://comet.douglinhas.mywire.org/manifest.json" \
  --media-type series \
  --imdb-id tt13918776:2:3 \
  --save-json /tmp/comet_series_tt13918776_s2e3.json
```

Pass criteria:
- manifest check is HTTP `200`
- stream check is HTTP `200`
- response contains a `streams` array
- for the local-first search path, confirm logs show the local phase before indexers:
  - standard episodic requests should now log a staged provider plan (`episode_exact`, `season_fallback`, `broad_title_rescue`)
  - exact-stage requests should not widen to broad title rescue unless the prior stages underperform
  - `MagneticoLocal`
  - `DMM` when enabled
  - RD account snapshot contribution when request account scraping is enabled
- confirm `Prowlarr`/`Jackett` only expand when local cached-quality results are insufficient

## Canonical Real-User Benchmark

Use this as the primary acceptance test for direct Comet cached-only tuning. It exercises the actual Stremio stream path using a real Comet manifest URL and saves synchronized container logs plus per-request metrics under `test_runs/realuser_<run_id>/`.

```bash
cd /home/ubuntu/aiostreams

export REAL_USER_MANIFEST_URL='<canonical direct Comet manifest URL>'
./scripts/run_real_user_benchmark.sh --limit 5
```

Current verified benchmark chain on 2026-03-08:
- smoke run: `test_runs/realuser_20260308_220638/`
  - requests_total: `5`
  - requests_completed: `5`
  - median_latency_s: `2.526`
  - p95_latency_s: `2.734`
  - budget_exhausted_count: `0`
- full acceptance run: `test_runs/realuser_20260308_221315/`
  - requests_total: `20`
  - requests_completed: `20`
  - median_latency_s: `4.031`
  - p95_latency_s: `13.542`
  - budget_exhausted_count: `1`
  - early_return_count: `1`
  - rd_cached_positive_count: `18`
  - comparison baseline: `test_runs/realuser_20260308_220707/`
  - delta_median_latency_s: `-6.679`
  - delta_budget_exhausted_count: `-13`
  - delta_median_stream_count: `+3.0`
- final acceptance rerun after edge-case fixes: `test_runs/realuser_20260308_222043/`
  - requests_total: `20`
  - requests_completed: `20`
  - median_latency_s: `2.486`
  - p95_latency_s: `13.778`
  - budget_exhausted_count: `0`
  - early_return_count: `1`
  - rd_cached_positive_count: `18`
  - comparison baseline: `test_runs/realuser_20260308_221315/`
  - delta_median_latency_s: `-1.545`
  - delta_budget_exhausted_count: `-1`
- RD chunked-probe acceptance run: `test_runs/realuser_20260308_223744/`
  - requests_total: `20`
  - requests_completed: `20`
  - median_latency_s: `1.727`
  - p95_latency_s: `10.977`
  - budget_exhausted_count: `0`
  - early_return_count: `0`
  - rd_cached_positive_count: `18`
  - comparison baseline: `test_runs/realuser_20260308_222043/`
  - delta_median_latency_s: `-0.759`
  - delta_p95_latency_s: `-2.801`
  - delta_median_stream_count: `-1.0`
- PostgreSQL live cutover validation with builder stopped: `test_runs/realuser_20260308_230636/`
  - requests_total: `20`
  - requests_completed: `20`
  - median_latency_s: `2.188`
  - p95_latency_s: `10.746`
  - budget_exhausted_count: `0`
  - early_return_count: `0`
  - rd_cached_positive_count: `18`
  - interpretation: PostgreSQL cutover is production-safe for the live node
- builder cold-start regression run: `test_runs/realuser_20260308_230223/`
  - requests_total: `20`
  - requests_completed: `20`
  - median_latency_s: `3.079`
  - p95_latency_s: `13.565`
  - budget_exhausted_count: `0`
  - early_return_count: `1`
  - interpretation: do not auto-start `comet-builder` during live traffic on this host until DMM ingestion/bootstrap is tuned further
- off-peak DMM preload run with builder stopped before benchmarking: `test_runs/realuser_20260308_232415/`
  - requests_total: `20`
  - requests_completed: `20`
  - median_latency_s: `1.835`
  - p95_latency_s: `12.224`
  - budget_exhausted_count: `0`
  - early_return_count: `0`
  - rd_cached_positive_count: `18`
  - median_stream_count: `6.5`
  - interpretation: DMM preload improved median latency with no stream-count loss, but p95 still needs work
- cached-only live DMM A/B acceptance run: `test_runs/realuser_20260308_234053/`
  - requests_total: `20`
  - requests_completed: `20`
  - median_latency_s: `1.793`
  - p95_latency_s: `8.022`
  - budget_exhausted_count: `0`
  - early_return_count: `2`
  - rd_cached_positive_count: `18`
  - median_stream_count: `7.0`
  - provider_mix:
    - `dmm=4`
    - `dmm_prowlarr=1`
    - `jackett=2`
    - `jackett_prowlarr=3`
    - `prowlarr=3`
    - `none=7`
  - interpretation: live DMM participation for cached-only RD requests is currently accepted on this host
- user-selected cached-only threshold run: `test_runs/realuser_20260308_235556/`
  - requests_total: `20`
  - requests_completed: `20`
  - median_latency_s: `3.911`
  - p95_latency_s: `8.787`
  - budget_exhausted_count: `0`
  - early_return_count: `0`
  - rd_cached_positive_count: `18`
  - median_stream_count: `7.0`
  - interpretation: keeping `LIVE_FAST_RETURN_RD_CACHED_RESULTS=10` reduced early returns and regressed latency versus the accepted DMM live baseline, while preserving cache-positive coverage

Interpretation:
- this benchmark is the current source of truth for "does the addon behave like a real cached-only Real-Debrid user request"
- prefer this over the legacy `run_protocol.sh` flow for Comet latency work

## Builder DMM Workflow

Builder is now the accepted automatic DMM refresh path for Cometouglas on this host.
The public `comet` service stays focused on live traffic, while `comet-builder`
keeps the shared PostgreSQL DMM cache fresh.

Start:

```bash
cd /home/ubuntu/aiostreams
docker compose up -d postgres comet-builder
```

Full-cycle validation:

```bash
cd /home/ubuntu/aiostreams
TIMEOUT_MINUTES=240 ./scripts/run_builder_dmm_cycle.sh
```

Incremental-refresh validation:

```bash
cd /home/ubuntu/aiostreams
TIMEOUT_MINUTES=30 ./scripts/check_builder_dmm_incremental.sh
```

Status:

```bash
cd /home/ubuntu/aiostreams
./scripts/status_builder_dmm.sh
```

Stop:

```bash
cd /home/ubuntu/aiostreams
./scripts/stop_builder_dmm.sh
```

Observed DMM state on 2026-03-08:
- `dmm_entries` grew from `0` to `24915` during an off-peak manual builder run
- `dmm_ingested_files` remained `0` on the interrupted run, so file-level checkpoint completion was not yet observed
- practical implication: DMM prewarming is real, but interrupted runs may still reprocess files on the next manual start
- live DMM A/B validation confirmed direct DMM contribution in request logs and returned streams, for example:
  - `tt1517268` (`Barbie`) returned early with `providers=[dmm]`
  - `tt0388629:1:2` (`One Piece S01E02`) returned early with `providers=[dmm]`
  - `tt5074352` (`Dangal`) included returned streams labeled `DMM`
- accepted runtime update on 2026-03-16:
  - `comet-builder` is no longer profile-gated in compose
  - DMM automatic refresh now runs through the dedicated builder container with `restart: unless-stopped`
  - live `comet` keeps `SCRAPE_DMM=live` and `DMM_INGEST_ENABLED=False`
  - builder keeps `DMM_INGEST_ENABLED=True` against the shared `postgres` database
- steady-state tooling added:
  - `scripts/run_builder_dmm_cycle.sh`
  - `scripts/check_builder_dmm_incremental.sh`

Jackett lane validation on 2026-03-08:
- after removing `HTTP_PROXY` / `HTTPS_PROXY` from `jackett` and `flaresolverr-jackett`, direct Jackett API tests recovered:
  - `1337x` returned results again
  - `kickasstorrents.to` returned results again
- validated with:
  - `GET /api/v2.0/indexers/1337x/results?...&Query=harry potter`
  - `GET /api/v2.0/indexers/kickasstorrents-to/results?...&Query=harry potter`

Prowlarr 1337x validation on 2026-03-08:
- `1337x` was unavailable because Prowlarr was proxying internal requests to `http://flaresolverr:8191/v1` and had a stale disabled/cookie state for provider `4`
- fix applied:
  - add `flaresolverr` to `NO_PROXY` / `no_proxy` for `prowlarr`
  - clear `IndexerStatus` failure/disabled/cookie state for provider `4`
- validation:
  - `GET /api/v1/search?query=harry%20potter&indexerIds=4`
  - returned `80` releases after restart

Current live-path tuning validation on 2026-03-08:
- cached-only live path now uses:
  - Prowlarr first wave
  - conditional skip of foreground Jackett when wave1 already has a useful candidate set
  - Jackett fast/slow indexer lanes when Jackett is still needed in the foreground
- active tuned Jackett split in `comet-fresh.env`:
  - `JACKETT_FAST_INDEXERS`
  - `JACKETT_SLOW_INDEXERS=["1337x","bitru"]`

## Current Log Capture Procedure

Use direct container logs for the active stack.

```bash
cd /home/ubuntu/aiostreams

docker logs --since 15m --tail 300 comet 2>&1 > /tmp/comet.log
docker logs --since 15m --tail 300 postgres 2>&1 > /tmp/postgres.log
docker logs --since 15m --tail 300 jackett 2>&1 > /tmp/jackett.log
docker logs --since 15m --tail 300 prowlarr 2>&1 > /tmp/prowlarr.log
docker logs --since 15m --tail 200 flaresolverr 2>&1 > /tmp/flaresolverr.log
docker logs --since 15m --tail 200 flaresolverr-jackett 2>&1 > /tmp/flaresolverr-jackett.log
```

Useful signal patterns:

### Comet

```bash
grep -Ei 'Starting search|challenge-bypass route|found [0-9]+ torrents|budget exhausted|returned early|Timeout|Exception' /tmp/comet.log
```

### Jackett

```bash
grep -Ei 'Manual search|Found [0-9]+ releases|Exception|TooManyRequests|cookies provided by FlareSolverr are not valid' /tmp/jackett.log
```

### Prowlarr

```bash
grep -Ei 'Searching indexer|error|warning|timeout|failed|exception' /tmp/prowlarr.log
```

### FlareSolverr

```bash
grep -Ei 'Challenge detected|Challenge solved|Response in' /tmp/flaresolverr.log /tmp/flaresolverr-jackett.log
```

## Interpreting Current Comet Behavior

Healthy scrape characteristics:
- Comet starts both Jackett and Prowlarr routes
- at least one provider returns torrents
- final response is HTTP `200`
- debrid cache check completes
- cached-only direct requests can return from verified RD cache without waiting for a full live scrape

Investigate:
- repeated `budget exhausted` messages on otherwise easy titles
- one provider consistently contributes zero results
- repeated FlareSolverr cookie or challenge failures on a high-value indexer

Fail:
- manifest or stream endpoint returns non-`200`
- both providers fail to contribute meaningful results
- repeated infra errors across Comet, Jackett, and Prowlarr together

Resolved on 2026-03-08:
- cached-only RD precheck timeout no longer returns `500`; it now falls back safely into the foreground scrape path
- validated in `test_runs/realuser_20260308_213132/` and `test_runs/realuser_20260308_213832/`

Current residual edge cases after the final acceptance rerun:
- `tt0388629:1:2` remains the slowest cache-positive title (~12.6s) because it still needs a foreground Prowlarr probe after partial precheck success
- `tt0421357:1:1` is now a tail-latency anime case (~11.0s) with only one verified cached hit
- `tt6473344:1:1` remains the main hard zero-hit miss (~8.2s) and correctly terminates with `reason=completed`
- the latency-first RD chunking policy improves median and p95 latency, but median stream count dropped by `1.0` versus `test_runs/realuser_20260308_222043/`
- after the PostgreSQL cutover, the stable default runtime is `comet + postgres` with `comet-builder` stopped unless explicitly launched off-peak
- first-run builder DMM ingestion is CPU-heavy and currently not validated as safe for always-on operation on this host

## Ranking / Rebalance Validation

### Overnight benchmark

```bash
cd /home/ubuntu/aiostreams

./scripts/run_overnight_indexer_benchmark.sh --limit-rows 5 --run-label smoke
```

Expected outputs:
- `test_runs/overnight_<run_id>/raw/`
- `test_runs/overnight_<run_id>/ranked/jackett_ranked.csv`
- `test_runs/overnight_<run_id>/ranked/prowlarr_ranked.csv`
- `test_runs/overnight_<run_id>/ranked/ranking_report.md`

### Indexer split generation

Preferred routine (30-title split benchmark, repeated):

```bash
cd /home/ubuntu/aiostreams

./scripts/run_indexer_split.sh --with-benchmark30
```

Smoke variant:

```bash
cd /home/ubuntu/aiostreams

./scripts/run_indexer_split.sh \
  --with-benchmark30 \
  --benchmark-repeats 1 \
  --benchmark-timeout 10
```

Build from an existing run:

```bash
cd /home/ubuntu/aiostreams

./scripts/run_indexer_split.sh --run-dir test_runs/splitbench_<run_id>
```

Publish Comet runtime artifact (auto-loaded at startup/fallback-safe):

```bash
cd /home/ubuntu/aiostreams

./scripts/run_indexer_split.sh --with-benchmark30 --apply
```

Expected outputs:
- `test_runs/splitbench_<run_id>/raw/jackett_probe.jsonl`
- `test_runs/splitbench_<run_id>/raw/prowlarr_probe.jsonl`
- `test_runs/splitbench_<run_id>/split/providers.csv`
- `test_runs/splitbench_<run_id>/split/owner_decisions.csv`
- `test_runs/splitbench_<run_id>/split/movies.csv`
- `test_runs/splitbench_<run_id>/split/series.csv`
- `test_runs/splitbench_<run_id>/split/anime.csv`
- `test_runs/splitbench_<run_id>/split/decision_report.md`
- `test_runs/splitbench_<run_id>/split/split_summary.md`
- `data/comet-fresh/indexer_split/current.json` (`--apply`)

Pass criteria:
- each provider has exactly one owner lane (`jackett` xor `prowlarr`)
- no provider duplication across owner lanes
- movies/series/anime lists reference providers from the providers inventory
- decision report includes degraded/empty providers, timeout signals, and low-stability providers

### Rebalance dry run

There is no dedicated shell wrapper for dry-run mode, so call the Python entrypoint directly.

```bash
cd /home/ubuntu/aiostreams

python3 ./scripts/rebalance_from_live_usage.py \
  --db-path data/comet-fresh/comet-fresh.db \
  --jackett-log-dir jackett/data/Jackett \
  --prowlarr-log-dir prowlarr/config/logs \
  --window-days 3 \
  --top-n 15 \
  --exploration-ratio 0.10 \
  --min-samples 20 \
  --max-membership-change 0.20 \
  --env-file comet-fresh.env \
  --compose-file compose.yaml \
  --service-name comet \
  --out-dir test_runs/manual_rebalance_check \
  --dry-run
```

Expected outputs:
- ranked CSV and JSON artifacts in the selected `out-dir`
- no env mutation when `--dry-run` is used

## IPTV Addon Smoke Test

Test the TV Doug addon at `https://iptv.douglinhas.mywire.org`.

### 1. Unit Tests
```bash
cd /home/ubuntu/aiostreams/iptv-addon && node test.js
```
Pass: `Personal addon tests passed`

### 2. Manifest Check
```bash
curl -s https://iptv.douglinhas.mywire.org/manifest.json | python3 -c "
import json, sys
m = json.load(sys.stdin())
print(f'Manifest: {m[\"name\"]} v{m[\"version\"]}')
print(f'Resources: {m[\"resources\"]}')
assert m['resources'] == ['catalog', 'stream', 'meta']
print('PASS')
"
```

### 3. Catalog Check
```bash
curl -s "https://iptv.douglinhas.mywire.org/catalog/tv/personal_iptv.json" | python3 -c "
import json, sys
d = json.load(sys.stdin())
metas = d.get('metas', [])
print(f'Catalog: {len(metas)} channels (page 1, max 100)')
assert len(metas) > 0
print('PASS')
"
```

### 4. Stream Response Validation
```bash
CHANNEL="amc"
curl -s "https://iptv.douglinhas.mywire.org/stream/tv/personal_tv_${CHANNEL}.json" | python3 -c "
import json, sys
d = json.load(sys.stdin())
streams = d.get('streams', [])
print(f'Streams: {len(streams)}')
assert len(streams) > 0, 'No streams returned'
for s in streams:
    h = s.get('behaviorHints', {})
    assert h.get('notWebReady') == True, 'notWebReady missing'
    assert 'request' in h.get('proxyHeaders', {}), 'proxyHeaders missing'
    ua = h.get('proxyHeaders', {}).get('request', {}).get('User-Agent', '')
    assert 'Chrome/120' in ua, f'Wrong UA: {ua}'
    fn = h.get('filename', '')
    assert fn.endswith('.m3u8'), f'filename missing: {fn}'
print(f'PASS: {len(streams)} streams validated')
"
```

### 5. Full Catalog Channel Count
```bash
curl -s "https://iptv.douglinhas.mywire.org/catalog/tv/personal_iptv.json" | python3 -c "
import json, sys
d = json.load(sys.stdin())
metas = d.get('metas', [])
# First page = 100 channels
print(f'Page 1: {len(metas)} channels')
"
# Check catalog total from addon_catalog.json directly:
python3 -c "
import json
with open('/home/ubuntu/aiostreams/iptv-aggregator/output/telegram_production/addon/addon_catalog.json') as f:
    d = json.load(f)
channels = d.get('channels', [])
ready = [c for c in channels if c.get('status') == 'ready']
print(f'Catalog total: {len(channels)} channels, {len(ready)} ready')
"
```

### Pass Criteria
- Unit tests pass
- Manifest returns 200 with correct resources
- Catalog returns channels
- All streams have `notWebReady: true`, `proxyHeaders.request.User-Agent`, and `filename` ending in `.m3u8`
- Catalog has ~391 ready channels

### Rebuild After Code Changes
If addon source is modified, rebuild before testing:
```bash
cd /home/ubuntu/aiostreams
sudo docker compose build iptv-addon
sudo docker compose up -d iptv-addon
sleep 3
# then run smoke tests
```

## Legacy Procedure Status

The following files are still present, but they target an older path and should not be treated as the current Comet runbook:
- `run_protocol.sh`

Verified mismatch:
- `run_protocol.sh` defaults to the old `mrdouglas2.duckdns.org` manifest path
- it captures logs from `aiostreams-custom`
- `aiostreams-custom` is not an active service in the current `docker compose ps`

Current rule:
- use `test_aiostreams_stream.py` with the explicit live Comet manifest for current smoke tests
- use direct `docker logs` for current log capture
- use the legacy protocol only if you intentionally revive the older `aiostreams-custom` flow

## Related Documentation

- `SCRIPTS_CATALOG.md` -- all scripts with purpose, usage, and env vars
- `COMET_OPERATIONS.md` -- Comet live path, wave tuning, indexer lane config
- `PROJECT_OVERVIEW.md` -- architecture, services, endpoints
