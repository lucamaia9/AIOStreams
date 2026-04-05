# Infrastructure

Traefik, VPN proxy, AdGuard proxy lanes, and compose structure.

## Compose Structure

Source: `compose.yaml`

### Networks
- `web` -- external network for Traefik + routed services
- `vpn-net` -- internal network (`172.22.0.0/16`) for VPN proxy services

### Active Services (default startup)
- `postgres` -- shared PostgreSQL for Comet
- `traefik` -- reverse proxy / TLS termination
- `aiostreams` -- Stremio addon service
- `comet` -- main Comet instance (builds from `./cometouglas`)
- `jackett` -- torrent indexer aggregator
- `prowlarr` -- torrent indexer manager
- `flaresolverr` -- Cloudflare bypass for Prowlarr
- `flaresolverr-clean` -- clean standalone FlareSolverr for manual Prowlarr testing
- `flaresolverr-eztv-helsinki` -- Helsinki-backed FlareSolverr dedicated to EZTV testing
- `flaresolverr-jackett` -- Cloudflare bypass for Jackett
- `adguard-proxy-helsinki` -- VPN proxy for addon traffic
- `adguard-proxy-amsterdam` -- VPN proxy (standby)
- `adguard-proxy-london` -- VPN proxy (standby)
- `adguard-proxy-nyc` -- VPN proxy (standby)

### Profile-Gated Services
- `comet-builder` -- DMM ingestion (no longer behind a profile as of 2026-03-16)
- `bitmagnet-postgres`, `bitmagnet` -- profile `bitmagnet`
- `prowlarr-clean` -- removed from active runtime

### Resource Limits
CPU/RAM/PID limits applied for active services in `compose.yaml`. Traefik log level: `INFO`.

## Traefik

Location: `traefik/`
Config: `traefik/docker-compose.yml`

- Version: v3 (originally v2.5 config, upgraded in compose)
- Ports: 80, 443, 8080 (dashboard)
- Docker socket mount for dynamic routing

### Routed Services
- `aiostreams.douglinhas.mywire.org` -> `aiostreams`
- `comet.douglinhas.mywire.org` -> `comet`
- `jackett.douglinhas.mywire.org` -> `jackett`
- `prowlarr.douglinhas.mywire.org` -> `prowlarr`
- `iptv.douglinhas.mywire.org` -> `iptv-addon`

### Known Gotcha
After deploy, if backend is `health: starting`/unhealthy, Traefik may filter it and return 404. Recheck after container health turns healthy.

## VPN Proxy

Location: `vpn-proxy/`

Components:
- **strongSwan** -- IKEv2 IPsec VPN client
- **Squid** -- HTTP/HTTPS proxy on port 8888
- **iptables** -- kill switch (only VPN-matched traffic allowed)

### Dockerfile
Ubuntu 22.04 with strongswan, squid, iptables, rsyslog.

### Startup Flow (`vpn-proxy/start.sh`)
1. Start rsyslog (idempotent -- handles stale PID)
2. Wait for VPN certificate
3. Configure strongSwan IKEv2
4. Boot-race tolerant VPN retry loop (5 attempts, 60s timeout each)
5. Restore Docker embedded DNS (`127.0.0.11`) after tunnel comes up
6. Configure iptables kill switch (IPsec policy matching)
7. Start Squid in foreground (single-process mode: `workers 0`)
8. Background VPN monitor loop

### Key Env Knobs
- `VPN_BOOT_MAX_ATTEMPTS=5`
- `VPN_WAIT_TIMEOUT=60`
- `VPN_RETRY_DELAY=10`

### Squid Config (`vpn-proxy/squid.conf`)
- Port: 8888
- ACLs: localhost + 172.22.0.0/16
- Single-process mode (`workers 0`) to prevent zombie children

## AdGuard Proxy Lanes (4-Lane Topology)

Implemented 2026-02-20. Four separate VPN/proxy containers with different exit IPs, renamed to location-based naming on 2026-04-04.

### Lane Map

| Service | Exit Location | Purpose |
|---------|--------------|---------|
| `adguard-proxy-helsinki` | Helsinki | Addon traffic (aiostreams/Torrentio-sensitive) |
| `adguard-proxy-amsterdam` | Amsterdam | Standby |
| `adguard-proxy-london` | London | Standby (renamed from `adguard-proxy-prowlarr`) |
| `adguard-proxy-nyc` | NYC | Standby |

### Resource Limits

All AdGuard proxy containers use optimized limits based on actual usage (~18-19 MiB, ~0.01-0.21% CPU):
- `cpus: "0.25"`
- `mem_limit: "96m"`
- `pids_limit: 128`

### Service Mapping
- `aiostreams` -> Helsinki: `ADDON_PROXY=http://adguard-proxy-helsinki:8888`
- `prowlarr` -> direct egress
- `flaresolverr` -> direct egress (default, not currently assigned to any Prowlarr indexer)
- `flaresolverr-clean` -> direct egress, production FlareSolverr for all non-EZTV Prowlarr indexers (tag `flaresolver`, ID 1). Connected via Prowlarr `IndexerProxy` row `FlareSolverr-Clean` at `http://flaresolverr-clean:8191`.
- `flaresolverr-eztv-helsinki` -> Helsinki VPN (via `HTTP_PROXY=http://adguard-proxy-helsinki:8888`), dedicated FlareSolverr for EZTV (tag 6). Also has matching `Http-EZTV-Helsinki` HTTP proxy on the same tag.
- `jackett`, `flaresolverr-jackett` -> direct egress (no proxy env configured)
- `flaresolverr-prowlarr` -> direct egress, NO_PROXY includes `adguard-proxy-london`

### Prowlarr FlareSolverr Lane Assignment
Prowlarr uses tags to assign FlareSolverr/HTTP-proxy instances to specific indexers. Check `/api/v1/indexerproxy` for the current mapping.

| Prowlarr IndexerProxy | Tag | Egress | Assigned Indexers |
|----------------------|-----|--------|-------------------|
| `FlareSolverr-Clean` (ID 1) | `flaresolver` (ID 1) | Direct | All non-EZTV indexers (kickasstorrents-ws, 1337x, etc.) |
| `FlareSolverr-EZTV-Helsinki` (ID 5) | `eztv-helsinki-flaresolverr` (ID 6) | Helsinki VPN | EZTV only |
| `Http-EZTV-Helsinki` (ID 6) | `eztv-helsinki-flaresolverr` (ID 6) | Helsinki HTTP proxy | EZTV only |

### Prowlarr Per-Indexer Proxy (TRaSH Guides Pattern)
- `EZTV` now uses a clean per-indexer proxy setup instead of whole-container proxying:
  - Prowlarr `IndexerProxy` row `Http-EZTV-Helsinki` applies the Helsinki HTTP proxy only to the EZTV tag
  - Prowlarr `IndexerProxy` row `FlareSolverr-EZTV-Helsinki` applies the Helsinki-backed FlareSolverr only to the EZTV tag
  - custom Cardigann definition `prowlarr/config/Definitions/Custom/eztv_douglas.yml` avoids requiring inline magnet links on search rows
- This matches the intended Prowlarr model from TRaSH Guides: per-indexer proxy assignment via tags instead of broad app-wide proxying.
- Reference: `https://trash-guides.info/Prowlarr/prowlarr-setup-proxy/`

### Addon Proxy Config
In `aiostreams.env`:
```
ADDON_PROXY=http://adguard-proxy-helsinki:8888
ADDON_PROXY_CONFIG="*:false,*.strem.fun:true"
```
Only `*.strem.fun` addon requests use the Helsinki VPN proxy.

### Verified Egress IPs
- Helsinki: `185.77.216.3`
- Amsterdam: `84.17.46.88`
- London: `154.47.24.154`
- NYC: `79.127.206.186`

### Health Checks
- Local deterministic check: `ipsec ESTABLISHED` + localhost:8888 reachability
- Window: `interval: 30s`, `timeout: 8s`, `retries: 5`, `start_period: 120s`

## DNS Fix (2026-03-18)

Root cause of Torrentio proxy failures was DNS resolution inside `adguard-proxy-helsinki`:
- Container could reach IP literals over VPN but hostname lookups via strongSwan-pushed resolver timed out
- Fix: restore Docker embedded DNS (`127.0.0.11`) after tunnel comes up
- Squid configured with `dns_v4_first on` to use that resolver

## Historical Fixes

### AdGuard Restart-Loop Fix (2026-03-01)
- Root cause: `rsyslogd` stale PID file after manual restart
- Fix: idempotent rsyslog startup in `vpn-proxy/start.sh`
- Rolled out to all three lanes

### Zombie Prevention (2026-03-01)
- Root cause: Squid master/worker pattern produced defunct children
- Fix: `workers 0` in `squid.conf` for single-process mode
- Host zombie scan returns empty after fix

### Boot Stability Hardening (2026-03-01)
- Root cause: VPN establishment race with strict health checks
- Fix: bounded retry loop, local health check instead of external endpoint, relaxed startup window

### Byparr Removal (2026-03-02)
- Removed `byparr`, `byparr-jackett`, `byparr-prowlarr` from compose
- Reason: `BYPARR_MODE=off` in Comet, so byparr was not in traffic path
- If needed again, must be reintroduced in compose (or opt-in profile)

## AIOStreams Service

- Image: `ghcr.io/viren070/aiostreams:latest` (v2.25.3 as of 2026-03-12)
- Build: not built locally; pulled from upstream
- Config: `config/aiostreams.env` (828 lines covering all addon settings)

### AIOStreams Custom (Archived)
- `aiostreams-custom/` moved to `_archive-delete/` (2026-03-23)
- Service was commented out in `compose.yaml`
- Previously hosted at `mrdouglas2.duckdns.org`

## Comet Custom Build (Archived)

- `comet-custom/Dockerfile` moved to `_archive-delete/` (2026-03-23)
- Was building from `lucamaia9/comet` fork (`deploy-parallel-20260217` branch)
- Now replaced by `./cometouglas` local source build

## AIOMetadata

- Services: `aiometadata`, `aiometadata_redis` added to compose (2026-03-11)
- Public URL: `https://aiometadata.douglinhas.mywire.org`
- Storage: SQLite in `./aiometadata/data`, Redis AOF in `./aiometadata/cache`
- Env: `config/aiometadata.env` (minimal; optional TMDB/TVDB/Fanart keys unset)

## TLS Certificates

Comodo CA certificates in `certs/`:
- `certs/comodo_amsterdam.crt`
- `certs/comodo_helsinki.crt`
- `certs/comodo_london.crt`
- `certs/comodo_nyc.crt`

These correspond to the VPN provider's server certificates for different exit locations.

## Operational Commands

```bash
cd /home/ubuntu/aiostreams

# All services status
sudo docker compose ps

# Rebuild specific service
sudo docker compose up -d --build <service>

# Force recreate (e.g., after image update)
sudo docker compose pull <service>
sudo docker compose up -d --force-recreate <service>

# Check proxy egress
docker exec adguard-proxy-helsinki curl -sS https://www.google.com/generate_204
docker exec adguard-proxy-amsterdam curl -sS https://www.google.com/generate_204
docker exec adguard-proxy-london curl -sS https://www.google.com/generate_204
docker exec adguard-proxy-nyc curl -sS https://www.google.com/generate_204

# Tail infra logs
docker logs --since 15m traefik
docker logs --since 15m adguard-proxy-helsinki
docker logs --since 15m adguard-proxy-amsterdam
docker logs --since 15m adguard-proxy-london
docker logs --since 15m adguard-proxy-nyc
```
