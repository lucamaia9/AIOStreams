# BitMagnet Pruning Runbook

## Overview

BitMagnet PostgreSQL is a **2-day rolling staging buffer**. Raw DHT torrents are
kept briefly for enrichment and promotion to SQLite, then pruned.

## Pruning Systems

### Primary: `daily_prune_bitmagnet.sh`

- **What**: Daily automated prune via direct SQL
- **When**: Scheduled (systemd timer or cron)
- **Retention**: 2 days when promotion is healthy, 3 days when lagging
- **Safety**: Checks promotion pipeline ran within 24h before using 2-day window
- **Cleans**: torrents, torrent_files (orphans), queue_jobs (7 days)
- **Vacuum**: VACUUM FULL on torrents + torrent_files

### Secondary: `prune_bitmagnet_workspace.py`

- **What**: Batch-aware prune with promotion tracking
- **When**: Manual trigger (`--apply` for dry-run first)
- **Retention**: Configurable (--retention-days, default 3)
- **Safety**: Only prunes rows confirmed in SQLite `live_promotions` table
- **Gated**: Blocked unless `wideDrainCompleted=true` (or `--allow-before-wide-drain`)

### Complementary: `cleanup_bitmagnet.sh`

- **What**: Content-level cleanup (rejected, unmatched)
- **When**: Daily at 3 AM
- **Cleans**: rejected content (1 day), unmatched content (7 days), queue jobs (7 days)
- **Vacuum**: VACUUM ANALYZE (lightweight)

## Retention Windows

| Data | Retention | Pruned By |
|------|-----------|-----------|
| Raw torrents | 2 days (healthy) / 3 days (lagging) | `daily_prune_bitmagnet.sh` |
| Torrent files | Cascading (orphan cleanup) | `daily_prune_bitmagnet.sh` |
| Queue jobs | 7 days | `daily_prune_bitmagnet.sh` |
| Rejected content | 1 day | `cleanup_bitmagnet.sh` |
| Unmatched content | 7 days | `cleanup_bitmagnet.sh` |

## Safety Rules

1. **Never prune before wide drain is complete** (unless `--allow-before-wide-drain`)
2. **Never prune if promotion pipeline is down** (daily prune falls back to 3-day window)
3. **Always dry-run first** (`prune_bitmagnet_workspace.py` without `--apply`)
4. **Back up PostgreSQL** before any manual prune operation

## Commands

```bash
# Dry-run: see what would be pruned
python3 scripts/bitmagnet/prune_bitmagnet_workspace.py

# Apply prune (promotion-aware, batched)
python3 scripts/bitmagnet/prune_bitmagnet_workspace.py --apply

# Manual daily prune (runs immediately)
bash scripts/bitmagnet/daily_prune_bitmagnet.sh

# Emergency: prune everything (DANGEROUS)
python3 scripts/bitmagnet/reset_bitmagnet_workspace.py
python3 scripts/bitmagnet/reset_bitmagnet_workspace.py --apply
```

## Monitoring

```bash
# Check promotion health
bash scripts/bitmagnet/check_bitmagnet_promotion_health.sh

# Check ID coverage
python3 scripts/bitmagnet/check_bitmagnet_id_coverage.py

# Check DB size
sudo docker exec bitmagnet-postgres psql -U postgres -d bitmagnet \
  -c "SELECT pg_size_pretty(pg_database_size('bitmagnet'));"
```

## Troubleshooting

### Promotion falling behind
- Check `live_promotion_meta.lastRunAt` in SQLite
- If >24h, daily prune automatically uses 3-day safety window
- Run `bash scripts/bitmagnet/run_bitmagnet_live_promotion.sh` manually

### Prune blocked by wideDrainCompleted=false
- Verify wide drain state: check `live_promotion_meta.wideDrainCompleted`
- If drain is actually complete, re-run drain to verify frontier equality
- Only use `--allow-before-wide-drain` in emergencies

### DB growing unexpectedly
- Check if promotion pipeline is running
- Check if prune timer is active: `systemctl list-timers | grep prune`
- Run manual prune with `--apply`
