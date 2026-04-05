# Staged Historical ID Backfill Procedure

## Context

Bulk enrichment is now stage-only on this host.

- Do **not** bulk-write directly to `data/comet-fresh/magnetico/active.search.sqlite3`.
- `scripts/backfill_lean_ids.py --apply` blocks that live DB by default unless `--allow-live-search-db` is set explicitly as an unsafe override.
- `aiostreams-lean-id-backfill.*` and `aiostreams-bitmagnet-live-promotion.*` are not installed on this host.
- The old `live_enrichment_worker.py` / `canonical_promotion_worker.py` bulk path is retired for host operations.

## Supported Workflow

### 1. Back up the live DB and create an offline stage copy

```bash
cd /home/ubuntu/aiostreams

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP="data/comet-fresh/magnetico/backups/${STAMP}-pre-enrichment.sqlite3"
STAGE_DB="data/comet-fresh/magnetico/${STAMP}-enriched-stage.sqlite3"

mkdir -p data/comet-fresh/magnetico/backups
cp data/comet-fresh/magnetico/active.search.sqlite3 "$BACKUP"
cp "$BACKUP" "$STAGE_DB"
```

### 2. Run enrichment against the stage DB only

Use direct audit commands or the wrapper. Both must point at the stage DB.

```bash
/home/ubuntu/aiostreams/cometouglas/.venv/bin/python ./scripts/backfill_lean_ids.py --search-db "$STAGE_DB" --phase resolve_series --max-groups 50 --audit-only
/home/ubuntu/aiostreams/cometouglas/.venv/bin/python ./scripts/backfill_lean_ids.py --search-db "$STAGE_DB" --phase resolve_anime --max-groups 25 --audit-only
/home/ubuntu/aiostreams/cometouglas/.venv/bin/python ./scripts/backfill_lean_ids.py --search-db "$STAGE_DB" --phase resolve_movies --max-groups 50 --audit-only

LEAN_ID_BACKFILL_SEARCH_DB="$STAGE_DB" \
LEAN_ID_BACKFILL_STATE="data/comet-fresh/magnetico/${STAMP}-enriched-stage.state.json" \
LEAN_ID_BACKFILL_LOG="logs/lean-id-backfill-${STAMP}.log" \
  ./scripts/run_lean_id_backfill.sh
```

### 3. Monitor the staged run

```bash
LEAN_ID_BACKFILL_STATE="data/comet-fresh/magnetico/${STAMP}-enriched-stage.state.json" \
LEAN_ID_BACKFILL_LOG="logs/lean-id-backfill-${STAMP}.log" \
  ./scripts/watch_lean_id_backfill.sh

tail -f "logs/lean-id-backfill-${STAMP}.log"
```

The state JSON is the authoritative progress artifact for the stage run. During the parser-backed `resolve_series` bootstrap, it intentionally stays at `status=rebuilding_series_clusters` until the helper tables are rebuilt; use the log tail to watch the new timing breadcrumbs for the resolved-group seed, mapping load, slow unresolved-series queries, and each rebuild batch.

### 4. Deploy with the rebuild/cutover workflow

```bash
python3 ./scripts/rebuild_live_sqlite.py \
  --search-db data/comet-fresh/magnetico/active.search.sqlite3 \
  --historical-seed-db "$STAGE_DB" \
  --integrity-check full \
  --cutover
```

That cutover path refreshes the live DB, updates the builder mirror, and keeps rollback artifacts under `data/comet-fresh/magnetico/rebuild-runs/` and `data/comet-fresh/magnetico/backups/`.

## Safety Rules

- Do **not** use `./scripts/run_backfill_24h.sh` for bulk enrichment; it is retired.
- Do **not** point `./scripts/run_lean_id_backfill.sh` at the live `active.search.sqlite3`.
- Treat `--allow-live-search-db` as a legacy debugging escape hatch only.
- If you need recurring live catch-up outside inline promotion, use `./scripts/run_bitmagnet_live_promotion.sh` manually or create a new scheduler deliberately; do not assume the old user timer exists.
