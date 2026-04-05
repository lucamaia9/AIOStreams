#!/usr/bin/env bash
set -euo pipefail

cat >&2 <<'EOF'
run_backfill_24h.sh is retired on this host.

Bulk ID enrichment must use the staged SQLite workflow:
  1. Back up and copy data/comet-fresh/magnetico/active.search.sqlite3 to an offline stage DB.
  2. Run scripts/backfill_lean_ids.py (or related bulk enrichment) only against that stage DB.
  3. Deploy with:
       python3 ./scripts/rebuild_live_sqlite.py --historical-seed-db <stage-db> --cutover

The old live_enrichment_worker/canonical_promotion backfill path is not supported for bulk runs here.
See BACKFILL_PROCEDURE.md for the current procedure.
EOF

exit 1
