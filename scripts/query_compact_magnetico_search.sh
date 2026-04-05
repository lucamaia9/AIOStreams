#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/ubuntu/aiostreams"
COMET_DIR="$ROOT_DIR/cometouglas"
SEARCH_DB="${MAGNETICOLOCAL_SEARCH_DB:-$ROOT_DIR/data/comet-fresh/magnetico/active.search.sqlite3}"

if [[ ! -f "$SEARCH_DB" ]]; then
  echo "search db not found: $SEARCH_DB" >&2
  exit 1
fi

cd "$COMET_DIR"
uv run python - "$SEARCH_DB" "$@" <<'PY'
import argparse
import json
import sys
from pathlib import Path

search_db = Path(sys.argv[1])
argv = sys.argv[2:]

root_dir = Path("/home/ubuntu/aiostreams/cometouglas")
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from comet.services.magnetico_local import _run_search

parser = argparse.ArgumentParser()
parser.add_argument("--title", required=True)
parser.add_argument("--year", type=int)
parser.add_argument("--season", type=int)
parser.add_argument("--episode", type=int)
parser.add_argument("--limit", type=int, default=20)
parser.add_argument("--candidate-limit", type=int, default=250)
args = parser.parse_args(argv)

results = _run_search(
    search_db,
    title=args.title,
    media_type="series" if args.episode is not None else "movie",
    media_id=None,
    aliases=None,
    year=args.year,
    season=args.season,
    episode=args.episode,
    limit=args.limit,
    candidate_limit=args.candidate_limit,
)
print(json.dumps(results, ensure_ascii=False, indent=2))
PY
