#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/ubuntu/aiostreams"
RUN_DIR="${1:-}"

echo "== BitMagnet status =="
docker exec bitmagnet sh -lc 'wget -qO- http://127.0.0.1:3333/status'
echo

echo "== BitMagnet DB counts =="
docker exec bitmagnet-postgres psql -U postgres -d bitmagnet -At -c \
  "select 'torrents|' || count(*) from torrents
   union all select 'torrent_files|' || count(*) from torrent_files
   union all select 'torrent_contents|' || count(*) from torrent_contents
   union all select 'content|' || count(*) from content;"
echo

echo "== BitMagnet DB size =="
docker exec bitmagnet-postgres psql -U postgres -d bitmagnet -At -c \
  "select pg_size_pretty(pg_database_size('bitmagnet'));"
echo

if [[ -n "${RUN_DIR}" ]]; then
  echo "== Import run state =="
  if [[ -f "${RUN_DIR}/progress.tsv" ]]; then
    tail -n 20 "${RUN_DIR}/progress.tsv"
  else
    echo "No progress.tsv found in ${RUN_DIR}"
  fi

  if [[ -f "${RUN_DIR}/import.log" ]]; then
    echo
    echo "== Import log tail =="
    tail -n 40 "${RUN_DIR}/import.log"
  fi
fi
