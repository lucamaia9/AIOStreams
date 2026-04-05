#!/usr/bin/env bash
set -euo pipefail

RUN_DIR="${1:-/home/ubuntu/aiostreams/bitmagnet/magnetico_dump/import-runs/magnetico_clean_full_debug}"

META_ENV="${RUN_DIR}/meta.env"
PROGRESS_TSV="${RUN_DIR}/progress.tsv"
RESUME_LOG="${RUN_DIR}/resume2.log"
IMPORT_LOG="${RUN_DIR}/import.log"

if [[ ! -d "${RUN_DIR}" ]]; then
  echo "Run dir not found: ${RUN_DIR}" >&2
  exit 1
fi

if [[ -f "${META_ENV}" ]]; then
  # shellcheck disable=SC1090
  source "${META_ENV}"
fi

INPUT_FILE="${input_file:-}"
LINES_PER_CHUNK="${lines_per_chunk:-100000}"

if [[ -z "${INPUT_FILE}" || ! -f "${INPUT_FILE}" ]]; then
  INPUT_FILE="/home/ubuntu/aiostreams/bitmagnet/magnetico_dump/exports-clean-full-parallel/kept.jsonl"
fi

if [[ ! -f "${INPUT_FILE}" ]]; then
  echo "Input file not found: ${INPUT_FILE}" >&2
  exit 1
fi

TOTAL_LINES="$(wc -l < "${INPUT_FILE}" | tr -d ' ')"
TOTAL_CHUNKS=$(( (TOTAL_LINES + LINES_PER_CHUNK - 1) / LINES_PER_CHUNK ))

DONE_CHUNKS=0
if [[ -d "${RUN_DIR}/chunks" ]]; then
  DONE_CHUNKS="$(find "${RUN_DIR}/chunks" -maxdepth 1 -name 'chunk-*.jsonl.done' | wc -l | tr -d ' ')"
fi

ROWS_DONE=$(( DONE_CHUNKS * LINES_PER_CHUNK ))
if (( ROWS_DONE > TOTAL_LINES )); then
  ROWS_DONE="${TOTAL_LINES}"
fi

PERCENT="0.00"
if (( TOTAL_LINES > 0 )); then
  PERCENT="$(awk -v done="${ROWS_DONE}" -v total="${TOTAL_LINES}" 'BEGIN { printf "%.2f", (done/total)*100 }')"
fi

CURRENT_CHUNK="none"
if [[ -f "${RESUME_LOG}" ]]; then
  CURRENT_CHUNK="$(grep -oE 'chunk-[0-9]{5}\.jsonl' "${RESUME_LOG}" | tail -n 1 || true)"
  CURRENT_CHUNK="${CURRENT_CHUNK:-none}"
fi

ACTIVE_IMPORTS="$(ps -ef | grep -E 'run_bitmagnet_import|bitmagnet:3333/import|curlimages/curl' | grep -v grep | wc -l | tr -d ' ')"

readarray -t DB_COUNTS < <(
  docker exec bitmagnet-postgres psql -U postgres -d bitmagnet -At -c \
    "select relname || '=' || n_live_tup::bigint
       from pg_stat_user_tables
      where relname in ('torrents','torrent_contents','content')
      order by relname;
     select 'db_size=' || pg_size_pretty(pg_database_size('bitmagnet'));"
)

echo "== BitMagnet Import Progress =="
echo "run_dir=${RUN_DIR}"
echo "input_file=${INPUT_FILE}"
echo "current_chunk=${CURRENT_CHUNK}"
echo "done_chunks=${DONE_CHUNKS}/${TOTAL_CHUNKS}"
echo "rows_done=${ROWS_DONE}/${TOTAL_LINES}"
echo "progress=${PERCENT}%"
echo "active_import_processes=${ACTIVE_IMPORTS}"
echo

echo "== BitMagnet Status =="
docker exec bitmagnet sh -lc 'wget -qO- http://127.0.0.1:3333/status'
echo
echo

echo "== Database =="
printf '%s\n' "${DB_COUNTS[@]}"
echo

if [[ -f "${PROGRESS_TSV}" ]]; then
  echo "== Last Completed Chunks =="
  tail -n 5 "${PROGRESS_TSV}"
  echo
fi

if [[ -f "${RESUME_LOG}" ]]; then
  echo "== Live Log Tail =="
  tail -n 12 "${RESUME_LOG}"
  echo
fi

if [[ -f "${IMPORT_LOG}" ]]; then
  echo "== Import Response Tail =="
  tail -n 12 "${IMPORT_LOG}"
fi
