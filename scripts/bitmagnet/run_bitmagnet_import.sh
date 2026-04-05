#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/ubuntu/aiostreams"
INPUT_FILE="${1:?usage: run_bitmagnet_import.sh <input.jsonl> [lines_per_chunk] [run_name]}"
LINES_PER_CHUNK="${2:-500000}"
RUN_NAME="${3:-import_$(date -u +%Y%m%d_%H%M%S)}"
RUN_DIR="${ROOT_DIR}/bitmagnet/magnetico_dump/import-runs/${RUN_NAME}"
CHUNK_DIR="${RUN_DIR}/chunks"
LOG_FILE="${RUN_DIR}/import.log"
PROGRESS_FILE="${RUN_DIR}/progress.tsv"
LOCK_FILE="${RUN_DIR}/import.lock"

mkdir -p "${CHUNK_DIR}"

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "another import runner is already active for ${RUN_NAME}" >&2
  exit 1
fi

if [[ ! -f "${INPUT_FILE}" ]]; then
  echo "input file not found: ${INPUT_FILE}" >&2
  exit 1
fi

db_counts() {
  docker exec bitmagnet-postgres psql -U postgres -d bitmagnet -At -F $'\t' -c \
    "select count(*) from torrents;
     select count(*) from torrent_contents;
     select count(*) from content;
     select pg_database_size('bitmagnet');"
}

if [[ ! -f "${RUN_DIR}/meta.env" ]]; then
  echo "run_name=${RUN_NAME}" | tee "${RUN_DIR}/meta.env"
  echo "input_file=${INPUT_FILE}" >> "${RUN_DIR}/meta.env"
  echo "lines_per_chunk=${LINES_PER_CHUNK}" >> "${RUN_DIR}/meta.env"
  echo "started_at=$(date -u +%FT%TZ)" >> "${RUN_DIR}/meta.env"
  wc -l "${INPUT_FILE}" | awk '{print "input_lines="$1}' >> "${RUN_DIR}/meta.env"
  stat -c 'input_bytes=%s' "${INPUT_FILE}" >> "${RUN_DIR}/meta.env"
fi

if ! find "${CHUNK_DIR}" -maxdepth 1 -name 'chunk-*' | grep -q .; then
  split -d -a 5 -l "${LINES_PER_CHUNK}" --additional-suffix=.jsonl "${INPUT_FILE}" "${CHUNK_DIR}/chunk-"
fi

if [[ ! -f "${PROGRESS_FILE}" ]]; then
  printf "chunk\tstatus\tstarted_at\tended_at\trows\tresponse\tbefore_torrents\tafter_torrents\tbefore_tc\tafter_tc\tbefore_content\tafter_content\tbefore_db_bytes\tafter_db_bytes\n" > "${PROGRESS_FILE}"
fi

mapfile -t CHUNKS < <(find "${CHUNK_DIR}" -maxdepth 1 -name 'chunk-*.jsonl' | sort)

for chunk in "${CHUNKS[@]}"; do
  chunk_base="$(basename "${chunk}")"
  done_marker="${chunk}.done"
  if [[ -f "${done_marker}" ]]; then
    continue
  fi

  rows="$(wc -l < "${chunk}")"
  started_at="$(date -u +%FT%TZ)"
  read -r before_torrents before_tc before_content before_db_bytes < <(db_counts | xargs)

  echo "[${started_at}] importing ${chunk_base} rows=${rows}" | tee -a "${LOG_FILE}"
  response="$(
    docker run --rm \
      --network aiostreams_vpn-net \
      -v "${chunk}:/data/chunk.jsonl:ro" \
      curlimages/curl:8.12.1 \
      -sS \
      -H 'Content-Type: application/json' \
      --data-binary @/data/chunk.jsonl \
      http://bitmagnet:3333/import
  )"
  ended_at="$(date -u +%FT%TZ)"
  printf '[%s] %s response:\n%s\n' "${ended_at}" "${chunk_base}" "${response}" | tee -a "${LOG_FILE}"

  response_summary="$(printf '%s' "${response}" | tail -n 2 | tr '\n' ' ' | sed 's/[[:space:]]\\+/ /g; s/[[:space:]]$//')"

  read -r after_torrents after_tc after_content after_db_bytes < <(db_counts | xargs)

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "${chunk_base}" \
    "done" \
    "${started_at}" \
    "${ended_at}" \
    "${rows}" \
    "${response_summary//$'\t'/ }" \
    "${before_torrents}" \
    "${after_torrents}" \
    "${before_tc}" \
    "${after_tc}" \
    "${before_content}" \
    "${after_content}" \
    "${before_db_bytes}" \
    "${after_db_bytes}" >> "${PROGRESS_FILE}"

  touch "${done_marker}"
done

echo "finished_at=$(date -u +%FT%TZ)" >> "${RUN_DIR}/meta.env"
