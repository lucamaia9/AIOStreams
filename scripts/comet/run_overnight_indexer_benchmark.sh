#!/usr/bin/env bash
set -euo pipefail

MANIFEST_URL="https://comet.mrdouglas.uk/manifest.json"
TITLES_FILE="test_plan/titles_90_template.csv"
REPEATS=1
CONCURRENCY=2
LOG_SINCE="4m"
RUN_LABEL=""
LIMIT_ROWS=0
INCLUDE_COMET_PROBE=0

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --manifest-url <url>
  --titles-file <path>
  --repeats <int>
  --concurrency <int>
  --log-since <duration>
  --run-label <text>
  --limit-rows <int>      # For smoke testing
  --include-comet-probe   # Also run run_protocol.sh (can trigger extra provider fanout)
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifest-url) MANIFEST_URL="$2"; shift 2 ;;
    --titles-file) TITLES_FILE="$2"; shift 2 ;;
    --repeats) REPEATS="$2"; shift 2 ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --log-since) LOG_SINCE="$2"; shift 2 ;;
    --run-label) RUN_LABEL="$2"; shift 2 ;;
    --limit-rows) LIMIT_ROWS="$2"; shift 2 ;;
    --include-comet-probe) INCLUDE_COMET_PROBE=1; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

for c in bash python3 curl; do
  if ! command -v "$c" >/dev/null 2>&1; then
    echo "Missing required command: $c"
    exit 1
  fi
done

if ! command -v jq >/dev/null 2>&1; then
  echo "Warning: jq not found; continuing (not required by current runner)."
fi

if command -v docker >/dev/null 2>&1 && docker ps >/dev/null 2>&1; then
  DOCKER_CMD="docker"
elif command -v sudo >/dev/null 2>&1 && sudo docker ps >/dev/null 2>&1; then
  DOCKER_CMD="sudo docker"
else
  echo "Docker is unavailable (direct or sudo)."
  exit 1
fi

if [[ ! -f "$TITLES_FILE" ]]; then
  echo "Titles file not found: $TITLES_FILE"
  exit 1
fi
if [[ "$REPEATS" -gt 1 ]]; then
  echo "Refusing repeats=$REPEATS in rate-safe mode. Use --repeats 1."
  exit 1
fi
if [[ ! -x "./run_protocol.sh" ]]; then
  echo "run_protocol.sh missing or not executable"
  exit 1
fi
if [[ ! -x "./scripts/probe_indexers.py" ]]; then
  echo "scripts/probe_indexers.py missing or not executable"
  exit 1
fi
if [[ ! -x "./scripts/rank_indexers_from_overnight.py" ]]; then
  echo "scripts/rank_indexers_from_overnight.py missing or not executable"
  exit 1
fi

echo "== Validate titles CSV =="
python3 - "$TITLES_FILE" <<'PY'
import csv
import re
import sys
from collections import Counter

path = sys.argv[1]
req = ["slot", "category", "media_type", "imdb_id", "series_ref", "title_hint", "year_bucket", "priority"]
allowed = {
    "movies_old", "movies_mid", "movies_new",
    "series_old", "series_mid", "series_new",
    "anime_old", "anime_mid", "anime_new", "mixed_edge",
}
with open(path, newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

if not rows:
    raise SystemExit("CSV has no rows")
if list(rows[0].keys()) != req:
    raise SystemExit(f"CSV header mismatch. Expected: {req}")
if len(rows) != 90:
    raise SystemExit(f"CSV must contain exactly 90 rows, got {len(rows)}")

counts = Counter(r["category"] for r in rows)
for cat in sorted(allowed):
    if counts.get(cat, 0) != 9:
        raise SystemExit(f"Category {cat} must have 9 rows, got {counts.get(cat, 0)}")

pat_movie = re.compile(r"^tt\d{7,8}$")
pat_series = re.compile(r"^tt\d{7,8}:\d{1,2}:\d{1,3}$")
pat_anime = re.compile(r"^(tt\d{7,8}:\d{1,2}:\d{1,3}|kitsu:\d+(:\d{1,2}:\d{1,3})?)$")
for i, r in enumerate(rows, start=2):
    mt = r["media_type"].strip()
    imdb = r["imdb_id"].strip()
    if mt not in {"movie", "series", "anime"}:
        raise SystemExit(f"line {i}: invalid media_type {mt}")
    if mt == "movie" and not pat_movie.match(imdb):
        raise SystemExit(f"line {i}: invalid movie imdb_id {imdb}")
    if mt == "series" and not pat_series.match(imdb):
        raise SystemExit(f"line {i}: invalid series imdb_id {imdb}")
    if mt == "anime" and not pat_anime.match(imdb):
        raise SystemExit(f"line {i}: invalid anime imdb_id {imdb}")

print("titles_csv_validation=ok")
PY

echo "== Manifest preflight =="
manifest_code=$(curl -sS -o /tmp/comet_manifest_$$.json -w '%{http_code}' "$MANIFEST_URL" || true)
if [[ "$manifest_code" != "200" ]]; then
  echo "Manifest check failed. status=$manifest_code url=$MANIFEST_URL"
  exit 1
fi

JACKETT_URL=$(awk -F= '/^JACKETT_URL=/{print $2}' comet-fresh.env | tail -n1)
JACKETT_API_KEY=$(awk -F= '/^JACKETT_API_KEY=/{print $2}' comet-fresh.env | tail -n1)
PROWLARR_URL=$(awk -F= '/^PROWLARR_URL=/{print $2}' comet-fresh.env | tail -n1)
PROWLARR_API_KEY=$(awk -F= '/^PROWLARR_API_KEY=/{print $2}' comet-fresh.env | tail -n1)

if [[ -z "$JACKETT_URL" || -z "$JACKETT_API_KEY" || -z "$PROWLARR_URL" || -z "$PROWLARR_API_KEY" ]]; then
  echo "Missing Jackett/Prowlarr settings in comet-fresh.env"
  exit 1
fi

# From host context, resolve internal docker hostnames to local mapped ports.
if [[ "$JACKETT_URL" == "http://jackett:9117" ]]; then
  JACKETT_URL="http://127.0.0.1:9117"
fi
if [[ "$PROWLARR_URL" == "http://prowlarr:9696" ]]; then
  PROWLARR_URL="http://127.0.0.1:9696"
fi

RUN_ID="overnight_$(date -u +%Y%m%d_%H%M%S)"
if [[ -n "$RUN_LABEL" ]]; then
  RUN_ID="${RUN_ID}_$(echo "$RUN_LABEL" | tr ' ' '_' | tr -cd '[:alnum:]_-')"
fi
OUT_DIR="test_runs/$RUN_ID"
RAW_DIR="$OUT_DIR/raw"
mkdir -p "$RAW_DIR"

cp "$TITLES_FILE" "$OUT_DIR/titles.csv"
cp compose.yaml "$OUT_DIR/compose.snapshot.yaml"
cp comet-fresh.env "$OUT_DIR/comet-fresh.snapshot.env"
$DOCKER_CMD compose ps > "$OUT_DIR/compose_ps.start.txt" || true

{
  echo "run_id=$RUN_ID"
  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "manifest_url=$MANIFEST_URL"
  echo "titles_file=$TITLES_FILE"
  echo "repeats=$REPEATS"
  echo "concurrency=$CONCURRENCY"
  echo "log_since=$LOG_SINCE"
  echo "include_comet_probe=$INCLUDE_COMET_PROBE"
  echo "skip_series_probe=1"
  echo "docker_cmd=$DOCKER_CMD"
} > "$OUT_DIR/run_meta.txt"

echo "seq,repeat,slot,category,media_type,imdb_id,protocol_run_id,start_utc,end_utc,exit_code" > "$OUT_DIR/runs_index.csv"

run_once() {
  local seq="$1"
  local rep="$2"
  local slot="$3"
  local category="$4"
  local media_type="$5"
  local imdb_id="$6"
  local title_hint="$7"

  local started ended protocol_out exit_code protocol_run_id
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  exit_code=0
  protocol_run_id=""

  if [[ "$INCLUDE_COMET_PROBE" == "1" ]]; then
    set +e
    protocol_out="$(MANIFEST_URL="$MANIFEST_URL" LOG_SINCE="$LOG_SINCE" SKIP_SERIES_PROBE=1 ./run_protocol.sh "$imdb_id" "$media_type" 2>&1)"
    exit_code=$?
    set -e

    echo "$protocol_out" > "$OUT_DIR/protocol_${seq}.log"
    protocol_run_id="$(printf '%s\n' "$protocol_out" | grep -Eo 'RUN_ID: [0-9_]+' | awk '{print $2}' | tail -n1 || true)"
  fi

  ended="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "$seq,$rep,$slot,$category,$media_type,$imdb_id,$protocol_run_id,$started,$ended,$exit_code" >> "$OUT_DIR/runs_index.csv"

  # Probe indexers per title.
  python3 ./scripts/probe_indexers.py \
    --provider jackett \
    --media-type "$media_type" \
    --imdb-id "$imdb_id" \
    --title-hint "$title_hint" \
    --timeout 8 \
    --concurrency "$CONCURRENCY" \
    --jackett-url "$JACKETT_URL" \
    --jackett-api-key "$JACKETT_API_KEY" \
    --output "$RAW_DIR/jackett_probe.jsonl" >> "$OUT_DIR/probe_stdout.log" 2>&1 || true

  python3 ./scripts/probe_indexers.py \
    --provider prowlarr \
    --media-type "$media_type" \
    --imdb-id "$imdb_id" \
    --title-hint "$title_hint" \
    --timeout 8 \
    --concurrency "$CONCURRENCY" \
    --prowlarr-url "$PROWLARR_URL" \
    --prowlarr-api-key "$PROWLARR_API_KEY" \
    --output "$RAW_DIR/prowlarr_probe.jsonl" >> "$OUT_DIR/probe_stdout.log" 2>&1 || true
}

SEQ=0
while IFS=, read -r slot category media_type imdb_id series_ref title_hint year_bucket priority; do
  [[ "$slot" == "slot" ]] && continue
  if [[ "$LIMIT_ROWS" -gt 0 && "$slot" -gt "$LIMIT_ROWS" ]]; then
    continue
  fi
  for rep in $(seq 1 "$REPEATS"); do
    SEQ=$((SEQ + 1))
    echo "== [$SEQ] slot=$slot rep=$rep imdb=$imdb_id media=$media_type =="
    run_once "$SEQ" "$rep" "$slot" "$category" "$media_type" "$imdb_id" "$title_hint"
  done
done < "$TITLES_FILE"

python3 ./scripts/rank_indexers_from_overnight.py --run-dir "$OUT_DIR"

$DOCKER_CMD compose ps > "$OUT_DIR/compose_ps.end.txt" || true

echo "ended_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$OUT_DIR/run_meta.txt"
echo "run_dir=$OUT_DIR"
echo "ranked_report=$OUT_DIR/ranked/ranking_report.md"
