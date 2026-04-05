#!/usr/bin/env bash
set -euo pipefail

MANIFEST_URL="https://comet.mrdouglas.uk/manifest.json"
TITLES_FILE="test_plan/titles_30_split_v2.csv"
REPEATS=3
CONCURRENCY=8
PROBE_TIMEOUT=30
RUN_LABEL="split30"
ONLINE_EVIDENCE=1
APPLY=0

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --manifest-url <url>
  --titles-file <path>
  --repeats <int>          # default: 3
  --concurrency <int>      # default: 8
  --probe-timeout <sec>    # default: 30
  --run-label <text>
  --no-online-evidence
  --apply                  # publish runtime split artifact
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifest-url) MANIFEST_URL="$2"; shift 2 ;;
    --titles-file) TITLES_FILE="$2"; shift 2 ;;
    --repeats) REPEATS="$2"; shift 2 ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --probe-timeout) PROBE_TIMEOUT="$2"; shift 2 ;;
    --run-label) RUN_LABEL="$2"; shift 2 ;;
    --no-online-evidence) ONLINE_EVIDENCE=0; shift 1 ;;
    --apply) APPLY=1; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

cd /home/ubuntu/aiostreams

if [[ ! -f "$TITLES_FILE" ]]; then
  echo "Titles file not found: $TITLES_FILE"
  exit 1
fi
if [[ ! -x "./scripts/probe_indexers.py" ]]; then
  echo "scripts/probe_indexers.py missing or not executable"
  exit 1
fi
if [[ ! -x "./scripts/build_indexer_split.py" ]]; then
  echo "scripts/build_indexer_split.py missing or not executable"
  exit 1
fi

echo "== Validate benchmark CSV =="
python3 - "$TITLES_FILE" <<'PY'
import csv
import re
import sys
from collections import Counter

path = sys.argv[1]
req = ["slot", "category", "media_type", "imdb_id", "series_ref", "title_hint", "year_bucket", "priority"]
with open(path, newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))
if not rows:
    raise SystemExit("CSV has no rows")
if list(rows[0].keys()) != req:
    raise SystemExit(f"CSV header mismatch. Expected: {req}")
if len(rows) != 30:
    raise SystemExit(f"CSV must contain exactly 30 rows, got {len(rows)}")

counts = Counter(r["media_type"].strip().lower() for r in rows)
for key in ("movie", "series", "anime"):
    if counts.get(key, 0) != 10:
        raise SystemExit(f"Expected 10 rows for media_type={key}, got {counts.get(key, 0)}")

pat_movie = re.compile(r"^tt\d{7,8}$")
pat_series_or_anime = re.compile(r"^(tt\d{7,8}|kitsu:\d+):\d{1,2}:\d{1,3}$")
for i, r in enumerate(rows, start=2):
    mt = r["media_type"].strip()
    imdb = r["imdb_id"].strip()
    if mt == "movie" and not pat_movie.match(imdb):
        raise SystemExit(f"line {i}: invalid movie imdb_id {imdb}")
    if mt in {"series", "anime"} and not pat_series_or_anime.match(imdb):
        raise SystemExit(f"line {i}: invalid {mt} imdb_id {imdb}")
print("titles_csv_validation=ok")
PY

echo "== Manifest preflight =="
manifest_code=$(curl -sS -o /tmp/comet_manifest_$$.json -w '%{http_code}' "$MANIFEST_URL" || true)
if [[ "$manifest_code" != "200" ]]; then
  echo "Manifest check failed. status=$manifest_code url=$MANIFEST_URL"
  exit 1
fi

CFG_FILE="/home/ubuntu/aiostreams/config/comet-fresh.env"
JACKETT_URL=$(awk -F= '/^JACKETT_URL=/{print $2}' "$CFG_FILE" | tail -n1)
JACKETT_API_KEY=$(awk -F= '/^JACKETT_API_KEY=/{print $2}' "$CFG_FILE" | tail -n1)
PROWLARR_URL=$(awk -F= '/^PROWLARR_URL=/{print $2}' "$CFG_FILE" | tail -n1)
PROWLARR_API_KEY=$(awk -F= '/^PROWLARR_API_KEY=/{print $2}' "$CFG_FILE" | tail -n1)

# Host-run probes cannot resolve compose DNS names directly. Use routed endpoints when internal URLs are configured.
if [[ "$JACKETT_URL" == "http://jackett:9117" || "$JACKETT_URL" == "https://jackett:9117" ]]; then
  JACKETT_URL="https://jackett.mrdouglas.uk"
fi
if [[ "$PROWLARR_URL" == "http://prowlarr:9696" || "$PROWLARR_URL" == "https://prowlarr:9696" ]]; then
  PROWLARR_URL="https://prowlarr.mrdouglas.uk"
fi

RUN_ID="splitbench_$(date -u +%Y%m%d_%H%M%S)"
if [[ -n "$RUN_LABEL" ]]; then
  RUN_ID="${RUN_ID}_$(echo "$RUN_LABEL" | tr ' ' '_' | tr -cd '[:alnum:]_-')"
fi
OUT_DIR="test_runs/$RUN_ID"
RAW_DIR="$OUT_DIR/raw"
mkdir -p "$RAW_DIR"

cp "$TITLES_FILE" "$OUT_DIR/titles.csv"
{
  echo "run_id=$RUN_ID"
  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "manifest_url=$MANIFEST_URL"
  echo "titles_file=$TITLES_FILE"
  echo "repeats=$REPEATS"
  echo "concurrency=$CONCURRENCY"
  echo "probe_timeout=$PROBE_TIMEOUT"
  echo "online_evidence=$ONLINE_EVIDENCE"
  echo "apply=$APPLY"
} > "$OUT_DIR/run_meta.txt"

echo "seq,repeat,slot,category,media_type,imdb_id,start_utc,end_utc" > "$OUT_DIR/runs_index.csv"

echo "== Provider preflight =="
curl -fsS "$JACKETT_URL/api/v2.0/indexers/all/results/torznab/api?apikey=$JACKETT_API_KEY&t=indexers" >/dev/null
curl -fsS -H "X-Api-Key: $PROWLARR_API_KEY" "$PROWLARR_URL/api/v1/indexer" >/dev/null

SEQ=0
while IFS=, read -r slot category media_type imdb_id series_ref title_hint year_bucket priority; do
  [[ "$slot" == "slot" ]] && continue
  for rep in $(seq 1 "$REPEATS"); do
    SEQ=$((SEQ + 1))
    started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "== [$SEQ] slot=$slot rep=$rep media=$media_type imdb=$imdb_id =="

    jackett_probe_output="$(
      python3 ./scripts/probe_indexers.py \
      --provider jackett \
      --media-type "$media_type" \
      --imdb-id "$imdb_id" \
      --title-hint "$title_hint" \
      --slot "$slot" \
      --category "$category" \
      --repeat-id "$rep" \
      --timeout "$PROBE_TIMEOUT" \
      --concurrency "$CONCURRENCY" \
      --jackett-url "$JACKETT_URL" \
      --jackett-api-key "$JACKETT_API_KEY" \
      --output "$RAW_DIR/jackett_probe.jsonl" 2>&1
    )" || true
    echo "$jackett_probe_output" >> "$OUT_DIR/probe_stdout.log"
    jackett_total="$(printf '%s\n' "$jackett_probe_output" | awk -F'total=' '/provider=jackett/{print $2}' | awk '{print $1}' | tail -n1)"
    if [[ -z "$jackett_total" || "$jackett_total" -eq 0 ]]; then
      echo "jackett probe failed or returned zero indexers for slot=$slot rep=$rep" | tee -a "$OUT_DIR/probe_stdout.log"
      exit 1
    fi

    prowlarr_probe_output="$(
      python3 ./scripts/probe_indexers.py \
      --provider prowlarr \
      --media-type "$media_type" \
      --imdb-id "$imdb_id" \
      --title-hint "$title_hint" \
      --slot "$slot" \
      --category "$category" \
      --repeat-id "$rep" \
      --timeout "$PROBE_TIMEOUT" \
      --concurrency "$CONCURRENCY" \
      --prowlarr-url "$PROWLARR_URL" \
      --prowlarr-api-key "$PROWLARR_API_KEY" \
      --output "$RAW_DIR/prowlarr_probe.jsonl" 2>&1
    )" || true
    echo "$prowlarr_probe_output" >> "$OUT_DIR/probe_stdout.log"
    prowlarr_total="$(printf '%s\n' "$prowlarr_probe_output" | awk -F'total=' '/provider=prowlarr/{print $2}' | awk '{print $1}' | tail -n1)"
    if [[ -z "$prowlarr_total" || "$prowlarr_total" -eq 0 ]]; then
      echo "prowlarr probe failed or returned zero indexers for slot=$slot rep=$rep" | tee -a "$OUT_DIR/probe_stdout.log"
      exit 1
    fi

    ended="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "$SEQ,$rep,$slot,$category,$media_type,$imdb_id,$started,$ended" >> "$OUT_DIR/runs_index.csv"
  done
done < "$TITLES_FILE"

split_cmd=(
  python3 ./scripts/build_indexer_split.py
  --run-dir "$OUT_DIR"
)
if [[ "$ONLINE_EVIDENCE" -eq 0 ]]; then
  split_cmd+=(--no-online-evidence)
fi
if [[ "$APPLY" -eq 1 ]]; then
  split_cmd+=(--apply)
fi
"${split_cmd[@]}"

echo "ended_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$OUT_DIR/run_meta.txt"
echo "run_dir=$OUT_DIR"
echo "split_dir=$OUT_DIR/split"
