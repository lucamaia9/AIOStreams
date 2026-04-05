#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <input-jsonl> [build-name]" >&2
  exit 1
fi

INPUT_FILE="$1"
BUILD_NAME="${2:-magnetico-clean-v1}"
CHECKPOINT_HOST="/home/ubuntu/aiostreams/data/comet-fresh/${BUILD_NAME}.checkpoint.json"
CHECKPOINT_CONTAINER="/app/data/${BUILD_NAME}.checkpoint.json"
LOG_DIR="/home/ubuntu/aiostreams/test_runs/${BUILD_NAME}"
mkdir -p "$LOG_DIR"

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "Input file not found: $INPUT_FILE" >&2
  exit 1
fi

START_LINE=0
if [[ -f "$CHECKPOINT_HOST" ]]; then
  START_LINE="$(python3 - <<PY
import json
from pathlib import Path
p = Path("$CHECKPOINT_HOST")
try:
    data = json.loads(p.read_text())
    print(int(data.get("last_line", 0)))
except Exception:
    print(0)
PY
)"
fi

echo "build_name=$BUILD_NAME"
echo "input_file=$INPUT_FILE"
echo "start_line=$START_LINE"

if [[ "$START_LINE" -gt 0 ]]; then
  tail -n +"$((START_LINE + 1))" "$INPUT_FILE" | docker exec -i comet sh -lc \
    "uv run python /app/scripts/build_magnetico_search_index.py \
      --input - \
      --build-name '$BUILD_NAME' \
      --source-path '$INPUT_FILE' \
      --checkpoint-file '$CHECKPOINT_CONTAINER' \
      --line-offset '$START_LINE'" | tee -a "$LOG_DIR/build.log"
else
  cat "$INPUT_FILE" | docker exec -i comet sh -lc \
    "uv run python /app/scripts/build_magnetico_search_index.py \
      --input - \
      --build-name '$BUILD_NAME' \
      --source-path '$INPUT_FILE' \
      --checkpoint-file '$CHECKPOINT_CONTAINER'" | tee -a "$LOG_DIR/build.log"
fi
