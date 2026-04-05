#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/ubuntu/aiostreams"
INPUT_PATH="${1:-}"
OUTPUT_DIR="${2:-$ROOT_DIR/bitmagnet-media/classifier/tmp/magnetico-build}"
PREFIX="${MAGNETICO_BUILD_PREFIX:-magnetico-active}"
DEPLOY_DIR="${MAGNETICO_DEPLOY_DIR:-$ROOT_DIR/data/comet-fresh/magnetico}"
BUILDER_DEPLOY_DIR="${MAGNETICO_BUILDER_DEPLOY_DIR:-}"

if [[ -z "$INPUT_PATH" ]]; then
  echo "usage: $0 /path/to/strictVideo.manifest.json [output_dir]" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"
sudo mkdir -p "$DEPLOY_DIR"
if [[ -n "$BUILDER_DEPLOY_DIR" ]]; then
  sudo mkdir -p "$BUILDER_DEPLOY_DIR"
fi

OUTPUT_DB="$OUTPUT_DIR/$PREFIX.search.sqlite3"
CHECKPOINT_JSON="$OUTPUT_DIR/$PREFIX.search.sqlite3.import.checkpoint.json"
REPORT_JSON="$OUTPUT_DIR/$PREFIX.search.sqlite3.import.report.json"

python3 "$ROOT_DIR/bitmagnet-media/classifier/import_compact_search_jsonl.py" \
  "$INPUT_PATH" \
  "$OUTPUT_DB" \
  --batch-size "${MAGNETICO_BUILD_BATCH_SIZE:-10000}" \
  --checkpoint-every-batches "${MAGNETICO_BUILD_CHECKPOINT_EVERY_BATCHES:-10}" \
  --progress-every-batches "${MAGNETICO_BUILD_PROGRESS_EVERY_BATCHES:-5}" \
  ${MAGNETICO_BUILD_EXTRA_ARGS:-}

STAGE_DIR="$(mktemp -d "$OUTPUT_DIR/.stage-$PREFIX-XXXXXX")"

mkdir -p "$STAGE_DIR"
cp "$OUTPUT_DB" "$STAGE_DIR/active.search.sqlite3"
cp "$REPORT_JSON" "$STAGE_DIR/active.import.report.json"
cp "$CHECKPOINT_JSON" "$STAGE_DIR/active.import.checkpoint.json"

sudo mv "$STAGE_DIR/active.search.sqlite3" "$DEPLOY_DIR/active.search.sqlite3"
sudo mv "$STAGE_DIR/active.import.report.json" "$DEPLOY_DIR/active.import.report.json"
sudo mv "$STAGE_DIR/active.import.checkpoint.json" "$DEPLOY_DIR/active.import.checkpoint.json"
rmdir "$STAGE_DIR"

if [[ -n "$BUILDER_DEPLOY_DIR" ]]; then
  BUILDER_STAGE_DIR="$(mktemp -d "$OUTPUT_DIR/.builder-stage-$PREFIX-XXXXXX")"
  mkdir -p "$BUILDER_STAGE_DIR"
  cp "$OUTPUT_DB" "$BUILDER_STAGE_DIR/active.search.sqlite3"
  cp "$REPORT_JSON" "$BUILDER_STAGE_DIR/active.import.report.json"
  cp "$CHECKPOINT_JSON" "$BUILDER_STAGE_DIR/active.import.checkpoint.json"
  sudo mv "$BUILDER_STAGE_DIR/active.search.sqlite3" "$BUILDER_DEPLOY_DIR/active.search.sqlite3"
  sudo mv "$BUILDER_STAGE_DIR/active.import.report.json" "$BUILDER_DEPLOY_DIR/active.import.report.json"
  sudo mv "$BUILDER_STAGE_DIR/active.import.checkpoint.json" "$BUILDER_DEPLOY_DIR/active.import.checkpoint.json"
  rmdir "$BUILDER_STAGE_DIR"
fi

echo "deployed search db: $DEPLOY_DIR/active.search.sqlite3"
