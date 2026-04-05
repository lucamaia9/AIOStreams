#!/usr/bin/env bash
set -euo pipefail

RUN_DIR=""
APPLY=0
ONLINE_EVIDENCE=1
WITH_OVERNIGHT=0
OVERNIGHT_LIMIT_ROWS=0
OVERNIGHT_LABEL="indexer_split"
WITH_BENCHMARK30=0
BENCHMARK_REPEATS=3
BENCHMARK_CONCURRENCY=8
BENCHMARK_TIMEOUT=30
BENCHMARK_LABEL="split30"
BENCHMARK_TITLES_FILE="test_plan/titles_30_split_v2.csv"
RUN_ALREADY_SPLIT=0

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --run-dir <path>               Existing overnight run dir (test_runs/overnight_*)
  --with-overnight               Generate a fresh overnight run before splitting
  --with-benchmark30             Generate a fresh 30-title split benchmark before splitting
  --overnight-limit-rows <int>   Optional smoke limit for overnight run
  --benchmark-titles-file <path> 30-title CSV for split benchmark
  --benchmark-repeats <int>      Repeat count for benchmark30 (default: 3)
  --benchmark-concurrency <int>  Per-probe concurrency (default: 8)
  --benchmark-timeout <sec>      Per-probe timeout seconds (default: 30)
  --apply                        Publish runtime artifact for Comet auto-load
  --no-online-evidence           Disable web evidence hints
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-dir) RUN_DIR="$2"; shift 2 ;;
    --with-overnight) WITH_OVERNIGHT=1; shift 1 ;;
    --with-benchmark30) WITH_BENCHMARK30=1; shift 1 ;;
    --overnight-limit-rows) OVERNIGHT_LIMIT_ROWS="$2"; shift 2 ;;
    --benchmark-titles-file) BENCHMARK_TITLES_FILE="$2"; shift 2 ;;
    --benchmark-repeats) BENCHMARK_REPEATS="$2"; shift 2 ;;
    --benchmark-concurrency) BENCHMARK_CONCURRENCY="$2"; shift 2 ;;
    --benchmark-timeout) BENCHMARK_TIMEOUT="$2"; shift 2 ;;
    --apply) APPLY=1; shift 1 ;;
    --no-online-evidence) ONLINE_EVIDENCE=0; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

if [[ "$WITH_OVERNIGHT" -eq 1 && "$WITH_BENCHMARK30" -eq 1 ]]; then
  echo "Use only one run generator: --with-overnight or --with-benchmark30"
  exit 1
fi

if [[ -n "$RUN_DIR" && ( "$WITH_OVERNIGHT" -eq 1 || "$WITH_BENCHMARK30" -eq 1 ) ]]; then
  echo "Do not combine --run-dir with --with-overnight/--with-benchmark30"
  exit 1
fi

if [[ "$WITH_OVERNIGHT" -eq 1 ]]; then
  cmd=(./scripts/run_overnight_indexer_benchmark.sh --run-label "$OVERNIGHT_LABEL")
  if [[ "$OVERNIGHT_LIMIT_ROWS" -gt 0 ]]; then
    cmd+=(--limit-rows "$OVERNIGHT_LIMIT_ROWS")
  fi
  output="$("${cmd[@]}")"
  echo "$output"
  RUN_DIR="$(printf '%s\n' "$output" | awk -F= '/^run_dir=/{print $2}' | tail -n1)"
fi

if [[ "$WITH_BENCHMARK30" -eq 1 ]]; then
  cmd=(
    ./scripts/run_indexer_split_benchmark.sh
    --titles-file "$BENCHMARK_TITLES_FILE"
    --repeats "$BENCHMARK_REPEATS"
    --concurrency "$BENCHMARK_CONCURRENCY"
    --probe-timeout "$BENCHMARK_TIMEOUT"
    --run-label "$BENCHMARK_LABEL"
  )
  if [[ "$ONLINE_EVIDENCE" -eq 0 ]]; then
    cmd+=(--no-online-evidence)
  fi
  if [[ "$APPLY" -eq 1 ]]; then
    cmd+=(--apply)
  fi
  output="$("${cmd[@]}")"
  echo "$output"
  RUN_DIR="$(printf '%s\n' "$output" | awk -F= '/^run_dir=/{print $2}' | tail -n1)"
  RUN_ALREADY_SPLIT=1
fi

if [[ -z "$RUN_DIR" ]]; then
  RUN_DIR="$(
    {
      ls -1dt test_runs/splitbench_* 2>/dev/null || true
      ls -1dt test_runs/overnight_* 2>/dev/null || true
    } | head -n1
  )"
fi

if [[ -z "$RUN_DIR" || ! -d "$RUN_DIR" ]]; then
  echo "No valid run dir found. Provide --run-dir or use --with-overnight."
  exit 1
fi

if [[ "$RUN_ALREADY_SPLIT" -eq 1 ]]; then
  echo "split_dir=$RUN_DIR/split"
  exit 0
fi

split_cmd=(python3 ./scripts/build_indexer_split.py --run-dir "$RUN_DIR")
if [[ "$APPLY" -eq 1 ]]; then split_cmd+=(--apply); fi
if [[ "$ONLINE_EVIDENCE" -eq 0 ]]; then split_cmd+=(--no-online-evidence); fi
"${split_cmd[@]}"
