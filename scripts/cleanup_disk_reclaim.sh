#!/usr/bin/env bash
set -eu

MODE="dry-run"
RETAIN_DAYS=14
RETAIN_RUNS=""
FLIGHT_KEEP_MODE="code-config"
DOCKER_PRUNE_LEVEL="safe"
VSCODE_PRUNE="true"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      MODE="dry-run"
      shift
      ;;
    --apply)
      MODE="apply"
      shift
      ;;
    --retain-days)
      RETAIN_DAYS="${2:-}"
      shift 2
      ;;
    --retain-runs)
      RETAIN_RUNS="${2:-}"
      shift 2
      ;;
    --flight-keep-mode)
      FLIGHT_KEEP_MODE="${2:-}"
      shift 2
      ;;
    --docker-prune-level)
      DOCKER_PRUNE_LEVEL="${2:-}"
      shift 2
      ;;
    --vscode-prune)
      VSCODE_PRUNE="${2:-}"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

if [[ "$FLIGHT_KEEP_MODE" != "code-config" ]]; then
  echo "Only --flight-keep-mode code-config is supported" >&2
  exit 1
fi
if [[ "$DOCKER_PRUNE_LEVEL" != "safe" && "$DOCKER_PRUNE_LEVEL" != "aggressive" ]]; then
  echo "--docker-prune-level must be safe|aggressive" >&2
  exit 1
fi
if [[ "$VSCODE_PRUNE" != "true" && "$VSCODE_PRUNE" != "false" ]]; then
  echo "--vscode-prune must be true|false" >&2
  exit 1
fi

ROOT="/home/ubuntu/aiostreams"
RUN_ID="cleanup_$(date -u +%Y%m%d_%H%M%S)"
OUT_DIR="$ROOT/test_runs/$RUN_ID"
mkdir -p "$OUT_DIR"

PRE="$OUT_DIR/pre_cleanup_report.txt"
ACTIONS="$OUT_DIR/actions_taken.txt"
POST="$OUT_DIR/post_cleanup_report.txt"
DELETED="$OUT_DIR/deleted_paths.txt"

touch "$ACTIONS" "$DELETED"

log_action() {
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$ACTIONS"
}

record_delete() {
  local path="$1"
  local size="unknown"
  if [[ -e "$path" ]]; then
    size=$(du -sh "$path" 2>/dev/null | awk '{print $1}') || true
  fi
  echo "${path}\t${size}" >> "$DELETED"
}

run_or_echo() {
  if [[ "$MODE" == "apply" ]]; then
    if ! eval "$@"; then
      if [[ "$1" == rm* ]]; then
        local sudo_cmd="${1/rm /sudo rm }"
        eval "$sudo_cmd"
      else
        return 1
      fi
    fi
  else
    echo "[dry-run] $@"
  fi
}

collect_pre() {
  {
    echo "run_id=$RUN_ID"
    echo "mode=$MODE"
    echo "retain_days=$RETAIN_DAYS"
    echo "retain_runs=${RETAIN_RUNS:-none}"
    echo "flight_keep_mode=$FLIGHT_KEEP_MODE"
    echo "docker_prune_level=$DOCKER_PRUNE_LEVEL"
    echo "vscode_prune=$VSCODE_PRUNE"
    echo
    echo "## df -h /"
    df -h /
    echo
    echo "## du -xh --max-depth=1 /home/ubuntu"
    du -xh --max-depth=1 /home/ubuntu 2>/dev/null | sort -h || true
    echo
    echo "## docker system df"
    sudo docker system df
    echo
    echo "## aiostreams compose ps"
    (cd "$ROOT" && sudo docker compose ps)
  } > "$PRE"
}

cleanup_test_runs() {
  local test_runs_dir="$ROOT/test_runs"
  [[ -d "$test_runs_dir" ]] || return 0

  log_action "Phase1: cleanup test_runs retention"

  local newest_run
  newest_run=$(find "$test_runs_dir" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' | sort -nr | head -n1 | awk '{print $2}')

  if [[ -n "$RETAIN_RUNS" ]]; then
    mapfile -t keep_paths < <(find "$test_runs_dir" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' | sort -nr | awk -v n="$RETAIN_RUNS" 'NR<=n{print $2}')
    declare -A keep_map=()
    for p in "${keep_paths[@]}"; do keep_map["$p"]=1; done
    [[ -n "$newest_run" ]] && keep_map["$newest_run"]=1

    while IFS= read -r path; do
      [[ -z "$path" ]] && continue
      if [[ -n "${keep_map[$path]:-}" ]]; then
        continue
      fi
      record_delete "$path"
      run_or_echo "rm -rf -- \"$path\""
    done < <(find "$test_runs_dir" -mindepth 1 -maxdepth 1 -type d)
  else
    while IFS= read -r path; do
      [[ -z "$path" ]] && continue
      if [[ -n "$newest_run" && "$path" == "$newest_run" ]]; then
        continue
      fi
      # Keep active/incomplete recent folders touched in last 24h.
      if [[ -n "$(find "$path" -maxdepth 0 -mtime -1 -print -quit 2>/dev/null || true)" ]]; then
        continue
      fi
      # Delete only if older than retention window.
      if [[ -n "$(find "$path" -maxdepth 0 -mtime "+$RETAIN_DAYS" -print -quit 2>/dev/null || true)" ]]; then
        record_delete "$path"
        run_or_echo "rm -rf -- \"$path\""
      fi
    done < <(find "$test_runs_dir" -mindepth 1 -maxdepth 1 -type d)
  fi
}

cleanup_flight_scraper() {
  local fs_dir="/home/ubuntu/flight_scraper"
  [[ -d "$fs_dir" ]] || return 0

  log_action "Phase2: cleanup flight_scraper artifacts"

  local targets=(
    "$fs_dir/standard_results"
    "$fs_dir/weekend_results"
    "$fs_dir/specific_dates_results"
    "$fs_dir/results"
    "$fs_dir/output"
    "$fs_dir/outputs"
    "$fs_dir/reports"
    "$fs_dir/report"
    "$fs_dir/tmp"
    "$fs_dir/temp"
    "$fs_dir/.pytest_cache"
    "$fs_dir/__pycache__"
  )

  for t in "${targets[@]}"; do
    if [[ -e "$t" ]]; then
      record_delete "$t"
      run_or_echo "rm -rf -- \"$t\""
    fi
  done

  while IFS= read -r file; do
    [[ -z "$file" ]] && continue
    case "$file" in
      */config*.json|*/package*.json|*/tsconfig*.json)
        continue
        ;;
      *)
        record_delete "$file"
        run_or_echo "rm -f -- \"$file\""
        ;;
    esac
  done < <(find "$fs_dir" -type f \( -name '*.csv' -o -name '*.tsv' -o -name '*.parquet' -o -name '*.html' -o -name '*.tmp' \))
}

cleanup_docker() {
  log_action "Phase3: docker cleanup ($DOCKER_PRUNE_LEVEL)"
  if [[ "$MODE" == "apply" ]]; then
    sudo docker image prune -a -f | tee -a "$ACTIONS"
    sudo docker builder prune -a -f | tee -a "$ACTIONS"
    sudo docker container prune -f | tee -a "$ACTIONS"
    if [[ "$DOCKER_PRUNE_LEVEL" == "aggressive" ]]; then
      sudo docker system prune -a -f | tee -a "$ACTIONS"
    fi
  else
    echo "[dry-run] sudo docker image prune -a -f" | tee -a "$ACTIONS"
    echo "[dry-run] sudo docker builder prune -a -f" | tee -a "$ACTIONS"
    echo "[dry-run] sudo docker container prune -f" | tee -a "$ACTIONS"
    if [[ "$DOCKER_PRUNE_LEVEL" == "aggressive" ]]; then
      echo "[dry-run] sudo docker system prune -a -f" | tee -a "$ACTIONS"
    fi
  fi
}

cleanup_vscode() {
  [[ "$VSCODE_PRUNE" == "true" ]] || return 0
  local vs_dir="/home/ubuntu/.vscode-server"
  [[ -d "$vs_dir" ]] || return 0

  log_action "Phase4: cleanup vscode-server caches"

  local logs_dir="$vs_dir/data/logs"
  local cached_vsix="$vs_dir/data/CachedExtensionVSIXs"
  local bin_dir="$vs_dir/bin"
  local cli_servers_dir="$vs_dir/cli/servers"

  for d in "$logs_dir" "$cached_vsix"; do
    if [[ -d "$d" ]]; then
      while IFS= read -r p; do
        [[ -z "$p" ]] && continue
        record_delete "$p"
        run_or_echo "rm -rf -- \"$p\""
      done < <(find "$d" -mindepth 1 -maxdepth 1)
    fi
  done

  # Keep newest server binary only.
  if [[ -d "$bin_dir" ]]; then
    local newest
    newest=$(find "$bin_dir" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' | sort -nr | head -n1 | awk '{print $2}')
    while IFS= read -r p; do
      [[ -z "$p" ]] && continue
      if [[ -n "$newest" && "$p" == "$newest" ]]; then
        continue
      fi
      record_delete "$p"
      run_or_echo "rm -rf -- \"$p\""
    done < <(find "$bin_dir" -mindepth 1 -maxdepth 1 -type d)
  fi

  # Keep newest CLI server only.
  if [[ -d "$cli_servers_dir" ]]; then
    local newest_cli
    newest_cli=$(find "$cli_servers_dir" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' | sort -nr | head -n1 | awk '{print $2}')
    while IFS= read -r p; do
      [[ -z "$p" ]] && continue
      if [[ -n "$newest_cli" && "$p" == "$newest_cli" ]]; then
        continue
      fi
      record_delete "$p"
      run_or_echo "rm -rf -- \"$p\""
    done < <(find "$cli_servers_dir" -mindepth 1 -maxdepth 1 -type d)
  fi
}

collect_post() {
  {
    echo "run_id=$RUN_ID"
    echo "mode=$MODE"
    echo
    echo "## df -h /"
    df -h /
    echo
    echo "## du -xh --max-depth=1 /home/ubuntu"
    du -xh --max-depth=1 /home/ubuntu 2>/dev/null | sort -h || true
    echo
    echo "## docker system df"
    sudo docker system df
    echo
    echo "## aiostreams compose ps"
    (cd "$ROOT" && sudo docker compose ps)
    echo
    echo "## comet health"
    sudo docker exec comet sh -lc 'wget -qO- http://127.0.0.1:8000/health' || true
    echo
    echo "## aiostreams status"
    curl -fsS https://aiostreams.mrdouglas.uk/api/v1/status || true
    echo
  } > "$POST"
}

log_action "Starting cleanup run_id=$RUN_ID mode=$MODE"
collect_pre
cleanup_test_runs
cleanup_flight_scraper
cleanup_docker
cleanup_vscode
collect_post
log_action "Cleanup finished. Artifacts: $OUT_DIR"

echo "Artifacts written to: $OUT_DIR"
