#!/usr/bin/env bash
set -euo pipefail

MANIFEST_URL_DEFAULT="https://mrdouglas2.duckdns.org/stremio/77299939-7aad-4662-9701-84a379347cb8/eyJpIjoiKzZyZ2lrWkdqaFh3dVRTOUJxM0pWUT09IiwiZSI6InlxbGNnRy9IZXRlVXFMV2pvL2FNUzQxQlRUc3JxekZqRkFKRjY0Zk1aRWs9IiwidCI6ImEifQ/manifest.json"
SERIES_PROBE_ID_DEFAULT="tt13833978:1:3"
LOG_SINCE_DEFAULT="20m"
SKIP_SERIES_PROBE_DEFAULT="0"

usage() {
  cat <<'EOF'
Usage:
  ./run_protocol.sh <imdb_id> [media_type]

Examples:
  ./run_protocol.sh tt0133093
  ./run_protocol.sh tt0133093 movie
  MANIFEST_URL="https://.../manifest.json" ./run_protocol.sh tt0133093

Optional env vars:
  MANIFEST_URL      Full stremio manifest URL (must end with /manifest.json)
  SERIES_PROBE_ID   Series probe id in format tt1234567:season:episode
  LOG_SINCE         Docker logs time window (default: 20m)
  SKIP_SERIES_PROBE Set to 1 to skip the additional series probe request
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 1
fi

IMDB_ID="$1"
MEDIA_TYPE="${2:-movie}"

if [[ "$MEDIA_TYPE" != "movie" && "$MEDIA_TYPE" != "series" ]]; then
  echo "Error: media_type must be 'movie' or 'series'."
  exit 1
fi

MANIFEST_URL="${MANIFEST_URL:-$MANIFEST_URL_DEFAULT}"
SERIES_PROBE_ID="${SERIES_PROBE_ID:-$SERIES_PROBE_ID_DEFAULT}"
LOG_SINCE="${LOG_SINCE:-$LOG_SINCE_DEFAULT}"
SKIP_SERIES_PROBE="${SKIP_SERIES_PROBE:-$SKIP_SERIES_PROBE_DEFAULT}"

if [[ ! "$MANIFEST_URL" =~ /manifest\.json$ ]]; then
  echo "Error: MANIFEST_URL must end with /manifest.json"
  exit 1
fi

for cmd in sudo docker curl python3; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: required command '$cmd' is not available."
    exit 1
  fi
done

if [[ ! -f "./test_aiostreams_stream.py" ]]; then
  echo "Error: ./test_aiostreams_stream.py not found. Run from /home/ubuntu/aiostreams"
  exit 1
fi

RUN_ID="$(date -u +%Y%m%d_%H%M%S)"
OUT_DIR="test_runs/$RUN_ID"
mkdir -p "$OUT_DIR"

{
  echo "run_id=$RUN_ID"
  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "imdb_id=$IMDB_ID"
  echo "media_type=$MEDIA_TYPE"
  echo "manifest_url=$MANIFEST_URL"
  echo "series_probe_id=$SERIES_PROBE_ID"
  echo "log_since=$LOG_SINCE"
  echo "skip_series_probe=$SKIP_SERIES_PROBE"
} > "$OUT_DIR/run_meta.txt"

echo "== RUN_ID: $RUN_ID =="
echo "== Preflight =="
sudo docker compose ps > "$OUT_DIR/compose_ps.txt"
curl -sS "https://mrdouglas2.duckdns.org/api/v1/status" > "$OUT_DIR/status.json" || true

echo "== Functional Probe =="
set +e
python3 ./test_aiostreams_stream.py \
  --manifest-url "$MANIFEST_URL" \
  --media-type "$MEDIA_TYPE" \
  --imdb-id "$IMDB_ID" \
  --save-json "$OUT_DIR/stream_response.json" \
  | tee "$OUT_DIR/stream_test.txt"
STREAM_TEST_EXIT=$?
set -e

BASE_URL="${MANIFEST_URL%/manifest.json}"
SERIES_URL="$BASE_URL/stream/series/$SERIES_PROBE_ID.json"
SERIES_HTTP_CODE="skipped"
if [[ "$SKIP_SERIES_PROBE" != "1" ]]; then
  SERIES_HTTP_CODE="$(curl -sS -o "$OUT_DIR/series_probe_response.json" -w '%{http_code}' "$SERIES_URL" || true)"
fi
echo "series_probe_url=$SERIES_URL" > "$OUT_DIR/series_probe.txt"
echo "series_probe_http_code=$SERIES_HTTP_CODE" >> "$OUT_DIR/series_probe.txt"

echo "== Log Capture =="
sudo docker compose logs --since="$LOG_SINCE" aiostreams --no-color > "$OUT_DIR/aiostreams.log" || true
sudo docker compose logs --since="$LOG_SINCE" jackett --no-color > "$OUT_DIR/jackett.log" || true
sudo docker compose logs --since="$LOG_SINCE" prowlarr --no-color > "$OUT_DIR/prowlarr.log" || true

echo "== Signal Extraction =="
grep -E "Handling stream request type=|\[Jackett RD\]|\[Prowlarr RD\]|Jackett per-indexer query returned partial results|torznab request error|Response: 200|Response: 404" "$OUT_DIR/aiostreams.log" > "$OUT_DIR/aiostreams_signals.txt" || true
grep -E "Torznab search in .*=> Found [0-9]+ releases|Error 1015|503 Service Temporarily Unavailable|TooManyRequests|Download selectors didn't match|Exception" "$OUT_DIR/jackett.log" > "$OUT_DIR/jackett_signals.txt" || true
grep -Ei "search|indexer|error|warning|timeout|failed|exception" "$OUT_DIR/prowlarr.log" > "$OUT_DIR/prowlarr_signals.txt" || true

python3 - "$OUT_DIR/aiostreams.log" "$IMDB_ID" "$MEDIA_TYPE" "$OUT_DIR/provider_timing_summary.txt" <<'PY'
import re
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
imdb_id = sys.argv[2]
media_type = sys.argv[3]
out_path = Path(sys.argv[4])

metrics = {
    "jackett_streams_raw": "",
    "prowlarr_streams_raw": "",
    "jackett_http_ms": "",
    "prowlarr_http_ms": "",
    "basic_filter_removed": "",
    "final_streams_logged": "",
    "prowlarr_failed_queries": "",
    "prowlarr_pending_at_deadline": "",
    "prowlarr_total_queries": "",
    "prowlarr_partial_timed_out": "",
}

if not log_path.exists():
    out_path.write_text("\n".join(f"{k}={v}" for k, v in metrics.items()) + "\n")
    print("\n".join(f"{k}={v}" for k, v in metrics.items()))
    sys.exit(0)

ansi_re = re.compile(r"\x1b\[[0-9;]*m")
lines = [ansi_re.sub("", line.rstrip("\n")) for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines()]

start_idx = None
for i, line in enumerate(lines):
    if f"Handling stream request type={media_type} id={imdb_id}" in line:
        start_idx = i

if start_idx is None:
    out_path.write_text("\n".join(f"{k}={v}" for k, v in metrics.items()) + "\n")
    print("\n".join(f"{k}={v}" for k, v in metrics.items()))
    sys.exit(0)

end_idx = None
marker = f"/stream/{media_type}/{imdb_id}.json"
for i in range(start_idx, len(lines)):
    line = lines[i]
    if "/stremio/" in line and marker in line and "Response: 200 -" in line:
        end_idx = i

if end_idx is None:
    end_idx = len(lines) - 1

window = lines[start_idx:end_idx + 1]

def extract_http_ms(provider: str) -> str:
    token = f"/builtins/{provider}/"
    pattern = re.compile(r"Response: 200 - ([0-9]+(?:\.[0-9]+)?)(ms|s)")
    for line in window:
        if token in line and "Response: 200 -" in line:
            m = pattern.search(line)
            if m:
                value = float(m.group(1))
                unit = m.group(2)
                if unit == "s":
                    value *= 1000
                return str(int(value))
    return ""

def extract_scrape_streams(label: str) -> str:
    for i, line in enumerate(window):
        if f"[{label} RD] Scrape Summary" in line:
            for j in range(i + 1, min(i + 20, len(window))):
                m = re.search(r"Streams\s*:\s*([0-9]+)", window[j])
                if m:
                    return m.group(1)
    return ""

def extract_partial_prowlarr() -> None:
    pat = re.compile(
        r"Prowlarr torrent search returned partial query results\..*failedQueries=([0-9]+).*pendingAtDeadline=([0-9]+).*totalQueries=([0-9]+).*timedOut=(true|false)"
    )
    for line in window:
        m = pat.search(line)
        if m:
            metrics["prowlarr_failed_queries"] = m.group(1)
            metrics["prowlarr_pending_at_deadline"] = m.group(2)
            metrics["prowlarr_total_queries"] = m.group(3)
            metrics["prowlarr_partial_timed_out"] = m.group(4)
            return

metrics["jackett_http_ms"] = extract_http_ms("torznab")
metrics["prowlarr_http_ms"] = extract_http_ms("prowlarr")
metrics["jackett_streams_raw"] = extract_scrape_streams("Jackett")
metrics["prowlarr_streams_raw"] = extract_scrape_streams("Prowlarr")

for line in window:
    m = re.search(r"Applied basic filters.*removed ([0-9]+) streams", line)
    if m:
        metrics["basic_filter_removed"] = m.group(1)
    m = re.search(r"Returning ([0-9]+) streams", line)
    if m:
        metrics["final_streams_logged"] = m.group(1)

extract_partial_prowlarr()

content = "\n".join(f"{k}={v}" for k, v in metrics.items()) + "\n"
out_path.write_text(content, encoding="utf-8")
print(content, end="")
PY

MANIFEST_STATUS="$(grep -Eo 'manifest_status=[0-9]+' "$OUT_DIR/stream_test.txt" | head -n1 | cut -d= -f2 || true)"
STREAM_STATUS="$(grep -Eo 'stream_status=[0-9]+' "$OUT_DIR/stream_test.txt" | head -n1 | cut -d= -f2 || true)"
STREAM_COUNT="$(grep -Eo 'streams_count=[0-9]+' "$OUT_DIR/stream_test.txt" | head -n1 | cut -d= -f2 || true)"

PY_METRICS="$(python3 - "$OUT_DIR/stream_response.json" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    print("normal_streams=0")
    print("error_streams=0")
    print("provider_mix=none")
    sys.exit(0)

try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    print("normal_streams=0")
    print("error_streams=0")
    print("provider_mix=invalid_json")
    sys.exit(0)

streams = payload.get("streams", [])
normal = 0
error = 0
providers = set()

for item in streams:
    stream_data = item.get("streamData", {})
    if isinstance(stream_data, dict) and stream_data.get("type") == "error":
        error += 1
    else:
        normal += 1

    name = str(item.get("name", "")).lower()
    if "jackett" in name:
        providers.add("jackett")
    if "prowlarr" in name:
        providers.add("prowlarr")

provider_mix = ",".join(sorted(providers)) if providers else "none"
print(f"normal_streams={normal}")
print(f"error_streams={error}")
print(f"provider_mix={provider_mix}")
PY
)"

eval "$PY_METRICS"

JACKETT_ISSUES="$(grep -Eci 'Error 1015|503 Service Temporarily Unavailable|TooManyRequests|Exception' "$OUT_DIR/jackett_signals.txt" || true)"
PROWLARR_TIMEOUTS="$(grep -Eci 'timeout|failed|exception' "$OUT_DIR/prowlarr_signals.txt" || true)"
eval "$(cat "$OUT_DIR/provider_timing_summary.txt")"

SCORE="INVESTIGATE"
REASON="Needs manual review."

if [[ "$STREAM_TEST_EXIT" -ne 0 ]]; then
  SCORE="FAIL"
  REASON="Stream probe command failed (exit $STREAM_TEST_EXIT)."
elif [[ "$STREAM_STATUS" != "200" || "$MANIFEST_STATUS" != "200" ]]; then
  SCORE="FAIL"
  REASON="Manifest or stream endpoint returned non-200."
elif [[ "${normal_streams:-0}" -gt 0 ]]; then
  if [[ "${provider_mix:-none}" == "jackett,prowlarr" && "${JACKETT_ISSUES:-0}" -eq 0 && "${PROWLARR_TIMEOUTS:-0}" -eq 0 ]]; then
    SCORE="GO"
    REASON="Normal streams returned from both providers without error burst signals."
  else
    SCORE="INVESTIGATE"
    REASON="Streams returned, but provider mix and/or backend error signals suggest instability."
  fi
elif [[ "${error_streams:-0}" -gt 0 ]]; then
  SCORE="FAIL"
  REASON="Only error streams returned."
fi

{
  echo "run_id=$RUN_ID"
  echo "imdb_id=$IMDB_ID"
  echo "media_type=$MEDIA_TYPE"
  echo "manifest_status=${MANIFEST_STATUS:-unknown}"
  echo "stream_status=${STREAM_STATUS:-unknown}"
  echo "streams_count=${STREAM_COUNT:-unknown}"
  echo "normal_streams=${normal_streams:-0}"
  echo "error_streams=${error_streams:-0}"
  echo "provider_mix=${provider_mix:-none}"
  echo "series_probe_http_code=${SERIES_HTTP_CODE:-unknown}"
  echo "jackett_http_ms=${jackett_http_ms:-}"
  echo "prowlarr_http_ms=${prowlarr_http_ms:-}"
  echo "jackett_streams_raw=${jackett_streams_raw:-}"
  echo "prowlarr_streams_raw=${prowlarr_streams_raw:-}"
  echo "basic_filter_removed=${basic_filter_removed:-}"
  echo "final_streams_logged=${final_streams_logged:-}"
  echo "prowlarr_failed_queries=${prowlarr_failed_queries:-}"
  echo "prowlarr_pending_at_deadline=${prowlarr_pending_at_deadline:-}"
  echo "prowlarr_total_queries=${prowlarr_total_queries:-}"
  echo "prowlarr_partial_timed_out=${prowlarr_partial_timed_out:-}"
  echo "jackett_issue_lines=${JACKETT_ISSUES:-0}"
  echo "prowlarr_timeout_or_failure_lines=${PROWLARR_TIMEOUTS:-0}"
  echo "score=$SCORE"
  echo "reason=$REASON"
} > "$OUT_DIR/summary.txt"

echo "== Summary =="
cat "$OUT_DIR/summary.txt"
echo
echo "Artifacts: $OUT_DIR"
