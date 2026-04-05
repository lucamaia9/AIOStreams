#!/usr/bin/env bash
set -euo pipefail

PHASE="${PHASE:-phase1}"
REPEATS="${REPEATS:-1}"
LIMIT="${LIMIT:-0}"

COMET_MANIFEST_DEFAULT='https://comet.mrdouglas.uk/eyJtYXhSZXN1bHRzUGVyUmVzb2x1dGlvbiI6NywibWF4U2l6ZSI6MTYxMDYxMjczNjAsImNhY2hlZE9ubHkiOnRydWUsInNvcnRDYWNoZWRVbmNhY2hlZFRvZ2V0aGVyIjpmYWxzZSwicmVtb3ZlVHJhc2giOnRydWUsInJlc3VsdEZvcm1hdCI6WyJhbGwiXSwiZGVicmlkU2VydmljZXMiOlt7InNlcnZpY2UiOiJyZWFsZGVicmlkIiwiYXBpS2V5IjoiRlU1S0MyVjRSRko1UVlGRTJWNDdFUkQ1SEZZSFhOTzVKSlZFVEhBWlgyWldaTjdCRUZPQSJ9XSwiZW5hYmxlVG9ycmVudCI6ZmFsc2UsImRlZHVwbGljYXRlU3RyZWFtcyI6dHJ1ZSwic2NyYXBlRGVicmlkQWNjb3VudFRvcnJlbnRzIjp0cnVlLCJkZWJyaWRTdHJlYW1Qcm94eVBhc3N3b3JkIjoiIiwibGFuZ3VhZ2VzIjp7InJlcXVpcmVkIjpbXSwiYWxsb3dlZCI6W10sImV4Y2x1ZGUiOltdLCJwcmVmZXJyZWQiOlsibXVsdGkiLCJlbiJdfSwicmVzb2x1dGlvbnMiOnt9LCJvcHRpb25zIjp7InJlbW92ZV9yYW5rc191bmRlciI6LTMwLCJhbGxvd19lbmdsaXNoX2luX2xhbmd1YWdlcyI6ZmFsc2UsInJlbW92ZV91bmtub3duX2xhbmd1YWdlcyI6ZmFsc2V9fQ==/manifest.json'
BASELINE_MANIFEST_DEFAULT='https://aiostreams.mrdouglas.uk/stremio/36144e90-8df1-492d-b660-d5216c23918a/eyJpIjoicngwVHIxenpkbSt1dEprK09BWDFpUT09IiwiZSI6Iko3bFo2ZEMreHJFWjZXdFhMUGZNdnV3ZGxQVlltZ0tqT05LK0dJZ2NnKzg9IiwidCI6ImEifQ/manifest.json'

COMET_MANIFEST="${COMET_MANIFEST:-$COMET_MANIFEST_DEFAULT}"
BASELINE_MANIFEST="${BASELINE_MANIFEST:-$BASELINE_MANIFEST_DEFAULT}"

cd /home/ubuntu/aiostreams
./scripts/run_reliability_investigation.py \
  --phase "$PHASE" \
  --repeats "$REPEATS" \
  --limit "$LIMIT" \
  --manifest-comet "$COMET_MANIFEST" \
  --manifest-baseline "$BASELINE_MANIFEST"
