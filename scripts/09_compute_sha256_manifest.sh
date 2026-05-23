#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"
OUTPUT="${OUTPUT:-}"
CUTOFF_DATE="${CUTOFF_DATE:-2025-10-29}"
FAIL_ON_MISSING="${FAIL_ON_MISSING:-0}"

args=(
  "$ROOT/utils/evidence_sha256_manifest.py"
  --cutoff-date "$CUTOFF_DATE"
)

if [[ -n "$OUTPUT" ]]; then
  args+=(--output "$OUTPUT")
fi

if [[ "$FAIL_ON_MISSING" == "1" ]]; then
  args+=(--fail-on-missing)
fi

if [[ "$#" -gt 0 ]]; then
  for artifact in "$@"; do
    args+=(--artifact "$artifact")
  done
fi

PYTHONNOUSERSITE=1 "$PYTHON_BIN" "${args[@]}"
