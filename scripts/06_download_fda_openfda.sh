#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"
OUTPUT_DIR="${OUTPUT_DIR:-$ROOT/data/raw/fda/openfda}"
MANIFEST_DIR="${MANIFEST_DIR:-$ROOT/data/manifests/fda}"
PAGE_SIZE="${PAGE_SIZE:-100}"
MAX_PAGES="${MAX_PAGES:-}"
MAX_RECORDS="${MAX_RECORDS:-}"
SEARCH="${SEARCH:-}"
SORT="${SORT:-effective_time:asc}"

args=(
  "$ROOT/utils/fda_download_openfda.py"
  --output-dir "$OUTPUT_DIR"
  --manifest-dir "$MANIFEST_DIR"
  --page-size "$PAGE_SIZE"
  --sort "$SORT"
)

if [[ -n "$SEARCH" ]]; then
  args+=(--search "$SEARCH")
fi

if [[ -n "$MAX_PAGES" ]]; then
  args+=(--max-pages "$MAX_PAGES")
fi

if [[ -n "$MAX_RECORDS" ]]; then
  args+=(--max-records "$MAX_RECORDS")
fi

PYTHONNOUSERSITE=1 "$PYTHON_BIN" "${args[@]}"
