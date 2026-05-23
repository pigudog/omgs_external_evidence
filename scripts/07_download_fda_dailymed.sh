#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"
OUTPUT_DIR="${OUTPUT_DIR:-$ROOT/data/raw/fda/dailymed}"
MANIFEST_DIR="${MANIFEST_DIR:-$ROOT/data/manifests/fda}"
STRATEGY="${STRATEGY:-latest-monthly-update}"
URL="${URL:-}"
PART="${PART:-}"
ALL_PARTS="${ALL_PARTS:-0}"
DRY_RUN="${DRY_RUN:-0}"

args=(
  "$ROOT/utils/fda_download_dailymed.py"
  --output-dir "$OUTPUT_DIR"
  --manifest-dir "$MANIFEST_DIR"
  --strategy "$STRATEGY"
)

if [[ -n "$URL" ]]; then
  args+=(--url "$URL")
fi

if [[ -n "$PART" ]]; then
  args+=(--part "$PART")
fi

if [[ "$ALL_PARTS" == "1" ]]; then
  args+=(--all-parts)
fi

if [[ "$DRY_RUN" == "1" ]]; then
  args+=(--dry-run)
fi

PYTHONNOUSERSITE=1 "$PYTHON_BIN" "${args[@]}"
