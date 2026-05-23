#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"
CUTOFF_DATE="${CUTOFF_DATE:-2025-10-29}"

PYTHONNOUSERSITE=1 "$PYTHON_BIN" "$ROOT/utils/fda_build_sqlite.py" \
  --cutoff-date "$CUTOFF_DATE" \
  "$@"
