#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"

printf "\n== Build PubMed bm25s index ==\n"
"$PYTHON_BIN" "$ROOT/utils/pubmed_build_bm25s_index.py" "$@"
