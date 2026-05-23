#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"

printf "\n== Stage 6: Gold + embeddings -> PostgreSQL ==\n"
"$PYTHON_BIN" "$ROOT/utils/pubmed_06_upload_to_postgres.py"
