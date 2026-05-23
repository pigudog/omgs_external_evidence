#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"

BASELINE_DIR="$ROOT/data/raw/pubmed/baseline"
UPDATE_DIR="$ROOT/data/raw/pubmed/updatefiles"

if [ ! -d "$BASELINE_DIR" ] || [ ! -d "$UPDATE_DIR" ]; then
  printf "\n[ERROR] Missing PubMed XML directories.\n" >&2
  printf "Expected:\n" >&2
  printf "  - %s\n" "$BASELINE_DIR" >&2
  printf "  - %s\n" "$UPDATE_DIR" >&2
  printf "Run:\n" >&2
  printf "  bash scripts/01_download_pubmed_baseline.sh\n" >&2
  printf "  bash scripts/02_download_pubmed_updatefiles.sh\n" >&2
  exit 1
fi

printf "\n== Stage 1: XML -> Bronze ==\n"
"$PYTHON_BIN" "$ROOT/utils/pubmed_01_xml_to_bronze.py"

printf "\n== Stage 2: Bronze -> Silver ==\n"
"$PYTHON_BIN" "$ROOT/utils/pubmed_02_bronze_to_silver.py"

printf "\n== Stage 3: Silver -> silver_with_if ==\n"
"$PYTHON_BIN" "$ROOT/utils/pubmed_03_enrich_jcr_metadata.py"

printf "\n== Stage 4: silver_with_if -> Gold ==\n"
"$PYTHON_BIN" "$ROOT/utils/pubmed_04_filter_to_gold.py"
