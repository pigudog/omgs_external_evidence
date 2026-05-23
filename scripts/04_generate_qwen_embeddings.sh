#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"

export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

printf "\n== Stage 5: Gold -> Qwen embeddings ==\n"
"$PYTHON_BIN" "$ROOT/utils/pubmed_05_generate_qwen_embeddings.py"
