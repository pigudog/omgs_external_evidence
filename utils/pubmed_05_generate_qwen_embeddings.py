from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pubmed_data_processing as dp
from pubmed_stage_runner_utils import resolve_from_prepare


def main() -> None:
    work = resolve_from_prepare(dp.WORK_DIR)
    gold_path = work / "gold.parquet"
    embeddings_path = resolve_from_prepare(dp.EMBEDDINGS_OUTPUT_PATH)
    embeddings_path.parent.mkdir(parents=True, exist_ok=True)
    model_name = str(resolve_from_prepare(dp.EMBEDDING_MODEL_NAME))
    ok = dp.stage5_generate_embeddings(
        str(gold_path),
        str(embeddings_path),
        model_name=model_name,
        batch_size=dp.EMBEDDING_BATCH_SIZE,
    )
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
