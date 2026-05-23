from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pubmed_data_processing as dp
from pubmed_stage_runner_utils import resolve_from_prepare


def main() -> None:
    work = resolve_from_prepare(dp.WORK_DIR)
    bronze_dir = work / "bronze"
    silver_path = work / "silver.parquet"
    dp.stage2_merge_to_silver(
        str(bronze_dir),
        str(silver_path),
        memory_limit=dp.DUCKDB_MEMORY_LIMIT,
    )


if __name__ == "__main__":
    main()
