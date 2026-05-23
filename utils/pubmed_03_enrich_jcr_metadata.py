from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pubmed_data_processing as dp
from pubmed_stage_runner_utils import resolve_from_prepare


def main() -> None:
    work = resolve_from_prepare(dp.WORK_DIR)
    silver_path = work / "silver.parquet"
    silver_if_path = work / "silver_with_if.parquet"
    if_json = resolve_from_prepare(dp.IMPACT_FACTOR_JSON or str(work / "5year.json"))
    dp.stage3_map_impact_factor(str(silver_path), str(silver_if_path), str(if_json))


if __name__ == "__main__":
    main()
