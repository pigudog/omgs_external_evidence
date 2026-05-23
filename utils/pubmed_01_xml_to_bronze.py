from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pubmed_data_processing as dp
from pubmed_stage_runner_utils import resolve_from_prepare


def main() -> None:
    work = resolve_from_prepare(dp.WORK_DIR)
    bronze_dir = work / "bronze"
    xml_dirs = [str(resolve_from_prepare(p)) for p in dp.XML_DIRS]
    if not xml_dirs:
        raise SystemExit("Stage 1 requires XML_DIRS to be configured.")
    bronze_dir.mkdir(parents=True, exist_ok=True)
    dp.stage1_xml_to_bronze(xml_dirs, str(bronze_dir), workers=dp.WORKERS)


if __name__ == "__main__":
    main()
