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
    gold_path = work / "gold.parquet"
    stage4_input = silver_if_path if silver_if_path.exists() else silver_path
    json_index_caution_journals = dp._load_index_caution_journals(
        resolve_from_prepare(dp.INDEX_CAUTION_JOURNALS_JSON)
    )
    merged_index_caution_journals = tuple(
        sorted(set(dp.INDEX_CAUTION_JOURNALS) | set(json_index_caution_journals))
    )
    dp.stage4_filter_to_gold(
        str(stage4_input),
        str(gold_path),
        min_pub_date=dp.MIN_PUB_DATE,
        max_pub_date=dp.MAX_PUB_DATE,
        min_impact_factor=dp.MIN_IMPACT_FACTOR,
        require_abstract=dp.REQUIRE_ABSTRACT,
        languages=dp.FILTER_LANGUAGES,
        allowed_quartiles=dp.ALLOWED_QUARTILES,
        excluded_publication_types=dp.EXCLUDED_PUBLICATION_TYPES,
        watchlist_journal_patterns=dp.WATCHLIST_JOURNAL_PATTERNS,
        index_caution_journals=merged_index_caution_journals,
        debug_breakdown=dp.STAGE4_DEBUG_BREAKDOWN,
    )


if __name__ == "__main__":
    main()
