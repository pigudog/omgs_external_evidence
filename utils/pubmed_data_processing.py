#!/usr/bin/env python3
"""
PubMed Data Processing Pipeline  (Step 1 of 2)
================================================
Transforms raw PubMed XML.gz files into two Parquet files ready for
database import by pubmed_upload_to_postgres.py.

Final outputs
-------------
  gold.parquet        -- Paper metadata (pmid, title, abstract, journal, ...)
  embeddings.parquet  -- Embedding vectors (pmid, emb[float32 x dim])

Processing stages (each independently skippable)
-------------------------------------------------
  Stage 1  XML.gz  -> Bronze Parquets        (lxml parsing, multiprocessing)
  Stage 2  Bronze  -> silver.parquet         (DuckDB merge & dedup by pmid)
  Stage 3  silver  -> silver_with_if.parquet (LEFT JOIN 5-year impact factor)
  Stage 4  silver_with_if -> gold.parquet    (date / IF / language filtering)
  Stage 5  gold    -> embeddings.parquet     (SentenceTransformer vectorisation)

Dependencies
------------
  pip install lxml pandas pyarrow tqdm duckdb polars
  pip install torch sentence-transformers   # Stage 5 only, GPU recommended

After completion, run pubmed_upload_to_postgres.py to load into PostgreSQL.
"""

import csv
import os
import sys
import gzip
import json
import re
import time
import shutil
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# ============================================================================
# Configuration  --  edit these variables before running
# ============================================================================

# Directories containing PubMed .xml.gz files (base + update)
XML_DIRS: list[str] = [
    r"../data/raw/pubmed/baseline",
    r"../data/raw/pubmed/updatefiles",
]

# Working directory where intermediate processed parquet assets are written
WORK_DIR: str = "../data/processed/pubmed_mainline"

# Which stages to execute (subset of {1, 2, 3, 4, 5})
STAGES_TO_RUN: set[int] = {1, 2, 3, 4}

# Stage 1 -- number of parallel worker processes
WORKERS: int = 8

# Stages 2-4 -- DuckDB memory budget
DUCKDB_MEMORY_LIMIT: str = "32GB"

# Stage 3 -- path to the 5year.json (ISSN -> impact factor mapping)
# Leave empty ("") to skip IF mapping; silver is copied as-is.
IMPACT_FACTOR_JSON: str = "./5year.json"
INDEX_CAUTION_JOURNALS_JSON: str = "./index_caution_journals.json"

# Stage 4 -- filtering thresholds
MIN_PUB_DATE: str = "2015-10-29"
MAX_PUB_DATE: str | None = "2025-10-29"
MIN_IMPACT_FACTOR: float = 3.0   # set to 0 or None to disable IF filter
REQUIRE_ABSTRACT: bool = True
FILTER_LANGUAGES: list[str] | None = ["eng"]
ALLOWED_QUARTILES: set[str] | None = {"Q1", "Q2"}
STAGE4_DEBUG_BREAKDOWN: bool = True
EXCLUDED_PUBLICATION_TYPES: tuple[str, ...] = (
    "Comment",
    "Published Erratum",
    "Editorial",
    "Letter",
    "Retracted Publication",
    "Retraction Notice",
    "News",
    "Interview",
    "Biography",
    "Portrait",
    "Personal Narrative",
    "Expression of Concern",
    "Conference Proceedings",
    "Patient Education Handout",
    "Webcast",
    "Lecture",
    "Address",
    "Case Reports",
    "Clinical Trial Protocol",
    "Scoping Review",
    "Video-Audio Media",
    "Historical Article",
    "Dataset",
    "Introductory Journal Article",
    "Technical Report",
    "Clinical Conference",
    "Corrected and Republished Article",
    "Duplicate Publication",
    "Interactive Tutorial",
    "Bibliography",
    "Periodical Index",
    "Legislation",
    "Directory",
)
WATCHLIST_JOURNAL_PATTERNS: tuple[str, ...] = (
    "Front ",
    "Frontiers",
    "Cancers (Basel)",
    "Diagnostics (Basel)",
    "J Clin Med",
    "Journal of Personalized Medicine",
    "Biomedicines",
)
INDEX_CAUTION_JOURNALS: tuple[str, ...] = tuple()

# Stage 5 -- embedding model & batching
EMBEDDING_MODEL_NAME: str = "../models/Qwen3-Embedding-0.6B"
EMBEDDINGS_OUTPUT_PATH: str = "../data/embeddings/qwen3_embedding_0_6b/embeddings.parquet"
EMBEDDING_BATCH_SIZE: int = 32
EMBEDDING_MAX_SEQ_LENGTH: int = 1024
ENABLE_FLASH_ATTENTION_2: bool = True


# ============================================================================
# Constants
# ============================================================================

MONTH_MAP: dict[str, str] = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
    "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12",
    "january": "01", "february": "02", "march": "03", "april": "04",
    "june": "06", "july": "07", "august": "08", "september": "09",
    "october": "10", "november": "11", "december": "12",
}

JOURNAL_TOKEN_ABBREVIATIONS: dict[str, str] = {
    "ACADEMY": "ACAD",
    "ADVANCES": "ADV",
    "ADVANCE": "ADV",
    "AMERICAN": "AM",
    "ANALGESIA": "ANALG",
    "ANESTHESIA": "ANESTH",
    "ANNALS": "ANN",
    "BRITISH": "BR",
    "CANADIAN": "CAN",
    "CHEMISTRY": "CHEM",
    "CLINICAL": "CLIN",
    "EUROPEAN": "EUR",
    "EXPERIMENTAL": "EXP",
    "GYNECOLOGY": "GYNECOL",
    "GYNAECOLOGY": "GYNAECOL",
    "INTERNATIONAL": "INT",
    "JOURNAL": "J",
    "MEDICAL": "MED",
    "MEDICINE": "MED",
    "NEUROLOGY": "NEUROL",
    "OBSTETRICS": "OBSTET",
    "ONCOLOGY": "ONCOL",
    "PHARMACOLOGY": "PHARMACOL",
    "PHYSICS": "PHYS",
    "RADIOLOGY": "RADIOL",
    "RESEARCH": "RES",
    "RESPIRATORY": "RESPIR",
    "REVIEW": "REV",
    "REVIEWS": "REV",
    "SCIENCE": "SCI",
    "SCIENCES": "SCI",
    "SURGERY": "SURG",
    "THERAPY": "THER",
}

JOURNAL_TOKEN_STOPWORDS: set[str] = {"AND", "OF", "THE", "FOR", "IN", "ON", "A", "AN"}


def _normalize_journal_label(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^A-Z0-9]+", "", str(value).upper())


def _journal_name_aliases(*labels: str | None) -> set[str]:
    aliases: set[str] = set()
    for label in labels:
        if not label:
            continue
        aliases.add(_normalize_journal_label(label))
        tokens = re.findall(r"[A-Z0-9]+", str(label).upper())
        if not tokens:
            continue
        kept_tokens = [t for t in tokens if t not in JOURNAL_TOKEN_STOPWORDS]
        if kept_tokens:
            aliases.add("".join(kept_tokens))
            aliases.add(
                "".join(
                    JOURNAL_TOKEN_ABBREVIATIONS.get(tok, tok[:6])
                    for tok in kept_tokens
                )
            )
    aliases.discard("")
    return aliases


def _load_index_caution_journals(json_path: str | None) -> tuple[str, ...]:
    """Load release-safe journal_iso values for the Stage 4 caution filter."""
    if not json_path:
        return tuple()
    path = Path(json_path)
    if not path.exists():
        print(f"  [WARN] Index-caution journal JSON not found, skipping: {path}")
        return tuple()

    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, dict):
        raw_journals = payload.get("journal_iso", [])
    else:
        raw_journals = payload
    journals = {str(journal).strip() for journal in raw_journals if str(journal).strip()}

    loaded = tuple(sorted(journals))
    print(f"  Loaded {len(loaded)} index-caution journals from {path}")
    return loaded


# ============================================================================
# Stage 1  --  PubMed XML.gz -> Bronze Parquets
# ============================================================================

def _format_date_robustly(year: str | None,
                          month: str | None,
                          day: str | None) -> str | None:
    """Format date components into YYYY-MM-DD.

    Handles textual month names (e.g. "Jan", "March") and missing parts.
    """
    if not year:
        return None
    if not month:
        month_num = "01"
    elif month.isdigit():
        month_num = month.zfill(2)
    else:
        month_num = MONTH_MAP.get(month.lower().strip(), "01")
    day_num = (day or "01").zfill(2)
    if day_num == "00":
        day_num = "01"
    if month_num == "00":
        month_num = "01"
    return f"{year}-{month_num}-{day_num}"


def _parse_article_element(article_element) -> dict:
    """Parse a single <PubmedArticle> XML element into a flat dict of fields."""
    from lxml import etree  # imported here so multiprocessing workers pick it up

    def get_text(xpath: str):
        node = article_element.find(xpath)
        return node.text if node is not None else None

    def get_texts(xpath: str):
        return [n.text for n in article_element.findall(xpath) if n.text]

    def get_attribute(xpath: str, attr: str):
        node = article_element.find(xpath)
        return node.get(attr) if node is not None else None

    pmid = get_text(".//PMID")
    version = get_attribute(".//PMID", "Version")
    title = get_text(".//ArticleTitle")
    language = get_text(".//Language")

    pub_date = _format_date_robustly(
        get_text(".//Journal/JournalIssue/PubDate/Year"),
        get_text(".//Journal/JournalIssue/PubDate/Month"),
        get_text(".//Journal/JournalIssue/PubDate/Day"),
    )
    date_revised = _format_date_robustly(
        get_text(".//DateRevised/Year"),
        get_text(".//DateRevised/Month"),
        get_text(".//DateRevised/Day"),
    )

    journal_iso = get_text(".//Journal/ISOAbbreviation")
    issn = get_text(".//Journal/ISSN")
    country = get_text(".//MedlineJournalInfo/Country")

    # Abstract (may contain multiple <AbstractText> sections)
    abstract_nodes = article_element.findall(".//Abstract/AbstractText")
    abstract = (
        "\n".join(
            etree.tostring(n, method="text", encoding="unicode")
            for n in abstract_nodes
        )
        if abstract_nodes
        else None
    )

    # Identifiers
    doi = pmc = pii = None
    for aid in article_element.findall(".//ArticleIdList/ArticleId"):
        id_type = aid.get("IdType")
        if id_type == "doi":
            doi = aid.text
        elif id_type == "pmc":
            pmc = aid.text
        elif id_type == "pii":
            pii = aid.text

    # Authors
    authors = []
    for author in article_element.findall(".//AuthorList/Author"):
        lastname = author.findtext("LastName")
        forename = author.findtext("ForeName")
        initials = author.findtext("Initials")
        if lastname:
            authors.append({
                "lastname": lastname,
                "forename": forename,
                "initials": initials,
            })

    # MeSH headings
    mesh = []
    for mh in article_element.findall(".//MeshHeadingList/MeshHeading"):
        descriptor = mh.findtext("DescriptorName")
        qualifiers = [q.text for q in mh.findall("QualifierName")]
        if descriptor:
            mesh.append({"descriptor": descriptor, "qualifiers": qualifiers})

    keywords = get_texts(".//KeywordList/Keyword")
    pub_types = get_texts(".//PublicationTypeList/PublicationType")

    return {
        "pmid": pmid,
        "version": version,
        "title": title,
        "journal_iso": journal_iso,
        "issn": issn,
        "abstract": abstract,
        "pub_date": pub_date,
        "date_revised": date_revised,
        "doi": doi,
        "pmc": pmc,
        "pii": pii,
        "authors": authors or None,
        "mesh": mesh or None,
        "pub_types": pub_types or None,
        "keywords": keywords or None,
        "language": language,
        "country": country,
    }


def _process_single_xml(src_file: str, target_dir: str) -> bool:
    """Convert one .xml.gz file to .parquet (called by multiprocessing workers)."""
    from lxml import etree

    src_path = Path(src_file)
    target_path = Path(target_dir)
    try:
        with gzip.open(src_path, "rb") as f:
            context = etree.iterparse(f, events=("end",), tag="PubmedArticle")
            articles = [_parse_article_element(elem) for _, elem in context]
        if not articles:
            return True
        df = pd.DataFrame(articles)
        df["source_file"] = src_path.name
        out_name = src_path.stem.replace(".xml", "") + ".parquet"
        df.to_parquet(target_path / out_name, index=False, engine="pyarrow")
        return True
    except Exception as e:
        print(f"  [ERROR] {src_path.name}: {e}")
        return False


def stage1_xml_to_bronze(xml_dirs: list[str],
                         bronze_dir: str,
                         workers: int = 8) -> None:
    """Stage 1: Batch-convert PubMed XML.gz files into Bronze Parquets.

    Already-converted files are automatically skipped.
    """
    print("\n" + "=" * 60)
    print("  Stage 1: PubMed XML.gz -> Bronze Parquets")
    print("=" * 60)

    bronze_path = Path(bronze_dir)
    bronze_path.mkdir(parents=True, exist_ok=True)

    # Collect .xml.gz files from all directories
    all_files: list[Path] = []
    for d in xml_dirs:
        p = Path(d)
        if not p.exists():
            print(f"  [WARN] Directory not found, skipping: {d}")
            continue
        found = sorted(p.glob("*.xml.gz"))
        print(f"  Found {len(found)} XML.gz files in: {d}")
        all_files.extend(found)

    if not all_files:
        print("  [ERROR] No .xml.gz files found!")
        return

    # Skip files whose output already exists
    existing = {f.stem for f in bronze_path.glob("*.parquet")}
    todo = [f for f in all_files if f.stem.replace(".xml", "") not in existing]
    print(f"  Total: {len(all_files)}, already done: {len(existing)}, "
          f"remaining: {len(todo)}")

    if not todo:
        print("  All files already processed, skipping.")
        return

    successes = failures = 0
    t0 = time.time()

    if workers <= 1:
        for fp in tqdm(todo, desc="  Parsing XML", unit="file"):
            ok = _process_single_xml(str(fp), str(bronze_path))
            successes += ok
            failures += (not ok)
    else:
        with tqdm(total=len(todo), desc="  Parsing XML", unit="file") as pbar:
            with ProcessPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(_process_single_xml, str(fp), str(bronze_path)): fp
                    for fp in todo
                }
                for fut in as_completed(futures):
                    try:
                        ok = fut.result()
                        successes += ok
                        failures += (not ok)
                    except Exception as e:
                        failures += 1
                        print(f"  [ERROR] worker exception: {e}")
                    pbar.update(1)

    elapsed = time.time() - t0
    print(f"  Done: {successes} succeeded, {failures} failed ({elapsed:.1f}s)")


# ============================================================================
# Stage 2  --  Bronze Parquets -> silver.parquet  (merge & dedup)
# ============================================================================

def stage2_merge_to_silver(bronze_dir: str,
                           silver_path: str,
                           memory_limit: str = "8GB") -> None:
    """Stage 2: Merge all Bronze Parquets into silver.parquet with DuckDB.

    Dedup logic: for each pmid, keep a single best row.

    The previous implementation joined on ``MAX(date_revised)`` which still
    allowed duplicates when multiple records shared the same revision date.
    Here we use ``ROW_NUMBER()`` with deterministic tie-breakers so the output
    is guaranteed to contain at most one row per PMID.
    """
    import duckdb

    print("\n" + "=" * 60)
    print("  Stage 2: Bronze Parquets -> silver.parquet (merge & dedup)")
    print("=" * 60)

    bronze = Path(bronze_dir)
    if not bronze.is_dir():
        print(f"  [ERROR] Bronze directory not found: {bronze_dir}")
        return

    target = Path(silver_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    glob_pattern = str(bronze / "*.parquet")

    query = f"""
    COPY (
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY pmid
                    ORDER BY
                        COALESCE(date_revised, '') DESC,
                        TRY_CAST(version AS BIGINT) DESC NULLS LAST,
                        CASE WHEN abstract IS NOT NULL AND abstract <> '' THEN 1 ELSE 0 END DESC,
                        CASE WHEN title IS NOT NULL AND title <> '' THEN 1 ELSE 0 END DESC,
                        CASE WHEN doi IS NOT NULL AND doi <> '' THEN 1 ELSE 0 END DESC,
                        CASE WHEN pmc IS NOT NULL AND pmc <> '' THEN 1 ELSE 0 END DESC,
                        CASE WHEN pii IS NOT NULL AND pii <> '' THEN 1 ELSE 0 END DESC,
                        COALESCE(source_file, '') DESC,
                        COALESCE(title, '') DESC
                ) AS rn
            FROM read_parquet('{glob_pattern}')
        )
        SELECT * EXCLUDE (rn)
        FROM ranked
        WHERE rn = 1
    ) TO '{target}' (FORMAT PARQUET, CODEC 'ZSTD');
    """

    t0 = time.time()
    con = duckdb.connect()
    try:
        con.execute(f"PRAGMA memory_limit = '{memory_limit}';")
        print(f"  DuckDB memory limit: {memory_limit}")
        print("  Running two-stage dedup query ...")
        con.execute(query)
        elapsed = time.time() - t0
        print(f"  Done: {target} ({elapsed:.1f}s)")
    except Exception as e:
        print(f"  [ERROR] {e}")
    finally:
        con.close()


# ============================================================================
# Stage 3  --  silver -> silver_with_if.parquet  (map impact factor)
# ============================================================================

def stage3_map_impact_factor(silver_path: str,
                             output_path: str,
                             impact_factor_json: str) -> None:
    """Stage 3: LEFT JOIN current-year journal metadata onto silver.

    The mapping is built from the latest entry in ``dataByYear`` for each
    journal in ``5year.json`` and currently carries:

    - ``impact_factor``
    - ``jcr_year``
    - ``quartile``
    - ``journal_name_canonical``
    - ``eissn``
    - ``primary_category``
    - ``primary_category_quartile``

    Matching priority:

    1. ISSN / eISSN
    2. normalized journal name fallback

    If *impact_factor_json* does not exist, silver is copied unchanged and the
    added metadata columns remain unavailable.
    """
    import duckdb

    print("\n" + "=" * 60)
    print("  Stage 3: Map impact factor -> silver_with_if.parquet")
    print("=" * 60)

    stage_pbar = tqdm(total=5, desc="  Stage 3 steps", unit="step")

    if not Path(impact_factor_json).exists():
        print(f"  [WARN] IF file not found: {impact_factor_json}")
        print("  Copying silver as-is (impact_factor = NULL).")
        import shutil
        shutil.copy2(silver_path, output_path)
        stage_pbar.update(5)
        stage_pbar.close()
        return

    # Parse 5year.json -> journal metadata rows keyed by ISSN/eISSN/name aliases.
    num_pat = re.compile(r"^\s*[-+]?\d+(\.\d+)?\s*$")

    def _parse_jif(x):
        if x is None:
            return None
        s = str(x).replace(",", "").strip()
        return float(s) if num_pat.match(s) else None

    with open(impact_factor_json, "r", encoding="utf-8") as f:
        raw = json.load(f)

    issn_rows: list[dict[str, object]] = []
    journal_rows: list[dict[str, object]] = []
    seen_issn_norm: set[str] = set()
    seen_journal_norm: set[str] = set()
    for journal_key, v in raw.items():
        meta = v.get("meta")
        if not (meta and v.get("dataByYear")):
            continue
        latest = v["dataByYear"][-1]
        jif = _parse_jif(latest.get("jif"))
        jcr_year = str(latest.get("jcrYear") or "").strip() or None
        quartile = str(latest.get("quartile") or "").strip().upper() or None
        canonical_name = str(meta.get("journalName") or journal_key or "").strip() or None
        eissn = str(meta.get("eissn") or "").strip()
        eissn = None if not eissn or eissn.upper() == "N/A" else eissn
        categories = latest.get("category") or []
        primary_category = None
        primary_category_quartile = None
        if categories:
            first_category = categories[0] or {}
            primary_category = str(first_category.get("category") or "").strip() or None
            primary_category_quartile = (
                str(first_category.get("quartile") or "").strip().upper() or None
            )
        meta_row = {
            "impact_factor": jif,
            "jcr_year": jcr_year,
            "quartile": quartile,
            "journal_name_canonical": canonical_name,
            "eissn": eissn,
            "primary_category": primary_category,
            "primary_category_quartile": primary_category_quartile,
        }
        for key in ("issn", "eissn"):
            raw_issn = meta.get(key)
            if not raw_issn:
                continue
            issn_norm = raw_issn.replace("-", "").upper()
            if issn_norm in seen_issn_norm:
                continue
            issn_rows.append({"issn_norm": issn_norm, **meta_row})
            seen_issn_norm.add(issn_norm)
        for journal_norm in _journal_name_aliases(journal_key, meta.get("journalName")):
            if journal_norm in seen_journal_norm:
                continue
            journal_rows.append({"journal_norm": journal_norm, **meta_row})
            seen_journal_norm.add(journal_norm)

    print(f"  Parsed {len(issn_rows)} ISSN/eISSN metadata mappings from 5year.json")
    print(f"  Parsed {len(journal_rows)} journal-name alias mappings from 5year.json")
    stage_pbar.update(1)

    # Build a lightweight dimension table (prefer Polars, fall back to Pandas)
    try:
        import polars as pl
        dim_issn = pl.DataFrame(issn_rows)
        dim_journal = pl.DataFrame(journal_rows)
    except ImportError:
        dim_issn = pd.DataFrame(issn_rows)
        dim_journal = pd.DataFrame(journal_rows)
    stage_pbar.update(1)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    con = duckdb.connect()
    try:
        con.execute("SET enable_progress_bar = true;")
        con.execute("SET enable_progress_bar_print = true;")
        con.execute("SET progress_bar_time = 500;")

        arrow_issn = dim_issn.to_arrow() if hasattr(dim_issn, "to_arrow") else dim_issn
        arrow_journal = dim_journal.to_arrow() if hasattr(dim_journal, "to_arrow") else dim_journal
        con.register("dim_issn", arrow_issn)
        con.register("dim_journal", arrow_journal)
        total_in = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{silver_path}')"
        ).fetchone()[0]
        print(f"  Input rows: {total_in:,}")
        stage_pbar.update(1)

        query = f"""
        COPY (
            SELECT s.*,
                   COALESCE(di.impact_factor, dj.impact_factor) AS impact_factor,
                   COALESCE(di.jcr_year, dj.jcr_year) AS jcr_year,
                   COALESCE(di.quartile, dj.quartile) AS quartile,
                   COALESCE(di.journal_name_canonical, dj.journal_name_canonical) AS journal_name_canonical,
                   COALESCE(di.eissn, dj.eissn) AS eissn,
                   COALESCE(di.primary_category, dj.primary_category) AS primary_category,
                   COALESCE(di.primary_category_quartile, dj.primary_category_quartile) AS primary_category_quartile
            FROM read_parquet('{silver_path}') AS s
            LEFT JOIN dim_issn di
                ON UPPER(REPLACE(COALESCE(s.issn, ''), '-', '')) = di.issn_norm
            LEFT JOIN dim_journal dj
                ON regexp_replace(UPPER(COALESCE(s.journal_iso, '')), '[^A-Z0-9]+', '', 'g') = dj.journal_norm
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
        con.execute(query)
        stage_pbar.update(1)
        total_out = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out}')"
        ).fetchone()[0]
        elapsed = time.time() - t0
        print(f"  Output rows: {total_out:,}")
        print(f"  Done: {out} ({elapsed:.1f}s)")
    except Exception as e:
        print(f"  [ERROR] {e}")
    finally:
        con.close()

    copied_if_json = out.parent / Path(impact_factor_json).name
    try:
        shutil.copy2(impact_factor_json, copied_if_json)
        print(f"  Copied IF metadata file -> {copied_if_json}")
    except Exception as e:
        print(f"  [WARN] Failed to copy IF metadata file to output dir: {e}")
    stage_pbar.update(1)
    stage_pbar.close()


# ============================================================================
# Stage 4  --  silver_with_if -> gold.parquet  (filtering)
# ============================================================================

def stage4_filter_to_gold(
    input_path: str,
    gold_path: str,
    min_pub_date: str = "2015-01-01",
    max_pub_date: str | None = None,
    min_impact_factor: float | None = 2.0,
    require_abstract: bool = False,
    languages: list[str] | None = None,
    allowed_quartiles: set[str] | None = None,
    excluded_publication_types: tuple[str, ...] = tuple(),
    watchlist_journal_patterns: tuple[str, ...] = tuple(),
    index_caution_journals: tuple[str, ...] = tuple(),
    debug_breakdown: bool = True,
) -> None:
    """Stage 4: Apply quality/recency filters to produce gold.parquet.

    Default filters (matching the original data_dev.ipynb logic):
      * ``impact_factor > min_impact_factor``  (NULLs excluded)
      * ``pub_date >= min_pub_date``
      * ``pub_date <= max_pub_date`` when provided

    Optional additional filters via *require_abstract* and *languages*.
    """
    import duckdb

    print("\n" + "=" * 60)
    print("  Stage 4: Filter -> gold.parquet")
    print("=" * 60)

    out = Path(gold_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    conditions: list[str] = []
    debug_steps: list[tuple[str, str]] = []

    if min_impact_factor is not None and min_impact_factor > 0:
        clause = f"impact_factor > {min_impact_factor}"
        conditions.append(clause)
        debug_steps.append((f"impact_factor > {min_impact_factor}", " AND ".join(conditions)))
    if min_pub_date:
        clause = f"pub_date >= '{min_pub_date}'"
        conditions.append(clause)
        debug_steps.append((f"pub_date >= {min_pub_date}", " AND ".join(conditions)))
    if max_pub_date:
        clause = f"pub_date <= '{max_pub_date}'"
        conditions.append(clause)
        debug_steps.append((f"pub_date <= {max_pub_date}", " AND ".join(conditions)))
    if require_abstract:
        clause = "abstract IS NOT NULL AND abstract <> ''"
        conditions.append(clause)
        debug_steps.append(("require abstract", " AND ".join(conditions)))
    if languages:
        lang_list = ", ".join(f"'{lang}'" for lang in languages)
        clause = f"LOWER(language) IN ({lang_list})"
        conditions.append(clause)
        debug_steps.append((f"languages in {languages}", " AND ".join(conditions)))
    if allowed_quartiles:
        quartile_list = ", ".join(f"'{q}'" for q in sorted(allowed_quartiles))
        clause = f"quartile IN ({quartile_list})"
        conditions.append(clause)
        debug_steps.append((f"quartile in {sorted(allowed_quartiles)}", " AND ".join(conditions)))
    if excluded_publication_types:
        pub_types_text = "LOWER(COALESCE(array_to_string(pub_types, '||'), ''))"
        for pub_type in sorted(excluded_publication_types):
            escaped = pub_type.replace("'", "''").lower()
            conditions.append(f"{pub_types_text} NOT LIKE '%{escaped}%'")
        debug_steps.append(
            (f"exclude publication types ({len(excluded_publication_types)})", " AND ".join(conditions))
        )

    def _exclude_prefix_group(label: str, patterns: tuple[str, ...]) -> None:
        if not patterns:
            return
        terms: list[str] = []
        for pattern in patterns:
            escaped = pattern.replace("'", "''")
            terms.append(f"journal_iso LIKE '{escaped}%'")
            terms.append(f"journal_name_canonical LIKE '{escaped.upper()}%'")
        positive = "(" + " OR ".join(terms) + ")"
        clause = f"NOT {positive}"
        conditions.append(clause)
        debug_steps.append((label, " AND ".join(conditions)))

    _exclude_prefix_group("exclude index caution journals", index_caution_journals)
    _exclude_prefix_group("exclude watchlist journals", watchlist_journal_patterns)

    where_clause = " AND ".join(conditions) if conditions else "TRUE"
    print("  Applied filters:")
    if min_impact_factor is not None and min_impact_factor > 0:
        print(f"    - impact_factor > {min_impact_factor}")
    if min_pub_date:
        print(f"    - pub_date >= {min_pub_date}")
    if max_pub_date:
        print(f"    - pub_date <= {max_pub_date}")
    if require_abstract:
        print("    - require abstract")
    if languages:
        print(f"    - languages = {languages}")
    if allowed_quartiles:
        print(f"    - quartiles = {sorted(allowed_quartiles)}")
    if excluded_publication_types:
        print(f"    - exclude publication types = {len(excluded_publication_types)}")
    if index_caution_journals:
        print(f"    - exclude index caution journals = {len(index_caution_journals)}")
    if watchlist_journal_patterns:
        print(f"    - exclude watchlist patterns = {len(watchlist_journal_patterns)}")
    stage_pbar = tqdm(total=5, desc="  Stage 4 steps", unit="step")

    t0 = time.time()
    con = duckdb.connect()
    try:
        con.execute("SET enable_progress_bar = true;")
        con.execute("SET enable_progress_bar_print = true;")
        con.execute("SET progress_bar_time = 500;")
        total = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{input_path}')"
        ).fetchone()[0]
        print(f"  Input rows: {total:,}")
        stage_pbar.update(1)

        breakdown_selects = []
        for idx, (_, cumulative_clause) in enumerate(debug_steps, start=1):
            breakdown_selects.append(
                f"SUM(CASE WHEN {cumulative_clause} THEN 1 ELSE 0 END) AS step_{idx}"
            )
        if debug_breakdown and breakdown_selects:
            breakdown_query = (
                f"SELECT {', '.join(breakdown_selects)} FROM read_parquet('{input_path}')"
            )
            breakdown_row = con.execute(breakdown_query).fetchone()
            remaining = total
            print("  Filter breakdown:")
            for idx, (label, _) in enumerate(debug_steps, start=1):
                after_count = int(breakdown_row[idx - 1] or 0)
                removed_now = remaining - after_count
                print(f"    - {label}: removed {removed_now:,}, remaining {after_count:,}")
                remaining = after_count
        stage_pbar.update(1)

        query = f"""
        COPY (
            SELECT * FROM read_parquet('{input_path}')
            WHERE {where_clause}
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
        con.execute(query)
        stage_pbar.update(1)

        filtered = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out}')"
        ).fetchone()[0]
        stage_pbar.update(1)
        elapsed = time.time() - t0
        print(f"  Before: {total:,} -> After: {filtered:,} ({elapsed:.1f}s)")
        stage_pbar.update(1)
    except Exception as e:
        print(f"  [ERROR] {e}")
    finally:
        con.close()
        stage_pbar.close()


# ============================================================================
# Stage 5  --  gold.parquet -> embeddings.parquet  (vectorisation)
# ============================================================================

def _load_sentence_transformer_model(model_name: str, device: str, torch):
    """Load the embedding model, enabling flash_attention_2 when available."""
    from importlib.util import find_spec

    from sentence_transformers import SentenceTransformer

    use_flash = (
        device == "cuda"
        and ENABLE_FLASH_ATTENTION_2
        and find_spec("flash_attn") is not None
    )
    if not use_flash:
        return SentenceTransformer(model_name, device=device), "standard"

    try:
        from sentence_transformers.models import Normalize, Pooling, Transformer

        model_path = Path(model_name)
        transformer = Transformer(
            model_name,
            model_args={
                "attn_implementation": "flash_attention_2",
                "torch_dtype": torch.float16,
            },
            tokenizer_args={"padding_side": "left"},
        )

        pooling_kwargs = {
            "pooling_mode_cls_token": False,
            "pooling_mode_mean_tokens": False,
            "pooling_mode_max_tokens": False,
            "pooling_mode_mean_sqrt_len_tokens": False,
            "pooling_mode_weightedmean_tokens": False,
            "pooling_mode_lasttoken": True,
            "include_prompt": True,
        }
        pooling_cfg_path = model_path / "1_Pooling" / "config.json"
        if pooling_cfg_path.exists():
            with open(pooling_cfg_path, "r", encoding="utf-8") as f:
                pooling_kwargs.update(json.load(f))
            pooling_kwargs.pop("word_embedding_dimension", None)

        pooling = Pooling(
            word_embedding_dimension=transformer.get_word_embedding_dimension(),
            **pooling_kwargs,
        )

        modules = [transformer, pooling]
        modules_json_path = model_path / "modules.json"
        if modules_json_path.exists():
            with open(modules_json_path, "r", encoding="utf-8") as f:
                module_defs = json.load(f)
            if any(m.get("type", "").endswith(".Normalize") for m in module_defs):
                modules.append(Normalize())

        prompts = None
        default_prompt_name = None
        st_cfg_path = model_path / "config_sentence_transformers.json"
        if st_cfg_path.exists():
            with open(st_cfg_path, "r", encoding="utf-8") as f:
                st_cfg = json.load(f)
            prompts = st_cfg.get("prompts")
            default_prompt_name = st_cfg.get("default_prompt_name")

        model = SentenceTransformer(
            modules=modules,
            device=device,
            prompts=prompts,
            default_prompt_name=default_prompt_name,
        )
        return model, "flash_attention_2"
    except Exception as e:
        print(f"  [WARN] Failed to enable flash_attention_2, falling back: {e}")
        return SentenceTransformer(model_name, device=device), "standard"

def stage5_generate_embeddings(
    gold_path: str,
    output_path: str,
    model_name: str = "Qwen/Qwen3-Embedding-0.6B",
    batch_size: int = 1024,
) -> bool:
    """Stage 5: Vectorise title+abstract with a SentenceTransformer model.

    Writes a streaming Parquet file.  GPU strongly recommended.
    """
    print("\n" + "=" * 60)
    print("  Stage 5: Generate embeddings")
    print("=" * 60)

    try:
        import torch
        from sentence_transformers import SentenceTransformer
    except ImportError:
        print("  [ERROR] Missing dependencies for Stage 5:")
        print("    pip install torch sentence-transformers")
        return

    import numpy as np
    from threading import Thread
    from queue import Queue

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"  Device: {device.upper()}")
    if device == "cpu":
        print("  [WARN] No GPU detected -- processing will be very slow!")

    print(f"  Model:  {model_name}")
    model, attention_backend = _load_sentence_transformer_model(
        model_name,
        device,
        torch,
    )
    if hasattr(model, "max_seq_length"):
        model.max_seq_length = EMBEDDING_MAX_SEQ_LENGTH
    print(f"  Attention backend: {attention_backend}")
    embedding_dim = model.get_sentence_embedding_dimension()
    print(f"  Embedding dimension: {embedding_dim}")
    print(f"  Max sequence length: {EMBEDDING_MAX_SEQ_LENGTH}")

    # Output Arrow schema
    output_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("pmid", pa.string()),
        pa.field("emb", pa.list_(pa.float32())),
        pa.field("model", pa.string()),
        pa.field("dim", pa.int32()),
        pa.field("created_at", pa.timestamp("ms")),
    ])

    output_tmp_path = f"{output_path}.merge_tmp"
    output_parts_dir = Path(f"{output_path}.parts")
    legacy_partial_path = Path(f"{output_path}.partial")
    stale_partial_new_path = Path(f"{output_path}.partial.new")
    output_parts_dir.mkdir(parents=True, exist_ok=True)

    if stale_partial_new_path.exists():
        print(f"  Removing stale incomplete shard: {stale_partial_new_path}")
        stale_partial_new_path.unlink()

    def _safe_row_count(path: Path, label: str) -> int:
        print(f"  Resume mode: scanning {label} -> {path}")
        pf = pq.ParquetFile(path)
        rows = 0
        for batch in tqdm(
            pf.iter_batches(batch_size=max(batch_size * 8, 4096)),
            desc=f"  Scan {label}",
            unit=" batches",
        ):
            rows += batch.num_rows
        return rows

    if legacy_partial_path.exists():
        try:
            pq.ParquetFile(legacy_partial_path)
            migrated_path = output_parts_dir / "part-legacy-partial.parquet"
            if migrated_path.exists():
                migrated_path.unlink()
            print(f"  Migrating legacy partial shard -> {migrated_path}")
            legacy_partial_path.replace(migrated_path)
        except Exception:
            print(f"  Removing broken legacy partial shard: {legacy_partial_path}")
            legacy_partial_path.unlink()

    main_rows = 0
    total_rows = 0
    if os.path.exists(output_path):
        main_rows = _safe_row_count(Path(output_path), "main embeddings")
        total_rows += main_rows

    part_files = sorted(output_parts_dir.glob("part-*.parquet"))
    part_rows = 0
    for part_path in part_files:
        part_rows += _safe_row_count(part_path, part_path.name)
    total_rows += part_rows
    row_counter = total_rows
    rows_to_skip = total_rows
    next_part_index = len(part_files)

    # Prefetch queue: overlap disk I/O with GPU compute
    QUEUE_SIZE = 8
    queue: Queue = Queue(maxsize=QUEUE_SIZE)

    def _reader():
        """Background reader that feeds Arrow batches into the queue."""
        try:
            pf = pq.ParquetFile(gold_path)
            remaining_skip = rows_to_skip
            for rg_idx in range(pf.num_row_groups):
                rg = pf.read_row_group(
                    rg_idx, columns=["pmid", "title", "abstract"]
                )
                if remaining_skip >= rg.num_rows:
                    remaining_skip -= rg.num_rows
                    continue
                if remaining_skip > 0:
                    rg = rg.slice(remaining_skip, rg.num_rows - remaining_skip)
                    remaining_skip = 0
                for i in range(0, rg.num_rows, batch_size):
                    queue.put(rg.slice(i, min(batch_size, rg.num_rows - i)))
        except Exception as e:
            print(f"  [ERROR] Reader thread: {e}")
        finally:
            queue.put(None)  # sentinel

    def _encode_with_retry(texts):
        try:
            return model.encode(
                texts,
                show_progress_bar=False,
                convert_to_tensor=False,
                normalize_embeddings=True,
            ).astype(np.float32)
        except RuntimeError as e:
            if "out of memory" not in str(e).lower() or len(texts) <= 1:
                raise
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
            mid = len(texts) // 2
            left = _encode_with_retry(texts[:mid])
            right = _encode_with_retry(texts[mid:])
            return np.concatenate([left, right], axis=0)

    def _iter_existing_sources() -> list[Path]:
        sources: list[Path] = []
        if os.path.exists(output_path):
            sources.append(Path(output_path))
        sources.extend(sorted(output_parts_dir.glob("part-*.parquet")))
        return sources

    SHARD_TARGET_ROWS = 8192
    current_shard_writer = None
    current_shard_tmp_path: Path | None = None
    current_shard_final_path: Path | None = None
    current_shard_rows = 0

    def _open_new_shard() -> None:
        nonlocal current_shard_writer, current_shard_tmp_path, current_shard_final_path
        nonlocal current_shard_rows, next_part_index
        current_shard_final_path = output_parts_dir / f"part-{next_part_index:08d}.parquet"
        current_shard_tmp_path = output_parts_dir / f"part-{next_part_index:08d}.parquet.writing"
        if current_shard_tmp_path.exists():
            current_shard_tmp_path.unlink()
        current_shard_writer = pq.ParquetWriter(str(current_shard_tmp_path), output_schema)
        current_shard_rows = 0
        next_part_index += 1

    def _close_current_shard(commit: bool) -> None:
        nonlocal current_shard_writer, current_shard_tmp_path, current_shard_final_path
        nonlocal current_shard_rows
        if current_shard_writer is None:
            return
        current_shard_writer.close()
        if commit and current_shard_rows > 0 and current_shard_tmp_path and current_shard_final_path:
            if current_shard_final_path.exists():
                current_shard_final_path.unlink()
            current_shard_tmp_path.replace(current_shard_final_path)
        elif current_shard_tmp_path and current_shard_tmp_path.exists():
            current_shard_tmp_path.unlink()
        current_shard_writer = None
        current_shard_tmp_path = None
        current_shard_final_path = None
        current_shard_rows = 0

    def _merge_all_parts_into_main() -> None:
        merge_sources = _iter_existing_sources()
        if not merge_sources:
            return
        if os.path.exists(output_tmp_path):
            os.remove(output_tmp_path)
        with pq.ParquetWriter(output_tmp_path, output_schema) as merge_writer:
            next_id = 0
            for src in merge_sources:
                pf = pq.ParquetFile(src)
                for batch in tqdm(
                    pf.iter_batches(batch_size=max(batch_size * 8, 4096)),
                    desc=f"  Merge {src.name}",
                    unit=" batches",
                ):
                    batch_table = pa.Table.from_batches([batch], schema=output_schema)
                    batch_rows = batch_table.num_rows
                    batch_table = batch_table.set_column(
                        0,
                        "id",
                        pa.array(range(next_id, next_id + batch_rows), type=pa.int64()),
                    )
                    merge_writer.write_table(batch_table)
                    next_id += batch_rows
        if os.path.exists(output_path):
            os.remove(output_path)
        os.replace(output_tmp_path, output_path)
        for src in sorted(output_parts_dir.glob("part-*.parquet")):
            src.unlink()

    reader = Thread(target=_reader, daemon=True)
    reader.start()

    t0 = time.time()
    stage5_ok = False

    try:
        with tqdm(unit=" rows", desc="  Embedding") as pbar:
            if total_rows:
                pbar.update(total_rows)
            while True:
                batch = queue.get()
                if batch is None:
                    break
                if batch.num_rows == 0:
                    continue

                pmids = [str(x) for x in batch.column("pmid").to_pylist()]
                titles = batch.column("title").to_pylist()
                abstracts = batch.column("abstract").to_pylist()
                n = len(pmids)

                texts = [
                    (ti or "") + "\n" + (ab or "")
                    for ti, ab in zip(titles, abstracts)
                ]

                embeddings = _encode_with_retry(texts)

                ids = pa.array(
                    range(row_counter, row_counter + n), type=pa.int64()
                )
                offsets = pa.array(
                    np.arange(
                        0, (n + 1) * embedding_dim, embedding_dim,
                        dtype=np.int32,
                    ).tolist(),
                    type=pa.int32(),
                )
                values = pa.array(embeddings.flatten(), type=pa.float32())
                emb_array = pa.ListArray.from_arrays(offsets, values)

                now = datetime.now()
                table = pa.Table.from_arrays(
                    [
                        ids,
                        pa.array(pmids, type=pa.string()),
                        emb_array,
                        pa.array([model_name] * n, type=pa.string()),
                        pa.array([embedding_dim] * n, type=pa.int32()),
                        pa.array([now] * n, type=pa.timestamp("ms")),
                    ],
                    schema=output_schema,
                )
                if current_shard_writer is None:
                    _open_new_shard()
                current_shard_writer.write_table(table)
                current_shard_rows += n

                row_counter += n
                total_rows += n
                pbar.update(batch.num_rows)
                if current_shard_rows >= SHARD_TARGET_ROWS:
                    _close_current_shard(commit=True)

                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
        _close_current_shard(commit=True)
        stage5_ok = True

    except KeyboardInterrupt:
        print("\n  [INFO] Ctrl+C detected, exiting safely ...")
    except Exception as e:
        print(f"\n  [ERROR] {e}")
    finally:
        reader.join(timeout=5)
        if stage5_ok:
            _merge_all_parts_into_main()
        else:
            _close_current_shard(commit=False)
            if os.path.exists(output_tmp_path):
                os.remove(output_tmp_path)
        elapsed = time.time() - t0
        print(f"  Done: {total_rows:,} rows in {elapsed:.1f}s -> {output_path}")
        if rows_to_skip:
            print(f"  Reused existing embeddings: {rows_to_skip:,}")
        if not stage5_ok:
            print("  Stage 5 incomplete; completed part shards preserved for future resume.")
    return stage5_ok


# ============================================================================
# Main entry point
# ============================================================================

if __name__ == "__main__":
    work = Path(WORK_DIR)
    work.mkdir(parents=True, exist_ok=True)

    # Derived artifact paths
    bronze_dir        = str(work / "bronze")
    silver_path       = str(work / "silver.parquet")
    silver_if_path    = str(work / "silver_with_if.parquet")
    gold_path         = str(work / "gold.parquet")            # final output 1
    embeddings_path   = str(Path(EMBEDDINGS_OUTPUT_PATH))     # final output 2
    Path(embeddings_path).parent.mkdir(parents=True, exist_ok=True)

    print("\n" + "#" * 60)
    print("  PubMed Data Processing Pipeline")
    print("#" * 60)
    print(f"  Work directory:   {work.resolve()}")
    print(f"  Embeddings path:  {Path(embeddings_path).resolve()}")
    print(f"  Stages to run:    {sorted(STAGES_TO_RUN)}")
    print(f"  XML directories:  {XML_DIRS}")
    print()

    # -- Stage 1 ----------------------------------------------------------
    if 1 in STAGES_TO_RUN:
        if not XML_DIRS:
            print("  [ERROR] Stage 1 requires XML_DIRS to be configured!")
            sys.exit(1)
        stage1_xml_to_bronze(XML_DIRS, bronze_dir, workers=WORKERS)

    # -- Stage 2 ----------------------------------------------------------
    if 2 in STAGES_TO_RUN:
        stage2_merge_to_silver(
            bronze_dir, silver_path, memory_limit=DUCKDB_MEMORY_LIMIT,
        )

    # -- Stage 3 ----------------------------------------------------------
    if 3 in STAGES_TO_RUN:
        if_json = IMPACT_FACTOR_JSON or str(work / "5year.json")
        stage3_map_impact_factor(silver_path, silver_if_path, if_json)

    # -- Stage 4 ----------------------------------------------------------
    if 4 in STAGES_TO_RUN:
        # If Stage 3 output is missing (skipped / IF file absent), use silver
        stage4_input = (
            silver_if_path if Path(silver_if_path).exists() else silver_path
        )
        json_index_caution_journals = _load_index_caution_journals(
            INDEX_CAUTION_JOURNALS_JSON
        )
        merged_index_caution_journals = tuple(
            sorted(set(INDEX_CAUTION_JOURNALS) | set(json_index_caution_journals))
        )
        stage4_filter_to_gold(
            stage4_input,
            gold_path,
            min_pub_date=MIN_PUB_DATE,
            max_pub_date=MAX_PUB_DATE,
            min_impact_factor=MIN_IMPACT_FACTOR,
            require_abstract=REQUIRE_ABSTRACT,
            languages=FILTER_LANGUAGES,
            allowed_quartiles=ALLOWED_QUARTILES,
            excluded_publication_types=EXCLUDED_PUBLICATION_TYPES,
            watchlist_journal_patterns=WATCHLIST_JOURNAL_PATTERNS,
            index_caution_journals=merged_index_caution_journals,
            debug_breakdown=STAGE4_DEBUG_BREAKDOWN,
        )

    # -- Stage 5 ----------------------------------------------------------
    if 5 in STAGES_TO_RUN:
        stage5_generate_embeddings(
            gold_path,
            embeddings_path,
            model_name=EMBEDDING_MODEL_NAME,
            batch_size=EMBEDDING_BATCH_SIZE,
        )

    # ======================================================================
    # Summary
    # ======================================================================
    print("\n" + "#" * 60)
    print("  All requested stages complete!")
    print("#" * 60)

    print("\n  Intermediate artifacts:")
    for label, fp in [
        ("Bronze directory",       bronze_dir),
        ("silver.parquet",         silver_path),
        ("silver_with_if.parquet", silver_if_path),
    ]:
        p = Path(fp)
        if p.exists():
            if p.is_dir():
                n = len(list(p.glob("*.parquet")))
                print(f"    [OK] {label}: {fp} ({n} files)")
            else:
                mb = p.stat().st_size / (1024 * 1024)
                print(f"    [OK] {label}: {fp} ({mb:.1f} MB)")
        else:
            print(f"    [--] {label}: (not generated)")

    print("\n  Final outputs (for pubmed_upload_to_postgres.py):")
    for label, fp in [
        ("gold.parquet",       gold_path),
        ("embeddings.parquet", embeddings_path),
    ]:
        p = Path(fp)
        if p.exists():
            mb = p.stat().st_size / (1024 * 1024)
            rows = pq.ParquetFile(fp).metadata.num_rows
            print(f"    [OK] {label}: {fp} ({mb:.1f} MB, {rows:,} rows)")
        else:
            print(f"    [--] {label}: (not generated)")

    if Path(gold_path).exists() and Path(embeddings_path).exists():
        print("\n  Next step -- import into PostgreSQL:")
        print("    python pubmed_upload_to_postgres.py")

    print()
