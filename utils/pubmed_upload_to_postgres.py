#!/usr/bin/env python3
"""
Parquet -> PostgreSQL Uploader  (Step 2 of 2)
===============================================
Loads gold.parquet + embeddings.parquet (produced by pubmed_data_processing.py)
into a PostgreSQL ``papers`` table for the search API (app.py).

Workflow
--------
  1. Create pgvector / pg_trgm extensions
  2. Create the ``papers`` table (22 columns incl. vector & tsvector)
  3. Stream-read Parquets and batch-insert rows
  4. Build indexes (GIN full-text, HNSW vector, B-tree helpers)
  5. Verify that the data is queryable by app.py

Two import modes are supported:
  * **memory**     -- load all embeddings into a Python dict first (fast, uses RAM)
  * **temp-table** -- load embeddings into a PG temp table then JOIN (low RAM)

Dependencies
------------
  pip install psycopg2-binary pyarrow pandas numpy tqdm
"""

import os
import sys
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm

import psycopg2
from psycopg2.extras import execute_batch

# ============================================================================
# Configuration  --  edit these variables before running
# ============================================================================

# Paths to the two Parquet files produced by pubmed_data_processing.py
GOLD_PATH: str       = "../data/processed/pubmed_mainline/gold.parquet"
EMBEDDINGS_PATH: str = "../data/embeddings/qwen3_embedding_0_6b/embeddings.parquet"

# PostgreSQL connection (matches the defaults used by app.py / env vars)
DB_NAME:     str = os.getenv("RAG_DB_NAME",     "medical_rag")
DB_USER:     str = os.getenv("RAG_DB_USER",     "postgres")
DB_PASSWORD: str = os.getenv("RAG_DB_PASSWORD", "")
DB_HOST:     str = os.getenv("RAG_DB_HOST",     "127.0.0.1")
DB_PORT:     int = int(os.getenv("RAG_DB_PORT", "5432"))

# Import mode: "memory" or "temp-table"
IMPORT_MODE: str = "memory"

# Set to 0 to auto-detect from the Parquet file (recommended)
EMBEDDING_DIM: int = 0

# Tuning knobs
BATCH_SIZE: int = 1000    # rows per execute_batch call
CHUNK_SIZE: int = 50_000  # rows per Parquet read batch

# Control flags
DROP_EXISTING_TABLE: bool = True   # False to keep existing data (INSERT … ON CONFLICT DO NOTHING)
CREATE_INDEXES: bool      = True   # False to skip index creation
RUN_VERIFICATION: bool    = True   # False to skip the post-load sanity check


# ============================================================================
# Helpers
# ============================================================================

def convert_to_json_serializable(obj):
    """Recursively convert numpy / pyarrow types to native Python for JSON."""
    if obj is None:
        return None
    if isinstance(obj, np.ndarray):
        return [convert_to_json_serializable(x) for x in obj]
    if isinstance(obj, (np.integer, np.floating)):
        return obj.item()
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, (np.str_, np.bytes_)):
        return str(obj)
    if isinstance(obj, (list, tuple)):
        return [convert_to_json_serializable(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): convert_to_json_serializable(v) for k, v in obj.items()}
    if hasattr(obj, "tolist") and callable(obj.tolist):
        try:
            return convert_to_json_serializable(obj.tolist())
        except Exception:
            pass
    if isinstance(obj, (str, int, float, bool)):
        return obj
    try:
        return str(obj)
    except Exception:
        return None


def safe_json_dumps(obj, field_name: str = "unknown") -> str | None:
    """Safely serialise *obj* to a JSON string."""
    try:
        return json.dumps(convert_to_json_serializable(obj))
    except Exception as e:
        print(f"  [WARN] JSON conversion failed for field '{field_name}': {e}")
        return None


def join_text_parts(parts) -> str:
    """Join non-empty text fragments into one whitespace-normalized string."""
    cleaned = [str(part).strip() for part in parts if part is not None and str(part).strip()]
    return " ".join(cleaned)


def extract_mesh_text(mesh_value) -> str:
    """Flatten mesh descriptors and qualifiers into a search-friendly string."""
    if not is_valid_value(mesh_value):
        return ""

    chunks: list[str] = []
    for item in mesh_value:
        if isinstance(item, dict):
            chunks.append(item.get("descriptor", ""))
            qualifiers = item.get("qualifiers")
            if isinstance(qualifiers, list):
                chunks.extend(str(q) for q in qualifiers if q)
        else:
            chunks.append(str(item))
    return join_text_parts(chunks)


def extract_keywords_text(keywords_value) -> str:
    """Flatten keyword arrays into a search-friendly string."""
    if not is_valid_value(keywords_value):
        return ""
    if isinstance(keywords_value, (list, tuple, np.ndarray)):
        return join_text_parts(keywords_value)
    return join_text_parts([keywords_value])


def is_valid_value(val) -> bool:
    """Return True if *val* is non-None, non-NaN, and non-empty."""
    if val is None:
        return False
    if isinstance(val, float) and pd.isna(val):
        return False
    if isinstance(val, np.ndarray):
        return val.size > 0
    if isinstance(val, (list, tuple, str)):
        return len(val) > 0
    return True


# ============================================================================
# Database operations
# ============================================================================

def create_extensions_and_table(conn, embedding_dim: int, drop_existing: bool = True):
    """Create pgvector / pg_trgm extensions and the ``papers`` table."""
    with conn.cursor() as cur:
        print("  Creating PostgreSQL extensions ...")
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

        if drop_existing:
            print("  Dropping existing table (if any) ...")
            cur.execute("DROP TABLE IF EXISTS papers CASCADE;")

        print(f"  Creating papers table (embedding dim = {embedding_dim}) ...")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS papers (
                pmid            TEXT PRIMARY KEY,
                version         TEXT,
                title           TEXT NOT NULL,
                journal_iso     TEXT,
                issn            TEXT,
                eissn           TEXT,
                journal_name_canonical TEXT,
                abstract        TEXT,
                pub_date        TEXT,
                date_revised    TEXT,
                doi             TEXT,
                pmc             TEXT,
                pii             TEXT,
                authors         JSONB,
                mesh            JSONB,
                pub_types       JSONB,
                keywords        JSONB,
                language        TEXT,
                country         TEXT,
                source_file     TEXT,
                jcr_year        TEXT,
                impact_factor   FLOAT,
                quartile        TEXT,
                primary_category TEXT,
                primary_category_quartile TEXT,
                text_search_vec tsvector,
                text_search_vec_weighted tsvector,
                embedding       vector({embedding_dim}),
                created_at      TIMESTAMP DEFAULT NOW()
            );
        """)
        cur.execute(
            "ALTER TABLE papers "
            "ADD COLUMN IF NOT EXISTS text_search_vec_weighted tsvector;"
        )
        conn.commit()
        print("  Table created successfully!")


def build_indexes(conn):
    """Create GIN full-text, HNSW vector, and B-tree auxiliary indexes."""
    with conn.cursor() as cur:
        print("  Creating GIN full-text index ...")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_papers_text_search "
            "ON papers USING GIN(text_search_vec);"
        )
        conn.commit()

        print("  Creating GIN weighted full-text index ...")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_papers_text_search_weighted "
            "ON papers USING GIN(text_search_vec_weighted);"
        )
        conn.commit()

        print("  Creating HNSW vector index (m=16, ef_construction=64) ...")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_papers_embedding
            ON papers USING hnsw(embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = 64);
        """)
        conn.commit()

        print("  Creating auxiliary B-tree indexes ...")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_papers_impact_factor "
            "ON papers(impact_factor DESC NULLS LAST);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_papers_pub_date ON papers(pub_date);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_papers_doi "
            "ON papers(doi) WHERE doi IS NOT NULL;"
        )
        conn.commit()
        print("  All indexes created!")


def print_statistics(conn):
    """Print basic row counts from the papers table."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM papers;")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM papers WHERE embedding IS NOT NULL;")
        with_emb = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM papers "
            "WHERE abstract IS NOT NULL AND abstract <> '';"
        )
        with_abs = cur.fetchone()[0]

    print(f"\n  Statistics:")
    print(f"    Total papers:         {total:,}")
    print(f"    With embedding:       {with_emb:,}")
    print(f"    With abstract:        {with_abs:,}")


# ============================================================================
# Import mode A  --  memory  (embeddings loaded into dict)
# ============================================================================

def _load_embeddings_to_dict(embeddings_path: str,
                             chunk_size: int) -> dict[str, list]:
    """Stream-read the embeddings Parquet into a {pmid: list[float]} dict."""
    emb_file = pq.ParquetFile(embeddings_path)
    total = emb_file.metadata.num_rows
    print(f"  Embeddings total rows: {total:,}")

    emb_dict: dict[str, list] = {}
    with tqdm(total=total, desc="  Loading embeddings", unit=" rows") as pbar:
        for batch in emb_file.iter_batches(batch_size=chunk_size):
            df = batch.to_pandas()
            df["pmid"] = df["pmid"].astype(str)
            for _, row in df.iterrows():
                emb = row["emb"]
                emb_dict[row["pmid"]] = emb if isinstance(emb, list) else emb.tolist()
            pbar.update(len(df))
            del df

    print(f"  Loaded {len(emb_dict):,} vectors into memory")
    return emb_dict


def insert_papers_memory_mode(conn, gold_path: str, embeddings_path: str,
                              batch_size: int, chunk_size: int):
    """Memory mode: load embeddings dict, then stream gold rows into PG."""
    print("\n  [Memory mode] Starting import ...")

    gold_file = pq.ParquetFile(gold_path)
    gold_total = gold_file.metadata.num_rows
    print(f"  Gold total rows: {gold_total:,}")

    # Step 1 -- load embeddings
    print("\n  Step 1/2: Load embeddings into memory ...")
    emb_dict = _load_embeddings_to_dict(embeddings_path, chunk_size)

    # Step 2 -- stream gold and insert
    print("\n  Step 2/2: Insert paper data ...")
    insert_sql = """
        INSERT INTO papers (
            pmid, version, title, journal_iso, issn, eissn, journal_name_canonical, abstract,
            pub_date, date_revised, doi, pmc, pii, authors, mesh,
            pub_types, keywords, language, country, source_file,
            jcr_year, impact_factor, quartile, primary_category, primary_category_quartile,
            text_search_vec, text_search_vec_weighted, embedding
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            to_tsvector('english', COALESCE(%s, '') || ' ' || COALESCE(%s, '')),
            setweight(to_tsvector('english', COALESCE(%s, '')), 'A') ||
            setweight(to_tsvector('english', COALESCE(%s, '')), 'B') ||
            setweight(to_tsvector('english', COALESCE(%s, '')), 'B') ||
            setweight(to_tsvector('english', COALESCE(%s, '')), 'C'),
            %s
        ) ON CONFLICT (pmid) DO NOTHING;
    """

    total_inserted = 0
    total_skipped  = 0

    with conn.cursor() as cur:
        rows_buf: list[tuple] = []

        with tqdm(total=gold_total, desc="  Inserting", unit=" rows") as pbar:
            for batch in gold_file.iter_batches(batch_size=chunk_size):
                df = batch.to_pandas()
                df["pmid"] = df["pmid"].astype(str)

                for _, row in df.iterrows():
                    pmid = str(row["pmid"])

                    if pmid not in emb_dict:
                        total_skipped += 1
                        pbar.update(1)
                        continue

                    title    = row["title"]    if pd.notna(row.get("title"))    else ""
                    abstract = row["abstract"] if pd.notna(row.get("abstract")) else ""
                    embedding = emb_dict[pmid]
                    mesh_text = extract_mesh_text(row.get("mesh"))
                    keywords_text = extract_keywords_text(row.get("keywords"))

                    authors  = safe_json_dumps(row["authors"],  "authors")  if is_valid_value(row.get("authors"))  else None
                    mesh     = safe_json_dumps(row["mesh"],     "mesh")     if is_valid_value(row.get("mesh"))     else None
                    pub_types = safe_json_dumps(row["pub_types"], "pub_types") if is_valid_value(row.get("pub_types")) else None
                    keywords = safe_json_dumps(row["keywords"], "keywords") if is_valid_value(row.get("keywords")) else None

                    rows_buf.append((
                        pmid,
                        row["version"]      if pd.notna(row.get("version"))      else None,
                        title,
                        row["journal_iso"]  if pd.notna(row.get("journal_iso"))  else None,
                        row["issn"]         if pd.notna(row.get("issn"))         else None,
                        row["eissn"]        if pd.notna(row.get("eissn"))        else None,
                        row["journal_name_canonical"] if pd.notna(row.get("journal_name_canonical")) else None,
                        abstract,
                        row["pub_date"]     if pd.notna(row.get("pub_date"))     else None,
                        row["date_revised"] if pd.notna(row.get("date_revised")) else None,
                        row["doi"]          if pd.notna(row.get("doi"))          else None,
                        row["pmc"]          if pd.notna(row.get("pmc"))          else None,
                        row["pii"]          if pd.notna(row.get("pii"))          else None,
                        authors,
                        mesh,
                        pub_types,
                        keywords,
                        row["language"]     if pd.notna(row.get("language"))     else None,
                        row["country"]      if pd.notna(row.get("country"))      else None,
                        row["source_file"]  if pd.notna(row.get("source_file"))  else None,
                        row["jcr_year"]     if pd.notna(row.get("jcr_year"))     else None,
                        float(row["impact_factor"]) if pd.notna(row.get("impact_factor")) else None,
                        row["quartile"]     if pd.notna(row.get("quartile"))     else None,
                        row["primary_category"] if pd.notna(row.get("primary_category")) else None,
                        row["primary_category_quartile"] if pd.notna(row.get("primary_category_quartile")) else None,
                        title,     # for tsvector
                        abstract,  # for tsvector
                        title,         # weighted title
                        keywords_text, # weighted keywords
                        mesh_text,     # weighted mesh
                        abstract,      # weighted abstract
                        embedding,
                    ))

                    if len(rows_buf) >= batch_size:
                        execute_batch(cur, insert_sql, rows_buf, page_size=batch_size)
                        conn.commit()
                        total_inserted += len(rows_buf)
                        rows_buf = []

                    pbar.update(1)

                del df

        if rows_buf:
            execute_batch(cur, insert_sql, rows_buf, page_size=len(rows_buf))
            conn.commit()
            total_inserted += len(rows_buf)

    print(f"\n  Insert finished: {total_inserted:,} inserted, "
          f"{total_skipped:,} skipped (no embedding)")


# ============================================================================
# Import mode B  --  temp-table  (low memory, big embeddings file)
# ============================================================================

def insert_papers_temp_table_mode(conn, gold_path: str, embeddings_path: str,
                                  batch_size: int, chunk_size: int,
                                  embedding_dim: int = 1024):
    """Temp-table mode: load embeddings into a PG temp table, then JOIN."""
    print("\n  [Temp-table mode] Starting import ...")

    gold_file = pq.ParquetFile(gold_path)
    emb_file  = pq.ParquetFile(embeddings_path)
    gold_total = gold_file.metadata.num_rows
    emb_total  = emb_file.metadata.num_rows

    print(f"  Gold total rows:       {gold_total:,}")
    print(f"  Embeddings total rows: {emb_total:,}")

    with conn.cursor() as cur:
        # Step 1 -- create temp table and bulk-load embeddings
        print("\n  Step 1/3: Create temp table & load embeddings ...")
        cur.execute("DROP TABLE IF EXISTS temp_embeddings;")
        cur.execute(f"""
            CREATE TEMP TABLE temp_embeddings (
                pmid TEXT PRIMARY KEY,
                emb  vector({embedding_dim})
            );
        """)
        conn.commit()

        emb_sql = (
            "INSERT INTO temp_embeddings (pmid, emb) "
            "VALUES (%s, %s) ON CONFLICT (pmid) DO NOTHING;"
        )
        with tqdm(total=emb_total, desc="  Loading embeddings", unit=" rows") as pbar:
            buf: list[tuple] = []
            for batch in emb_file.iter_batches(batch_size=chunk_size):
                df = batch.to_pandas()
                df["pmid"] = df["pmid"].astype(str)
                for _, row in df.iterrows():
                    emb = row["emb"] if isinstance(row["emb"], list) else row["emb"].tolist()
                    buf.append((str(row["pmid"]), emb))
                    if len(buf) >= batch_size:
                        execute_batch(cur, emb_sql, buf, page_size=batch_size)
                        conn.commit()
                        buf = []
                pbar.update(len(df))
                del df
            if buf:
                execute_batch(cur, emb_sql, buf, page_size=len(buf))
                conn.commit()

        # Step 2 -- ANALYZE
        print("\n  Step 2/3: Analyse temp table ...")
        cur.execute("ANALYZE temp_embeddings;")
        conn.commit()

        # Step 3 -- insert papers with embedding sub-select
        print("\n  Step 3/3: Insert paper data ...")
        insert_sql = """
            INSERT INTO papers (
                pmid, version, title, journal_iso, issn, eissn, journal_name_canonical, abstract,
                pub_date, date_revised, doi, pmc, pii, authors, mesh,
                pub_types, keywords, language, country, source_file,
                jcr_year, impact_factor, quartile, primary_category, primary_category_quartile,
                text_search_vec, text_search_vec_weighted, embedding
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                to_tsvector('english', COALESCE(%s, '') || ' ' || COALESCE(%s, '')),
                setweight(to_tsvector('english', COALESCE(%s, '')), 'A') ||
                setweight(to_tsvector('english', COALESCE(%s, '')), 'B') ||
                setweight(to_tsvector('english', COALESCE(%s, '')), 'B') ||
                setweight(to_tsvector('english', COALESCE(%s, '')), 'C'),
                (SELECT emb FROM temp_embeddings WHERE pmid = %s)
            ) ON CONFLICT (pmid) DO NOTHING;
        """

        total_inserted = 0
        with tqdm(total=gold_total, desc="  Inserting", unit=" rows") as pbar:
            rows_buf: list[tuple] = []
            for batch in gold_file.iter_batches(batch_size=chunk_size):
                df = batch.to_pandas()
                df["pmid"] = df["pmid"].astype(str)

                for _, row in df.iterrows():
                    pmid     = str(row["pmid"])
                    title    = row["title"]    if pd.notna(row.get("title"))    else ""
                    abstract = row["abstract"] if pd.notna(row.get("abstract")) else ""
                    mesh_text = extract_mesh_text(row.get("mesh"))
                    keywords_text = extract_keywords_text(row.get("keywords"))

                    authors   = safe_json_dumps(row["authors"],   "authors")   if is_valid_value(row.get("authors"))   else None
                    mesh      = safe_json_dumps(row["mesh"],      "mesh")      if is_valid_value(row.get("mesh"))      else None
                    pub_types = safe_json_dumps(row["pub_types"], "pub_types") if is_valid_value(row.get("pub_types")) else None
                    keywords  = safe_json_dumps(row["keywords"],  "keywords")  if is_valid_value(row.get("keywords"))  else None

                    rows_buf.append((
                        pmid,
                        row["version"]      if pd.notna(row.get("version"))      else None,
                        title,
                        row["journal_iso"]  if pd.notna(row.get("journal_iso"))  else None,
                        row["issn"]         if pd.notna(row.get("issn"))         else None,
                        row["eissn"]        if pd.notna(row.get("eissn"))        else None,
                        row["journal_name_canonical"] if pd.notna(row.get("journal_name_canonical")) else None,
                        abstract,
                        row["pub_date"]     if pd.notna(row.get("pub_date"))     else None,
                        row["date_revised"] if pd.notna(row.get("date_revised")) else None,
                        row["doi"]          if pd.notna(row.get("doi"))          else None,
                        row["pmc"]          if pd.notna(row.get("pmc"))          else None,
                        row["pii"]          if pd.notna(row.get("pii"))          else None,
                        authors,
                        mesh,
                        pub_types,
                        keywords,
                        row["language"]     if pd.notna(row.get("language"))     else None,
                        row["country"]      if pd.notna(row.get("country"))      else None,
                        row["source_file"]  if pd.notna(row.get("source_file"))  else None,
                        row["jcr_year"]     if pd.notna(row.get("jcr_year"))     else None,
                        float(row["impact_factor"]) if pd.notna(row.get("impact_factor")) else None,
                        row["quartile"]     if pd.notna(row.get("quartile"))     else None,
                        row["primary_category"] if pd.notna(row.get("primary_category")) else None,
                        row["primary_category_quartile"] if pd.notna(row.get("primary_category_quartile")) else None,
                        title,     # for tsvector
                        abstract,  # for tsvector
                        title,         # weighted title
                        keywords_text, # weighted keywords
                        mesh_text,     # weighted mesh
                        abstract,      # weighted abstract
                        pmid,      # for embedding sub-select
                    ))

                    if len(rows_buf) >= batch_size:
                        execute_batch(cur, insert_sql, rows_buf, page_size=batch_size)
                        conn.commit()
                        total_inserted += len(rows_buf)
                        rows_buf = []

                pbar.update(len(df))
                del df

            if rows_buf:
                execute_batch(cur, insert_sql, rows_buf, page_size=len(rows_buf))
                conn.commit()
                total_inserted += len(rows_buf)

    print(f"\n  Insert finished: {total_inserted:,} rows")


# ============================================================================
# Post-load verification  (mirrors the queries app.py will run)
# ============================================================================

def verify_for_app(conn):
    """Verify that the papers table is usable by app.py."""
    with conn.cursor() as cur:
        # 1. Searchable paper count
        cur.execute(
            "SELECT COUNT(*) FROM papers "
            "WHERE embedding IS NOT NULL AND abstract IS NOT NULL AND abstract <> '';"
        )
        searchable = cur.fetchone()[0]
        print(f"    Searchable papers: {searchable:,}")

        if searchable == 0:
            print("    [WARN] No searchable papers (embedding + abstract both non-empty)!")
            return

        # 2. Check indexes
        cur.execute("""
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'papers' AND indexname IN (
                'idx_papers_text_search', 'idx_papers_embedding',
                'idx_papers_text_search_weighted', 'idx_papers_impact_factor',
                'idx_papers_pub_date', 'idx_papers_doi'
            );
        """)
        indexes = [r[0] for r in cur.fetchall()]
        print(f"    Indexes present: {', '.join(indexes) if indexes else '(none)'}")

        # 3. Smoke-test vector query (self-similarity = 1.0)
        cur.execute("""
            SELECT pmid, title, 1 - (embedding <=> embedding) AS similarity
            FROM papers
            WHERE embedding IS NOT NULL AND abstract IS NOT NULL AND abstract <> ''
            LIMIT 1;
        """)
        row = cur.fetchone()
        if row:
            print(f"    Smoke test OK: pmid={row[0]}, self-similarity={row[2]:.4f}")
        else:
            print("    [WARN] Smoke test returned no rows!")

        # 4. Verify hnsw.ef_search (used by app.py)
        try:
            cur.execute("SET hnsw.ef_search = 1000;")
            cur.execute("SHOW hnsw.ef_search;")
            ef = cur.fetchone()[0]
            print(f"    hnsw.ef_search = {ef} (OK)")
        except Exception:
            print("    [WARN] Could not set hnsw.ef_search -- check pgvector version")

    print("    Verification passed!")


# ============================================================================
# Main entry point
# ============================================================================

if __name__ == "__main__":
    # Validate input files
    for label, fp in [("gold", GOLD_PATH), ("embeddings", EMBEDDINGS_PATH)]:
        if not Path(fp).exists():
            print(f"[ERROR] {label} file not found: {fp}")
            sys.exit(1)

    # Auto-detect embedding dimension
    embedding_dim = EMBEDDING_DIM
    if embedding_dim == 0:
        print("  Auto-detecting embedding dimension ...")
        emb_file = pq.ParquetFile(EMBEDDINGS_PATH)
        first_batch = next(emb_file.iter_batches(batch_size=1))
        first_emb = first_batch.column("emb")[0].as_py()
        embedding_dim = len(first_emb)
        print(f"  Detected embedding dimension: {embedding_dim}")

    db_config = {
        "dbname":   DB_NAME,
        "user":     DB_USER,
        "password": DB_PASSWORD,
        "host":     DB_HOST,
        "port":     DB_PORT,
    }

    print("\n" + "=" * 60)
    print("  Parquet -> PostgreSQL Uploader")
    print("=" * 60)
    print(f"  Database:     {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"  Gold:         {GOLD_PATH}")
    print(f"  Embeddings:   {EMBEDDINGS_PATH}")
    print(f"  Import mode:  {IMPORT_MODE}")
    print(f"  Vector dim:   {embedding_dim}")
    print()

    t0 = time.time()

    print("  Connecting to database ...")
    conn = psycopg2.connect(**db_config)

    try:
        # Step 1 -- create table
        print("\n  [Step 1] Create extensions & table ...")
        create_extensions_and_table(
            conn,
            embedding_dim=embedding_dim,
            drop_existing=DROP_EXISTING_TABLE,
        )

        # Step 2 -- import data
        print("\n  [Step 2] Import data ...")
        if IMPORT_MODE == "memory":
            insert_papers_memory_mode(
                conn, GOLD_PATH, EMBEDDINGS_PATH,
                batch_size=BATCH_SIZE, chunk_size=CHUNK_SIZE,
            )
        else:
            insert_papers_temp_table_mode(
                conn, GOLD_PATH, EMBEDDINGS_PATH,
                batch_size=BATCH_SIZE, chunk_size=CHUNK_SIZE,
                embedding_dim=embedding_dim,
            )

        # Step 3 -- indexes
        if CREATE_INDEXES:
            print("\n  [Step 3] Build indexes ...")
            build_indexes(conn)
        else:
            print("\n  [Step 3] Skipping index creation")

        # Statistics
        print_statistics(conn)

        # Step 4 -- verification
        if RUN_VERIFICATION:
            print("\n  [Step 4] Verify database readiness ...")
            verify_for_app(conn)
        else:
            print("\n  [Step 4] Skipping verification")

        elapsed = time.time() - t0
        print(f"\n{'=' * 60}")
        print(f"  All done!  Total time: {elapsed:.1f}s")
        print(f"{'=' * 60}")
        print(f"\n  You can now start the search service:")
        print(f"    python app.py")
        print(f"    # API:  http://localhost:8198/api/search-paper")
        print(f"    # Docs: http://localhost:8198/docs\n")

    except Exception as e:
        print(f"\n  [ERROR] {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        raise
    finally:
        conn.close()
