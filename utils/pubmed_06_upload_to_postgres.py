from __future__ import annotations

import time
from pathlib import Path
import sys

import pyarrow.parquet as pq
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pubmed_upload_to_postgres as up
from pubmed_stage_runner_utils import resolve_from_prepare


def main() -> None:
    gold_path = resolve_from_prepare(up.GOLD_PATH)
    embeddings_path = resolve_from_prepare(up.EMBEDDINGS_PATH)

    for label, fp in [("gold", gold_path), ("embeddings", embeddings_path)]:
        if not fp.exists():
            raise SystemExit(f"{label} file not found: {fp}")

    embedding_dim = up.EMBEDDING_DIM
    if embedding_dim == 0:
        emb_file = pq.ParquetFile(str(embeddings_path))
        first_batch = next(emb_file.iter_batches(batch_size=1))
        first_emb = first_batch.column("emb")[0].as_py()
        embedding_dim = len(first_emb)

    db_config = {
        "dbname": up.DB_NAME,
        "user": up.DB_USER,
        "password": up.DB_PASSWORD,
        "host": up.DB_HOST,
        "port": up.DB_PORT,
    }

    t0 = time.time()
    conn = psycopg2.connect(**db_config)
    try:
        up.create_extensions_and_table(
            conn,
            embedding_dim=embedding_dim,
            drop_existing=up.DROP_EXISTING_TABLE,
        )
        if up.IMPORT_MODE == "memory":
            up.insert_papers_memory_mode(
                conn,
                str(gold_path),
                str(embeddings_path),
                batch_size=up.BATCH_SIZE,
                chunk_size=up.CHUNK_SIZE,
            )
        else:
            up.insert_papers_temp_table_mode(
                conn,
                str(gold_path),
                str(embeddings_path),
                batch_size=up.BATCH_SIZE,
                chunk_size=up.CHUNK_SIZE,
                embedding_dim=embedding_dim,
            )
        if up.CREATE_INDEXES:
            up.build_indexes(conn)
        up.print_statistics(conn)
        if up.RUN_VERIFICATION:
            up.verify_for_app(conn)
        elapsed = time.time() - t0
        print(f"Stage 6 upload complete in {elapsed:.1f}s")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
