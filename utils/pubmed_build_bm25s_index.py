from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

import bm25s
import psycopg2
import psycopg2.extras
from tqdm import tqdm


BM25S_METHOD = "lucene"
BM25S_K1 = 1.5
BM25S_B = 0.75
BM25S_STOPWORDS = "english"

FIELD_REPEAT_WEIGHTS: dict[str, int] = {
    "title": 4,
    "keywords": 3,
    "mesh": 3,
    "abstract": 1,
}


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the PubMed bm25s index from the PostgreSQL papers table.")
    parser.add_argument(
        "--output-dir",
        default=str(repo_root() / "data" / "bm25s_pubmed"),
        help="Directory for the saved bm25s index.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional row limit for smoke builds; 0 means full corpus.",
    )
    parser.add_argument(
        "--fetch-size",
        type=int,
        default=5000,
        help="Server-side cursor fetch size.",
    )
    return parser.parse_args()


def db_config() -> dict[str, Any]:
    return {
        "dbname": os.getenv("RAG_DB_NAME", "medical_rag"),
        "user": os.getenv("RAG_DB_USER", "postgres"),
        "password": os.getenv("RAG_DB_PASSWORD", ""),
        "host": os.getenv("RAG_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("RAG_DB_PORT", "5432")),
    }


def _join_text_parts(parts: list[str]) -> str:
    return " ".join(str(part).strip() for part in parts if part and str(part).strip())


def _flatten_keywords(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, list):
        return _join_text_parts([str(item) for item in value])
    return str(value).strip()


def _flatten_mesh(value: Any) -> str:
    if not value:
        return ""
    chunks: list[str] = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                chunks.append(str(item.get("descriptor", "")).strip())
                qualifiers = item.get("qualifiers")
                if isinstance(qualifiers, list):
                    chunks.extend(str(q).strip() for q in qualifiers if q)
            else:
                chunks.append(str(item).strip())
    else:
        chunks.append(str(value).strip())
    return _join_text_parts(chunks)


def _repeat_text(text: str, times: int) -> str:
    text = str(text or "").strip()
    if not text or times <= 0:
        return ""
    return " ".join([text] * times)


def build_weighted_doc_text(row: dict[str, Any]) -> str:
    return _join_text_parts(
        [
            _repeat_text(str(row.get("title") or ""), FIELD_REPEAT_WEIGHTS["title"]),
            _repeat_text(_flatten_keywords(row.get("keywords")), FIELD_REPEAT_WEIGHTS["keywords"]),
            _repeat_text(_flatten_mesh(row.get("mesh")), FIELD_REPEAT_WEIGHTS["mesh"]),
            _repeat_text(str(row.get("abstract") or ""), FIELD_REPEAT_WEIGHTS["abstract"]),
        ]
    )


def fetch_total_rows(conn: psycopg2.extensions.connection, limit: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM papers")
        total = int(cur.fetchone()[0])
    return min(total, limit) if limit > 0 else total


def fetch_corpus(
    conn: psycopg2.extensions.connection,
    total_rows: int,
    limit: int,
    fetch_size: int,
) -> tuple[list[str], list[dict[str, Any]]]:
    texts: list[str] = []
    corpus_meta: list[dict[str, Any]] = []
    sql = """
        SELECT
            pmid,
            title,
            journal_iso,
            pub_date,
            doi,
            impact_factor,
            quartile,
            keywords,
            mesh,
            abstract
        FROM papers
        ORDER BY pmid
    """
    if limit > 0:
        sql += f"\nLIMIT {int(limit)}"

    with conn.cursor(name="bm25s_build_cursor", cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.itersize = fetch_size
        cur.execute(sql)
        progress = tqdm(total=total_rows, desc="Fetch papers", unit="rows")
        for row in cur:
            row_dict = dict(row)
            texts.append(build_weighted_doc_text(row_dict))
            corpus_meta.append(
                {
                    "pmid": str(row_dict["pmid"]),
                    "title": row_dict.get("title"),
                    "journal": row_dict.get("journal_iso"),
                    "pub_date": str(row_dict.get("pub_date") or ""),
                    "doi": row_dict.get("doi"),
                    "impact_factor": row_dict.get("impact_factor"),
                    "quartile": row_dict.get("quartile"),
                }
            )
            progress.update(1)
        progress.close()
    return texts, corpus_meta


def write_manifest(output_dir: Path, total_rows: int, elapsed_seconds: float) -> None:
    manifest = {
        "source_table": "papers",
        "num_documents": total_rows,
        "bm25_method": BM25S_METHOD,
        "k1": BM25S_K1,
        "b": BM25S_B,
        "stopwords": BM25S_STOPWORDS,
        "field_repeat_weights": FIELD_REPEAT_WEIGHTS,
        "elapsed_seconds": round(elapsed_seconds, 3),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Build PubMed bm25s index")
    print(f"  Output dir: {output_dir}")
    print(f"  Method: {BM25S_METHOD}, k1={BM25S_K1}, b={BM25S_B}")
    if args.limit > 0:
        print(f"  Row limit: {args.limit}")

    started = time.time()
    conn = psycopg2.connect(**db_config())
    try:
        total_rows = fetch_total_rows(conn, limit=args.limit)
        texts, corpus_meta = fetch_corpus(conn, total_rows=total_rows, limit=args.limit, fetch_size=args.fetch_size)
    finally:
        conn.close()

    corpus_tokens = bm25s.tokenize(texts, stopwords=BM25S_STOPWORDS, show_progress=True)
    retriever = bm25s.BM25(method=BM25S_METHOD, k1=BM25S_K1, b=BM25S_B)
    retriever.index(corpus_tokens, show_progress=True)
    retriever.save(output_dir, corpus=corpus_meta)

    elapsed = time.time() - started
    write_manifest(output_dir, total_rows=total_rows, elapsed_seconds=elapsed)
    print(f"Done: {total_rows} docs in {elapsed:.1f}s -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
