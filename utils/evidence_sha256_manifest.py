#!/usr/bin/env python3
"""Write SHA256 manifests for final derived evidence artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


DEFAULT_ARTIFACTS = (
    "data/processed/pubmed_mainline/gold.parquet",
    "data/processed/fda/fda_effective_date_le_20251029.sqlite",
    "data/processed/conferences/ovarian_cancer_multiconference_2025_cutoff.json",
)


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def parquet_metadata(path: Path) -> dict[str, Any]:
    meta = pq.ParquetFile(path).metadata
    return {
        "format": "parquet",
        "row_count": meta.num_rows,
        "column_count": meta.num_columns,
        "schema": [field.name for field in pq.read_schema(path)],
    }


def sqlite_metadata(path: Path) -> dict[str, Any]:
    tables: dict[str, int] = {}
    with sqlite3.connect(path) as connection:
        rows = connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        ).fetchall()
        for (table_name,) in rows:
            count = connection.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
            tables[table_name] = int(count)
    return {"format": "sqlite", "tables": tables}


def json_metadata(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return {"format": "json", "record_count": len(data)}
    if isinstance(data, dict):
        return {"format": "json", "top_level_keys": sorted(data.keys())}
    return {"format": "json"}


def describe_artifact(path: Path, root: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    item: dict[str, Any] = {
        "path": str(path),
        "relative_path": str(path.relative_to(root)) if path.is_relative_to(root) else str(path),
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }
    if suffix == ".parquet":
        item.update(parquet_metadata(path))
    elif suffix in {".sqlite", ".db"}:
        item.update(sqlite_metadata(path))
    elif suffix == ".json":
        item.update(json_metadata(path))
    else:
        item["format"] = suffix.lstrip(".") or "unknown"
    return item


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute SHA256 checksums for final evidence artifacts.")
    parser.add_argument(
        "--artifact",
        action="append",
        default=None,
        help="Artifact path relative to repo root or absolute. Repeatable.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Manifest JSON path. Defaults to data/manifests/artifacts/artifact_manifest_<UTC>.json.",
    )
    parser.add_argument("--cutoff-date", default="2025-10-29")
    parser.add_argument("--fail-on-missing", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = repo_root()
    artifact_names = args.artifact or list(DEFAULT_ARTIFACTS)
    artifacts: list[dict[str, Any]] = []
    missing: list[str] = []

    for name in artifact_names:
        path = Path(name)
        if not path.is_absolute():
            path = root / path
        if not path.exists():
            missing.append(str(path))
            continue
        artifacts.append(describe_artifact(path.resolve(), root))

    if missing and args.fail_on_missing:
        raise SystemExit("Missing artifacts:\n" + "\n".join(missing))

    output = args.output
    if output is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output = root / "data" / "manifests" / "artifacts" / f"artifact_manifest_{timestamp}.json"
    elif not output.is_absolute():
        output = root / output
    output.parent.mkdir(parents=True, exist_ok=True)

    manifest = {
        "manifest_type": "omgs_external_evidence_final_artifacts",
        "created_at_utc": utc_now_iso(),
        "cutoff_date": args.cutoff_date,
        "artifacts": artifacts,
        "missing_artifacts": missing,
        "notes": "Checksums apply to final derived artifacts, not to raw source-download state.",
    }
    output.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote artifact manifest: {output}")
    print(f"Artifact count: {len(artifacts)}")
    if missing:
        print(f"Missing artifacts skipped: {len(missing)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
