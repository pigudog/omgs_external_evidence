#!/usr/bin/env python3
"""Write SHA256 manifests for final derived evidence artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


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


def is_ignored_manifest_file(path: Path) -> bool:
    parts = set(path.parts)
    return (
        ".ipynb_checkpoints" in parts
        or "__pycache__" in parts
        or path.name == ".DS_Store"
        or path.suffix == ".pyc"
    )


def iter_artifact_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(p for p in path.rglob("*") if p.is_file() and not is_ignored_manifest_file(p))


def describe_artifact(path: Path, root: Path) -> dict[str, str]:
    filename = str(path.relative_to(root)) if path.is_relative_to(root) else str(path)
    return {
        "filename": filename,
        "sha256": sha256_file(path),
    }


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
    artifacts: list[dict[str, str]] = []
    missing: list[str] = []

    for name in artifact_names:
        path = Path(name)
        if not path.is_absolute():
            path = root / path
        if not path.exists():
            missing.append(str(path))
            continue
        for file_path in iter_artifact_files(path.resolve()):
            artifacts.append(describe_artifact(file_path, root))

    if missing and args.fail_on_missing:
        raise SystemExit("Missing artifacts:\n" + "\n".join(missing))

    output = args.output
    if output is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output = root / "data" / "manifests" / "artifacts" / f"artifact_manifest_{timestamp}.json"
    elif not output.is_absolute():
        output = root / output
    output.parent.mkdir(parents=True, exist_ok=True)

    output.write_text(json.dumps(artifacts, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote artifact manifest: {output}")
    print(f"Artifact count: {len(artifacts)}")
    if missing:
        print(f"Missing artifacts skipped: {len(missing)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
