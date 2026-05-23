#!/usr/bin/env python3
"""Download openFDA drug-label pages into local raw storage."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from tqdm import tqdm


API_URL = "https://api.fda.gov/drug/label.json"
DEFAULT_PAGE_SIZE = 100
DEFAULT_TIMEOUT_SECONDS = 60.0
SOURCE_NAME = "openfda_drug_label"


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def next_run_dir(output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    existing = sorted(output_dir.glob("run_*"))
    next_id = 1
    if existing:
        last = existing[-1].name.replace("run_", "")
        if last.isdigit():
            next_id = int(last) + 1
    run_dir = output_dir / f"run_{next_id:06d}"
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def parse_next_link(link_header: str | None) -> str | None:
    if not link_header:
        return None
    match = re.search(r'<([^>]+)>\s*;\s*rel="?next"?', link_header, flags=re.IGNORECASE)
    return match.group(1) if match else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download openFDA drug-label JSON pages.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=repo_root() / "data" / "raw" / "fda" / "openfda",
    )
    parser.add_argument(
        "--manifest-dir",
        type=Path,
        default=repo_root() / "data" / "manifests" / "fda",
    )
    parser.add_argument("--search", default=None)
    parser.add_argument("--sort", default="effective_time:asc")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--max-records", type=int, default=None)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--api-key-env", default="OPENFDA_API_KEY")
    return parser.parse_args()


def initial_params(args: argparse.Namespace, api_key: str | None) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": args.page_size, "sort": args.sort}
    if args.search:
        params["search"] = args.search
    if api_key:
        params["api_key"] = api_key
    return params


def main() -> int:
    args = parse_args()
    if args.page_size < 1 or args.page_size > 1000:
        raise SystemExit("--page-size must be between 1 and 1000 for openFDA.")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.manifest_dir.mkdir(parents=True, exist_ok=True)
    run_dir = next_run_dir(args.output_dir)
    run_id = run_dir.name.replace("run_", "")
    api_key = os.getenv(args.api_key_env)

    manifest: dict[str, Any] = {
        "source": SOURCE_NAME,
        "run_id": run_id,
        "started_at_utc": utc_now_iso(),
        "api_url": API_URL,
        "request": {
            "search": args.search,
            "sort": args.sort,
            "page_size": args.page_size,
            "max_pages": args.max_pages,
            "max_records": args.max_records,
        },
        "pages": [],
        "records_downloaded": 0,
        "total_available": None,
    }

    try:
        next_url: str | None = None
        params = initial_params(args, api_key)
        page_number = 0
        records_downloaded = 0
        progress = None

        with httpx.Client(timeout=args.timeout_seconds, follow_redirects=True) as client:
            while True:
                if args.max_pages is not None and page_number >= args.max_pages:
                    break
                if args.max_records is not None and records_downloaded >= args.max_records:
                    break

                response = client.get(next_url or API_URL, params=None if next_url else params)
                response.raise_for_status()
                payload = response.json()
                page_records = payload.get("results", [])
                total_available = payload.get("meta", {}).get("results", {}).get("total")
                if total_available is not None:
                    manifest["total_available"] = total_available
                    if progress is None:
                        progress = tqdm(total=total_available, desc="openfda", unit="record")

                page_number += 1
                page_path = run_dir / f"page_{page_number:06d}.json"
                raw_bytes = response.content
                page_path.write_bytes(raw_bytes)
                checksum = sha256_hex(raw_bytes)

                records_downloaded += len(page_records)
                if progress is not None:
                    progress.update(len(page_records))
                manifest["records_downloaded"] = records_downloaded
                manifest["pages"].append(
                    {
                        "page_number": page_number,
                        "path": str(page_path),
                        "record_count": len(page_records),
                        "sha256": checksum,
                        "next_url": parse_next_link(response.headers.get("Link")),
                    }
                )

                next_url = parse_next_link(response.headers.get("Link"))
                if not next_url or not page_records:
                    break
        if progress is not None:
            progress.close()

        manifest["finished_at_utc"] = utc_now_iso()
        manifest["status"] = "completed"
    except Exception as exc:
        manifest["finished_at_utc"] = utc_now_iso()
        manifest["status"] = "failed"
        manifest["error"] = str(exc)
        raise
    finally:
        manifest_path = args.manifest_dir / f"openfda_drug_label_run_{run_id}.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"Manifest: {manifest_path}")

    print(
        "Completed openFDA label download:",
        f"run_id={run_id}",
        f"pages={len(manifest['pages'])}",
        f"records={manifest['records_downloaded']}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
