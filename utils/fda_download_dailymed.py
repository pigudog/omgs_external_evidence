#!/usr/bin/env python3
"""Download DailyMed SPL archives into local raw storage."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from tqdm import tqdm


DAILYMED_RESOURCE_URL = "https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm"
DEFAULT_TIMEOUT_SECONDS = 120.0
SOURCE_NAME = "dailymed_spl"
FULL_RELEASE_HUMAN_RX_PATTERN = re.compile(
    r'https://[^"\']*dm_spl_release_human_rx_part\d+\.zip',
    flags=re.IGNORECASE,
)
MONTHLY_UPDATE_PATTERN = re.compile(
    r'https://[^"\']*dm_spl_monthly_update[^"\']*\.zip',
    flags=re.IGNORECASE,
)


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


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


def extract_labeled_value(block: str, label: str) -> str | None:
    match = re.search(
        rf"<strong>\s*{re.escape(label)}:\s*</strong>\s*([^<]+)",
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return match.group(1).strip() if match else None


def parse_full_release_human_rx_parts(html: str) -> list[dict[str, Any]]:
    parts: list[dict[str, Any]] = []
    block_pattern = re.compile(
        r'<li[^>]*data-ddfilter="human prescription labels"[^>]*>(.*?)</li>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    for block_match in block_pattern.finditer(html):
        block = block_match.group(1)
        url_match = FULL_RELEASE_HUMAN_RX_PATTERN.search(block)
        if not url_match:
            continue
        url = url_match.group(0)
        filename = Path(httpx.URL(url).path).name
        match = re.search(r"part(\d+)\.zip$", filename, flags=re.IGNORECASE)
        if not match:
            continue
        parts.append(
            {
                "part_number": int(match.group(1)),
                "filename": filename,
                "url": url,
                "number_of_files": extract_labeled_value(block, "Number of files"),
                "file_size": extract_labeled_value(block, "File size"),
                "md5_checksum": extract_labeled_value(block, "MD5 checksum"),
                "last_modified": extract_labeled_value(block, "Last Modified"),
            }
        )
    return sorted(parts, key=lambda item: item["part_number"])


def resolve_download_targets(client: httpx.Client, args: argparse.Namespace) -> list[dict[str, Any]]:
    if args.strategy == "url":
        if not args.url:
            raise ValueError("--url is required when --strategy url is used.")
        return [{"url": args.url, "filename": Path(httpx.URL(args.url).path).name}]

    response = client.get(DAILYMED_RESOURCE_URL)
    response.raise_for_status()
    html = response.text

    if args.strategy == "latest-monthly-update":
        match = MONTHLY_UPDATE_PATTERN.search(html)
        if not match:
            raise RuntimeError(f"Could not find DailyMed monthly update URL on {DAILYMED_RESOURCE_URL}")
        url = match.group(0)
        return [{"url": url, "filename": Path(httpx.URL(url).path).name}]

    parts = parse_full_release_human_rx_parts(html)
    if not parts:
        raise RuntimeError(f"Could not find DailyMed full human prescription release URLs on {DAILYMED_RESOURCE_URL}")
    if args.part is not None:
        parts = [part for part in parts if part["part_number"] == args.part]
        if not parts:
            raise RuntimeError(f"Could not find DailyMed human_rx part{args.part}.")
    elif not args.all_parts:
        parts = parts[:1]
    return parts


def filename_from_headers_or_url(response: httpx.Response, url: str) -> str:
    content_disposition = response.headers.get("content-disposition", "")
    match = re.search(r'filename="?([^";]+)"?', content_disposition, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return Path(httpx.URL(url).path).name or "dailymed_download.zip"


def download_file(client: httpx.Client, url: str, destination: Path) -> dict[str, Any]:
    with client.stream("GET", url, follow_redirects=True) as response:
        response.raise_for_status()
        filename = filename_from_headers_or_url(response, url)
        file_path = destination / filename
        total_bytes = int(response.headers.get("content-length", "0") or 0)
        print(f"Downloading DailyMed archive: {filename}")
        with file_path.open("wb") as handle, tqdm(
            total=total_bytes if total_bytes > 0 else None,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc="download",
        ) as progress:
            for chunk in response.iter_bytes():
                if chunk:
                    handle.write(chunk)
                    progress.update(len(chunk))

    return {
        "url": url,
        "path": str(file_path),
        "size_bytes": file_path.stat().st_size,
        "sha256": sha256_file(file_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download DailyMed SPL zip archives.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=repo_root() / "data" / "raw" / "fda" / "dailymed",
    )
    parser.add_argument(
        "--manifest-dir",
        type=Path,
        default=repo_root() / "data" / "manifests" / "fda",
    )
    parser.add_argument(
        "--strategy",
        choices=["latest-monthly-update", "full-release-human-rx", "url"],
        default="latest-monthly-update",
    )
    parser.add_argument("--url", default=None)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--part", type=int, default=None)
    parser.add_argument("--all-parts", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.strategy != "full-release-human-rx" and (args.part or args.all_parts):
        raise SystemExit("--part and --all-parts only apply to --strategy full-release-human-rx.")
    if args.part is not None and args.all_parts:
        raise SystemExit("Use either --part or --all-parts, not both.")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.manifest_dir.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=args.timeout_seconds) as client:
        targets = resolve_download_targets(client, args)

    if args.dry_run:
        print(f"DailyMed strategy: {args.strategy}")
        print(f"Resolved archive count: {len(targets)}")
        for target in targets:
            print(f"- {target.get('filename')}: {target['url']}")
        return 0

    run_dir = next_run_dir(args.output_dir)
    run_id = run_dir.name.replace("run_", "")
    manifest: dict[str, Any] = {
        "source": SOURCE_NAME,
        "run_id": run_id,
        "started_at_utc": utc_now_iso(),
        "resource_page": DAILYMED_RESOURCE_URL,
        "request": {
            "strategy": args.strategy,
            "url": args.url,
            "part": args.part,
            "all_parts": args.all_parts,
        },
        "resolved_targets": targets,
        "downloads": [],
    }
    try:
        with httpx.Client(timeout=args.timeout_seconds) as client:
            for target in targets:
                manifest["downloads"].append(download_file(client, target["url"], run_dir))
        manifest["finished_at_utc"] = utc_now_iso()
        manifest["status"] = "completed"
    except Exception as exc:
        manifest["finished_at_utc"] = utc_now_iso()
        manifest["status"] = "failed"
        manifest["error"] = str(exc)
        raise
    finally:
        manifest_path = args.manifest_dir / f"dailymed_spl_run_{run_id}.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"Manifest: {manifest_path}")

    print(
        "Completed DailyMed download:",
        f"run_id={run_id}",
        f"archives={len(manifest['downloads'])}",
        f"total_size_bytes={sum(item['size_bytes'] for item in manifest['downloads'])}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
