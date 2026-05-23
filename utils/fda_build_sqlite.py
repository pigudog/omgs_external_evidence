#!/usr/bin/env python3
"""Build cutoff-filtered FDA SQLite assets from downloaded openFDA/DailyMed files."""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lxml import etree


SOURCE_OPENFDA = "openfda_drug_label"
SOURCE_DAILYMED = "dailymed_spl"
DEFAULT_CUTOFF_DATE = "2025-10-29"
HL7_NS = {"hl7": "urn:hl7-org:v3"}
XML_PARSER = etree.XMLParser(huge_tree=True)
HIGHLIGHTS_PREFIX_RE = re.compile(
    r"^These highlights do not include all the information needed to use .*? See full prescribing information for .*?\.\s*",
    flags=re.IGNORECASE,
)


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_cutoff_date(value: str) -> str:
    digits = re.sub(r"\D", "", value)
    if len(digits) != 8:
        raise ValueError(f"cutoff date must resolve to YYYYMMDD, got {value!r}")
    return digits


def normalize_effective_date(value: str | None) -> str | None:
    if not value:
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits[:8] if len(digits) >= 8 else None


def is_allowed_effective_date(value: str | None, cutoff_yyyymmdd: str) -> tuple[bool, str | None, str | None]:
    normalized = normalize_effective_date(value)
    if normalized is None:
        return False, None, "missing_effective_date"
    if normalized > cutoff_yyyymmdd:
        return False, normalized, "after_cutoff"
    return True, normalized, None


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS build_metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_artifacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        artifact_type TEXT NOT NULL,
        artifact_path TEXT NOT NULL UNIQUE,
        checksum TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fda_labels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        set_id TEXT NOT NULL,
        spl_id TEXT,
        version TEXT,
        brand_name TEXT,
        generic_name TEXT,
        manufacturer_name TEXT,
        substance_name TEXT,
        product_type TEXT,
        route TEXT,
        effective_date TEXT NOT NULL,
        indications_text TEXT,
        dosage_text TEXT,
        warnings_text TEXT,
        boxed_warning_text TEXT,
        contraindications_text TEXT,
        adverse_reactions_text TEXT,
        source_url TEXT,
        raw_artifact_id INTEGER,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source, set_id, version),
        FOREIGN KEY(raw_artifact_id) REFERENCES raw_artifacts(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dailymed_labels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        set_id TEXT NOT NULL,
        document_id TEXT,
        package_root TEXT,
        xml_path TEXT NOT NULL,
        title TEXT,
        effective_date TEXT NOT NULL,
        indications_text TEXT,
        dosage_text TEXT,
        warnings_text TEXT,
        boxed_warning_text TEXT,
        contraindications_text TEXT,
        raw_artifact_id INTEGER,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source, set_id, xml_path),
        FOREIGN KEY(raw_artifact_id) REFERENCES raw_artifacts(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS regulatory_label_sections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        canonical_source TEXT NOT NULL,
        source_priority INTEGER NOT NULL,
        set_id TEXT NOT NULL UNIQUE,
        dailymed_set_id TEXT,
        openfda_set_id TEXT,
        title TEXT,
        brand_name TEXT,
        generic_name TEXT,
        manufacturer_name TEXT,
        effective_date TEXT NOT NULL,
        indications_text TEXT,
        dosage_text TEXT,
        warnings_text TEXT,
        boxed_warning_text TEXT,
        contraindications_text TEXT,
        section_source_indications TEXT,
        section_source_dosage TEXT,
        section_source_warnings TEXT,
        section_source_boxed_warning TEXT,
        section_source_contraindications TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
]

INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_raw_artifacts_source ON raw_artifacts(source)",
    "CREATE INDEX IF NOT EXISTS idx_fda_labels_brand_name ON fda_labels(brand_name)",
    "CREATE INDEX IF NOT EXISTS idx_fda_labels_generic_name ON fda_labels(generic_name)",
    "CREATE INDEX IF NOT EXISTS idx_fda_labels_effective_date ON fda_labels(effective_date)",
    "CREATE INDEX IF NOT EXISTS idx_fda_labels_set_id_nocase ON fda_labels(set_id COLLATE NOCASE)",
    "CREATE INDEX IF NOT EXISTS idx_dailymed_labels_title ON dailymed_labels(title)",
    "CREATE INDEX IF NOT EXISTS idx_dailymed_labels_effective_date ON dailymed_labels(effective_date)",
    "CREATE INDEX IF NOT EXISTS idx_dailymed_labels_set_id_nocase ON dailymed_labels(set_id COLLATE NOCASE)",
    "CREATE INDEX IF NOT EXISTS idx_regulatory_brand_name ON regulatory_label_sections(brand_name)",
    "CREATE INDEX IF NOT EXISTS idx_regulatory_generic_name ON regulatory_label_sections(generic_name)",
    "CREATE INDEX IF NOT EXISTS idx_regulatory_title ON regulatory_label_sections(title)",
    "CREATE INDEX IF NOT EXISTS idx_regulatory_effective_date ON regulatory_label_sections(effective_date)",
]


def connect_sqlite(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    return connection


def initialize_database(db_path: Path, *, cutoff_date: str) -> None:
    connection = connect_sqlite(db_path)
    try:
        for statement in SCHEMA_STATEMENTS:
            connection.execute(statement)
        for statement in INDEX_STATEMENTS:
            connection.execute(statement)
        metadata = {
            "artifact_type": "fda_effective_date_cutoff_sqlite",
            "cutoff_date": cutoff_date,
            "cutoff_field": "label effective_date",
            "records_without_parseable_effective_date": "excluded",
            "created_at_utc": utc_now_iso(),
        }
        for key, value in metadata.items():
            connection.execute(
                "INSERT OR REPLACE INTO build_metadata (key, value) VALUES (?, ?)",
                (key, value),
            )
        connection.commit()
    finally:
        connection.close()


def find_latest_run_dir(base_dir: Path) -> Path:
    run_dirs = sorted(base_dir.glob("run_*"))
    if not run_dirs:
        raise FileNotFoundError(f"No run_* directories found under {base_dir}")
    return run_dirs[-1]


def find_openfda_page_files(input_dir: Path, *, all_runs: bool) -> list[Path]:
    if all_runs:
        files: list[Path] = []
        for run_dir in sorted(input_dir.glob("run_*")):
            files.extend(sorted(run_dir.glob("page_*.json")))
        return files
    return sorted(find_latest_run_dir(input_dir).glob("page_*.json"))


def find_dailymed_expanded_dirs(input_dir: Path) -> list[Path]:
    run_dir = find_latest_run_dir(input_dir)
    candidate_dirs = sorted(run_dir.glob("unpacked/prescription_expanded"))
    candidate_dirs.extend(sorted(run_dir.glob("expanded/*/prescription_expanded")))
    if not candidate_dirs:
        raise FileNotFoundError(f"No prescription_expanded directory found under {run_dir}")
    return candidate_dirs


def find_dailymed_xml_files(input_dir: Path) -> list[Path]:
    xml_files: list[Path] = []
    for expanded_dir in find_dailymed_expanded_dirs(input_dir):
        xml_files.extend(sorted(expanded_dir.glob("*/*.xml")))
    return xml_files


def get_raw_artifact_id(connection: sqlite3.Connection, artifact_path: Path) -> int | None:
    row = connection.execute(
        "SELECT id FROM raw_artifacts WHERE artifact_path = ?",
        (str(artifact_path),),
    ).fetchone()
    return int(row[0]) if row else None


def ensure_raw_artifact(connection: sqlite3.Connection, source: str, artifact_type: str, artifact_path: Path) -> int | None:
    raw_artifact_id = get_raw_artifact_id(connection, artifact_path)
    if raw_artifact_id is not None:
        return raw_artifact_id
    connection.execute(
        "INSERT OR IGNORE INTO raw_artifacts (source, artifact_type, artifact_path, checksum) VALUES (?, ?, ?, ?)",
        (source, artifact_type, str(artifact_path), None),
    )
    return get_raw_artifact_id(connection, artifact_path)


def first_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        for item in value:
            text = str(item).strip()
            if text:
                return text
        return None
    text = str(value).strip()
    return text or None


def join_values(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        parts = [str(item).strip() for item in value if str(item).strip()]
        return "\n\n".join(parts) if parts else None
    text = str(value).strip()
    return text or None


def build_source_url(set_id: str | None) -> str | None:
    if not set_id:
        return None
    return f'https://api.fda.gov/drug/label.json?search=set_id:"{set_id}"'


def upsert_openfda_label(
    connection: sqlite3.Connection,
    record: dict[str, Any],
    artifact_path: Path,
    *,
    cutoff_yyyymmdd: str,
) -> str:
    allowed, effective_date, reason = is_allowed_effective_date(record.get("effective_time"), cutoff_yyyymmdd)
    if not allowed:
        return reason or "skipped"

    openfda = record.get("openfda", {}) or {}
    set_id = first_value(record.get("set_id")) or first_value(openfda.get("spl_set_id"))
    version = first_value(record.get("version"))
    if not set_id:
        return "missing_set_id"
    raw_artifact_id = ensure_raw_artifact(connection, SOURCE_OPENFDA, "json_page", artifact_path)
    connection.execute(
        """
        INSERT INTO fda_labels (
            source, set_id, spl_id, version, brand_name, generic_name, manufacturer_name,
            substance_name, product_type, route, effective_date, indications_text, dosage_text,
            warnings_text, boxed_warning_text, contraindications_text, adverse_reactions_text,
            source_url, raw_artifact_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source, set_id, version)
        DO UPDATE SET
            spl_id = excluded.spl_id,
            brand_name = excluded.brand_name,
            generic_name = excluded.generic_name,
            manufacturer_name = excluded.manufacturer_name,
            substance_name = excluded.substance_name,
            product_type = excluded.product_type,
            route = excluded.route,
            effective_date = excluded.effective_date,
            indications_text = excluded.indications_text,
            dosage_text = excluded.dosage_text,
            warnings_text = excluded.warnings_text,
            boxed_warning_text = excluded.boxed_warning_text,
            contraindications_text = excluded.contraindications_text,
            adverse_reactions_text = excluded.adverse_reactions_text,
            source_url = excluded.source_url,
            raw_artifact_id = excluded.raw_artifact_id
        """,
        (
            SOURCE_OPENFDA,
            set_id,
            first_value(openfda.get("spl_id")),
            version,
            first_value(openfda.get("brand_name")),
            first_value(openfda.get("generic_name")),
            first_value(openfda.get("manufacturer_name")),
            first_value(openfda.get("substance_name")),
            first_value(openfda.get("product_type")),
            first_value(openfda.get("route")),
            effective_date,
            join_values(record.get("indications_and_usage")),
            join_values(record.get("dosage_and_administration")),
            join_values(record.get("warnings")),
            join_values(record.get("boxed_warning")),
            join_values(record.get("contraindications")),
            join_values(record.get("adverse_reactions")),
            build_source_url(set_id),
            raw_artifact_id,
        ),
    )
    return "inserted"


def parse_openfda_into_sqlite(
    db_path: Path,
    input_dir: Path,
    *,
    cutoff_yyyymmdd: str,
    all_runs: bool = False,
    max_pages: int | None = None,
) -> dict[str, Any]:
    page_files = find_openfda_page_files(input_dir, all_runs=all_runs)
    if max_pages is not None:
        page_files = page_files[:max_pages]
    stats = {
        "pages": 0,
        "records_seen": 0,
        "records_inserted": 0,
        "skipped_after_cutoff": 0,
        "skipped_missing_effective_date": 0,
        "skipped_missing_set_id": 0,
    }
    connection = connect_sqlite(db_path)
    try:
        for page_path in page_files:
            payload = json.loads(page_path.read_text(encoding="utf-8"))
            for record in payload.get("results", []):
                result = upsert_openfda_label(connection, record, page_path, cutoff_yyyymmdd=cutoff_yyyymmdd)
                stats["records_seen"] += 1
                if result == "inserted":
                    stats["records_inserted"] += 1
                elif result == "after_cutoff":
                    stats["skipped_after_cutoff"] += 1
                elif result == "missing_effective_date":
                    stats["skipped_missing_effective_date"] += 1
                elif result == "missing_set_id":
                    stats["skipped_missing_set_id"] += 1
            stats["pages"] += 1
            if stats["pages"] % 25 == 0:
                connection.commit()
        connection.commit()
    finally:
        connection.close()
    return stats


def get_first_attr(tree: etree._ElementTree, xpath: str, attr: str) -> str | None:
    nodes = tree.xpath(xpath, namespaces=HL7_NS)
    if not nodes:
        return None
    value = nodes[0].get(attr)
    return value.strip() if value else None


def get_first_text(tree: etree._ElementTree, xpath: str) -> str | None:
    nodes = tree.xpath(xpath, namespaces=HL7_NS)
    if not nodes:
        return None
    text = " ".join(" ".join(nodes[0].itertext()).split())
    return text or None


def normalize_heading(text: str | None) -> str | None:
    if not text:
        return None
    return " ".join(text.lower().split())


def extract_section_texts(tree: etree._ElementTree) -> dict[str, str | None]:
    sections = {
        "indications_text": None,
        "dosage_text": None,
        "warnings_text": None,
        "boxed_warning_text": None,
        "contraindications_text": None,
    }
    for section in tree.xpath("//hl7:section", namespaces=HL7_NS):
        heading_nodes = section.xpath("./hl7:title", namespaces=HL7_NS)
        heading = None
        if heading_nodes:
            heading = normalize_heading(" ".join(" ".join(heading_nodes[0].itertext()).split()))
        if not heading:
            continue
        section_text = " ".join(" ".join(section.itertext()).split())
        if not section_text:
            continue
        if sections["indications_text"] is None and "indications and usage" in heading:
            sections["indications_text"] = section_text
        elif sections["dosage_text"] is None and "dosage and administration" in heading:
            sections["dosage_text"] = section_text
        elif sections["warnings_text"] is None and "warnings and precautions" in heading:
            sections["warnings_text"] = section_text
        elif sections["boxed_warning_text"] is None and "boxed warning" in heading:
            sections["boxed_warning_text"] = section_text
        elif sections["contraindications_text"] is None and "contraindications" in heading:
            sections["contraindications_text"] = section_text
    return sections


def upsert_dailymed_label(connection: sqlite3.Connection, xml_path: Path, *, cutoff_yyyymmdd: str) -> str:
    tree = etree.parse(str(xml_path), parser=XML_PARSER)
    raw_effective_date = get_first_attr(tree, "/hl7:document/hl7:effectiveTime", "value")
    allowed, effective_date, reason = is_allowed_effective_date(raw_effective_date, cutoff_yyyymmdd)
    if not allowed:
        return reason or "skipped"

    set_id = get_first_attr(tree, "/hl7:document/hl7:setId", "root")
    document_id = get_first_attr(tree, "/hl7:document/hl7:id", "root")
    title = get_first_text(tree, "/hl7:document/hl7:title")
    sections = extract_section_texts(tree)
    if not set_id:
        return "missing_set_id"
    raw_artifact_id = ensure_raw_artifact(connection, SOURCE_DAILYMED, "xml_file", xml_path)
    connection.execute(
        """
        INSERT INTO dailymed_labels (
            source, set_id, document_id, package_root, xml_path, title, effective_date,
            indications_text, dosage_text, warnings_text, boxed_warning_text, contraindications_text, raw_artifact_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source, set_id, xml_path)
        DO UPDATE SET
            document_id = excluded.document_id,
            package_root = excluded.package_root,
            title = excluded.title,
            effective_date = excluded.effective_date,
            indications_text = excluded.indications_text,
            dosage_text = excluded.dosage_text,
            warnings_text = excluded.warnings_text,
            boxed_warning_text = excluded.boxed_warning_text,
            contraindications_text = excluded.contraindications_text,
            raw_artifact_id = excluded.raw_artifact_id
        """,
        (
            SOURCE_DAILYMED,
            set_id,
            document_id,
            xml_path.parent.name,
            str(xml_path),
            title,
            effective_date,
            sections["indications_text"],
            sections["dosage_text"],
            sections["warnings_text"],
            sections["boxed_warning_text"],
            sections["contraindications_text"],
            raw_artifact_id,
        ),
    )
    return "inserted"


def parse_dailymed_into_sqlite(
    db_path: Path,
    input_dir: Path,
    *,
    cutoff_yyyymmdd: str,
    max_files: int | None = None,
) -> dict[str, Any]:
    xml_files = find_dailymed_xml_files(input_dir)
    if max_files is not None:
        xml_files = xml_files[:max_files]
    stats = {
        "files_seen": 0,
        "records_inserted": 0,
        "skipped_after_cutoff": 0,
        "skipped_missing_effective_date": 0,
        "skipped_missing_set_id": 0,
    }
    connection = connect_sqlite(db_path)
    try:
        for xml_path in xml_files:
            result = upsert_dailymed_label(connection, xml_path, cutoff_yyyymmdd=cutoff_yyyymmdd)
            stats["files_seen"] += 1
            if result == "inserted":
                stats["records_inserted"] += 1
            elif result == "after_cutoff":
                stats["skipped_after_cutoff"] += 1
            elif result == "missing_effective_date":
                stats["skipped_missing_effective_date"] += 1
            elif result == "missing_set_id":
                stats["skipped_missing_set_id"] += 1
            if stats["files_seen"] % 500 == 0:
                connection.commit()
        connection.commit()
    finally:
        connection.close()
    return stats


def normalize_dailymed_title(value: str | None) -> str | None:
    if not value:
        return None
    text = " ".join(value.split())
    if not text:
        return None
    text = HIGHLIGHTS_PREFIX_RE.sub("", text)
    text = re.sub(r"\s*Initial U\.S\. Approval:.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*To reduce the development of drug-resistant bacteria.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s{2,}", " ", text).strip(" .;")
    return text or None


def build_regulatory_label_sections(db_path: Path) -> dict[str, Any]:
    connection = connect_sqlite(db_path)
    try:
        connection.create_function("normalize_dailymed_title", 1, normalize_dailymed_title)
        connection.execute("DELETE FROM regulatory_label_sections")
        connection.execute(
            """
            WITH latest_dailymed AS (
                SELECT *
                FROM (
                    SELECT
                        d.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY d.set_id
                            ORDER BY d.effective_date DESC, d.xml_path DESC
                        ) AS rn
                    FROM dailymed_labels d
                )
                WHERE rn = 1
            )
            INSERT INTO regulatory_label_sections (
                canonical_source, source_priority, set_id, dailymed_set_id, openfda_set_id,
                title, brand_name, generic_name, manufacturer_name, effective_date,
                indications_text, dosage_text, warnings_text, boxed_warning_text, contraindications_text,
                section_source_indications, section_source_dosage, section_source_warnings,
                section_source_boxed_warning, section_source_contraindications
            )
            SELECT
                'dailymed',
                1,
                d.set_id,
                d.set_id,
                f.set_id,
                COALESCE(
                    NULLIF(TRIM(normalize_dailymed_title(d.title)), ''),
                    NULLIF(TRIM(d.title), ''),
                    NULLIF(TRIM(f.brand_name), ''),
                    NULLIF(TRIM(f.generic_name), '')
                ),
                f.brand_name,
                f.generic_name,
                f.manufacturer_name,
                COALESCE(NULLIF(TRIM(d.effective_date), ''), NULLIF(TRIM(f.effective_date), '')),
                COALESCE(NULLIF(TRIM(d.indications_text), ''), NULLIF(TRIM(f.indications_text), '')),
                COALESCE(NULLIF(TRIM(d.dosage_text), ''), NULLIF(TRIM(f.dosage_text), '')),
                COALESCE(NULLIF(TRIM(d.warnings_text), ''), NULLIF(TRIM(f.warnings_text), '')),
                COALESCE(NULLIF(TRIM(d.boxed_warning_text), ''), NULLIF(TRIM(f.boxed_warning_text), '')),
                COALESCE(NULLIF(TRIM(d.contraindications_text), ''), NULLIF(TRIM(f.contraindications_text), '')),
                CASE WHEN d.indications_text IS NOT NULL AND TRIM(d.indications_text) != '' THEN 'dailymed' WHEN f.indications_text IS NOT NULL AND TRIM(f.indications_text) != '' THEN 'openfda' ELSE NULL END,
                CASE WHEN d.dosage_text IS NOT NULL AND TRIM(d.dosage_text) != '' THEN 'dailymed' WHEN f.dosage_text IS NOT NULL AND TRIM(f.dosage_text) != '' THEN 'openfda' ELSE NULL END,
                CASE WHEN d.warnings_text IS NOT NULL AND TRIM(d.warnings_text) != '' THEN 'dailymed' WHEN f.warnings_text IS NOT NULL AND TRIM(f.warnings_text) != '' THEN 'openfda' ELSE NULL END,
                CASE WHEN d.boxed_warning_text IS NOT NULL AND TRIM(d.boxed_warning_text) != '' THEN 'dailymed' WHEN f.boxed_warning_text IS NOT NULL AND TRIM(f.boxed_warning_text) != '' THEN 'openfda' ELSE NULL END,
                CASE WHEN d.contraindications_text IS NOT NULL AND TRIM(d.contraindications_text) != '' THEN 'dailymed' WHEN f.contraindications_text IS NOT NULL AND TRIM(f.contraindications_text) != '' THEN 'openfda' ELSE NULL END
            FROM latest_dailymed d
            LEFT JOIN fda_labels f ON d.set_id = f.set_id COLLATE NOCASE
            """
        )
        connection.execute(
            """
            WITH latest_dailymed AS (
                SELECT *
                FROM (
                    SELECT
                        d.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY d.set_id
                            ORDER BY d.effective_date DESC, d.xml_path DESC
                        ) AS rn
                    FROM dailymed_labels d
                )
                WHERE rn = 1
            )
            INSERT INTO regulatory_label_sections (
                canonical_source, source_priority, set_id, dailymed_set_id, openfda_set_id,
                title, brand_name, generic_name, manufacturer_name, effective_date,
                indications_text, dosage_text, warnings_text, boxed_warning_text, contraindications_text,
                section_source_indications, section_source_dosage, section_source_warnings,
                section_source_boxed_warning, section_source_contraindications
            )
            SELECT
                'openfda',
                2,
                f.set_id,
                NULL,
                f.set_id,
                COALESCE(NULLIF(TRIM(f.brand_name), ''), NULLIF(TRIM(f.generic_name), '')),
                f.brand_name,
                f.generic_name,
                f.manufacturer_name,
                f.effective_date,
                f.indications_text,
                f.dosage_text,
                f.warnings_text,
                f.boxed_warning_text,
                f.contraindications_text,
                CASE WHEN f.indications_text IS NOT NULL AND TRIM(f.indications_text) != '' THEN 'openfda' ELSE NULL END,
                CASE WHEN f.dosage_text IS NOT NULL AND TRIM(f.dosage_text) != '' THEN 'openfda' ELSE NULL END,
                CASE WHEN f.warnings_text IS NOT NULL AND TRIM(f.warnings_text) != '' THEN 'openfda' ELSE NULL END,
                CASE WHEN f.boxed_warning_text IS NOT NULL AND TRIM(f.boxed_warning_text) != '' THEN 'openfda' ELSE NULL END,
                CASE WHEN f.contraindications_text IS NOT NULL AND TRIM(f.contraindications_text) != '' THEN 'openfda' ELSE NULL END
            FROM fda_labels f
            LEFT JOIN latest_dailymed d ON d.set_id = f.set_id COLLATE NOCASE
            WHERE d.set_id IS NULL
            """
        )
        rows = connection.execute(
            """
            SELECT 'fda_labels', COUNT(*) FROM fda_labels
            UNION ALL SELECT 'dailymed_labels', COUNT(*) FROM dailymed_labels
            UNION ALL SELECT 'regulatory_label_sections', COUNT(*) FROM regulatory_label_sections
            """
        ).fetchall()
        connection.commit()
    finally:
        connection.close()
    return {name: int(count) for name, count in rows}


def write_summary_metadata(db_path: Path, payload: dict[str, Any]) -> None:
    connection = connect_sqlite(db_path)
    try:
        connection.execute(
            "INSERT OR REPLACE INTO build_metadata (key, value) VALUES (?, ?)",
            ("build_summary", json.dumps(payload, ensure_ascii=False, sort_keys=True)),
        )
        connection.commit()
    finally:
        connection.close()


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(description="Build cutoff-filtered FDA SQLite assets.")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=root / "data" / "processed" / "fda" / "fda_effective_date_le_20251029.sqlite",
    )
    parser.add_argument("--openfda-dir", type=Path, default=root / "data" / "raw" / "fda" / "openfda")
    parser.add_argument("--dailymed-dir", type=Path, default=root / "data" / "raw" / "fda" / "dailymed")
    parser.add_argument("--cutoff-date", default=DEFAULT_CUTOFF_DATE)
    parser.add_argument("--openfda-max-pages", type=int, default=None)
    parser.add_argument("--dailymed-max-files", type=int, default=None)
    parser.add_argument("--all-openfda-runs", action="store_true")
    parser.add_argument("--skip-openfda", action="store_true")
    parser.add_argument("--skip-dailymed", action="store_true")
    parser.add_argument("--incremental", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cutoff_yyyymmdd = normalize_cutoff_date(args.cutoff_date)
    if args.db_path.exists() and not args.incremental:
        os.remove(args.db_path)
    initialize_database(args.db_path, cutoff_date=cutoff_yyyymmdd)
    payload: dict[str, Any] = {
        "db_path": str(args.db_path),
        "cutoff_date": cutoff_yyyymmdd,
        "cutoff_field": "effective_date",
        "openfda": None,
        "dailymed": None,
        "tables": None,
    }
    if not args.skip_openfda:
        payload["openfda"] = parse_openfda_into_sqlite(
            args.db_path,
            args.openfda_dir,
            cutoff_yyyymmdd=cutoff_yyyymmdd,
            all_runs=args.all_openfda_runs,
            max_pages=args.openfda_max_pages,
        )
    if not args.skip_dailymed:
        payload["dailymed"] = parse_dailymed_into_sqlite(
            args.db_path,
            args.dailymed_dir,
            cutoff_yyyymmdd=cutoff_yyyymmdd,
            max_files=args.dailymed_max_files,
        )
    payload["tables"] = build_regulatory_label_sections(args.db_path)
    write_summary_metadata(args.db_path, payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
