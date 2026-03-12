"""Cloud ingest command: read files with DuckDB, upload as Parquet.

    kanoniv ingest ./data/crm_contacts.csv
    kanoniv ingest ./data/*.csv
    kanoniv ingest ./data/                     # all CSVs in directory
    kanoniv ingest crm.csv --id-column crm_id  # explicit ID column
    kanoniv ingest ./data/ --entity-type person # only ingest files with person identity fields
"""
from __future__ import annotations

import glob
import os
import re
import sys
import tempfile
from argparse import Namespace
from pathlib import Path
from typing import Any


def cmd_ingest(args: Namespace, client: Any) -> None:
    """Read CSV/JSON files with DuckDB, convert to Parquet, upload to Cloud."""
    try:
        import duckdb  # noqa: F401
    except ImportError:
        sys.stderr.write(
            "Ingest requires duckdb and pyarrow. Install with:\n"
            "  pip install duckdb pyarrow\n"
        )
        sys.exit(1)

    try:
        import pyarrow.parquet as pq  # noqa: F401
    except ImportError:
        sys.stderr.write(
            "Ingest requires pyarrow. Install with:\n"
            "  pip install pyarrow\n"
        )
        sys.exit(1)

    files = _resolve_files(args.files)
    if not files:
        sys.stderr.write("No CSV or JSON files found.\n")
        sys.exit(1)

    entity_types_raw = getattr(args, "entity_type", None)
    id_column = getattr(args, "id_column", None)

    # Normalize entity_type: None means no filter, list means multi-type
    if entity_types_raw and isinstance(entity_types_raw, list):
        entity_types = entity_types_raw
    elif entity_types_raw and isinstance(entity_types_raw, str):
        entity_types = [entity_types_raw]
    else:
        entity_types = [None]

    grand_new = 0
    grand_updated = 0
    grand_unchanged = 0
    grand_skipped = 0

    for et in entity_types:
        if et:
            print(f"  Filtering for entity type: {et}\n")

        total_new = 0
        total_updated = 0
        total_unchanged = 0
        skipped = 0

        for filepath in files:
            source_name = _derive_source_name(filepath)

            # If entity-type is specified, profile the file first and skip
            # files that don't have identity-relevant fields for that type
            if et:
                signals = _detect_identity_signals(filepath, et)
                if not signals:
                    print(f"  skip: {source_name} (no {et} identity fields)")
                    skipped += 1
                    continue
                else:
                    sig_str = ", ".join(signals)
                    print(f"  scan: {source_name} -> {sig_str}")

            result = _ingest_file(client, filepath, source_name, id_column, et)
            if result is None:
                continue

            new = result.get("new", 0)
            updated = result.get("updated", 0)
            unchanged = result.get("unchanged", 0)
            total = new + updated + unchanged
            total_new += new
            total_updated += updated
            total_unchanged += unchanged

            print(f"  {source_name}: {total} records ({new} new, {updated} updated)")

        grand_new += total_new
        grand_updated += total_updated
        grand_unchanged += total_unchanged
        grand_skipped += skipped

        if len(entity_types) > 1 and et != entity_types[-1]:
            ingested = len(files) - skipped
            grand = total_new + total_updated + total_unchanged
            print(f"\n  Subtotal ({et}): {grand} records across {ingested} sources")
            if skipped:
                print(f"  ({skipped} skipped)")
            print()

    print()
    grand = grand_new + grand_updated + grand_unchanged
    if grand_skipped:
        print(f"  Total: {grand} records ({grand_skipped} source-type pairs skipped)")
    else:
        print(f"  Total: {grand} records")


# ---------------------------------------------------------------------------
# Identity signal detection (value-based, not column-name based)
# ---------------------------------------------------------------------------

# Patterns detected by inspecting sampled VALUES, not column names.
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$", re.IGNORECASE)
_PHONE_RE = re.compile(r"^[\+]?\d[\d\s\-\(\)]{6,14}$")  # no dots (avoids prices)
_NAME_RE = re.compile(r"^[A-Z][a-z]{1,15} [A-Z][a-z]{1,15}$")
_CORP_SUFFIXES = re.compile(
    r"\b(inc|ltd|llc|corp|co|gmbh|plc|sa|ag|bv|pty|srl|group|partners|labs|"
    r"technologies|solutions|systems|consulting|services|enterprises|industries)\b",
    re.IGNORECASE,
)
_SKU_RE = re.compile(r"^[A-Za-z0-9]{2,}[\-_][A-Za-z0-9\-_]{2,}$")
_PRICE_RE = re.compile(r"^\$?\d+[\.,]\d{2}$")


# Which signal types indicate which entity types
_ENTITY_SIGNALS: dict[str, list[str]] = {
    "person": ["email", "phone", "person_name"],
    "company": ["company_name", "domain"],
    "product": ["sku", "product_name", "price"],
}

# Minimum number of detected signals required to include a file.
# Product needs 2+ to avoid false positives (person files with a numeric
# column matching price, or multi-word strings matching product_name).
_MIN_SIGNALS: dict[str, int] = {
    "product": 2,
}

# Minimum ratio of signal columns to total columns. Filters out transactional
# tables that merely *reference* an entity type (e.g. app_events with a
# product_sku column) vs tables *about* that entity type (product catalogs
# where most columns are product attributes).
_MIN_SIGNAL_DENSITY: dict[str, float] = {
    "product": 0.25,
}


def _detect_identity_signals(filepath: str, entity_type: str) -> list[str]:
    """Sample values from a file and check for identity signals.

    Returns a list of detected signal names (e.g. ["email", "phone"]),
    or empty list if no relevant signals found.
    """
    import duckdb

    required_signals = _ENTITY_SIGNALS.get(entity_type, [])
    if not required_signals:
        # Unknown entity type - don't filter, ingest everything
        return [entity_type]

    con = duckdb.connect()
    try:
        ext = Path(filepath).suffix.lower()
        if ext == ".json":
            query = f"SELECT * FROM read_json_auto('{filepath}') LIMIT 100"
        else:
            query = f"SELECT * FROM read_csv_auto('{filepath}') LIMIT 100"

        rel = con.sql(query)
        columns = rel.columns
        rows = rel.fetchall()
    except Exception:
        return []
    finally:
        con.close()

    if not rows:
        return []

    found: list[str] = []
    total_cols = len(columns)
    signal_cols = 0  # columns that matched any signal pattern

    for col_idx, col_name in enumerate(columns):
        # Skip ID columns - they contain synthetic identifiers, not identity data
        if col_name.lower().endswith("_id") or col_name.lower() == "id":
            total_cols -= 1  # don't count ID cols in density ratio
            continue

        # Sample non-null string values from this column
        values = []
        for row in rows:
            v = row[col_idx]
            if v is not None:
                s = str(v).strip()
                if s:
                    values.append(s)
        if not values:
            continue

        sample = values[:50]
        col_matched = False

        # Check each value pattern against the sample
        if "email" in required_signals and "email" not in found:
            email_hits = sum(1 for v in sample if _EMAIL_RE.match(v))
            if email_hits >= len(sample) * 0.3:
                found.append("email")
                col_matched = True

        if "phone" in required_signals and "phone" not in found:
            phone_hits = sum(1 for v in sample if _PHONE_RE.match(v))
            if phone_hits >= len(sample) * 0.3:
                found.append("phone")
                col_matched = True

        if "person_name" in required_signals and "person_name" not in found:
            name_hits = sum(1 for v in sample if _NAME_RE.match(v))
            # Person names: at least 50% match "Firstname Lastname" pattern
            # AND less than 10% have corporate suffixes (to avoid company names)
            corp_hits = sum(1 for v in sample if _CORP_SUFFIXES.search(v))
            if name_hits >= len(sample) * 0.5 and corp_hits < len(sample) * 0.1:
                found.append("person_name")
                col_matched = True

        if "company_name" in required_signals and "company_name" not in found:
            corp_hits = sum(1 for v in sample if _CORP_SUFFIXES.search(v))
            if corp_hits >= len(sample) * 0.1:
                found.append("company_name")
                col_matched = True

        if "sku" in required_signals and "sku" not in found:
            sku_hits = sum(1 for v in sample if _SKU_RE.match(v))
            if sku_hits >= len(sample) * 0.3:
                found.append("sku")
                col_matched = True

        if "price" in required_signals and "price" not in found:
            price_hits = sum(1 for v in sample if _PRICE_RE.match(str(v)))
            if price_hits >= len(sample) * 0.3:
                found.append("price")
                col_matched = True

        if "domain" in required_signals and "domain" not in found:
            # Domains: values like "example.com" without @ (not emails)
            domain_hits = sum(
                1 for v in sample
                if "." in v and "@" not in v and " " not in v and len(v) < 100
            )
            if domain_hits >= len(sample) * 0.3:
                found.append("domain")
                col_matched = True

        if "product_name" in required_signals and "product_name" not in found:
            # Product names: multi-word strings with digits or model numbers.
            # Real product names have specs: "MacBook Air M3 13-inch",
            # "SANDISK EXTREME PRO 128GB", "Webcam 1080p Autofocus".
            # Person fields ("VP of Sales", "Keystone Data") lack digits.
            product_like = sum(
                1 for v in sample
                if " " in v and len(v) > 5
                and re.search(r"\d", v)
                and not _EMAIL_RE.match(v)
                and not _NAME_RE.match(v)
                and not _CORP_SUFFIXES.search(v)
            )
            if product_like >= len(sample) * 0.3:
                found.append("product_name")
                col_matched = True

        if col_matched:
            signal_cols += 1

    # Product entity type requires at least 2 signals to avoid false positives.
    min_signals = _MIN_SIGNALS.get(entity_type, 1)
    if len(found) < min_signals:
        return []

    # Signal density check: reject tables that merely *reference* this entity
    # type (e.g. app_events with a product_sku column) vs tables *about* this
    # entity type (product catalogs where most columns are product attributes).
    min_density = _MIN_SIGNAL_DENSITY.get(entity_type, 0.0)
    if min_density > 0 and total_cols > 0:
        density = signal_cols / total_cols
        if density < min_density:
            return []

    return found


# ---------------------------------------------------------------------------
# File resolution and upload
# ---------------------------------------------------------------------------

def _resolve_files(file_args: list[str]) -> list[str]:
    """Expand globs, directories, and individual paths into a list of files."""
    files: list[str] = []
    for arg in file_args:
        path = Path(arg)
        if path.is_dir():
            # All CSVs and JSONs in the directory
            for ext in ("*.csv", "*.json"):
                files.extend(sorted(glob.glob(str(path / ext))))
        elif "*" in arg or "?" in arg:
            files.extend(sorted(glob.glob(arg)))
        elif path.is_file():
            files.append(str(path))
        else:
            sys.stderr.write(f"  skip: {arg} (not found)\n")
    return files


def _derive_source_name(filepath: str) -> str:
    """Derive a source name from the filename (without extension)."""
    return Path(filepath).stem



def _ingest_file(
    client: Any,
    filepath: str,
    source_name: str,
    id_column: str | None,
    entity_type: str | None = None,
) -> dict | None:
    """Read a file with DuckDB, convert to Parquet, upload."""
    import duckdb
    import pyarrow.parquet as pq

    con = duckdb.connect()
    try:
        # DuckDB auto-detects CSV format, types, delimiters
        ext = Path(filepath).suffix.lower()
        if ext == ".json":
            query = f"SELECT * FROM read_json_auto('{filepath}')"
        else:
            query = f"SELECT * FROM read_csv_auto('{filepath}')"

        arrow_table = con.sql(query).fetch_arrow_table()
    except Exception as e:
        sys.stderr.write(f"  skip: {filepath} ({e})\n")
        return None
    finally:
        con.close()

    if arrow_table.num_rows == 0:
        sys.stderr.write(f"  skip: {filepath} (empty)\n")
        return None

    # Detect and rename ID column to external_id
    columns = arrow_table.column_names
    actual_id_col = id_column
    if not actual_id_col:
        actual_id_col = _detect_id_column_from_names(columns)

    if actual_id_col and actual_id_col in columns:
        # Rename the ID column to external_id
        new_names = [
            "external_id" if c == actual_id_col else c
            for c in columns
        ]
        arrow_table = arrow_table.rename_columns(new_names)

    # Write to temp Parquet and upload
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        pq.write_table(arrow_table, tmp_path, compression="zstd")
        result = _upload_parquet(client, source_name, tmp_path, entity_type)
        return result
    finally:
        os.unlink(tmp_path)


def _detect_id_column_from_names(columns: list[str]) -> str | None:
    """Pick the best ID column from a list of column names."""
    for col in columns:
        if col.lower().endswith("_id") or col.lower() == "id":
            return col
    return None


def _upload_parquet(
    client: Any,
    source_name: str,
    parquet_path: str,
    entity_type: str | None = None,
) -> dict:
    """Upload a Parquet file via multipart POST to /v1/ingest/parquet."""
    import httpx

    with open(parquet_path, "rb") as f:
        file_bytes = f.read()

    form_data: dict[str, str] = {"source_name": source_name}
    if entity_type:
        form_data["entity_type"] = entity_type

    # Use a separate httpx client for multipart upload (longer timeout)
    with httpx.Client(
        base_url=client._base_url,
        headers={"X-API-Key": client._api_key},
        timeout=httpx.Timeout(connect=10.0, read=120.0, write=60.0, pool=10.0),
    ) as upload_client:
        response = upload_client.post(
            "/v1/ingest/parquet",
            data=form_data,
            files={"file": ("data.parquet", file_bytes, "application/octet-stream")},
        )

    if response.status_code >= 400:
        try:
            body = response.json()
            msg = body.get("error") or body.get("message") or response.text
        except Exception:
            msg = response.text
        sys.stderr.write(f"  error: {source_name} ({response.status_code}): {msg}\n")
        return {"new": 0, "updated": 0, "unchanged": 0}

    return response.json()
