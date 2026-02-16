"""Cloud I/O utilities -- Arrow-based read/write for warehouse tables.

Requires the ``dataplane`` extra: ``pip install kanoniv[cloud,dataplane]``.
"""
from __future__ import annotations

import os
import tempfile
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse


def _parse_snowflake_url(connection_string: str) -> dict[str, str]:
    """Parse a SQLAlchemy-style Snowflake URL into connector kwargs."""
    parsed = urlparse(connection_string)
    params: dict[str, str] = {}

    if parsed.username:
        params["user"] = unquote(parsed.username)
    if parsed.password:
        params["password"] = unquote(parsed.password)
    if parsed.hostname:
        params["account"] = parsed.hostname

    # Path: /database/schema
    path_parts = [p for p in parsed.path.split("/") if p]
    if len(path_parts) >= 1:
        params["database"] = path_parts[0]
    if len(path_parts) >= 2:
        params["schema"] = path_parts[1]

    # Query string: warehouse, role, etc.
    qs = parse_qs(parsed.query)
    for key in ("warehouse", "role"):
        if key in qs:
            params[key] = qs[key][0]

    return params


def read_arrow(table_name: str, connection_string: str) -> "pyarrow.Table":
    """Read a Snowflake table as an Arrow table via the native connector.

    Uses Snowflake's ``fetch_arrow_all()`` for efficient columnar transfer.

    Args:
        table_name: Fully-qualified or bare table/view name.
        connection_string: SQLAlchemy-style ``snowflake://...`` URL.

    Returns:
        A ``pyarrow.Table`` with all rows from the table.
    """
    import snowflake.connector

    sf_params = _parse_snowflake_url(connection_string)
    conn = snowflake.connector.connect(**sf_params)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table_name}")  # noqa: S608
        table = cur.fetch_arrow_all()
        cur.close()
        return table
    finally:
        conn.close()


def write_parquet_to_warehouse(
    tables: dict[str, "pyarrow.Table"],
    connection_string: str,
    schema: str = "KANONIV_RESOLVED",
) -> dict[str, int]:
    """Write Arrow tables to Snowflake via Parquet PUT + COPY INTO.

    For each entry in *tables*, writes a temp Parquet file, stages it in
    Snowflake, and loads it into the target table.

    Args:
        tables: Mapping of ``{table_name: arrow_table}``.
        connection_string: SQLAlchemy-style ``snowflake://...`` URL.
        schema: Target schema in Snowflake.

    Returns:
        Mapping of ``{table_name: row_count}`` written.
    """
    import pyarrow.parquet as pq
    import snowflake.connector

    sf_params = _parse_snowflake_url(connection_string)
    conn = snowflake.connector.connect(**sf_params)
    counts: dict[str, int] = {}

    try:
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        for table_name, arrow_table in tables.items():
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_path = tmp.name

            try:
                pq.write_table(arrow_table, tmp_path, compression="zstd")

                fq_table = f"{schema}.{table_name}"
                stage = f"@{schema}.%{table_name}"

                # Create target table from Parquet schema
                col_defs = ", ".join(
                    f"{field.name} VARCHAR"
                    for field in arrow_table.schema
                )
                cur.execute(
                    f"CREATE OR REPLACE TABLE {fq_table} ({col_defs})"
                )

                # PUT file to internal stage
                cur.execute(
                    f"PUT 'file://{tmp_path}' {stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                )

                # COPY INTO from stage
                cur.execute(
                    f"COPY INTO {fq_table} FROM {stage} "
                    f"FILE_FORMAT=(TYPE=PARQUET) "
                    f"MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE "
                    f"PURGE=TRUE"
                )

                counts[table_name] = arrow_table.num_rows
            finally:
                os.unlink(tmp_path)

        cur.close()
    finally:
        conn.close()

    return counts


def _extract_connection_string(sources: list[Any]) -> str | None:
    """Extract a warehouse connection string from a list of sources.

    Returns the first connection string found, or None if all sources
    are non-warehouse (CSV, pandas, etc.).
    """
    for source in sources:
        conn = getattr(source, "connection_string", None)
        if isinstance(conn, str) and conn:
            return conn
    return None
