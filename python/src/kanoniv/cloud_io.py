"""Cloud I/O utilities -- Arrow-based read/write for warehouse tables.

Requires the ``dataplane``, ``databricks``, or ``bigquery`` extra:
``pip install kanoniv[cloud,dataplane]``, ``pip install kanoniv[cloud,databricks]``,
or ``pip install kanoniv[cloud,bigquery]``.
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


def _parse_databricks_url(connection_string: str) -> dict[str, str]:
    """Parse a ``databricks://`` URL into connector kwargs."""
    parsed = urlparse(connection_string)
    params: dict[str, str] = {}

    if parsed.password:
        params["access_token"] = unquote(parsed.password)
    if parsed.hostname:
        params["server_hostname"] = parsed.hostname

    qs = parse_qs(parsed.query)
    for key in ("http_path", "catalog", "schema"):
        if key in qs:
            params[key] = qs[key][0]

    return params


def read_arrow_databricks(table_name: str, connection_string: str) -> "pyarrow.Table":
    """Read a Databricks table as an Arrow table via the native connector.

    Uses ``fetchall_arrow()`` for efficient columnar transfer.

    Args:
        table_name: Fully-qualified or bare table/view name.
        connection_string: ``databricks://...`` URL.

    Returns:
        A ``pyarrow.Table`` with all rows from the table.
    """
    from databricks import sql as dbsql

    db_params = _parse_databricks_url(connection_string)
    conn = dbsql.connect(**db_params)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table_name}")  # noqa: S608
        table = cur.fetchall_arrow()
        cur.close()
        return table
    finally:
        conn.close()


def write_parquet_to_databricks(
    tables: dict[str, "pyarrow.Table"],
    connection_string: str,
    volume_path: str = "/Volumes/kanoniv/resolved/staging",
) -> dict[str, int]:
    """Write Arrow tables to Databricks via Unity Catalog Volumes + COPY INTO.

    For each entry in *tables*, writes a temp Parquet file, uploads it to
    a Unity Catalog Volume, and loads it into the target table.

    Args:
        tables: Mapping of ``{table_name: arrow_table}``.
        connection_string: ``databricks://...`` URL.
        volume_path: Volume path for staging Parquet files.

    Returns:
        Mapping of ``{table_name: row_count}`` written.
    """
    import pyarrow.parquet as pq
    from databricks import sql as dbsql

    db_params = _parse_databricks_url(connection_string)
    conn = dbsql.connect(**db_params)
    counts: dict[str, int] = {}

    try:
        cur = conn.cursor()

        for table_name, arrow_table in tables.items():
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_path = tmp.name

            try:
                pq.write_table(arrow_table, tmp_path, compression="zstd")

                staged = f"{volume_path}/{table_name}.parquet"

                # Upload file to Volume via PUT
                cur.execute(
                    f"PUT '{tmp_path}' INTO '{staged}' OVERWRITE"
                )

                # Create target table from Parquet schema
                col_defs = ", ".join(
                    f"{field.name} STRING"
                    for field in arrow_table.schema
                )
                cur.execute(
                    f"CREATE OR REPLACE TABLE {table_name} ({col_defs})"
                )

                # COPY INTO from Volume
                cur.execute(
                    f"COPY INTO {table_name} FROM '{staged}' "
                    f"FILEFORMAT = PARQUET "
                    f"COPY_OPTIONS ('mergeSchema' = 'true')"
                )

                counts[table_name] = arrow_table.num_rows
            finally:
                os.unlink(tmp_path)

        cur.close()
    finally:
        conn.close()

    return counts


def _parse_bigquery_url(connection_string: str) -> dict[str, str]:
    """Parse a ``bigquery://`` URL into BigQuery client kwargs.

    Format: ``bigquery://project_id/dataset?location=US``

    Project ID comes from the hostname, dataset from the first path
    segment, and optional ``location`` from the query string. Uses
    Application Default Credentials (ADC) when no explicit key is
    provided.
    """
    parsed = urlparse(connection_string)
    params: dict[str, str] = {}

    if parsed.hostname:
        params["project"] = parsed.hostname

    # Path: /dataset
    path_parts = [p for p in parsed.path.split("/") if p]
    if path_parts:
        params["dataset"] = path_parts[0]

    # Query string: location
    qs = parse_qs(parsed.query)
    if "location" in qs:
        params["location"] = qs["location"][0]

    return params


def read_arrow_bigquery(table_name: str, connection_string: str) -> "pyarrow.Table":
    """Read a BigQuery table as an Arrow table via the native client.

    Uses ``client.list_rows(table_ref).to_arrow()`` for efficient columnar
    transfer.

    Args:
        table_name: Fully-qualified (``project.dataset.table``) or bare
            table name. If bare, the dataset from the connection string
            is prepended.
        connection_string: ``bigquery://project/dataset?location=US`` URL.

    Returns:
        A ``pyarrow.Table`` with all rows from the table.
    """
    from google.cloud import bigquery

    bq_params = _parse_bigquery_url(connection_string)
    project = bq_params.get("project")
    dataset = bq_params.get("dataset")
    location = bq_params.get("location")

    client = bigquery.Client(project=project, location=location)
    try:
        # Build fully-qualified table reference if needed
        if "." not in table_name and dataset:
            fq_table = f"{project}.{dataset}.{table_name}" if project else f"{dataset}.{table_name}"
        else:
            fq_table = table_name

        table_ref = client.get_table(fq_table)
        rows = client.list_rows(table_ref)
        return rows.to_arrow()
    finally:
        client.close()


def write_parquet_to_bigquery(
    tables: dict[str, "pyarrow.Table"],
    connection_string: str,
    dataset: str | None = None,
) -> dict[str, int]:
    """Write Arrow tables to BigQuery via load jobs.

    For each entry in *tables*, converts the Arrow table to Parquet in a
    temp file and loads it into the target BigQuery table using a load job
    with ``WRITE_TRUNCATE`` disposition.

    Args:
        tables: Mapping of ``{table_name: arrow_table}``.
        connection_string: ``bigquery://project/dataset`` URL.
        dataset: Override dataset name. Defaults to the dataset in the URL.

    Returns:
        Mapping of ``{table_name: row_count}`` written.
    """
    import pyarrow.parquet as pq
    from google.cloud import bigquery

    bq_params = _parse_bigquery_url(connection_string)
    project = bq_params.get("project")
    location = bq_params.get("location")
    if dataset is None:
        dataset = bq_params.get("dataset", "kanoniv_resolved")

    client = bigquery.Client(project=project, location=location)
    # Resolve effective project from client (handles ADC default)
    effective_project = project or client.project
    counts: dict[str, int] = {}

    try:
        # Create dataset if it does not exist
        dataset_ref = bigquery.DatasetReference(effective_project, dataset)
        ds = bigquery.Dataset(dataset_ref)
        if location:
            ds.location = location
        client.create_dataset(ds, exists_ok=True)

        for table_name, arrow_table in tables.items():
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_path = tmp.name

            try:
                pq.write_table(arrow_table, tmp_path, compression="zstd")

                fq_table = f"{effective_project}.{dataset}.{table_name}"

                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                )

                with open(tmp_path, "rb") as f:
                    load_job = client.load_table_from_file(
                        f, fq_table, job_config=job_config
                    )

                load_job.result()  # Wait for completion
                counts[table_name] = arrow_table.num_rows
            finally:
                os.unlink(tmp_path)
    finally:
        client.close()

    return counts


def detect_warehouse_scheme(connection_string: str) -> str:
    """Detect the warehouse type from a connection string.

    Returns ``"snowflake"``, ``"databricks"``, or ``"bigquery"`` based on
    the URL scheme.
    """
    parsed = urlparse(connection_string)
    if parsed.scheme == "databricks":
        return "databricks"
    if parsed.scheme == "bigquery":
        return "bigquery"
    return "snowflake"


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
