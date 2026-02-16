"""DuckDB-based staging for Cloud reconciliation.

Reads warehouse sources via Arrow, unifies them in DuckDB with canonical
column mappings, and exports as Parquet for bulk upload.

Requires the ``dataplane`` extra: ``pip install kanoniv[cloud,dataplane]``.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow

    from .source import Source
    from .spec import Spec


def stage_sources(
    sources: list[Source],
    spec: Spec,
    connection_string: str,
) -> "pyarrow.Table":
    """Read warehouse sources, UNION via DuckDB, return a unified Arrow table.

    Steps:
        1. For each source, read Arrow table via ``read_arrow()``.
        2. Register each in an in-memory DuckDB instance.
        3. Generate ``UNION ALL`` SQL with spec attribute mappings.
        4. Add ``external_id``, ``source_name`` columns.
        5. ``ORDER BY source_name, external_id`` (determinism).
        6. Return the unified ``pyarrow.Table``.

    Args:
        sources: List of warehouse sources to read.
        spec: Identity spec with attribute mappings per source.
        connection_string: Snowflake connection string.

    Returns:
        A unified ``pyarrow.Table`` with canonical column names.
    """
    import duckdb

    from .cloud_io import read_arrow

    # Build attribute mappings from spec: {source_name: {source_col: canonical_col}}
    attr_maps: dict[str, dict[str, str]] = {}
    for spec_src in spec.sources:
        src_name = spec_src.get("name", "")
        attrs = spec_src.get("attributes", {})
        if attrs and src_name:
            # spec has canonical -> source mapping; we need source -> canonical
            attr_maps[src_name] = {v: k for k, v in attrs.items()}

    # Determine primary key field per source
    pk_map: dict[str, str | None] = {s.name: s.primary_key for s in sources}

    # Determine canonical columns from spec (union of all sources' attributes)
    seen: set[str] = set()
    canonical_cols: list[str] = []
    for spec_src in spec.sources:
        for col in spec_src.get("attributes", {}).keys():
            if col not in seen:
                seen.add(col)
                canonical_cols.append(col)

    con = duckdb.connect()
    union_parts: list[str] = []

    for source in sources:
        # Find the table name from the adapter
        adapter = source._adapter
        table_name = getattr(adapter, "_table", source.name)

        # Read Arrow table from Snowflake
        arrow_tbl = read_arrow(table_name, connection_string)
        # Snowflake returns UPPERCASE column names; normalize to lowercase
        arrow_tbl = arrow_tbl.rename_columns(
            [c.lower() for c in arrow_tbl.column_names]
        )
        view_name = f"src_{source.name}"
        con.register(view_name, arrow_tbl)

        # Build SELECT with column mappings
        reverse_map = attr_maps.get(source.name, {})
        pk = pk_map.get(source.name)

        select_parts: list[str] = []
        for canon_col in canonical_cols:
            # Find the source column that maps to this canonical column
            source_col: str | None = None
            for src_c, can_c in reverse_map.items():
                if can_c == canon_col:
                    source_col = src_c
                    break

            if source_col:
                select_parts.append(f'CAST("{source_col}" AS VARCHAR) AS "{canon_col}"')
            else:
                # Column doesn't exist in this source - NULL
                select_parts.append(f'NULL AS "{canon_col}"')

        # Add external_id and source_name
        if pk:
            select_parts.append(f'CAST("{pk}" AS VARCHAR) AS "external_id"')
        else:
            select_parts.append("CAST(ROW_NUMBER() OVER () AS VARCHAR) AS \"external_id\"")
        select_parts.append(f"'{source.name}' AS \"source_name\"")

        select_sql = ", ".join(select_parts)
        union_parts.append(f'SELECT {select_sql} FROM "{view_name}"')

    union_sql = " UNION ALL ".join(union_parts)
    final_sql = f"{union_sql} ORDER BY source_name, external_id"

    result = con.execute(final_sql).fetch_arrow_table()
    con.close()
    return result


def export_parquet(table: "pyarrow.Table", path: str) -> str:
    """Write an Arrow table to a Parquet file with zstd compression.

    Args:
        table: Arrow table to write.
        path: Output file path.

    Returns:
        The output file path.
    """
    import pyarrow.parquet as pq

    pq.write_table(table, path, compression="zstd")
    return path
