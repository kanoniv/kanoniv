"""DuckDB adapter."""
from __future__ import annotations

from typing import Any, Iterator

from ..source import ColumnSchema, SourceSchema


def _duckdb_type_to_string(type_str: str) -> str:
    """Map a DuckDB type string to a Kanoniv schema type string."""
    t = type_str.upper()
    if any(kw in t for kw in ("INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL", "SMALLINT", "BIGINT", "TINYINT", "HUGEINT")):
        return "number"
    if "BOOL" in t:
        return "boolean"
    if any(kw in t for kw in ("DATE", "TIME", "TIMESTAMP", "INTERVAL")):
        return "date"
    return "string"


class DuckDBAdapter:
    """Adapter that reads from a DuckDB connection via SQL query or table name."""

    def __init__(self, connection: Any, query: str) -> None:
        try:
            import duckdb
        except ImportError:
            raise ImportError(
                "Source.from_duckdb() requires duckdb: pip install duckdb"
            ) from None

        if not isinstance(connection, duckdb.DuckDBPyConnection):
            raise TypeError(f"Expected duckdb.DuckDBPyConnection, got {type(connection).__name__}")
        self._con = connection
        # If the query looks like a bare table name, wrap it in SELECT *
        stripped = query.strip()
        if " " not in stripped and "(" not in stripped:
            self._query = f"SELECT * FROM {stripped}"
        else:
            self._query = query

    def schema(self) -> SourceSchema:
        result = self._con.execute(f"SELECT * FROM ({self._query}) _kv LIMIT 5")
        desc = result.description
        sample_rows = result.fetchall()

        columns = []
        for i, col_desc in enumerate(desc):
            col_name = col_desc[0]
            col_type = col_desc[1] if len(col_desc) > 1 else "VARCHAR"
            samples = [str(row[i]) for row in sample_rows if row[i] is not None]
            has_null = any(row[i] is None for row in sample_rows)
            columns.append(
                ColumnSchema(
                    name=col_name,
                    dtype=_duckdb_type_to_string(str(col_type)),
                    nullable=has_null,
                    sample_values=samples[:5],
                )
            )
        return SourceSchema(columns=columns)

    def iter_rows(self) -> Iterator[dict[str, str]]:
        result = self._con.execute(self._query)
        col_names = [d[0] for d in result.description]
        while True:
            chunk = result.fetchmany(1000)
            if not chunk:
                break
            for row in chunk:
                yield {
                    col: "" if val is None else str(val)
                    for col, val in zip(col_names, row)
                }

    def row_count(self) -> int | None:
        return None  # Avoid expensive COUNT(*)
