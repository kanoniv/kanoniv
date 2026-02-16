"""Warehouse adapter using SQLAlchemy for streaming reads."""
from __future__ import annotations

from typing import Any, Iterator

from ..source import ColumnSchema, SourceSchema


def _sql_type_to_string(sql_type: Any) -> str:
    """Map a SQLAlchemy column type to a Kanoniv schema type string."""
    name = type(sql_type).__name__.upper()
    if name in ("INTEGER", "BIGINT", "SMALLINT", "NUMERIC", "FLOAT", "REAL", "DOUBLE", "DECIMAL"):
        return "number"
    if name in ("BOOLEAN",):
        return "boolean"
    if name in ("DATE", "DATETIME", "TIMESTAMP"):
        return "date"
    return "string"


class WarehouseAdapter:
    """Adapter that reads from a warehouse table via SQLAlchemy.

    The connection is **not** opened until ``schema()`` or ``iter_rows()``
    is called, keeping construction cheap.
    """

    def __init__(
        self,
        table: str,
        *,
        connection_string: str,
        batch_size: int = 5000,
        **extra: Any,
    ) -> None:
        try:
            import sqlalchemy  # noqa: F401
        except ImportError:
            raise ImportError(
                "Source.from_warehouse() requires sqlalchemy: pip install sqlalchemy"
            ) from None

        self._table = table
        self._connection_string = connection_string
        self._batch_size = batch_size
        self._extra = extra
        self._engine: Any = None

    def _get_engine(self) -> Any:
        from sqlalchemy import create_engine

        if self._engine is None:
            self._engine = create_engine(self._connection_string)
        return self._engine

    def schema(self) -> SourceSchema:
        from sqlalchemy import inspect as sa_inspect

        engine = self._get_engine()
        inspector = sa_inspect(engine)

        # Parse schema.table if present
        parts = self._table.rsplit(".", 1)
        schema_name = parts[0] if len(parts) == 2 else None
        table_name = parts[-1]

        columns: list[ColumnSchema] = []
        for col in inspector.get_columns(table_name, schema=schema_name):
            columns.append(
                ColumnSchema(
                    name=col["name"],
                    dtype=_sql_type_to_string(col["type"]),
                    nullable=col.get("nullable", True),
                )
            )

        return SourceSchema(columns=columns)

    def iter_rows(self) -> Iterator[dict[str, str]]:
        from sqlalchemy import text

        engine = self._get_engine()
        with engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(
                text(f"SELECT * FROM {self._table}")  # noqa: S608
            )
            while True:
                batch = result.fetchmany(self._batch_size)
                if not batch:
                    break
                keys = result.keys()
                for row in batch:
                    yield {str(k): ("" if v is None else str(v)) for k, v in zip(keys, row)}

    def row_count(self) -> int | None:
        return None  # avoid expensive COUNT(*)
