"""PyArrow Table adapter."""
from __future__ import annotations

from typing import Any, Iterator

from ..source import ColumnSchema, SourceSchema


def _arrow_type_to_string(arrow_type: Any) -> str:
    """Map a PyArrow type to a Kanoniv schema type string."""
    import pyarrow as pa

    if pa.types.is_integer(arrow_type) or pa.types.is_floating(arrow_type) or pa.types.is_decimal(arrow_type):
        return "number"
    if pa.types.is_boolean(arrow_type):
        return "boolean"
    if pa.types.is_date(arrow_type) or pa.types.is_time(arrow_type) or pa.types.is_timestamp(arrow_type):
        return "date"
    return "string"


class ArrowAdapter:
    """Adapter that reads from a PyArrow Table."""

    def __init__(self, table: Any) -> None:
        try:
            import pyarrow as pa
        except ImportError:
            raise ImportError(
                "Source.from_arrow() requires pyarrow: pip install pyarrow"
            ) from None

        if not isinstance(table, pa.Table):
            raise TypeError(f"Expected pyarrow Table, got {type(table).__name__}")
        self._table = table

    def schema(self) -> SourceSchema:
        pa_schema = self._table.schema
        columns = []
        for i in range(len(pa_schema)):
            col_field = pa_schema.field(i)
            col = self._table.column(i)
            columns.append(
                ColumnSchema(
                    name=col_field.name,
                    dtype=_arrow_type_to_string(col_field.type),
                    nullable=bool(col.null_count > 0),
                    sample_values=[
                        str(v.as_py())
                        for v in col.drop_null().slice(0, 5)
                    ],
                )
            )
        return SourceSchema(columns=columns, row_count=self._table.num_rows)

    def iter_rows(self) -> Iterator[dict[str, str]]:
        columns = self._table.column_names
        for batch in self._table.to_batches():
            for row_idx in range(batch.num_rows):
                yield {
                    col: "" if batch.column(col)[row_idx].as_py() is None else str(batch.column(col)[row_idx].as_py())
                    for col in columns
                }

    def row_count(self) -> int:
        return self._table.num_rows
