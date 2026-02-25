"""Polars DataFrame adapter."""
from __future__ import annotations

from typing import Any, Iterator

from ..source import ColumnSchema, SourceSchema


def _polars_dtype_to_string(dtype: Any) -> str:
    """Map a Polars dtype to a Kanoniv schema type string."""
    name = str(dtype)
    if "Int" in name or "UInt" in name or "Float" in name or "Decimal" in name:
        return "number"
    if "Boolean" in name:
        return "boolean"
    if "Date" in name or "Time" in name or "Datetime" in name or "Duration" in name:
        return "date"
    return "string"


class PolarsAdapter:
    """Adapter that reads from a Polars DataFrame."""

    def __init__(self, df: Any) -> None:
        try:
            import polars as pl
        except ImportError:
            raise ImportError(
                "Source.from_polars() requires polars: pip install polars"
            ) from None

        if not isinstance(df, pl.DataFrame):
            raise TypeError(f"Expected polars DataFrame, got {type(df).__name__}")
        self._df = df

    def schema(self) -> SourceSchema:
        columns = [
            ColumnSchema(
                name=col,
                dtype=_polars_dtype_to_string(self._df[col].dtype),
                nullable=bool(self._df[col].null_count() > 0),
                sample_values=[str(v) for v in self._df[col].drop_nulls().head(5).to_list()],
            )
            for col in self._df.columns
        ]
        return SourceSchema(columns=columns, row_count=len(self._df))

    def iter_rows(self) -> Iterator[dict[str, str]]:
        for row in self._df.iter_rows(named=True):
            yield {
                col: "" if v is None else str(v)
                for col, v in row.items()
            }

    def row_count(self) -> int:
        return len(self._df)
