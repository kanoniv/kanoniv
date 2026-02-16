"""Pandas DataFrame adapter."""
from __future__ import annotations

import math
from typing import Any, Iterator

from ..source import ColumnSchema, SourceSchema


def _dtype_to_string(dtype: Any) -> str:
    """Map a pandas dtype to a Kanoniv schema type string."""
    name = str(dtype)
    if "int" in name or "float" in name:
        return "number"
    if "bool" in name:
        return "boolean"
    if "datetime" in name:
        return "date"
    return "string"


class PandasAdapter:
    """Adapter that reads from a pandas DataFrame."""

    def __init__(self, df: Any) -> None:
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "Source.from_pandas() requires pandas: pip install pandas"
            ) from None

        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Expected pandas DataFrame, got {type(df).__name__}")
        self._df = df

    def schema(self) -> SourceSchema:
        columns = [
            ColumnSchema(
                name=col,
                dtype=_dtype_to_string(self._df[col].dtype),
                nullable=bool(self._df[col].isnull().any()),
                sample_values=[str(v) for v in self._df[col].dropna().head(5).tolist()],
            )
            for col in self._df.columns
        ]
        return SourceSchema(columns=columns, row_count=len(self._df))

    def iter_rows(self) -> Iterator[dict[str, str]]:
        for _, row in self._df.iterrows():
            yield {
                col: "" if (isinstance(v, float) and math.isnan(v)) or v is None else str(v)
                for col, v in row.items()
            }

    def row_count(self) -> int:
        return len(self._df)
