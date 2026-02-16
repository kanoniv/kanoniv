"""CSV and JSON file adapters (stdlib only, no pandas)."""
from __future__ import annotations

import csv
import json
from typing import Iterator

from ..source import ColumnSchema, SourceSchema


def _infer_type(value: str) -> str:
    """Probe a string value to infer its likely type."""
    if not value:
        return "string"
    if value.lower() in ("true", "false"):
        return "boolean"
    try:
        float(value)
        return "number"
    except ValueError:
        return "string"


def _consensus_type(types: list[str]) -> str:
    """Pick the most specific type that covers all observed values."""
    unique = set(types) - {"string"}
    if len(unique) == 1:
        return unique.pop()
    if unique:
        return "string"  # mixed → fall back to string
    return "string"


class CsvAdapter:
    """Reads a CSV file using the stdlib ``csv`` module."""

    def __init__(self, path: str) -> None:
        self._path = path

    def schema(self) -> SourceSchema:
        columns: list[ColumnSchema] = []
        with open(self._path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                return SourceSchema(columns=[])

            # Sample up to 100 rows for type inference
            sample_rows: list[dict[str, str]] = []
            for i, row in enumerate(reader):
                if i >= 100:
                    break
                sample_rows.append(row)

            for field_name in reader.fieldnames:
                values = [r.get(field_name, "") for r in sample_rows]
                non_empty = [v for v in values if v]
                inferred = _consensus_type([_infer_type(v) for v in non_empty]) if non_empty else "string"
                has_null = any(v == "" for v in values)
                columns.append(
                    ColumnSchema(
                        name=field_name,
                        dtype=inferred,
                        nullable=has_null,
                        sample_values=non_empty[:5],
                    )
                )

        row_count = len(sample_rows) if sample_rows else None
        return SourceSchema(columns=columns, row_count=row_count)

    def iter_rows(self) -> Iterator[dict[str, str]]:
        with open(self._path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield {k: (v or "") for k, v in row.items()}

    def row_count(self) -> int | None:
        return None  # avoid scanning the file twice


class JsonAdapter:
    """Reads a JSON file containing an array of objects."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._data: list[dict] | None = None

    def _load(self) -> list[dict]:
        if self._data is None:
            with open(self._path, encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, list):
                raise ValueError("JSON source must be an array of objects")
            self._data = raw
        return self._data

    def schema(self) -> SourceSchema:
        data = self._load()
        if not data:
            return SourceSchema(columns=[])

        # Collect all keys and sample values from first 100 records
        sample = data[:100]
        all_keys: list[str] = []
        seen: set[str] = set()
        for obj in sample:
            for k in obj:
                if k not in seen:
                    seen.add(k)
                    all_keys.append(k)

        columns: list[ColumnSchema] = []
        for key in all_keys:
            values = [str(obj[key]) for obj in sample if key in obj and obj[key] is not None]
            has_null = any(key not in obj or obj[key] is None for obj in sample)
            inferred = _consensus_type([_infer_type(v) for v in values]) if values else "string"
            columns.append(
                ColumnSchema(
                    name=key,
                    dtype=inferred,
                    nullable=has_null,
                    sample_values=values[:5],
                )
            )

        return SourceSchema(columns=columns, row_count=len(data))

    def iter_rows(self) -> Iterator[dict[str, str]]:
        for obj in self._load():
            yield {str(k): ("" if v is None else str(v)) for k, v in obj.items()}

    def row_count(self) -> int | None:
        return len(self._load())
