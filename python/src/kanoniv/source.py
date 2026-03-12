"""Source adapters for local identity resolution."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator, Protocol, runtime_checkable


@dataclass
class ColumnSchema:
    """Schema description of a single column."""

    name: str
    dtype: str  # "string" | "number" | "boolean" | "date"
    nullable: bool = True
    sample_values: list[str] = field(default_factory=list)


@dataclass
class SourceSchema:
    """Schema description of a source."""

    columns: list[ColumnSchema]
    primary_key: str | None = None
    row_count: int | None = None


@runtime_checkable
class SourceAdapter(Protocol):
    """Protocol that all adapters must implement."""

    def schema(self) -> SourceSchema: ...
    def iter_rows(self) -> Iterator[dict[str, str]]: ...
    def row_count(self) -> int | None: ...


class Source:
    """A data source for identity resolution.

    Create via factory class methods: ``from_pandas``, ``from_polars``,
    ``from_arrow``, ``from_duckdb``, ``from_csv``, ``from_json``,
    ``from_warehouse``, ``from_dbt``.
    """

    def __init__(self, name: str, adapter: SourceAdapter, primary_key: str | None = None):
        self.name = name
        self.primary_key = primary_key
        self._adapter = adapter

    # -- Factory methods ----------------------------------------------------

    @classmethod
    def from_pandas(cls, name: str, df: Any, primary_key: str | None = None) -> Source:
        """Create a source from a pandas DataFrame."""
        from .adapters.pandas import PandasAdapter

        return cls(name, PandasAdapter(df), primary_key=primary_key)

    @classmethod
    def from_polars(cls, name: str, df: Any, primary_key: str | None = None) -> Source:
        """Create a source from a Polars DataFrame."""
        from .adapters.polars import PolarsAdapter

        return cls(name, PolarsAdapter(df), primary_key=primary_key)

    @classmethod
    def from_arrow(cls, name: str, table: Any, primary_key: str | None = None) -> Source:
        """Create a source from a PyArrow Table."""
        from .adapters.arrow import ArrowAdapter

        return cls(name, ArrowAdapter(table), primary_key=primary_key)

    @classmethod
    def from_duckdb(
        cls,
        name: str,
        connection: Any,
        query: str,
        primary_key: str | None = None,
    ) -> Source:
        """Create a source from a DuckDB connection and SQL query."""
        from .adapters.duckdb import DuckDBAdapter

        return cls(name, DuckDBAdapter(connection, query), primary_key=primary_key)

    @classmethod
    def from_csv(cls, name: str, path: str, primary_key: str | None = None) -> Source:
        """Create a source from a CSV file."""
        from .adapters.file import CsvAdapter

        return cls(name, CsvAdapter(path), primary_key=primary_key)

    @classmethod
    def from_json(cls, name: str, path: str, primary_key: str | None = None) -> Source:
        """Create a source from a JSON array file."""
        from .adapters.file import JsonAdapter

        return cls(name, JsonAdapter(path), primary_key=primary_key)

    @classmethod
    def from_warehouse(
        cls,
        name: str,
        table: str,
        connection_string: str,
        **kw: Any,
    ) -> Source:
        """Create a source from a warehouse table (requires ``sqlalchemy``)."""
        from .adapters.warehouse import WarehouseAdapter

        return cls(
            name,
            WarehouseAdapter(table, connection_string=connection_string, **kw),
            primary_key=kw.get("primary_key"),
        )

    @classmethod
    def from_dbt(
        cls,
        name: str,
        model: str,
        manifest_path: str = "target/manifest.json",
        **kw: Any,
    ) -> Source:
        """Create a source from a dbt model (requires ``sqlalchemy``)."""
        from .adapters.dbt import DbtAdapter

        return cls(
            name,
            DbtAdapter(model, manifest_path=manifest_path, **kw),
            primary_key=kw.get("primary_key"),
        )

    @classmethod
    def _from_rows(cls, name: str, rows: list[dict[str, str]], primary_key: str | None = None) -> Source:
        """Internal: create a source from a pre-materialized list of row dicts."""
        return cls(name, _ListAdapter(rows), primary_key=primary_key)

    # -- Properties ---------------------------------------------------------

    @property
    def connection_string(self) -> str | None:
        """Return the warehouse connection string, or None for non-warehouse sources."""
        adapter = self._adapter
        return getattr(adapter, "_connection_string", None)

    # -- Delegated methods --------------------------------------------------

    def schema(self) -> SourceSchema:
        """Return the inferred schema of this source."""
        return self._adapter.schema()

    def iter_rows(self) -> Iterator[dict[str, str]]:
        """Iterate over rows, yielding ``{column: value}`` dicts with all values stringified."""
        return self._adapter.iter_rows()

    # -- Entity bridge ------------------------------------------------------

    def to_entities(self, entity_type: str, tenant_id: str | None = None) -> list[dict[str, Any]]:
        """Convert adapter rows into ``NormalizedEntity``-compatible dicts.

        Each dict has the shape expected by ``reconcile_local``:
        ``{id, tenant_id, external_id, entity_type, data, source_name, last_updated}``.
        """
        tid = tenant_id or "00000000-0000-0000-0000-000000000000"
        now = datetime.now(timezone.utc).isoformat()
        entities: list[dict[str, Any]] = []

        for row in self.iter_rows():
            ext_id = row.get(self.primary_key, "") if self.primary_key else ""
            if not ext_id:
                ext_id = str(uuid.uuid4())

            entities.append(
                {
                    "id": str(uuid.uuid4()),
                    "tenant_id": tid,
                    "external_id": ext_id,
                    "entity_type": entity_type,
                    "data": row,
                    "source_name": self.name,
                    "valid_from": None,
                    "valid_to": None,
                    "last_updated": now,
                }
            )

        return entities


class _ListAdapter:
    """Adapter that wraps a pre-materialized list of row dicts."""

    def __init__(self, rows: list[dict[str, str]]):
        self._rows = rows

    def schema(self) -> SourceSchema:
        columns = []
        if self._rows:
            for col in self._rows[0]:
                columns.append(ColumnSchema(name=col, dtype="string"))
        return SourceSchema(columns=columns, row_count=len(self._rows))

    def iter_rows(self) -> Iterator[dict[str, str]]:
        return iter(self._rows)

    def row_count(self) -> int | None:
        return len(self._rows)
