"""dbt model adapter - resolves ref() to warehouse tables."""
from __future__ import annotations

import json
import re
from typing import Any, Iterator

from ..source import ColumnSchema, SourceSchema
from .warehouse import WarehouseAdapter


def _strip_ref(model: str) -> str:
    """Normalise ``ref('name')``, ``ref("name")``, or bare ``name`` to just ``name``."""
    m = re.match(r"""ref\(\s*['"](.+?)['"]\s*\)""", model)
    if m:
        return m.group(1)
    return model


class DbtAdapter:
    """Adapter that resolves a dbt model name via ``manifest.json`` and reads from the warehouse.

    Requires a ``connection_string`` kwarg for the underlying warehouse read.
    """

    def __init__(
        self,
        model: str,
        *,
        manifest_path: str = "target/manifest.json",
        connection_string: str | None = None,
        **extra: Any,
    ) -> None:
        self._model_name = _strip_ref(model)
        self._manifest_path = manifest_path
        self._connection_string = connection_string
        self._extra = extra
        self._warehouse: WarehouseAdapter | None = None
        self._manifest: dict | None = None

    def _load_manifest(self) -> dict:
        if self._manifest is None:
            with open(self._manifest_path, encoding="utf-8") as f:
                self._manifest = json.load(f)
        return self._manifest

    def _resolve_table(self) -> str:
        """Resolve the model name to ``database.schema.alias``."""
        manifest = self._load_manifest()
        nodes = manifest.get("nodes", {})

        for node in nodes.values():
            if node.get("resource_type") != "model":
                continue
            if node.get("name") == self._model_name:
                db = node.get("database", "")
                schema = node.get("schema", "")
                alias = node.get("alias") or node.get("name", "")
                parts = [p for p in (db, schema, alias) if p]
                return ".".join(parts)

        raise ValueError(
            f"Model '{self._model_name}' not found in dbt manifest at {self._manifest_path}"
        )

    def _get_warehouse(self) -> WarehouseAdapter:
        if self._warehouse is None:
            table = self._resolve_table()
            self._warehouse = WarehouseAdapter(
                table,
                connection_string=self._connection_string,
                **self._extra,
            )
        return self._warehouse

    def schema(self) -> SourceSchema:
        manifest = self._load_manifest()
        nodes = manifest.get("nodes", {})

        for node in nodes.values():
            if node.get("resource_type") != "model":
                continue
            if node.get("name") == self._model_name:
                node_columns = node.get("columns", {})
                if node_columns:
                    columns = [
                        ColumnSchema(
                            name=col_info.get("name", col_key),
                            dtype=col_info.get("data_type", "string") or "string",
                            nullable=True,
                        )
                        for col_key, col_info in node_columns.items()
                    ]
                    return SourceSchema(columns=columns)

        # Fall back to warehouse schema reflection
        return self._get_warehouse().schema()

    def iter_rows(self) -> Iterator[dict[str, str]]:
        return self._get_warehouse().iter_rows()

    def row_count(self) -> int | None:
        return None
