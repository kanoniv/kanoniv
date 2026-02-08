"""Sources resource — CRUD, sync, preview."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .._transport import AsyncTransport, SyncTransport


class SourcesResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._t = transport

    def list(self) -> list[dict[str, Any]]:
        return self._t.request("GET", "/v1/sources")

    def get(self, id: str) -> dict[str, Any]:
        return self._t.request("GET", f"/v1/sources/{id}")

    def create(
        self,
        *,
        name: str,
        source_type: str,
        config: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"name": name, "source_type": source_type, **kwargs}
        if config is not None:
            body["config"] = config
        return self._t.request("POST", "/v1/sources", json=body)

    def update(
        self,
        id: str,
        *,
        name: str | None = None,
        config: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {**kwargs}
        if name is not None:
            body["name"] = name
        if config is not None:
            body["config"] = config
        return self._t.request("PUT", f"/v1/sources/{id}", json=body)

    def delete(self, id: str) -> None:
        self._t.request("DELETE", f"/v1/sources/{id}")

    def sync(self, id: str) -> dict[str, Any]:
        return self._t.request("POST", f"/v1/sources/{id}/sync")

    def preview(self, id: str) -> dict[str, Any]:
        return self._t.request("GET", f"/v1/ingest/sources/{id}/preview")

    def get_mapping(self, id: str) -> dict[str, Any]:
        return self._t.request("GET", f"/v1/ingest/sources/{id}/mapping")

    def upsert_mapping(self, mapping: dict[str, Any]) -> dict[str, Any]:
        return self._t.request("POST", "/v1/ingest/sources/mapping", json=mapping)


class AsyncSourcesResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._t = transport

    async def list(self) -> list[dict[str, Any]]:
        return await self._t.request("GET", "/v1/sources")

    async def get(self, id: str) -> dict[str, Any]:
        return await self._t.request("GET", f"/v1/sources/{id}")

    async def create(
        self,
        *,
        name: str,
        source_type: str,
        config: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"name": name, "source_type": source_type, **kwargs}
        if config is not None:
            body["config"] = config
        return await self._t.request("POST", "/v1/sources", json=body)

    async def update(
        self,
        id: str,
        *,
        name: str | None = None,
        config: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {**kwargs}
        if name is not None:
            body["name"] = name
        if config is not None:
            body["config"] = config
        return await self._t.request("PUT", f"/v1/sources/{id}", json=body)

    async def delete(self, id: str) -> None:
        await self._t.request("DELETE", f"/v1/sources/{id}")

    async def sync(self, id: str) -> dict[str, Any]:
        return await self._t.request("POST", f"/v1/sources/{id}/sync")

    async def preview(self, id: str) -> dict[str, Any]:
        return await self._t.request("GET", f"/v1/ingest/sources/{id}/preview")

    async def get_mapping(self, id: str) -> dict[str, Any]:
        return await self._t.request("GET", f"/v1/ingest/sources/{id}/mapping")

    async def upsert_mapping(self, mapping: dict[str, Any]) -> dict[str, Any]:
        return await self._t.request("POST", "/v1/ingest/sources/mapping", json=mapping)
