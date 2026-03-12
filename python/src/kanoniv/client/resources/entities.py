"""Entities resource — search, get, linked, history."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .._transport import AsyncTransport, SyncTransport


class EntitiesResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._t = transport

    def search(
        self,
        *,
        q: str | None = None,
        entity_type: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> dict[str, Any]:
        return self._t.request(
            "GET",
            "/v1/entities",
            params={"q": q, "entity_type": entity_type, "limit": limit, "offset": offset},
        )

    def get(self, id: str) -> dict[str, Any]:
        return self._t.request("GET", f"/v1/canonical/{id}")

    def get_linked(self, id: str) -> dict[str, Any]:
        return self._t.request("GET", f"/v1/canonical/{id}/linked")

    def get_linked_bulk(self, entity_ids: list[str]) -> dict[str, Any]:
        """Fetch linked entities for multiple canonical entities in one call.

        Args:
            entity_ids: Up to 1000 canonical entity IDs.

        Returns:
            ``{"results": {"id1": [linked...], "id2": [linked...], ...}}``
        """
        return self._t.request(
            "POST", "/v1/entities/linked/bulk", json={"entity_ids": entity_ids}
        )

    def history(self, id: str) -> dict[str, Any]:
        return self._t.request("GET", f"/v1/entities/{id}/history")

    def lock(self, id: str) -> dict[str, Any]:
        return self._t.request("POST", f"/v1/entities/{id}/lock")

    def revert(self, id: str, event_id: str) -> dict[str, Any]:
        return self._t.request("POST", f"/v1/entities/{id}/revert/{event_id}")


class AsyncEntitiesResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._t = transport

    async def search(
        self,
        *,
        q: str | None = None,
        entity_type: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> dict[str, Any]:
        return await self._t.request(
            "GET",
            "/v1/entities",
            params={"q": q, "entity_type": entity_type, "limit": limit, "offset": offset},
        )

    async def get(self, id: str) -> dict[str, Any]:
        return await self._t.request("GET", f"/v1/canonical/{id}")

    async def get_linked(self, id: str) -> dict[str, Any]:
        return await self._t.request("GET", f"/v1/canonical/{id}/linked")

    async def get_linked_bulk(self, entity_ids: list[str]) -> dict[str, Any]:
        """Fetch linked entities for multiple canonical entities in one call.

        Args:
            entity_ids: Up to 1000 canonical entity IDs.

        Returns:
            ``{"results": {"id1": [linked...], "id2": [linked...], ...}}``
        """
        return await self._t.request(
            "POST", "/v1/entities/linked/bulk", json={"entity_ids": entity_ids}
        )

    async def history(self, id: str) -> dict[str, Any]:
        return await self._t.request("GET", f"/v1/entities/{id}/history")

    async def lock(self, id: str) -> dict[str, Any]:
        return await self._t.request("POST", f"/v1/entities/{id}/lock")

    async def revert(self, id: str, event_id: str) -> dict[str, Any]:
        return await self._t.request("POST", f"/v1/entities/{id}/revert/{event_id}")
