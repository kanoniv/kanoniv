"""Overrides resource — list, create, delete."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .._transport import AsyncTransport, SyncTransport


class OverridesResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._t = transport

    def list(self) -> list[dict[str, Any]]:
        return self._t.request("GET", "/v1/overrides")

    def create(
        self,
        *,
        override_type: str,
        entity_a_id: str,
        entity_b_id: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "override_type": override_type,
            "entity_a_id": entity_a_id,
            "entity_b_id": entity_b_id,
            **kwargs,
        }
        return self._t.request("POST", "/v1/overrides", json=body)

    def delete(self, id: str) -> None:
        self._t.request("DELETE", f"/v1/overrides/{id}")


class AsyncOverridesResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._t = transport

    async def list(self) -> list[dict[str, Any]]:
        return await self._t.request("GET", "/v1/overrides")

    async def create(
        self,
        *,
        override_type: str,
        entity_a_id: str,
        entity_b_id: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "override_type": override_type,
            "entity_a_id": entity_a_id,
            "entity_b_id": entity_b_id,
            **kwargs,
        }
        return await self._t.request("POST", "/v1/overrides", json=body)

    async def delete(self, id: str) -> None:
        await self._t.request("DELETE", f"/v1/overrides/{id}")
