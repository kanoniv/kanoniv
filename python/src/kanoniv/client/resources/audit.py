"""Audit resource - list events, entity trail."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .._transport import AsyncTransport, SyncTransport


class AuditResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._t = transport

    def list(
        self,
        *,
        limit: int | None = None,
        offset: int | None = None,
        event_type: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._t.request(
            "GET",
            "/v1/audit",
            params={"limit": limit, "offset": offset, "event_type": event_type},
        )

    def entity_trail(self, entity_id: str) -> list[dict[str, Any]]:
        return self._t.request("GET", f"/v1/audit/entity/{entity_id}")


class AsyncAuditResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._t = transport

    async def list(
        self,
        *,
        limit: int | None = None,
        offset: int | None = None,
        event_type: str | None = None,
    ) -> list[dict[str, Any]]:
        return await self._t.request(
            "GET",
            "/v1/audit",
            params={"limit": limit, "offset": offset, "event_type": event_type},
        )

    async def entity_trail(self, entity_id: str) -> list[dict[str, Any]]:
        return await self._t.request("GET", f"/v1/audit/entity/{entity_id}")
