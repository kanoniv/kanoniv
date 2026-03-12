"""Review queue resource - list pending, decide."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .._transport import AsyncTransport, SyncTransport


class ReviewsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._t = transport

    def list(
        self,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._t.request(
            "GET",
            "/v1/resolve/pending",
            params={"limit": limit, "offset": offset},
        )

    def decide(
        self,
        *,
        entity_a_id: str,
        entity_b_id: str,
        decision: str,
        reason: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "entity_a_id": entity_a_id,
            "entity_b_id": entity_b_id,
            "decision": decision,
        }
        if reason is not None:
            body["reason"] = reason
        return self._t.request("POST", "/v1/resolve/quick", json=body)


class AsyncReviewsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._t = transport

    async def list(
        self,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        return await self._t.request(
            "GET",
            "/v1/resolve/pending",
            params={"limit": limit, "offset": offset},
        )

    async def decide(
        self,
        *,
        entity_a_id: str,
        entity_b_id: str,
        decision: str,
        reason: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "entity_a_id": entity_a_id,
            "entity_b_id": entity_b_id,
            "decision": decision,
        }
        if reason is not None:
            body["reason"] = reason
        return await self._t.request("POST", "/v1/resolve/quick", json=body)
