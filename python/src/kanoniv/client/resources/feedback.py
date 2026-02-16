"""Feedback resource - list, create, delete feedback labels for active learning."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .._transport import AsyncTransport, SyncTransport


class FeedbackResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._t = transport

    def list(
        self,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        """List feedback labels for the current tenant."""
        return self._t.request(
            "GET",
            "/v1/feedback",
            params={"limit": limit, "offset": offset},
        )

    def create(
        self,
        *,
        labels: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Create feedback labels (batch).

        Each label dict should have: entity_a_id, entity_b_id, source_a,
        source_b, label ('match' or 'no_match'), and optionally reason.
        """
        return self._t.request(
            "POST",
            "/v1/feedback",
            json={"labels": labels},
        )

    def delete(self, id: str) -> None:
        """Delete a feedback label by ID."""
        self._t.request("DELETE", f"/v1/feedback/{id}")


class AsyncFeedbackResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._t = transport

    async def list(
        self,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        """List feedback labels for the current tenant."""
        return await self._t.request(
            "GET",
            "/v1/feedback",
            params={"limit": limit, "offset": offset},
        )

    async def create(
        self,
        *,
        labels: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Create feedback labels (batch).

        Each label dict should have: entity_a_id, entity_b_id, source_a,
        source_b, label ('match' or 'no_match'), and optionally reason.
        """
        return await self._t.request(
            "POST",
            "/v1/feedback",
            json={"labels": labels},
        )

    async def delete(self, id: str) -> None:
        """Delete a feedback label by ID."""
        await self._t.request("DELETE", f"/v1/feedback/{id}")
