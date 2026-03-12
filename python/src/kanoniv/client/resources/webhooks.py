"""Webhooks resource - create, list, get, delete, test, and view deliveries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .._transport import AsyncTransport, SyncTransport


class WebhooksResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._t = transport

    def create(
        self,
        *,
        url: str,
        event_types: list[str],
        description: str | None = None,
    ) -> dict[str, Any]:
        """Create a webhook subscription.

        The response includes the signing secret (shown only once).

        Args:
            url: HTTPS endpoint to receive events.
            event_types: Events to subscribe to (e.g. ``["entity.merged"]``).
            description: Optional human-readable label.
        """
        body: dict[str, Any] = {"url": url, "event_types": event_types}
        if description is not None:
            body["description"] = description
        return self._t.request("POST", "/v1/outbound-webhooks", json=body)

    def list(self) -> list[dict[str, Any]]:
        """List all webhooks for the current tenant (secrets masked)."""
        return self._t.request("GET", "/v1/outbound-webhooks")

    def get(self, id: str) -> dict[str, Any]:
        """Get a single webhook by ID (secret masked)."""
        return self._t.request("GET", f"/v1/outbound-webhooks/{id}")

    def delete(self, id: str) -> None:
        """Deactivate a webhook."""
        self._t.request("DELETE", f"/v1/outbound-webhooks/{id}")

    def test(self, id: str) -> dict[str, Any]:
        """Send a test event to verify the webhook endpoint."""
        return self._t.request("POST", f"/v1/outbound-webhooks/{id}/test")

    def deliveries(
        self,
        id: str,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        """List recent deliveries for a webhook.

        Args:
            id: Webhook ID.
            limit: Max results (default 50, max 200).
            offset: Pagination offset.
        """
        return self._t.request(
            "GET",
            f"/v1/outbound-webhooks/{id}/deliveries",
            params={"limit": limit, "offset": offset},
        )


class AsyncWebhooksResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._t = transport

    async def create(
        self,
        *,
        url: str,
        event_types: list[str],
        description: str | None = None,
    ) -> dict[str, Any]:
        """Create a webhook subscription.

        The response includes the signing secret (shown only once).

        Args:
            url: HTTPS endpoint to receive events.
            event_types: Events to subscribe to (e.g. ``["entity.merged"]``).
            description: Optional human-readable label.
        """
        body: dict[str, Any] = {"url": url, "event_types": event_types}
        if description is not None:
            body["description"] = description
        return await self._t.request("POST", "/v1/outbound-webhooks", json=body)

    async def list(self) -> list[dict[str, Any]]:
        """List all webhooks for the current tenant (secrets masked)."""
        return await self._t.request("GET", "/v1/outbound-webhooks")

    async def get(self, id: str) -> dict[str, Any]:
        """Get a single webhook by ID (secret masked)."""
        return await self._t.request("GET", f"/v1/outbound-webhooks/{id}")

    async def delete(self, id: str) -> None:
        """Deactivate a webhook."""
        await self._t.request("DELETE", f"/v1/outbound-webhooks/{id}")

    async def test(self, id: str) -> dict[str, Any]:
        """Send a test event to verify the webhook endpoint."""
        return await self._t.request("POST", f"/v1/outbound-webhooks/{id}/test")

    async def deliveries(
        self,
        id: str,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        """List recent deliveries for a webhook.

        Args:
            id: Webhook ID.
            limit: Max results (default 50, max 200).
            offset: Pagination offset.
        """
        return await self._t.request(
            "GET",
            f"/v1/outbound-webhooks/{id}/deliveries",
            params={"limit": limit, "offset": offset},
        )
