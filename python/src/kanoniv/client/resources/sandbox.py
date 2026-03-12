"""Sandbox resource - check status and reset sandbox environments."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .._transport import AsyncTransport, SyncTransport


class SandboxResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._t = transport

    def status(self) -> dict[str, Any]:
        """Get sandbox usage and limits.

        Returns:
            Dict with ``is_sandbox``, ``entity_count``, ``entity_limit``,
            and ``source_count``.
        """
        return self._t.request("GET", "/v1/sandbox/status")

    def reset(self) -> dict[str, Any]:
        """Delete all data in the sandbox and start fresh.

        Returns:
            Dict with ``message`` and ``entities_deleted`` count.
        """
        return self._t.request("POST", "/v1/sandbox/reset")


class AsyncSandboxResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._t = transport

    async def status(self) -> dict[str, Any]:
        """Get sandbox usage and limits.

        Returns:
            Dict with ``is_sandbox``, ``entity_count``, ``entity_limit``,
            and ``source_count``.
        """
        return await self._t.request("GET", "/v1/sandbox/status")

    async def reset(self) -> dict[str, Any]:
        """Delete all data in the sandbox and start fresh.

        Returns:
            Dict with ``message`` and ``entities_deleted`` count.
        """
        return await self._t.request("POST", "/v1/sandbox/reset")
