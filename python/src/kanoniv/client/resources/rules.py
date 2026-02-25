"""Rules resource - list active, create version, history."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .._transport import AsyncTransport, SyncTransport


class RulesResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._t = transport

    def list(self) -> list[dict[str, Any]]:
        return self._t.request("GET", "/v1/rules")

    def create(
        self,
        *,
        name: str,
        rule_type: str,
        config: dict[str, Any],
        weight: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"name": name, "rule_type": rule_type, "config": config, **kwargs}
        if weight is not None:
            body["weight"] = weight
        return self._t.request("POST", "/v1/rules", json=body)

    def history(self, name: str) -> list[dict[str, Any]]:
        return self._t.request("GET", f"/v1/rules/{name}/history")


class AsyncRulesResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._t = transport

    async def list(self) -> list[dict[str, Any]]:
        return await self._t.request("GET", "/v1/rules")

    async def create(
        self,
        *,
        name: str,
        rule_type: str,
        config: dict[str, Any],
        weight: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"name": name, "rule_type": rule_type, "config": config, **kwargs}
        if weight is not None:
            body["weight"] = weight
        return await self._t.request("POST", "/v1/rules", json=body)

    async def history(self, name: str) -> list[dict[str, Any]]:
        return await self._t.request("GET", f"/v1/rules/{name}/history")
