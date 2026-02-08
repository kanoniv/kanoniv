"""Identity specs resource — list, get, ingest."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .._transport import AsyncTransport, SyncTransport


class SpecsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._t = transport

    def list(self) -> list[dict[str, Any]]:
        return self._t.request("GET", "/v1/identity/specs")

    def get(self, version: int | str) -> dict[str, Any]:
        return self._t.request("GET", f"/v1/identity/specs/{version}")

    def ingest(
        self,
        yaml_content: str,
        *,
        compile: bool = False,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"spec_yaml": yaml_content, "compile": compile}
        return self._t.request("POST", "/v1/identity/specs", json=body)


class AsyncSpecsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._t = transport

    async def list(self) -> list[dict[str, Any]]:
        return await self._t.request("GET", "/v1/identity/specs")

    async def get(self, version: int | str) -> dict[str, Any]:
        return await self._t.request("GET", f"/v1/identity/specs/{version}")

    async def ingest(
        self,
        yaml_content: str,
        *,
        compile: bool = False,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"spec_yaml": yaml_content, "compile": compile}
        return await self._t.request("POST", "/v1/identity/specs", json=body)
