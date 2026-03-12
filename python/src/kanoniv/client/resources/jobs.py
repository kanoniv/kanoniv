"""Jobs resource - list, get, run, cancel."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .._transport import AsyncTransport, SyncTransport


class JobsResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._t = transport

    def list(
        self,
        *,
        limit: int | None = None,
        job_type: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._t.request(
            "GET",
            "/v1/jobs",
            params={"limit": limit, "job_type": job_type},
        )

    def get(self, id: str) -> dict[str, Any]:
        return self._t.request("GET", f"/v1/jobs/{id}")

    def run(
        self,
        job_type: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"job_type": job_type}
        if payload is not None:
            body["payload"] = payload
        return self._t.request("POST", "/v1/jobs/run", json=body)

    def cancel(self, id: str) -> dict[str, Any]:
        return self._t.request("POST", f"/v1/jobs/{id}/cancel")


class AsyncJobsResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._t = transport

    async def list(
        self,
        *,
        limit: int | None = None,
        job_type: str | None = None,
    ) -> list[dict[str, Any]]:
        return await self._t.request(
            "GET",
            "/v1/jobs",
            params={"limit": limit, "job_type": job_type},
        )

    async def get(self, id: str) -> dict[str, Any]:
        return await self._t.request("GET", f"/v1/jobs/{id}")

    async def run(
        self,
        job_type: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"job_type": job_type}
        if payload is not None:
            body["payload"] = payload
        return await self._t.request("POST", "/v1/jobs/run", json=body)

    async def cancel(self, id: str) -> dict[str, Any]:
        return await self._t.request("POST", f"/v1/jobs/{id}/cancel")
