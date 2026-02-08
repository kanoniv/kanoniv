"""Kanoniv API client — sync and async variants."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ._transport import AsyncTransport, SyncTransport
from .resources.audit import AsyncAuditResource, AuditResource
from .resources.entities import AsyncEntitiesResource, EntitiesResource
from .resources.jobs import AsyncJobsResource, JobsResource
from .resources.overrides import AsyncOverridesResource, OverridesResource
from .resources.reviews import AsyncReviewsResource, ReviewsResource
from .resources.rules import AsyncRulesResource, RulesResource
from .resources.sources import AsyncSourcesResource, SourcesResource
from .resources.specs import AsyncSpecsResource, SpecsResource


class KanonivClient:
    """Synchronous client for the Kanoniv identity resolution API.

    Usage::

        client = KanonivClient(api_key="kn_...", base_url="https://api.kanoniv.com")
        result = client.resolve(system="crm", external_id="sf_123")
        client.close()

    Or as a context manager::

        with KanonivClient(api_key="kn_...") as client:
            result = client.resolve(system="crm", external_id="sf_123")
    """

    def __init__(
        self,
        *,
        base_url: str = "http://localhost:3010",
        api_key: str | None = None,
        access_token: str | None = None,
        timeout: float = 30.0,
        max_retries: int = 2,
    ) -> None:
        self._transport = SyncTransport(
            base_url=base_url,
            api_key=api_key,
            access_token=access_token,
            timeout=timeout,
            max_retries=max_retries,
        )
        self.entities = EntitiesResource(self._transport)
        self.sources = SourcesResource(self._transport)
        self.rules = RulesResource(self._transport)
        self.jobs = JobsResource(self._transport)
        self.reviews = ReviewsResource(self._transport)
        self.overrides = OverridesResource(self._transport)
        self.audit = AuditResource(self._transport)
        self.specs = SpecsResource(self._transport)

    # -- Top-level convenience methods -----------------------------------------

    def resolve(
        self,
        system: str | None = None,
        external_id: str | None = None,
        *,
        query: str | None = None,
    ) -> dict[str, Any]:
        """Resolve an identity by system+external_id or free-text query."""
        return self._transport.request(
            "GET",
            "/v1/resolve",
            params={"system": system, "id": external_id, "query": query},
        )

    def ingest(
        self,
        source_id: str,
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Ingest records via the webhook endpoint."""
        return self._transport.request(
            "POST",
            f"/v1/ingest/webhook/{source_id}",
            json=records,
        )

    def ingest_file(
        self,
        source_id: str,
        path: str | Path,
        *,
        id_field: str = "id",
        entity_type: str | None = None,
    ) -> dict[str, Any]:
        """Upload and process a file for ingestion."""
        p = Path(path)
        form: dict[str, str] = {"source_id": source_id, "id_field": id_field}
        if entity_type:
            form["entity_type"] = entity_type
        with open(p, "rb") as f:
            return self._transport.request(
                "POST",
                "/v1/ingest/file/process",
                files={"file": (p.name, f)},
                data=form,
            )

    def stats(self) -> dict[str, Any]:
        """Get dashboard statistics."""
        return self._transport.request("GET", "/v1/stats")

    # -- Lifecycle -------------------------------------------------------------

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> KanonivClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


class KanonivAsyncClient:
    """Asynchronous client for the Kanoniv identity resolution API.

    Usage::

        async with KanonivAsyncClient(api_key="kn_...") as client:
            result = await client.resolve(system="crm", external_id="sf_123")
    """

    def __init__(
        self,
        *,
        base_url: str = "http://localhost:3010",
        api_key: str | None = None,
        access_token: str | None = None,
        timeout: float = 30.0,
        max_retries: int = 2,
    ) -> None:
        self._transport = AsyncTransport(
            base_url=base_url,
            api_key=api_key,
            access_token=access_token,
            timeout=timeout,
            max_retries=max_retries,
        )
        self.entities = AsyncEntitiesResource(self._transport)
        self.sources = AsyncSourcesResource(self._transport)
        self.rules = AsyncRulesResource(self._transport)
        self.jobs = AsyncJobsResource(self._transport)
        self.reviews = AsyncReviewsResource(self._transport)
        self.overrides = AsyncOverridesResource(self._transport)
        self.audit = AsyncAuditResource(self._transport)
        self.specs = AsyncSpecsResource(self._transport)

    # -- Top-level convenience methods -----------------------------------------

    async def resolve(
        self,
        system: str | None = None,
        external_id: str | None = None,
        *,
        query: str | None = None,
    ) -> dict[str, Any]:
        """Resolve an identity by system+external_id or free-text query."""
        return await self._transport.request(
            "GET",
            "/v1/resolve",
            params={"system": system, "id": external_id, "query": query},
        )

    async def ingest(
        self,
        source_id: str,
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Ingest records via the webhook endpoint."""
        return await self._transport.request(
            "POST",
            f"/v1/ingest/webhook/{source_id}",
            json=records,
        )

    async def ingest_file(
        self,
        source_id: str,
        path: str | Path,
        *,
        id_field: str = "id",
        entity_type: str | None = None,
    ) -> dict[str, Any]:
        """Upload and process a file for ingestion."""
        p = Path(path)
        form: dict[str, str] = {"source_id": source_id, "id_field": id_field}
        if entity_type:
            form["entity_type"] = entity_type
        with open(p, "rb") as f:
            return await self._transport.request(
                "POST",
                "/v1/ingest/file/process",
                files={"file": (p.name, f)},
                data=form,
            )

    async def stats(self) -> dict[str, Any]:
        """Get dashboard statistics."""
        return await self._transport.request("GET", "/v1/stats")

    # -- Lifecycle -------------------------------------------------------------

    async def close(self) -> None:
        await self._transport.close()

    async def __aenter__(self) -> KanonivAsyncClient:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()
