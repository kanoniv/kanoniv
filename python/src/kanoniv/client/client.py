"""Kanoniv API client — sync and async variants."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ._transport import AsyncTransport, SyncTransport
from .resources.audit import AsyncAuditResource, AuditResource
from .resources.entities import AsyncEntitiesResource, EntitiesResource
from .resources.feedback import AsyncFeedbackResource, FeedbackResource
from .resources.jobs import AsyncJobsResource, JobsResource
from .resources.overrides import AsyncOverridesResource, OverridesResource
from .resources.resolve import AsyncResolveResource, ResolveResource
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
        base_url: str = "https://api.kanoniv.com",
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
        self.feedback = FeedbackResource(self._transport)
        self.audit = AuditResource(self._transport)
        self.specs = SpecsResource(self._transport)
        self.resolve_rt = ResolveResource(self._transport)

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
        source_name: str,
        records: list[dict[str, Any]],
        *,
        entity_type: str = "entity",
    ) -> dict[str, Any]:
        """Ingest records via the batch endpoint."""
        return self._transport.request(
            "POST",
            "/v1/ingest/batch",
            json={
                "source_name": source_name,
                "entity_type": entity_type,
                "entities": records,
            },
        )

    def ingest_file(
        self,
        source_id: str,
        path: str | Path,
    ) -> dict[str, Any]:
        """Upload and process a file for ingestion."""
        p = Path(path)
        with open(p, "rb") as f:
            return self._transport.request(
                "POST",
                "/v1/ingest/file/process",
                files={"file": (p.name, f)},
                data={"source_id": source_id},
            )

    def ingest_parquet(
        self,
        source_name: str,
        path: str | Path,
        *,
        entity_type: str = "entity",
    ) -> dict[str, Any]:
        """Upload a Parquet file for bulk ingestion.

        Args:
            source_name: Name of the data source.
            path: Path to a local ``.parquet`` file.
            entity_type: Entity type label (default ``"entity"``).

        Returns:
            Ingest summary with ``new``, ``updated``, ``unchanged`` counts.
        """
        import os as _os

        p = Path(path)
        with open(p, "rb") as f:
            return self._transport.request(
                "POST",
                "/v1/ingest/parquet",
                files={"file": (_os.path.basename(str(p)), f, "application/octet-stream")},
                data={"source_name": source_name, "entity_type": entity_type},
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
        base_url: str = "https://api.kanoniv.com",
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
        self.feedback = AsyncFeedbackResource(self._transport)
        self.audit = AsyncAuditResource(self._transport)
        self.specs = AsyncSpecsResource(self._transport)
        self.resolve_rt = AsyncResolveResource(self._transport)

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
        source_name: str,
        records: list[dict[str, Any]],
        *,
        entity_type: str = "entity",
    ) -> dict[str, Any]:
        """Ingest records via the batch endpoint."""
        return await self._transport.request(
            "POST",
            "/v1/ingest/batch",
            json={
                "source_name": source_name,
                "entity_type": entity_type,
                "entities": records,
            },
        )

    async def ingest_file(
        self,
        source_id: str,
        path: str | Path,
    ) -> dict[str, Any]:
        """Upload and process a file for ingestion."""
        p = Path(path)
        with open(p, "rb") as f:
            return await self._transport.request(
                "POST",
                "/v1/ingest/file/process",
                files={"file": (p.name, f)},
                data={"source_id": source_id},
            )

    async def ingest_parquet(
        self,
        source_name: str,
        path: str | Path,
        *,
        entity_type: str = "entity",
    ) -> dict[str, Any]:
        """Upload a Parquet file for bulk ingestion.

        Args:
            source_name: Name of the data source.
            path: Path to a local ``.parquet`` file.
            entity_type: Entity type label (default ``"entity"``).

        Returns:
            Ingest summary with ``new``, ``updated``, ``unchanged`` counts.
        """
        import os as _os

        p = Path(path)
        with open(p, "rb") as f:
            return await self._transport.request(
                "POST",
                "/v1/ingest/parquet",
                files={"file": (_os.path.basename(str(p)), f, "application/octet-stream")},
                data={"source_name": source_name, "entity_type": entity_type},
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
