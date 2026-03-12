"""Resolve resource - realtime and bulk resolution against the identity graph."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .._transport import AsyncTransport, SyncTransport


class ResolveResource:
    def __init__(self, transport: SyncTransport) -> None:
        self._t = transport

    def realtime(
        self,
        *,
        source_name: str,
        external_id: str,
        data: dict[str, Any],
        context: str | None = None,
        extract: bool = True,
    ) -> dict[str, Any]:
        """Resolve a single record in real-time against the identity graph.

        Ingests the record, matches it against existing canonical entities,
        and returns the resolved entity. Creates a new entity if no match.

        Args:
            source_name: Name of the data source (e.g. ``"crm"``).
            external_id: Unique ID of this record in the source system.
            data: Record fields to match on (e.g. ``{"email": "...", "name": "..."}``).
            context: Natural language text (conversation, email, ticket) to extract
                identity fields from. Extracted fields fill gaps in ``data`` but
                never override explicit values.
            extract: Set to ``False`` to disable LLM extraction from context.
                Defaults to ``True``.

        Returns:
            Dict with ``entity_id``, ``canonical_data``, ``is_new``,
            ``matched_source``, ``confidence``, and ``extracted_fields``
            (when context was provided).
        """
        body: dict[str, Any] = {
            "source_name": source_name,
            "external_id": external_id,
            "data": data,
        }
        if context is not None:
            body["context"] = context
        if not extract:
            body["extract"] = False
        return self._t.request("POST", "/v1/resolve/realtime", json=body)

    def bulk(
        self,
        lookups: list[dict[str, str]],
    ) -> dict[str, Any]:
        """Batch resolve multiple source+id pairs against the identity graph.

        Uses Redis reverse index for fast lookups, falls back to DB.

        Args:
            lookups: List of ``{"source": "...", "id": "..."}`` dicts.
                Maximum 1000 per request.

        Returns:
            Dict with ``results`` (list of resolved entries),
            ``resolved`` count, and ``not_found`` count.
        """
        return self._t.request(
            "POST",
            "/v1/resolve/bulk",
            json={"lookups": lookups},
        )


class AsyncResolveResource:
    def __init__(self, transport: AsyncTransport) -> None:
        self._t = transport

    async def realtime(
        self,
        *,
        source_name: str,
        external_id: str,
        data: dict[str, Any],
        context: str | None = None,
        extract: bool = True,
    ) -> dict[str, Any]:
        """Resolve a single record in real-time against the identity graph.

        Ingests the record, matches it against existing canonical entities,
        and returns the resolved entity. Creates a new entity if no match.

        Args:
            source_name: Name of the data source (e.g. ``"crm"``).
            external_id: Unique ID of this record in the source system.
            data: Record fields to match on (e.g. ``{"email": "...", "name": "..."}``).
            context: Natural language text (conversation, email, ticket) to extract
                identity fields from. Extracted fields fill gaps in ``data`` but
                never override explicit values.
            extract: Set to ``False`` to disable LLM extraction from context.
                Defaults to ``True``.

        Returns:
            Dict with ``entity_id``, ``canonical_data``, ``is_new``,
            ``matched_source``, ``confidence``, and ``extracted_fields``
            (when context was provided).
        """
        body: dict[str, Any] = {
            "source_name": source_name,
            "external_id": external_id,
            "data": data,
        }
        if context is not None:
            body["context"] = context
        if not extract:
            body["extract"] = False
        return await self._t.request("POST", "/v1/resolve/realtime", json=body)

    async def bulk(
        self,
        lookups: list[dict[str, str]],
    ) -> dict[str, Any]:
        """Batch resolve multiple source+id pairs against the identity graph.

        Uses Redis reverse index for fast lookups, falls back to DB.

        Args:
            lookups: List of ``{"source": "...", "id": "..."}`` dicts.
                Maximum 1000 per request.

        Returns:
            Dict with ``results`` (list of resolved entries),
            ``resolved`` count, and ``not_found`` count.
        """
        return await self._t.request(
            "POST",
            "/v1/resolve/bulk",
            json={"lookups": lookups},
        )
