"""Cloud sync - push local memories to cloud, pull shared memories back.

The bridge between local SDK memory and cloud identity resolution.
When pushing, entity_fields get resolved to canonical entity IDs via
the cloud's resolve endpoint. This is where identity resolution becomes
the killer feature.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .entry import MemoryEntry
from .store import MemoryStore


class MemorySync:
    """Sync local memories with the Kanoniv cloud.

    Push flow:
        1. Collect unsynced local memories
        2. POST /v1/memory/sync-local with entity_fields
        3. Cloud resolves entity_fields to canonical entity IDs
        4. Mark local entries as synced

    Pull flow:
        1. GET /v1/memory?visibility=shared (since last sync)
        2. Upsert into local store
    """

    def __init__(self, store: MemoryStore) -> None:
        self._store = store

    def push(
        self,
        client: Any,
        *,
        entries: list[MemoryEntry] | None = None,
        since: datetime | None = None,
    ) -> dict[str, Any]:
        """Push local memories to cloud.

        Args:
            client: A ``kanoniv.Client`` instance.
            entries: Specific entries to push. If None, pushes all unsynced.
            since: Only push entries created after this time.

        Returns:
            Push summary with synced count and resolution results.
        """
        to_push = entries or self._store.get_unsynced(since=since)
        if not to_push:
            return {"pushed": 0, "resolved": []}

        # Build sync-local payload
        payload_entries = []
        for entry in to_push:
            payload_entries.append({
                "local_id": entry.id,
                "title": entry.title,
                "content": entry.content,
                "entry_type": entry.entry_type,
                "entity_fields": entry.entity_fields,
                "entity_ids": entry.entity_ids,
                "author": entry.author,
                "visibility": entry.visibility,
                "metadata": entry.metadata,
                "created_at": entry.created_at.isoformat(),
            })

        try:
            result = client._transport.request(
                "POST",
                "/v1/memory/sync-local",
                json={"entries": payload_entries},
            )
        except Exception:
            # If sync-local endpoint not available, fall back to regular sync
            result = self._push_via_regular_sync(client, to_push)

        # Mark entries as synced
        synced_ids = [e.id for e in to_push]
        self._store.mark_synced(synced_ids)

        return {
            "pushed": len(to_push),
            "resolved": result.get("resolved", []),
        }

    def pull(
        self,
        client: Any,
        *,
        since: datetime | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        """Pull shared memories from cloud to local store.

        Args:
            client: A ``kanoniv.Client`` instance.
            since: Only pull entries updated after this time.
            limit: Maximum entries to pull.

        Returns:
            Pull summary with count of new/updated entries.
        """
        params: dict[str, Any] = {
            "limit": limit,
        }
        if since:
            params["since"] = since.isoformat()

        try:
            cloud_entries = client._transport.request(
                "GET",
                "/v1/memory",
                params=params,
            )
        except Exception:
            return {"pulled": 0, "error": "Failed to fetch from cloud"}

        if not isinstance(cloud_entries, list):
            cloud_entries = cloud_entries.get("entries", [])

        pulled = 0
        for data in cloud_entries:
            entry = MemoryEntry(
                id=data.get("id", ""),
                content=data.get("content", ""),
                entry_type=data.get("entry_type", "knowledge"),
                title=data.get("title", ""),
                entity_ids=[str(eid) for eid in data.get("linked_entities", [])],
                entity_fields=None,
                author=data.get("author", "unknown"),
                visibility=data.get("visibility", "shared"),
                metadata=data.get("metadata", {}),
                created_at=_parse_dt(data.get("created_at")),
                updated_at=_parse_dt(data.get("updated_at")),
                synced_at=datetime.now(timezone.utc),
                status=data.get("status", "active"),
                slug=data.get("slug", ""),
            )
            self._store.upsert_from_cloud(entry)
            pulled += 1

        return {"pulled": pulled}

    def sync(self, client: Any) -> dict[str, Any]:
        """Push then pull. Cloud wins on conflicts.

        Args:
            client: A ``kanoniv.Client`` instance.

        Returns:
            Combined sync summary.
        """
        push_result = self.push(client)
        pull_result = self.pull(client)
        return {
            "pushed": push_result.get("pushed", 0),
            "pulled": pull_result.get("pulled", 0),
            "resolved": push_result.get("resolved", []),
        }

    def _push_via_regular_sync(
        self, client: Any, entries: list[MemoryEntry]
    ) -> dict[str, Any]:
        """Fallback: push via the regular /v1/memory/sync endpoint."""
        sync_entries = []
        for entry in entries:
            sync_entries.append({
                "slug": entry.slug or entry.id,
                "title": entry.title,
                "content": entry.content,
                "entry_type": entry.entry_type,
            })

        try:
            result = client._transport.request(
                "POST",
                "/v1/memory/sync",
                json={"entries": sync_entries},
            )
            return result
        except Exception:
            return {"resolved": []}


def _parse_dt(val: Any) -> datetime:
    if val is None:
        return datetime.now(timezone.utc)
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return datetime.now(timezone.utc)
