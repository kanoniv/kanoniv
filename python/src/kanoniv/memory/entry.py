"""Memory entry data model."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
import uuid


@dataclass
class MemoryEntry:
    """A single memory entry stored locally or synced to cloud."""

    id: str
    content: str
    entry_type: str
    title: str
    entity_ids: list[str] = field(default_factory=list)
    entity_fields: dict[str, Any] | None = None
    author: str = "default"
    visibility: str = "shared"
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    synced_at: datetime | None = None
    status: str = "active"
    slug: str = ""
    similarity: float | None = None

    @staticmethod
    def new(
        content: str,
        *,
        title: str = "",
        entry_type: str = "knowledge",
        entity_ids: list[str] | None = None,
        entity_fields: dict[str, Any] | None = None,
        author: str = "default",
        visibility: str = "shared",
        metadata: dict[str, Any] | None = None,
    ) -> MemoryEntry:
        now = datetime.now(timezone.utc)
        entry_id = str(uuid.uuid4())
        slug = _slugify(title or content[:80])
        return MemoryEntry(
            id=entry_id,
            content=content,
            entry_type=entry_type,
            title=title or content[:200],
            entity_ids=entity_ids or [],
            entity_fields=entity_fields,
            author=author,
            visibility=visibility,
            metadata=metadata or {},
            created_at=now,
            updated_at=now,
            synced_at=None,
            status="active",
            slug=slug,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "content": self.content,
            "entry_type": self.entry_type,
            "title": self.title,
            "entity_ids": self.entity_ids,
            "entity_fields": self.entity_fields,
            "author": self.author,
            "visibility": self.visibility,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "synced_at": self.synced_at.isoformat() if self.synced_at else None,
            "status": self.status,
            "slug": self.slug,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MemoryEntry:
        def _parse_dt(v: Any) -> datetime | None:
            if v is None:
                return None
            if isinstance(v, datetime):
                return v
            return datetime.fromisoformat(v)

        return cls(
            id=data["id"],
            content=data.get("content", ""),
            entry_type=data.get("entry_type", "knowledge"),
            title=data.get("title", ""),
            entity_ids=data.get("entity_ids", []),
            entity_fields=data.get("entity_fields"),
            author=data.get("author", "default"),
            visibility=data.get("visibility", "shared"),
            metadata=data.get("metadata", {}),
            created_at=_parse_dt(data.get("created_at")) or datetime.now(timezone.utc),
            updated_at=_parse_dt(data.get("updated_at")) or datetime.now(timezone.utc),
            synced_at=_parse_dt(data.get("synced_at")),
            status=data.get("status", "active"),
            slug=data.get("slug", ""),
            similarity=data.get("similarity"),
        )


def _slugify(text: str) -> str:
    """Generate a URL-safe slug from text."""
    import re

    slug = text.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = slug.strip("-")
    return slug[:200]
