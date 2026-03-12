"""Local memory - zero config, works immediately with SQLite + optional embeddings."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .embeddings import (
    LocalEmbedder,
    bytes_to_vector,
    cosine_similarity,
    vector_to_bytes,
)
from .entry import MemoryEntry
from .extract import FactExtractor
from .store import MemoryStore

_VALID_ENTRY_TYPES = frozenset(
    {"decision", "investigation", "pattern", "intent", "knowledge", "expertise", "task"}
)
_VALID_VISIBILITY = frozenset({"shared", "agent"})
_DEFAULT_DB_PATH = "~/.kanoniv/memory.db"


class LocalMemory:
    """Local memory store for AI agents.

    Uses SQLite for persistence and sentence-transformers for optional
    semantic search. Works offline, zero config, zero signup.

    Usage::

        import kanoniv
        mem = kanoniv.get_memory(agent_name="support-agent", api_key="sk-...")

        # Add memories from a conversation (LLM extracts facts automatically)
        mem.add([
            {"role": "user", "content": "I'm Bill from Acme, switch me to annual billing"},
            {"role": "assistant", "content": "Done! Updated to annual billing."},
        ], user_id="bill@acme.com")

        # Search memories
        mem.search("billing preferences")

        # Get all memories for a user
        mem.get_all(user_id="bill@acme.com")

    Args:
        db_path: Path to SQLite database. Defaults to ``~/.kanoniv/memory.db``.
        agent_name: Name of the agent using this memory. Used as ``author``.
        api_key: OpenAI API key for LLM-powered fact extraction. Falls back to
            ``OPENAI_API_KEY`` env var. Without this, ``add()`` is unavailable
            but ``memorize()``/``search()`` still work.
        model: LLM model for fact extraction. Defaults to ``gpt-4.1-nano``.
        api_base_url: Base URL for OpenAI-compatible API.
        enable_embeddings: Whether to generate embeddings for semantic search.
            Requires ``sentence-transformers`` to be installed. Defaults to True
            (but gracefully falls back if not available).
    """

    def __init__(
        self,
        db_path: str | Path | None = None,
        agent_name: str = "default",
        api_key: str | None = None,
        model: str = "gpt-4.1-nano",
        api_base_url: str = "https://api.openai.com",
        enable_embeddings: bool = True,
    ) -> None:
        self._agent_name = agent_name
        self._store = MemoryStore(db_path or _DEFAULT_DB_PATH)
        self._embedder: LocalEmbedder | None = None
        self._enable_embeddings = enable_embeddings
        self._extractor = FactExtractor(
            api_key=api_key, model=model, base_url=api_base_url,
        )

        if enable_embeddings:
            self._embedder = LocalEmbedder()

    @property
    def agent_name(self) -> str:
        return self._agent_name

    @property
    def store(self) -> MemoryStore:
        return self._store

    @property
    def embeddings_available(self) -> bool:
        return self._embedder is not None and self._embedder.available

    def memorize(
        self,
        content: str,
        *,
        title: str = "",
        entry_type: str = "knowledge",
        entity_ids: list[str] | None = None,
        entity_fields: dict[str, Any] | None = None,
        visibility: str = "shared",
        metadata: dict[str, Any] | None = None,
    ) -> MemoryEntry:
        """Store a memory locally.

        Args:
            content: The memory content (main body).
            title: Short title. Defaults to first 200 chars of content.
            entry_type: One of: decision, investigation, pattern, intent,
                knowledge, expertise, task.
            entity_ids: Entity IDs to link this memory to.
            entity_fields: Raw entity fields (email, name, phone) for cloud
                resolution when syncing.
            visibility: 'shared' (visible to all agents) or 'agent' (private).
            metadata: Arbitrary metadata dict.

        Returns:
            The created MemoryEntry.
        """
        if entry_type not in _VALID_ENTRY_TYPES:
            raise ValueError(
                f"Invalid entry_type '{entry_type}'. Must be one of: {', '.join(sorted(_VALID_ENTRY_TYPES))}"
            )
        if visibility not in _VALID_VISIBILITY:
            raise ValueError(f"Invalid visibility '{visibility}'. Must be 'shared' or 'agent'.")

        entry = MemoryEntry.new(
            content,
            title=title,
            entry_type=entry_type,
            entity_ids=entity_ids,
            entity_fields=entity_fields,
            author=self._agent_name,
            visibility=visibility,
            metadata=metadata,
        )

        self._store.insert(entry)
        self._embed_entry(entry)
        return entry

    def add(
        self,
        messages: list[dict[str, str]],
        *,
        user_id: str | None = None,
        agent_id: str | None = None,
        run_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        deduplicate: bool = True,
    ) -> list[MemoryEntry]:
        """Add memories from a conversation. LLM extracts facts automatically.

        This is the primary way to populate memory. Pass in raw chat messages
        and the LLM extracts discrete facts, identifies entity information,
        and stores each fact as a separate memory entry.

        Requires an API key (set via ``api_key`` on init or ``OPENAI_API_KEY`` env var).

        Args:
            messages: Chat messages, each with "role" and "content" keys.
            user_id: User identifier (email, phone, or ID). Used to scope
                memories and as entity_fields for identity resolution.
            agent_id: Agent identifier for scoping.
            run_id: Session/run identifier for scoping.
            metadata: Extra metadata attached to all extracted memories.
            deduplicate: Whether to check existing memories and skip/update
                duplicates. Costs one extra LLM call. Defaults to True.

        Returns:
            List of created/updated MemoryEntry objects.

        Raises:
            RuntimeError: If no API key is configured.
        """
        if not self._extractor.available:
            raise RuntimeError(
                "add() requires an LLM API key. Pass api_key= to get_memory() "
                "or set OPENAI_API_KEY env var."
            )

        # Extract facts from conversation
        facts = self._extractor.extract(messages)
        if not facts:
            return []

        # Build entity_fields from user_id
        entity_fields = self._build_entity_fields(user_id)

        # Override entity_fields from extraction if user didn't provide one
        for fact in facts:
            if fact.get("entity_fields") and not entity_fields:
                entity_fields = fact["entity_fields"]
                break

        # Deduplicate against existing memories
        if deduplicate and entity_fields:
            existing = self._store.find_by_entity_fields(entity_fields, limit=50)
            if existing:
                existing_for_dedup = [
                    {"id": e.id, "content": e.content} for e in existing
                ]
                decisions = self._extractor.deduplicate(facts, existing_for_dedup)
            else:
                decisions = [
                    {"action": "add", **f, "existing_id": None} for f in facts
                ]
        else:
            decisions = [
                {"action": "add", **f, "existing_id": None} for f in facts
            ]

        # Build metadata with scoping info
        entry_metadata = dict(metadata or {})
        if agent_id:
            entry_metadata["agent_id"] = agent_id
        if run_id:
            entry_metadata["run_id"] = run_id
        if user_id:
            entry_metadata["user_id"] = user_id

        # Execute decisions
        results: list[MemoryEntry] = []
        for decision in decisions:
            action = decision.get("action", "add")
            fact_text = decision.get("fact", "")
            category = decision.get("category", "knowledge")
            fact_entity_fields = entity_fields or decision.get("entity_fields")

            if action == "skip":
                continue

            entry_type = FactExtractor.category_to_entry_type(category)

            if action == "update" and decision.get("existing_id"):
                # Update existing memory
                existing_id = decision["existing_id"]
                self._store.update_content(existing_id, fact_text)
                entry = self._store.get(existing_id)
                if entry:
                    results.append(entry)
            else:
                # Add new memory
                entry = self.memorize(
                    fact_text,
                    entry_type=entry_type,
                    entity_fields=fact_entity_fields,
                    metadata=entry_metadata,
                )
                results.append(entry)

        return results

    def get_all(
        self,
        *,
        user_id: str | None = None,
        agent_id: str | None = None,
        run_id: str | None = None,
        limit: int = 100,
    ) -> list[MemoryEntry]:
        """Get all memories, optionally filtered by user/agent/run.

        Args:
            user_id: Filter by user identifier (matches entity_fields).
            agent_id: Filter by agent_id in metadata.
            run_id: Filter by run_id in metadata.
            limit: Maximum results to return.

        Returns:
            List of MemoryEntry objects.
        """
        if user_id:
            entity_fields = self._build_entity_fields(user_id)
            if entity_fields:
                entries = self._store.find_by_entity_fields(entity_fields, limit=limit)
            else:
                entries = self._store.list_entries(limit=limit)
        else:
            entries = self._store.list_entries(limit=limit)

        # Filter by metadata fields if specified
        if agent_id or run_id:
            filtered = []
            for e in entries:
                if agent_id and e.metadata.get("agent_id") != agent_id:
                    continue
                if run_id and e.metadata.get("run_id") != run_id:
                    continue
                filtered.append(e)
            return filtered

        return entries

    @staticmethod
    def _build_entity_fields(user_id: str | None) -> dict[str, str] | None:
        """Infer entity_fields from a user_id string."""
        if not user_id:
            return None
        if "@" in user_id:
            return {"email": user_id}
        if user_id.replace("+", "").replace("-", "").replace(" ", "").isdigit():
            return {"phone": user_id}
        return {"external_id": user_id}

    def recall(
        self,
        entity_id: str | None = None,
        *,
        entity_fields: dict[str, Any] | None = None,
        limit: int = 50,
    ) -> list[MemoryEntry]:
        """Recall memories by entity ID or entity fields.

        Args:
            entity_id: Look up memories linked to this entity ID.
            entity_fields: Look up memories by raw field values (local fuzzy match).
            limit: Maximum results to return.

        Returns:
            List of matching MemoryEntry objects.
        """
        if entity_id:
            return self._store.find_by_entity_id(entity_id, limit=limit)
        if entity_fields:
            return self._store.find_by_entity_fields(entity_fields, limit=limit)
        return self._store.list_entries(limit=limit)

    def search(self, query: str, *, limit: int = 20) -> list[MemoryEntry]:
        """Search memories by semantic similarity or keyword.

        If sentence-transformers is available, uses cosine similarity on
        embeddings. Otherwise falls back to FTS5 full-text search, then
        keyword LIKE matching.

        Args:
            query: Search query string.
            limit: Maximum results to return.

        Returns:
            List of matching MemoryEntry objects, ranked by relevance.
        """
        # Try semantic search first
        if self._embedder and self._embedder.available:
            results = self._semantic_search(query, limit=limit)
            if results:
                return results

        # Fallback to FTS5
        try:
            results = self._store.search_fts(query, limit=limit)
            if results:
                return results
        except Exception:
            pass

        # Final fallback to keyword search
        return self._store.search_keyword(query, limit=limit)

    def forget(self, entry_id: str) -> bool:
        """Delete a memory by ID.

        Returns:
            True if the entry was deleted, False if not found.
        """
        return self._store.delete(entry_id)

    def list(
        self,
        *,
        entry_type: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[MemoryEntry]:
        """List memories with optional filters.

        Args:
            entry_type: Filter by entry type.
            status: Filter by status.
            limit: Maximum results to return.

        Returns:
            List of MemoryEntry objects sorted by creation time (newest first).
        """
        return self._store.list_entries(
            entry_type=entry_type,
            author=None,
            status=status,
            limit=limit,
        )

    def count(self) -> int:
        """Return the total number of memories stored."""
        return self._store.count()

    def sync(self, client: Any) -> dict[str, Any]:
        """Sync local memories to cloud.

        Push unsynced local memories, pull shared memories from cloud.
        Cloud wins on conflicts (it has identity resolution).

        Args:
            client: A ``kanoniv.Client`` instance with API key.

        Returns:
            Sync summary with pushed/pulled counts.
        """
        from .sync import MemorySync

        syncer = MemorySync(self._store)
        return syncer.sync(client)

    def close(self) -> None:
        """Close the underlying database connection."""
        self._store.close()

    def __enter__(self) -> LocalMemory:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # -- Internal helpers ---------------------------------------------------

    def _embed_entry(self, entry: MemoryEntry) -> None:
        """Generate and store embedding for an entry."""
        if not self._embedder or not self._embedder.available:
            return
        text = f"{entry.title}\n{entry.content}" if entry.title != entry.content[:200] else entry.content
        vec = self._embedder.embed(text)
        if vec:
            self._store.store_embedding(
                entry.id,
                vector_to_bytes(vec),
                self._embedder.model_name,
                self._embedder.dims,
            )

    def _semantic_search(self, query: str, *, limit: int = 20) -> list[MemoryEntry]:
        """Search by cosine similarity on embeddings."""
        if not self._embedder:
            return []

        query_vec = self._embedder.embed(query)
        if not query_vec:
            return []

        all_embeddings = self._store.get_all_embeddings()
        if not all_embeddings:
            return []

        scored: list[tuple[float, str]] = []
        for memory_id, vec_bytes in all_embeddings:
            stored_vec = bytes_to_vector(vec_bytes)
            sim = cosine_similarity(query_vec, stored_vec)
            scored.append((sim, memory_id))

        scored.sort(key=lambda x: x[0], reverse=True)

        results: list[MemoryEntry] = []
        for sim, memory_id in scored[:limit]:
            entry = self._store.get(memory_id)
            if entry and entry.status != "archived":
                entry.similarity = sim
                results.append(entry)

        return results
