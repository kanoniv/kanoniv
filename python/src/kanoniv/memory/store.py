"""SQLite backend for local memory storage with FTS5 and vector similarity."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .entry import MemoryEntry

_SCHEMA = """
CREATE TABLE IF NOT EXISTS memories (
    id TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    entry_type TEXT NOT NULL DEFAULT 'knowledge',
    title TEXT NOT NULL DEFAULT '',
    entity_ids TEXT NOT NULL DEFAULT '[]',
    entity_fields TEXT,
    author TEXT NOT NULL DEFAULT 'default',
    visibility TEXT NOT NULL DEFAULT 'shared' CHECK(visibility IN ('shared', 'agent')),
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    synced_at TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    slug TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_memories_entry_type ON memories(entry_type);
CREATE INDEX IF NOT EXISTS idx_memories_author ON memories(author);
CREATE INDEX IF NOT EXISTS idx_memories_status ON memories(status);
CREATE INDEX IF NOT EXISTS idx_memories_slug ON memories(slug);

CREATE TABLE IF NOT EXISTS embeddings (
    memory_id TEXT PRIMARY KEY REFERENCES memories(id) ON DELETE CASCADE,
    vector BLOB NOT NULL,
    model TEXT NOT NULL DEFAULT 'all-MiniLM-L6-v2',
    dims INTEGER NOT NULL DEFAULT 384
);
"""

_FTS_SCHEMA = """
CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
    title, content, entry_type,
    content='memories',
    content_rowid='rowid'
);

CREATE TRIGGER IF NOT EXISTS memories_ai AFTER INSERT ON memories BEGIN
    INSERT INTO memories_fts(rowid, title, content, entry_type)
    VALUES (new.rowid, new.title, new.content, new.entry_type);
END;

CREATE TRIGGER IF NOT EXISTS memories_ad AFTER DELETE ON memories BEGIN
    INSERT INTO memories_fts(memories_fts, rowid, title, content, entry_type)
    VALUES ('delete', old.rowid, old.title, old.content, old.entry_type);
END;

CREATE TRIGGER IF NOT EXISTS memories_au AFTER UPDATE ON memories BEGIN
    INSERT INTO memories_fts(memories_fts, rowid, title, content, entry_type)
    VALUES ('delete', old.rowid, old.title, old.content, old.entry_type);
    INSERT INTO memories_fts(rowid, title, content, entry_type)
    VALUES (new.rowid, new.title, new.content, new.entry_type);
END;
"""


class MemoryStore:
    """SQLite-backed memory store with FTS5 full-text search and optional vector similarity."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path).expanduser()
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.executescript(_SCHEMA)
        try:
            self._conn.executescript(_FTS_SCHEMA)
        except sqlite3.OperationalError:
            # FTS triggers may already exist
            pass

    def insert(self, entry: MemoryEntry) -> None:
        self._conn.execute(
            """INSERT INTO memories
               (id, content, entry_type, title, entity_ids, entity_fields,
                author, visibility, metadata, created_at, updated_at, synced_at, status, slug)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                entry.id,
                entry.content,
                entry.entry_type,
                entry.title,
                json.dumps(entry.entity_ids),
                json.dumps(entry.entity_fields) if entry.entity_fields else None,
                entry.author,
                entry.visibility,
                json.dumps(entry.metadata),
                entry.created_at.isoformat(),
                entry.updated_at.isoformat(),
                entry.synced_at.isoformat() if entry.synced_at else None,
                entry.status,
                entry.slug,
            ),
        )
        self._conn.commit()

    def get(self, entry_id: str) -> MemoryEntry | None:
        row = self._conn.execute(
            "SELECT * FROM memories WHERE id = ?", (entry_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_entry(row)

    def delete(self, entry_id: str) -> bool:
        cur = self._conn.execute("DELETE FROM memories WHERE id = ?", (entry_id,))
        self._conn.execute("DELETE FROM embeddings WHERE memory_id = ?", (entry_id,))
        self._conn.commit()
        return cur.rowcount > 0

    def update_content(self, entry_id: str, content: str) -> bool:
        """Update the content of an existing memory entry."""
        now = datetime.now(timezone.utc).isoformat()
        cur = self._conn.execute(
            "UPDATE memories SET content = ?, updated_at = ? WHERE id = ?",
            (content, now, entry_id),
        )
        self._conn.commit()
        return cur.rowcount > 0

    def update_status(self, entry_id: str, status: str) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        cur = self._conn.execute(
            "UPDATE memories SET status = ?, updated_at = ? WHERE id = ?",
            (status, now, entry_id),
        )
        self._conn.commit()
        return cur.rowcount > 0

    def list_entries(
        self,
        *,
        entry_type: str | None = None,
        author: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[MemoryEntry]:
        sql = "SELECT * FROM memories WHERE status != 'archived'"
        params: list[Any] = []
        if entry_type:
            sql += " AND entry_type = ?"
            params.append(entry_type)
        if author:
            sql += " AND author = ?"
            params.append(author)
        if status:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_entry(r) for r in rows]

    def search_fts(self, query: str, *, limit: int = 20) -> list[MemoryEntry]:
        """Full-text search using FTS5."""
        rows = self._conn.execute(
            """SELECT m.*, rank
               FROM memories_fts fts
               JOIN memories m ON m.rowid = fts.rowid
               WHERE memories_fts MATCH ?
               AND m.status != 'archived'
               ORDER BY rank
               LIMIT ?""",
            (query, limit),
        ).fetchall()
        return [self._row_to_entry(r) for r in rows]

    def search_keyword(self, query: str, *, limit: int = 20) -> list[MemoryEntry]:
        """Fallback keyword search using LIKE."""
        pattern = f"%{query}%"
        rows = self._conn.execute(
            """SELECT * FROM memories
               WHERE (title LIKE ? OR content LIKE ?)
               AND status != 'archived'
               ORDER BY created_at DESC LIMIT ?""",
            (pattern, pattern, limit),
        ).fetchall()
        return [self._row_to_entry(r) for r in rows]

    def find_by_entity_id(self, entity_id: str, *, limit: int = 50) -> list[MemoryEntry]:
        """Find memories linked to a specific entity ID."""
        rows = self._conn.execute(
            """SELECT * FROM memories
               WHERE entity_ids LIKE ?
               AND status != 'archived'
               ORDER BY created_at DESC LIMIT ?""",
            (f'%"{entity_id}"%', limit),
        ).fetchall()
        return [self._row_to_entry(r) for r in rows]

    def find_by_entity_fields(
        self, fields: dict[str, Any], *, limit: int = 50
    ) -> list[MemoryEntry]:
        """Find memories whose entity_fields overlap with the given fields.

        Uses simple value matching - if any field value matches, include it.
        This is a local fuzzy match; cloud sync resolves to canonical entities.
        """
        results: list[MemoryEntry] = []
        needle_values = {str(v).lower().strip() for v in fields.values() if v}

        # Cap the scan to avoid loading the entire table into memory.
        # We apply the limit * 10 heuristic since not all rows will match.
        scan_limit = max(limit * 10, 500)
        rows = self._conn.execute(
            """SELECT * FROM memories
               WHERE entity_fields IS NOT NULL
               AND status != 'archived'
               ORDER BY created_at DESC
               LIMIT ?""",
            (scan_limit,),
        ).fetchall()

        for row in rows:
            entry = self._row_to_entry(row)
            if entry.entity_fields:
                row_values = {str(v).lower().strip() for v in entry.entity_fields.values() if v}
                if needle_values & row_values:
                    results.append(entry)
                    if len(results) >= limit:
                        break

        return results

    def get_unsynced(self, *, since: datetime | None = None) -> list[MemoryEntry]:
        """Get entries that haven't been synced to cloud."""
        if since:
            rows = self._conn.execute(
                """SELECT * FROM memories
                   WHERE synced_at IS NULL AND created_at >= ?
                   ORDER BY created_at ASC""",
                (since.isoformat(),),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM memories WHERE synced_at IS NULL ORDER BY created_at ASC"
            ).fetchall()
        return [self._row_to_entry(r) for r in rows]

    def mark_synced(self, entry_ids: list[str], synced_at: datetime | None = None) -> None:
        """Mark entries as synced to cloud."""
        ts = (synced_at or datetime.now(timezone.utc)).isoformat()
        for eid in entry_ids:
            self._conn.execute(
                "UPDATE memories SET synced_at = ? WHERE id = ?", (ts, eid)
            )
        self._conn.commit()

    def upsert_from_cloud(self, entry: MemoryEntry) -> None:
        """Upsert an entry pulled from cloud. Cloud wins on conflicts."""
        existing = self.get(entry.id)
        if existing:
            self._conn.execute(
                """UPDATE memories
                   SET content = ?, title = ?, entry_type = ?, entity_ids = ?,
                       entity_fields = ?, visibility = ?, metadata = ?,
                       updated_at = ?, synced_at = ?, status = ?
                   WHERE id = ?""",
                (
                    entry.content,
                    entry.title,
                    entry.entry_type,
                    json.dumps(entry.entity_ids),
                    json.dumps(entry.entity_fields) if entry.entity_fields else None,
                    entry.visibility,
                    json.dumps(entry.metadata),
                    entry.updated_at.isoformat(),
                    entry.synced_at.isoformat() if entry.synced_at else datetime.now(timezone.utc).isoformat(),
                    entry.status,
                    entry.id,
                ),
            )
        else:
            entry.synced_at = entry.synced_at or datetime.now(timezone.utc)
            self.insert(entry)
            return
        self._conn.commit()

    # -- Embedding storage ---------------------------------------------------

    def store_embedding(self, memory_id: str, vector: bytes, model: str, dims: int) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO embeddings (memory_id, vector, model, dims)
               VALUES (?, ?, ?, ?)""",
            (memory_id, vector, model, dims),
        )
        self._conn.commit()

    def get_embedding(self, memory_id: str) -> bytes | None:
        row = self._conn.execute(
            "SELECT vector FROM embeddings WHERE memory_id = ?", (memory_id,)
        ).fetchone()
        return row["vector"] if row else None

    def get_all_embeddings(self) -> list[tuple[str, bytes]]:
        """Return (memory_id, vector_bytes) for all entries with embeddings."""
        rows = self._conn.execute(
            "SELECT memory_id, vector FROM embeddings"
        ).fetchall()
        return [(r["memory_id"], r["vector"]) for r in rows]

    def count(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) as c FROM memories").fetchone()
        return row["c"]

    def close(self) -> None:
        self._conn.close()

    # -- Internal helpers ----------------------------------------------------

    def _row_to_entry(self, row: sqlite3.Row) -> MemoryEntry:
        ef = row["entity_fields"]
        return MemoryEntry(
            id=row["id"],
            content=row["content"],
            entry_type=row["entry_type"],
            title=row["title"],
            entity_ids=json.loads(row["entity_ids"]),
            entity_fields=json.loads(ef) if ef else None,
            author=row["author"],
            visibility=row["visibility"],
            metadata=json.loads(row["metadata"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            synced_at=datetime.fromisoformat(row["synced_at"]) if row["synced_at"] else None,
            status=row["status"],
            slug=row["slug"],
        )
