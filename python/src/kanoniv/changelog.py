"""Entity-level change detection between reconciliation runs."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .reconcile import ReconcileResult


@dataclass
class EntityChange:
    """A single entity-level change between two reconciliation runs.

    Attributes:
        kanoniv_id: The canonical entity ID this change is about.
        change_type: One of: created, grown, merged, split, removed.
        source_records: All (source_system, source_id) members of this entity.
        new_records: Source records that are new in this run.
        previous_kanoniv_ids: For grown/merged - the previous entity IDs involved.
            For split - the current entity IDs members scattered to.
        field_changes: Golden record fields that changed: {field: (old, new)}.
    """

    kanoniv_id: str
    change_type: str
    source_records: list[tuple[str, str]]
    new_records: list[tuple[str, str]] = field(default_factory=list)
    previous_kanoniv_ids: list[str] = field(default_factory=list)
    field_changes: dict[str, tuple[str, str]] = field(default_factory=dict)

    def __repr__(self) -> str:
        parts = [f"{self.change_type} {self.kanoniv_id[:20]}"]
        n = len(self.source_records)
        parts.append(f"{n} member{'s' if n != 1 else ''}")
        if self.new_records:
            parts.append(f"{len(self.new_records)} new")
        if self.previous_kanoniv_ids and self.change_type == "merged":
            parts.append(f"from {len(self.previous_kanoniv_ids)} previous")
        if self.field_changes:
            parts.append(f"{len(self.field_changes)} fields changed")
        return f"<EntityChange: {', '.join(parts)}>"


@dataclass
class ChangeLog:
    """Entity-level changes between two reconciliation runs.

    Iterate directly or use filtered properties (created, grown, merged, etc.).
    """

    changes: list[EntityChange]
    unchanged_count: int = 0

    @property
    def created(self) -> list[EntityChange]:
        """Entities that are entirely new (all members are new records)."""
        return [c for c in self.changes if c.change_type == "created"]

    @property
    def grown(self) -> list[EntityChange]:
        """Existing entities that absorbed new records."""
        return [c for c in self.changes if c.change_type == "grown"]

    @property
    def merged(self) -> list[EntityChange]:
        """Entities formed by combining multiple previous entities."""
        return [c for c in self.changes if c.change_type == "merged"]

    @property
    def split(self) -> list[EntityChange]:
        """Previous entities whose members scattered to multiple current entities."""
        return [c for c in self.changes if c.change_type == "split"]

    @property
    def removed(self) -> list[EntityChange]:
        """Previous entities with no members in the current result."""
        return [c for c in self.changes if c.change_type == "removed"]

    @property
    def summary(self) -> str:
        """One-line human-readable summary of all changes."""
        parts = []
        for label, items in [
            ("created", self.created),
            ("grown", self.grown),
            ("merged", self.merged),
            ("split", self.split),
            ("removed", self.removed),
        ]:
            if items:
                parts.append(f"{len(items)} {label}")
        if self.unchanged_count > 0:
            parts.append(f"{self.unchanged_count} unchanged")
        return ", ".join(parts) if parts else "no changes"

    def to_pandas(self) -> Any:
        """One row per changed entity as a pandas DataFrame."""
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "ChangeLog.to_pandas() requires pandas: pip install pandas"
            ) from None

        rows = []
        for c in self.changes:
            rows.append({
                "change_type": c.change_type,
                "kanoniv_id": c.kanoniv_id,
                "member_count": len(c.source_records),
                "new_record_count": len(c.new_records),
                "source_records": c.source_records,
                "previous_kanoniv_ids": c.previous_kanoniv_ids,
            })
        if not rows:
            return pd.DataFrame(
                columns=[
                    "change_type", "kanoniv_id", "member_count",
                    "new_record_count", "source_records", "previous_kanoniv_ids",
                ]
            )
        return pd.DataFrame(rows)

    def __len__(self) -> int:
        return len(self.changes)

    def __iter__(self):
        return iter(self.changes)

    def __repr__(self) -> str:
        return f"<ChangeLog: {self.summary}>"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_source_to_kanoniv(
    result: ReconcileResult,
) -> dict[tuple[str, str], str]:
    """Map (source_system, source_id) -> kanoniv_id."""
    entity_map = getattr(result, "_entity_map", {})

    uuid_to_kid: dict[str, str] = {}
    for i, cluster in enumerate(result.clusters):
        if i < len(result.golden_records):
            kid = result.golden_records[i].get("kanoniv_id", "")
            for uuid in cluster:
                uuid_to_kid[uuid] = kid

    lookup: dict[tuple[str, str], str] = {}
    for uuid, src_info in entity_map.items():
        kid = uuid_to_kid.get(uuid, "")
        if kid:
            # Normalize to tuple in case loaded from JSON as list
            lookup[tuple(src_info)] = kid
    return lookup


def _build_kanoniv_to_sources(
    lookup: dict[tuple[str, str], str],
) -> dict[str, set[tuple[str, str]]]:
    """Invert: kanoniv_id -> set of (source_system, source_id)."""
    result: dict[str, set[tuple[str, str]]] = {}
    for src_info, kid in lookup.items():
        result.setdefault(kid, set()).add(src_info)
    return result


def _compute_changes(
    previous: ReconcileResult,
    current: ReconcileResult,
) -> ChangeLog:
    """Compute entity-level changes between two reconciliation runs.

    Detects: created, grown, merged, split, removed entities.
    """
    prev_lookup = _build_source_to_kanoniv(previous)
    curr_lookup = _build_source_to_kanoniv(current)

    prev_entities = _build_kanoniv_to_sources(prev_lookup)
    curr_entities = _build_kanoniv_to_sources(curr_lookup)

    changes: list[EntityChange] = []
    unchanged_count = 0

    # Track which previous kanoniv_ids are referenced from current side
    prev_kids_seen: set[str] = set()

    # --- Current-side analysis ---
    for curr_kid, curr_members in curr_entities.items():
        new_records = sorted(m for m in curr_members if m not in prev_lookup)
        existing_records = [m for m in curr_members if m in prev_lookup]

        # Which previous entities do existing members come from?
        prev_kids = set()
        for m in existing_records:
            prev_kids.add(prev_lookup[m])
        prev_kids_seen.update(prev_kids)

        if not existing_records:
            # All members are new -> entity created
            changes.append(EntityChange(
                kanoniv_id=curr_kid,
                change_type="created",
                source_records=sorted(curr_members),
                new_records=new_records,
            ))
        elif len(prev_kids) > 1:
            # Members from multiple previous entities -> merge
            changes.append(EntityChange(
                kanoniv_id=curr_kid,
                change_type="merged",
                source_records=sorted(curr_members),
                new_records=new_records,
                previous_kanoniv_ids=sorted(prev_kids),
            ))
        elif new_records:
            # One previous entity + new records -> grown
            changes.append(EntityChange(
                kanoniv_id=curr_kid,
                change_type="grown",
                source_records=sorted(curr_members),
                new_records=new_records,
                previous_kanoniv_ids=sorted(prev_kids),
            ))
        else:
            # No new records, one previous entity -> unchanged
            unchanged_count += 1

    # --- Previous-side analysis (splits and removals) ---
    for prev_kid, prev_members in prev_entities.items():
        if prev_kid not in prev_kids_seen:
            # No current entity references this -> removed
            changes.append(EntityChange(
                kanoniv_id=prev_kid,
                change_type="removed",
                source_records=sorted(prev_members),
            ))
            continue

        # Check if members scattered to multiple current entities
        curr_kids_for_members: set[str] = set()
        for m in prev_members:
            if m in curr_lookup:
                curr_kids_for_members.add(curr_lookup[m])

        if len(curr_kids_for_members) > 1:
            changes.append(EntityChange(
                kanoniv_id=prev_kid,
                change_type="split",
                source_records=sorted(prev_members),
                previous_kanoniv_ids=sorted(curr_kids_for_members),
            ))

    return ChangeLog(changes=changes, unchanged_count=unchanged_count)
