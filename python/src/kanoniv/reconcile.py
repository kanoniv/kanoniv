"""Top-level reconcile() function for local identity resolution."""
from __future__ import annotations

import json
import warnings as _warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .source import Source
from .spec import Spec
from .validate import validate


@dataclass
class ReconcileResult:
    """Result of a local reconciliation run."""

    clusters: list[list[str]]
    golden_records: list[dict[str, str]]
    decisions: list[dict[str, Any]]
    telemetry: dict[str, Any]

    @property
    def cluster_count(self) -> int:
        return len(self.clusters)

    @property
    def merge_rate(self) -> float:
        """Fraction of input entities that were merged (1 - clusters/entities)."""
        total = sum(len(c) for c in self.clusters)
        if total == 0:
            return 0.0
        return 1.0 - (len(self.clusters) / total)

    def evaluate(self, ground_truth: Any = None) -> Any:
        """Evaluate this reconciliation run.

        Without ground_truth: returns structural + stability metrics (L1+L2).
        With ground_truth: adds precision/recall/F1 (L3).

        Args:
            ground_truth: Optional labeled data as a dict
                ``{entity_id: [(source_name, external_id), ...]}``
                or a pandas DataFrame with columns
                ``[record_id, source_name, true_entity_id]``.

        Returns:
            EvaluateResult with evaluation metrics.
        """
        from .evaluate import _evaluate
        return _evaluate(self, ground_truth)

    def save(self, path: str) -> None:
        """Persist this reconciliation result to a .knv file."""
        data = {
            "version": 1,
            "clusters": self.clusters,
            "golden_records": self.golden_records,
            "decisions": self.decisions,
            "telemetry": self.telemetry,
            "entity_map": {k: list(v) for k, v in self._entity_map.items()}
            if hasattr(self, "_entity_map")
            else {},
            "trained_fs_params": getattr(self, "_trained_fs_params", None),
            "spec_hash": getattr(self, "_spec_hash", None),
            "entities": getattr(self, "_entities", None),
        }
        Path(path).write_text(json.dumps(data))

    @classmethod
    def load(cls, path: str) -> ReconcileResult:
        """Load a previously saved reconciliation result."""
        raw = json.loads(Path(path).read_text())
        result = cls(
            clusters=raw["clusters"],
            golden_records=raw["golden_records"],
            decisions=raw["decisions"],
            telemetry=raw["telemetry"],
        )
        result._entity_map = {  # type: ignore[attr-defined]
            k: tuple(v) for k, v in raw.get("entity_map", {}).items()
        }
        result._trained_fs_params = raw.get("trained_fs_params")  # type: ignore[attr-defined]
        result._spec_hash = raw.get("spec_hash")  # type: ignore[attr-defined]
        result._entities = raw.get("entities")  # type: ignore[attr-defined]
        return result

    @property
    def entity_lookup(self) -> Any:
        """Reverse index: maps every source record to its canonical kanoniv_id.

        Returns a pandas DataFrame with columns:
            source_system, source_id, kanoniv_id

        Every team that needs to JOIN operational data back to canonical
        entities uses this table.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "entity_lookup requires pandas: pip install pandas"
            ) from None

        entity_map = getattr(self, "_entity_map", {})
        if not entity_map:
            return pd.DataFrame(columns=["source_system", "source_id", "kanoniv_id"])

        # UUID -> kanoniv_id via cluster alignment with golden records
        uuid_to_kid: dict[str, str] = {}
        for i, cluster in enumerate(self.clusters):
            if i < len(self.golden_records):
                kid = self.golden_records[i].get("kanoniv_id", "")
                for uuid in cluster:
                    uuid_to_kid[uuid] = kid

        rows = []
        for uuid, src_info in entity_map.items():
            kid = uuid_to_kid.get(uuid, "")
            if kid:
                rows.append({
                    "source_system": src_info[0],
                    "source_id": src_info[1],
                    "kanoniv_id": kid,
                })

        return pd.DataFrame(rows)

    def changes_since(self, previous: ReconcileResult) -> Any:
        """Detect entity-level changes between this result and a previous one.

        Returns a ChangeLog with: created, grown, merged, split, removed.
        """
        from .changelog import _compute_changes
        return _compute_changes(previous, self)

    def to_pandas(self) -> Any:
        """Convert golden records to a pandas DataFrame (requires pandas)."""
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "ReconcileResult.to_pandas() requires pandas: pip install pandas"
            ) from None
        return pd.DataFrame(self.golden_records)


def _validate_source_attributes(
    sources: list[Source],
    spec_sources: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    """Check source columns against spec attribute mappings.

    Returns (errors, warnings).
    """
    errors: list[str] = []
    warnings: list[str] = []

    spec_by_name = {s.get("name", ""): s for s in spec_sources}

    for source in sources:
        if source.name not in spec_by_name:
            errors.append(
                f"Source '{source.name}' not in spec. "
                f"Declared sources: {sorted(spec_by_name)}"
            )
            continue

        attrs = spec_by_name[source.name].get("attributes", {})
        if not attrs:
            continue

        actual = {col.name for col in source.schema().columns}
        lower_actual = {c.lower(): c for c in actual}
        for canonical, source_col in attrs.items():
            if source_col not in actual and source_col.lower() not in lower_actual:
                msg = (
                    f"Source '{source.name}': column '{source_col}' "
                    f"(mapped to '{canonical}') not found. "
                    f"Available: {sorted(actual)}"
                )
                errors.append(msg)

    for name in spec_by_name:
        if not any(s.name == name for s in sources):
            warnings.append(f"Spec declares source '{name}' but none provided")

    return errors, warnings


def reconcile(
    sources: list[Source],
    spec: Spec,
    previous: ReconcileResult | str | None = None,
) -> ReconcileResult:
    """Run local identity resolution across sources using the given spec.

    1. Validate the spec
    2. Collect entities from all sources
    3. Run the Rust reconciliation engine via PyO3
    4. Return structured results

    For incremental runs (``previous`` is set), new entities are scored
    against existing entities via the Rust engine's FS scoring. Missing
    fields are handled correctly (contribute 0.0, not a penalty).

    Args:
        sources: List of data sources to reconcile.
        spec: The identity spec defining rules and thresholds.
        previous: Optional previous result for incremental reconciliation.
            Can be a ``ReconcileResult`` or a path to a ``.knv`` file.
    """
    from kanoniv._native import reconcile_local, hash as spec_hash

    # 1. Validate spec
    result = validate(spec)
    result.raise_on_error()

    # 2. Pre-flight: check source columns match spec attribute mappings
    src_errors, src_warnings = _validate_source_attributes(sources, spec.sources)
    if src_errors:
        raise ValueError(
            "Source-spec mismatch:\n" + "\n".join(f"  - {e}" for e in src_errors)
        )
    for w in src_warnings:
        _warnings.warn(w, UserWarning, stacklevel=2)

    # 3. Load previous result if path given
    if isinstance(previous, str):
        previous = ReconcileResult.load(previous)

    # 4. Compute spec hash for version checking
    current_spec_hash = spec_hash(spec.raw)

    # 5. If previous result, check spec hash
    if previous is not None:
        prev_hash = getattr(previous, "_spec_hash", None)
        if prev_hash is not None and prev_hash != current_spec_hash:
            _warnings.warn(
                "Spec has changed since previous reconciliation. "
                "Results may be inconsistent. Consider a full re-run.",
                UserWarning,
                stacklevel=2,
            )

    # 6. Extract entity_type from spec
    entity = spec.entity
    if isinstance(entity, dict):
        entity_type = entity.get("name", "entity")
    else:
        entity_type = entity or "entity"

    # 7. Build attribute mappings from spec
    attr_maps: dict[str, dict[str, str]] = {}
    for spec_src in spec.sources:
        src_name = spec_src.get("name", "")
        attrs = spec_src.get("attributes", {})
        if attrs and src_name:
            attr_maps[src_name] = {v: k for k, v in attrs.items()}

    # 8. Collect entities from all sources, applying attribute mappings
    #    Case-insensitive: Snowflake returns UPPERCASE columns, spec uses lowercase.
    all_entities: list[dict[str, Any]] = []
    for source in sources:
        entities = source.to_entities(entity_type)
        reverse_map = attr_maps.get(source.name, {})
        if reverse_map:
            lower_map = {k.lower(): v for k, v in reverse_map.items()}
            for entity_dict in entities:
                raw_data = entity_dict["data"]
                mapped: dict[str, str] = {}
                for col, val in raw_data.items():
                    canonical = reverse_map.get(col) or lower_map.get(col.lower(), col)
                    mapped[canonical] = val
                entity_dict["data"] = mapped
        all_entities.extend(entities)

    # 9. Build entity map (UUID -> (source_name, external_id))
    entity_map = {
        e["id"]: (e["source_name"], e["external_id"])
        for e in all_entities
    }

    # 10. Incremental or full reconciliation
    if previous is not None:
        from kanoniv._native import reconcile_incremental

        existing_entities = getattr(previous, "_entities", None) or []
        existing_clusters = previous.clusters
        trained_fs_params = getattr(previous, "_trained_fs_params", None)

        raw = reconcile_incremental(
            spec.raw,
            json.dumps(all_entities),
            json.dumps(existing_entities),
            json.dumps(existing_clusters),
            trained_fs_params,
        )

        # Merge entity maps: previous + new
        prev_entity_map = getattr(previous, "_entity_map", {})
        entity_map = {**prev_entity_map, **entity_map}

        # All entities = existing + new for storage
        stored_entities = existing_entities + all_entities

        result = ReconcileResult(
            clusters=raw.get("clusters", []),
            golden_records=raw.get("golden_records", []),
            decisions=raw.get("decisions", []),
            telemetry=raw.get("telemetry", {}),
        )
        result._entity_map = entity_map  # type: ignore[attr-defined]
        result._trained_fs_params = raw.get("trained_fs_params")  # type: ignore[attr-defined]
        result._spec_hash = current_spec_hash  # type: ignore[attr-defined]
        result._entities = stored_entities  # type: ignore[attr-defined]
        return result

    # Full reconciliation
    entities_json = json.dumps(all_entities)
    raw = reconcile_local(spec.raw, entities_json)

    result = ReconcileResult(
        clusters=raw.get("clusters", []),
        golden_records=raw.get("golden_records", []),
        decisions=raw.get("decisions", []),
        telemetry=raw.get("telemetry", {}),
    )
    result._entity_map = entity_map  # type: ignore[attr-defined]
    result._trained_fs_params = raw.get("trained_fs_params")  # type: ignore[attr-defined]
    result._spec_hash = current_spec_hash  # type: ignore[attr-defined]
    result._entities = all_entities  # type: ignore[attr-defined]
    return result
