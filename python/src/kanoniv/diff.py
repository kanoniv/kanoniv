"""Spec diffing - compare two spec versions."""
from kanoniv._native import diff as _diff
from kanoniv.spec import Spec


class DiffResult:
    """Result of comparing two spec versions.

    Provides granular change detection across all spec sections:
    rules, sources, entity, blocking, thresholds, survivorship, and scoring.
    """

    def __init__(self, data: dict):
        self._data = data

    # -- Rules --

    @property
    def rules_added(self) -> list[str]:
        """Rule names present in spec_b but not spec_a."""
        return self._data.get("rules_added", [])

    @property
    def rules_removed(self) -> list[str]:
        """Rule names present in spec_a but not spec_b."""
        return self._data.get("rules_removed", [])

    @property
    def rules_modified(self) -> list[dict]:
        """Rules that exist in both specs but have changed fields.

        Each entry is a dict with keys: ``name``, ``field``, ``old_value``,
        ``new_value``.
        """
        return self._data.get("rules_modified", [])

    # -- Sources --

    @property
    def sources_added(self) -> list[str]:
        """Source names present in spec_b but not spec_a."""
        return self._data.get("sources_added", [])

    @property
    def sources_removed(self) -> list[str]:
        """Source names present in spec_a but not spec_b."""
        return self._data.get("sources_removed", [])

    @property
    def sources_modified(self) -> list[dict]:
        """Sources that exist in both specs but have changed fields.

        Each entry is a dict with keys: ``name``, ``field``, ``old_value``,
        ``new_value``.
        """
        return self._data.get("sources_modified", [])

    # -- Entity --

    @property
    def entity_changed(self) -> bool:
        """Whether the entity definition changed (name, type, etc.)."""
        return self._data.get("entity_changed", False)

    @property
    def entity_changes(self) -> list[dict]:
        """Field-level entity changes. Each dict has ``path``, ``old_value``, ``new_value``."""
        return self._data.get("entity_changes", [])

    # -- Blocking --

    @property
    def blocking_changed(self) -> bool:
        """Whether blocking strategy or keys changed."""
        return self._data.get("blocking_changed", False)

    @property
    def blocking_changes(self) -> list[dict]:
        """Field-level blocking changes. Each dict has ``path``, ``old_value``, ``new_value``."""
        return self._data.get("blocking_changes", [])

    # -- Thresholds / Decision --

    @property
    def thresholds_changed(self) -> bool:
        """Whether decision thresholds (match, review, reject) changed."""
        return self._data.get("thresholds_changed", False)

    @property
    def decision_changes(self) -> list[dict]:
        """Field-level decision changes. Each dict has ``path``, ``old_value``, ``new_value``."""
        return self._data.get("decision_changes", [])

    # -- Survivorship --

    @property
    def survivorship_changed(self) -> bool:
        """Whether survivorship rules changed."""
        return self._data.get("survivorship_changed", False)

    @property
    def survivorship_changes(self) -> list[dict]:
        """Field-level survivorship changes. Each dict has ``path``, ``old_value``, ``new_value``."""
        return self._data.get("survivorship_changes", [])

    # -- Scoring --

    @property
    def scoring_changed(self) -> bool:
        """Whether scoring model configuration changed."""
        return self._data.get("scoring_changed", False)

    @property
    def scoring_changes(self) -> list[dict]:
        """Field-level scoring changes. Each dict has ``path``, ``old_value``, ``new_value``."""
        return self._data.get("scoring_changes", [])

    # -- Metadata --

    @property
    def metadata_changed(self) -> bool:
        """Whether spec metadata changed."""
        return self._data.get("metadata_changed", False)

    @property
    def metadata_changes(self) -> list[dict]:
        """Field-level metadata changes. Each dict has ``path``, ``old_value``, ``new_value``."""
        return self._data.get("metadata_changes", [])

    # -- Version --

    @property
    def version_changed(self) -> bool:
        """Whether identity_version changed."""
        return self._data.get("version_changed", False)

    # -- Aggregate --

    @property
    def summary(self) -> str:
        """Human-readable summary of all changes."""
        return self._data.get("summary", "")

    @property
    def has_changes(self) -> bool:
        """Return True if any part of the spec changed."""
        return bool(
            self.rules_added
            or self.rules_removed
            or self.rules_modified
            or self.sources_added
            or self.sources_removed
            or self.sources_modified
            or self.entity_changed
            or self.blocking_changed
            or self.thresholds_changed
            or self.survivorship_changed
            or self.scoring_changed
            or self.metadata_changed
            or self.version_changed
        )

    def __repr__(self) -> str:
        if not self.has_changes:
            return "<DiffResult: no changes>"
        return f"<DiffResult: {self.summary}>"


def diff(spec_a: Spec, spec_b: Spec) -> DiffResult:
    """Compare two spec versions and return a detailed diff.

    Args:
        spec_a: The baseline (old) spec.
        spec_b: The updated (new) spec.

    Returns:
        A ``DiffResult`` with granular change information across rules,
        sources, entity, blocking, thresholds, survivorship, and scoring.
    """
    data = _diff(spec_a.raw, spec_b.raw)
    return DiffResult(data)
