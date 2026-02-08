"""Spec diffing — compare two spec versions."""
from kanoniv._native import diff as _diff
from kanoniv.spec import Spec


class DiffResult:
    def __init__(self, data: dict):
        self._data = data

    @property
    def rules_added(self) -> list[str]:
        return self._data.get("rules_added", [])

    @property
    def rules_removed(self) -> list[str]:
        return self._data.get("rules_removed", [])

    @property
    def rules_modified(self) -> list[dict]:
        return self._data.get("rules_modified", [])

    @property
    def thresholds_changed(self) -> bool:
        return self._data.get("thresholds_changed", False)

    @property
    def summary(self) -> str:
        return self._data.get("summary", "")

    def __repr__(self) -> str:
        return f"<DiffResult: {self.summary}>"


def diff(spec_a: Spec, spec_b: Spec) -> DiffResult:
    data = _diff(spec_a.raw, spec_b.raw)
    return DiffResult(data)
