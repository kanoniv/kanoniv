"""Execution planning — thin wrapper over Rust planner."""
from kanoniv._native import plan as _plan
from kanoniv.spec import Spec

class PlanResult:
    """Structured execution plan for an identity spec."""

    def __init__(self, data: dict):
        self._data = data

    @property
    def entity(self) -> str:
        return self._data.get("entity", "")

    @property
    def plan_hash(self) -> str:
        return self._data.get("plan_hash", "")

    @property
    def execution_stages(self) -> list[dict]:
        return self._data.get("execution_stages", [])

    @property
    def match_strategies(self) -> list[dict]:
        return self._data.get("match_strategies", [])

    @property
    def survivorship(self) -> list[dict]:
        return self._data.get("survivorship_summary", [])

    @property
    def blocking(self) -> dict:
        return self._data.get("blocking_analysis", {})

    @property
    def risk_flags(self) -> list[dict]:
        return self._data.get("risk_flags", [])

    def summary(self) -> str:
        """Human-readable plan summary."""
        return self._data.get("summary", "")

    def to_dict(self) -> dict:
        """Full plan as serializable dict (for CI/CD, storage, comparison)."""
        return self._data

    def __repr__(self) -> str:
        return self.summary()

def plan(spec: Spec) -> PlanResult:
    data = _plan(spec.raw)
    return PlanResult(data)
