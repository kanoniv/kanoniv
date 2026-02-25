"""Kanoniv - Identity resolution as code."""

from .spec import Spec
from .validate import validate
from .plan import plan
from .diff import diff
from .source import Source
from .reconcile import reconcile, ReconcileResult
from .evaluate import EvaluateResult
from .changelog import ChangeLog, EntityChange

__version__ = "0.3.1"


def __getattr__(name: str):
    """Lazy-load cloud extras - they require httpx/pydantic (pip install kanoniv[cloud])."""
    import importlib as _il
    import sys as _sys

    if name == "cloud":
        try:
            _cloud = _il.import_module("kanoniv.cloud")
        except ImportError:
            raise ImportError(
                "kanoniv.cloud requires the 'cloud' extra: pip install kanoniv[cloud]"
            ) from None
        # Cache on the module so __getattr__ isn't called again
        _sys.modules[__name__].cloud = _cloud  # type: ignore[attr-defined]
        return _cloud
    if name in ("Client", "AsyncClient"):
        try:
            from .client import Client, AsyncClient
        except ImportError:
            raise ImportError(
                f"kanoniv.{name} requires the 'cloud' extra: pip install kanoniv[cloud]"
            ) from None
        if name == "Client":
            return Client
        return AsyncClient
    raise AttributeError(f"module 'kanoniv' has no attribute {name!r}")


__all__ = [
    "Spec",
    "Source",
    "validate",
    "plan",
    "diff",
    "reconcile",
    "ReconcileResult",
    "EvaluateResult",
    "ChangeLog",
    "EntityChange",
    "Client",
    "AsyncClient",
    "cloud",
]
