"""Kanoniv — Identity resolution as code."""

from .spec import Spec
from .validate import validate
from .plan import plan
from .diff import diff

__version__ = "0.2.0"


def __getattr__(name: str):
    """Lazy-load Client/AsyncClient — they require httpx/pydantic (pip install kanoniv[cloud])."""
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


__all__ = ["Spec", "validate", "plan", "diff", "Client", "AsyncClient"]
