"""Kanoniv — Identity resolution as code."""

__version__ = "0.2.0"

_NATIVE_MODULES = {"Spec", "validate", "plan", "diff"}
_CLOUD_MODULES = {"Client", "AsyncClient"}


def __getattr__(name: str):
    if name in _NATIVE_MODULES:
        if name == "Spec":
            from .spec import Spec
            return Spec
        elif name == "validate":
            from .validate import validate
            return validate
        elif name == "plan":
            from .plan import plan
            return plan
        elif name == "diff":
            from .diff import diff
            return diff

    if name in _CLOUD_MODULES:
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
