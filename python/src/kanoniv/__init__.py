"""Kanoniv - Identity resolution as code."""

from .spec import Spec
from .validate import validate
from .plan import plan
from .diff import diff
from .source import Source
from .reconcile import reconcile, ReconcileResult
from .evaluate import EvaluateResult
from .changelog import ChangeLog, EntityChange

__version__ = "0.3.8"


def get_memory(
    agent_name: str = "default",
    db_path: str | None = None,
    api_key: str | None = None,
    model: str = "gpt-4.1-nano",
    api_base_url: str = "https://api.openai.com",
) -> "LocalMemory":
    """Get a local memory instance. Zero config, works immediately.

    Usage::

        import kanoniv
        mem = kanoniv.get_memory(agent_name="support-agent", api_key="sk-...")

        # Dump conversations in, facts get extracted automatically
        mem.add([
            {"role": "user", "content": "I'm Bill from Acme, switch to annual billing"},
            {"role": "assistant", "content": "Done!"},
        ], user_id="bill@acme.com")

        # Search by meaning
        mem.search("billing preferences")

    Args:
        agent_name: Name of the agent using this memory.
        db_path: Path to SQLite database. Defaults to ``~/.kanoniv/memory.db``.
        api_key: OpenAI API key for LLM-powered fact extraction in ``add()``.
            Falls back to ``OPENAI_API_KEY`` env var. Without this, ``add()``
            is unavailable but ``memorize()``/``search()`` still work.
        model: LLM model for extraction. Defaults to ``gpt-4.1-nano``.
        api_base_url: Base URL for OpenAI-compatible API.

    Returns:
        A LocalMemory instance ready to use.
    """
    from .memory.local import LocalMemory

    return LocalMemory(
        db_path=db_path,
        agent_name=agent_name,
        api_key=api_key,
        model=model,
        api_base_url=api_base_url,
    )


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
    if name == "LocalMemory":
        from .memory.local import LocalMemory

        return LocalMemory
    if name == "MemoryEntry":
        from .memory.entry import MemoryEntry

        return MemoryEntry
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
    "get_memory",
    "LocalMemory",
    "MemoryEntry",
]
