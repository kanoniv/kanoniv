"""Local memory for AI agents - zero config, works immediately.

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

    # Get all memories for a user
    mem.get_all(user_id="bill@acme.com")
"""

from .entry import MemoryEntry
from .extract import FactExtractor
from .local import LocalMemory
from .store import MemoryStore

__all__ = ["MemoryEntry", "FactExtractor", "LocalMemory", "MemoryStore"]
