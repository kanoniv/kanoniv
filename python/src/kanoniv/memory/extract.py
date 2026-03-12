"""LLM-powered fact extraction from conversations.

Converts raw chat messages into structured memory entries. The LLM
extracts discrete facts, identifies entity information (names, emails,
companies), and classifies each fact by type.

Supports any OpenAI-compatible API (OpenAI, Anthropic via proxy,
Ollama, vLLM, etc).
"""

from __future__ import annotations

import json
import os
from typing import Any

_EXTRACTION_PROMPT = """\
You are a memory extraction system. Analyze the conversation below and extract \
discrete, standalone facts worth remembering for future conversations.

For each fact, extract:
- "fact": A concise, third-person statement (e.g., "User prefers annual billing")
- "category": One of: preference, decision, personal_info, relationship, project, action_item, knowledge
- "entity_fields": Any identifying info about the person/org mentioned (name, email, phone, company). \
Only include fields that are explicitly stated in the conversation. Return as a dict or null.

Rules:
- Extract ONLY facts explicitly stated or strongly implied. Do not infer or speculate.
- Each fact should be a single, atomic piece of information.
- Write facts in third person using the user's name if known, otherwise "User".
- Merge duplicate or redundant facts into one.
- Skip greetings, filler, and conversational pleasantries.
- If no facts worth remembering, return an empty list.

Respond with a JSON object:
{"facts": [{"fact": "...", "category": "...", "entity_fields": {...} or null}]}

Conversation:
{conversation}"""

_UPDATE_PROMPT = """\
You are a memory management system. You have existing memories and new facts \
extracted from a recent conversation. Decide what to do with each new fact.

Existing memories:
{existing_memories}

New facts:
{new_facts}

For each new fact, decide:
- "action": "add" (new information), "update" (replaces/refines an existing memory), \
"skip" (already known, no change needed)
- "fact": The fact text (updated version if action is "update")
- "category": The category
- "entity_fields": Entity fields or null
- "existing_id": If action is "update", the ID of the memory being updated. Null otherwise.

Rules:
- "update" means the new fact supersedes or refines an existing memory. Include the updated text.
- "skip" means the fact is already captured in an existing memory with no new information.
- "add" means this is genuinely new information not covered by any existing memory.
- When in doubt, prefer "add" over "skip".

Respond with a JSON object:
{"decisions": [{"action": "...", "fact": "...", "category": "...", "entity_fields": {...} or null, "existing_id": "..." or null}]}"""

# Map extraction categories to our entry_type system
_CATEGORY_TO_ENTRY_TYPE = {
    "preference": "knowledge",
    "decision": "decision",
    "personal_info": "knowledge",
    "relationship": "knowledge",
    "project": "investigation",
    "action_item": "task",
    "knowledge": "knowledge",
}


class FactExtractor:
    """Extract facts from conversations using an LLM.

    Works with any OpenAI-compatible API. Provide an API key and
    optionally a custom base URL for non-OpenAI providers.

    Args:
        api_key: OpenAI API key (or compatible provider key).
            Falls back to ``OPENAI_API_KEY`` env var.
        model: Model to use for extraction. Defaults to ``gpt-4.1-nano``.
        base_url: Base URL for the API. Defaults to OpenAI.
    """

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "gpt-4.1-nano",
        base_url: str = "https://api.openai.com",
    ) -> None:
        self._api_key = api_key or os.environ.get("OPENAI_API_KEY", "")
        self._model = model
        self._base_url = base_url.rstrip("/")
        self._http: Any = None

    @property
    def available(self) -> bool:
        return bool(self._api_key)

    def _get_http(self) -> Any:
        if self._http is None:
            try:
                import httpx

                self._http = httpx.Client(timeout=30.0)
            except ImportError:
                import urllib.request

                self._http = "urllib"
        return self._http

    def _call_llm(self, system: str, user: str) -> str:
        """Call an OpenAI-compatible chat completions endpoint."""
        url = f"{self._base_url}/v1/chat/completions"
        payload = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": 0,
            "response_format": {"type": "json_object"},
        }

        http = self._get_http()

        if http == "urllib":
            return self._call_llm_urllib(url, payload)

        resp = http.post(
            url,
            json=payload,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]

    def _call_llm_urllib(self, url: str, payload: dict) -> str:
        """Fallback for when httpx is not installed."""
        import urllib.request

        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode(),
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        return data["choices"][0]["message"]["content"]

    def extract(self, messages: list[dict[str, str]]) -> list[dict[str, Any]]:
        """Extract facts from a conversation.

        Args:
            messages: List of chat messages, each with "role" and "content" keys.

        Returns:
            List of extracted facts, each with "fact", "category", and
            "entity_fields" keys.
        """
        if not self.available:
            return []

        conversation = self._format_messages(messages)
        raw = self._call_llm(
            "You are a memory extraction system. Respond only with valid JSON.",
            _EXTRACTION_PROMPT.replace("{conversation}", conversation),
        )

        try:
            parsed = json.loads(raw)
            facts = parsed.get("facts", [])
            # Validate structure
            return [
                {
                    "fact": f.get("fact", ""),
                    "category": f.get("category", "knowledge"),
                    "entity_fields": f.get("entity_fields"),
                }
                for f in facts
                if f.get("fact")
            ]
        except (json.JSONDecodeError, AttributeError):
            return []

    def deduplicate(
        self,
        new_facts: list[dict[str, Any]],
        existing_memories: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Decide which facts to add, update, or skip given existing memories.

        Args:
            new_facts: Facts extracted from a new conversation.
            existing_memories: Existing memories (id, content pairs).

        Returns:
            List of decisions with "action", "fact", "category",
            "entity_fields", and "existing_id" keys.
        """
        if not self.available or not new_facts:
            return [
                {"action": "add", **f, "existing_id": None}
                for f in new_facts
            ]

        if not existing_memories:
            return [
                {"action": "add", **f, "existing_id": None}
                for f in new_facts
            ]

        # Remap real UUIDs to sequential integers so the LLM can't
        # hallucinate non-existent IDs. Map back after parsing.
        idx_to_real_id: dict[str, str] = {}
        for i, m in enumerate(existing_memories):
            idx_to_real_id[str(i)] = m["id"]

        existing_str = "\n".join(
            f"[{i}] {m['content']}" for i, m in enumerate(existing_memories)
        )
        new_str = "\n".join(
            f"- {f['fact']}" for f in new_facts
        )

        raw = self._call_llm(
            "You are a memory management system. Respond only with valid JSON.",
            _UPDATE_PROMPT.replace(
                "{existing_memories}", existing_str,
            ).replace(
                "{new_facts}", new_str,
            ),
        )

        try:
            parsed = json.loads(raw)
            decisions = parsed.get("decisions", [])
            return [
                {
                    "action": d.get("action", "add"),
                    "fact": d.get("fact", ""),
                    "category": d.get("category", "knowledge"),
                    "entity_fields": d.get("entity_fields"),
                    "existing_id": idx_to_real_id.get(
                        str(d.get("existing_id")), d.get("existing_id")
                    ),
                }
                for d in decisions
                if d.get("fact")
            ]
        except (json.JSONDecodeError, AttributeError):
            # On parse failure, just add everything
            return [
                {"action": "add", **f, "existing_id": None}
                for f in new_facts
            ]

    @staticmethod
    def _format_messages(messages: list[dict[str, str]]) -> str:
        """Format chat messages into a readable conversation string."""
        lines = []
        for msg in messages:
            role = msg.get("role", "user").capitalize()
            content = msg.get("content", "")
            lines.append(f"{role}: {content}")
        return "\n".join(lines)

    @staticmethod
    def category_to_entry_type(category: str) -> str:
        return _CATEGORY_TO_ENTRY_TYPE.get(category, "knowledge")
