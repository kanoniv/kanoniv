"""Credential storage for the Kanoniv CLI.

Credentials are persisted at ``~/.config/kanoniv/credentials.json`` with
0600 permissions. Resolution order:

1. ``--api-key`` flag
2. ``KANONIV_API_KEY`` environment variable
3. Stored credentials file
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

_CONFIG_DIR = Path.home() / ".config" / "kanoniv"
_CREDS_FILE = _CONFIG_DIR / "credentials.json"


def _read_creds() -> dict[str, Any]:
    if _CREDS_FILE.exists():
        try:
            return json.loads(_CREDS_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _write_creds(data: dict[str, Any]) -> None:
    _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    _CREDS_FILE.write_text(json.dumps(data, indent=2))
    try:
        os.chmod(_CREDS_FILE, 0o600)
    except OSError:
        pass  # Windows doesn't support Unix permissions


def save_credentials(api_key: str, api_url: str) -> None:
    """Persist API key and URL."""
    creds = _read_creds()
    creds["api_key"] = api_key
    creds["api_url"] = api_url
    _write_creds(creds)


def clear_credentials() -> None:
    """Remove stored credentials."""
    if _CREDS_FILE.exists():
        _CREDS_FILE.unlink()


def resolve_api_key(flag: str | None) -> str | None:
    """Return the API key from flag > env > file."""
    if flag:
        return flag
    env = os.environ.get("KANONIV_API_KEY")
    if env:
        return env
    return _read_creds().get("api_key")


def resolve_api_url(flag: str | None) -> str:
    """Return the API URL from flag > env > file > default."""
    if flag:
        return flag.rstrip("/")
    env = os.environ.get("KANONIV_API_URL")
    if env:
        return env.rstrip("/")
    stored = _read_creds().get("api_url")
    if stored:
        return stored.rstrip("/")
    return "https://api.kanoniv.com"


def save_last_conversation(conversation_id: str) -> None:
    """Persist the last conversation ID for auto-continuation."""
    creds = _read_creds()
    creds["last_conversation"] = conversation_id
    _write_creds(creds)


def get_last_conversation() -> str | None:
    """Return the last conversation ID, if any."""
    return _read_creds().get("last_conversation")


def clear_last_conversation() -> None:
    """Remove the stored last conversation ID."""
    creds = _read_creds()
    creds.pop("last_conversation", None)
    _write_creds(creds)


def get_context() -> dict[str, Any]:
    """Return the current stored context for display."""
    creds = _read_creds()
    key = creds.get("api_key", "")
    if not key:
        masked = "(not set)"
    elif len(key) > 12:
        masked = f"{key[:8]}...{key[-4:]}"
    else:
        masked = f"{key[:4]}..."
    return {
        "api_key": masked,
        "api_url": creds.get("api_url", "https://api.kanoniv.com"),
        "credentials_file": str(_CREDS_FILE),
    }
