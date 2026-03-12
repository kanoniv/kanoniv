"""HTTP client for the Kanoniv CLI (cloud commands).

Uses httpx for HTTP requests and manual SSE parsing for streaming.
Requires ``pip install kanoniv[cloud]`` (httpx is in the cloud extra).
"""
from __future__ import annotations

import json
import sys
from typing import Any, Generator


def _require_httpx():
    """Lazy-import httpx or print a helpful error."""
    try:
        import httpx
        return httpx
    except ImportError:
        sys.stderr.write(
            "This command requires httpx. Install with:\n"
            "  pip install kanoniv[cloud]\n"
        )
        sys.exit(1)


class CliHttpClient:
    """Thin HTTP wrapper for CLI cloud commands."""

    def __init__(self, base_url: str, api_key: str) -> None:
        httpx = _require_httpx()
        self._base_url = base_url
        self._api_key = api_key
        self._client = httpx.Client(
            base_url=base_url,
            headers={"X-API-Key": api_key},
            timeout=30.0,
        )

    def _handle_error(self, response: Any) -> None:
        if response.status_code >= 400:
            try:
                body = response.json()
                msg = body.get("detail") or body.get("error") or body.get("message") or response.text
            except Exception:
                msg = response.text
            sys.stderr.write(f"API error ({response.status_code}): {msg}\n")
            sys.exit(1)

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        resp = self._client.get(path, params=params)
        self._handle_error(resp)
        if not resp.text:
            return None
        return resp.json()

    def post(
        self, path: str, body: dict[str, Any] | None = None, timeout: float | None = None,
    ) -> Any:
        resp = self._client.post(path, json=body or {}, timeout=timeout)
        self._handle_error(resp)
        if not resp.text:
            return None
        return resp.json()

    def put(self, path: str, body: dict[str, Any] | None = None) -> Any:
        resp = self._client.put(path, json=body or {})
        self._handle_error(resp)
        if not resp.text:
            return None
        return resp.json()

    def delete(self, path: str) -> Any:
        resp = self._client.delete(path)
        self._handle_error(resp)
        if not resp.text:
            return None
        return resp.json()

    def stream_sse(
        self, path: str, body: dict[str, Any]
    ) -> Generator[tuple[str | None, str | None], None, None]:
        """POST with SSE streaming. Yields (event_type, data) tuples.

        Events: conversation_id, delta/text, usage, done, error.
        Uses a 120s read timeout for long-running LLM responses.
        """
        httpx = _require_httpx()
        # Use a separate client with longer timeout for streaming
        with httpx.Client(
            base_url=self._base_url,
            headers={"X-API-Key": self._api_key},
            timeout=httpx.Timeout(connect=10.0, read=120.0, write=10.0, pool=10.0),
        ) as stream_client:
            with stream_client.stream("POST", path, json=body) as response:
                if response.status_code >= 400:
                    response.read()
                    try:
                        err = response.json()
                        msg = err.get("detail") or err.get("error") or err.get("message") or response.text
                    except Exception:
                        msg = response.text
                    sys.stderr.write(f"API error ({response.status_code}): {msg}\n")
                    sys.exit(1)

                # Parse SSE events from the stream
                buffer = ""
                for chunk in response.iter_text():
                    buffer += chunk
                    while "\n\n" in buffer:
                        event_block, buffer = buffer.split("\n\n", 1)
                        event_type, data = _parse_sse_block(event_block)
                        yield event_type, data

    def close(self) -> None:
        self._client.close()


def _parse_sse_block(block: str) -> tuple[str | None, str | None]:
    """Parse a single SSE event block into (event_type, data)."""
    event_type = None
    data_lines: list[str] = []

    for line in block.split("\n"):
        if line.startswith("event:"):
            event_type = line[len("event:"):].strip()
        elif line.startswith("data:"):
            # Strip exactly one space after "data:" per SSE spec, not all whitespace
            raw = line[len("data:"):]
            data_lines.append(raw[1:] if raw.startswith(" ") else raw)
        elif line.startswith(":"):
            pass  # SSE comment

    data = "\n".join(data_lines) if data_lines else None
    return event_type, data
