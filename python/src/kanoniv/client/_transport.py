"""HTTP transport layer - wraps httpx with auth, retries, and error mapping."""

from __future__ import annotations

import time
from typing import Any

import httpx

from ..exceptions import (
    AuthenticationError,
    ConflictError,
    ForbiddenError,
    KanonivError,
    NotFoundError,
    RateLimitError,
    ServerError,
    ValidationError,
)

_STATUS_MAP: dict[int, type[KanonivError]] = {
    400: ValidationError,
    401: AuthenticationError,
    403: ForbiddenError,
    404: NotFoundError,
    409: ConflictError,
    429: RateLimitError,
}

DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_RETRIES = 2
_RETRYABLE = frozenset({408, 429, 502, 503, 504})


def _build_auth_headers(
    api_key: str | None,
    access_token: str | None,
) -> dict[str, str]:
    headers: dict[str, str] = {}
    if api_key:
        headers["X-API-Key"] = api_key
    elif access_token:
        headers["Authorization"] = f"Bearer {access_token}"
    return headers


def _raise_for_status(response: httpx.Response) -> None:
    if response.is_success:
        return

    try:
        body = response.json()
    except Exception:
        body = response.text

    message = body.get("error", response.reason_phrase) if isinstance(body, dict) else str(body)
    status = response.status_code

    if status == 429:
        retry_after = response.headers.get("Retry-After")
        raise RateLimitError(
            message,
            retry_after=float(retry_after) if retry_after else None,
            status_code=status,
            body=body,
        )

    exc_cls = _STATUS_MAP.get(status)
    if exc_cls is None:
        exc_cls = ServerError if status >= 500 else KanonivError

    raise exc_cls(message, status_code=status, body=body)


class SyncTransport:
    """Synchronous HTTP transport using httpx."""

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        access_token: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        self._max_retries = max_retries
        self._client = httpx.Client(
            base_url=base_url,
            headers=_build_auth_headers(api_key, access_token),
            timeout=timeout,
        )

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        data: Any | None = None,
        files: Any | None = None,
    ) -> Any:
        last_exc: Exception | None = None
        for attempt in range(1 + self._max_retries):
            try:
                response = self._client.request(
                    method,
                    path,
                    params=_clean_params(params),
                    json=json,
                    data=data,
                    files=files,
                )
                if response.status_code in _RETRYABLE and attempt < self._max_retries:
                    _backoff(attempt, response)
                    continue
                _raise_for_status(response)
                if response.status_code == 204:
                    return None
                return response.json()
            except (httpx.ConnectError, httpx.ReadTimeout) as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    time.sleep(0.5 * 2**attempt)
                    continue
                raise KanonivError(f"Connection failed: {exc}") from exc
        raise KanonivError(f"Request failed after {self._max_retries + 1} attempts") from last_exc

    def close(self) -> None:
        self._client.close()


class AsyncTransport:
    """Asynchronous HTTP transport using httpx."""

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        access_token: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        self._max_retries = max_retries
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers=_build_auth_headers(api_key, access_token),
            timeout=timeout,
        )

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        data: Any | None = None,
        files: Any | None = None,
    ) -> Any:
        import asyncio

        last_exc: Exception | None = None
        for attempt in range(1 + self._max_retries):
            try:
                response = await self._client.request(
                    method,
                    path,
                    params=_clean_params(params),
                    json=json,
                    data=data,
                    files=files,
                )
                if response.status_code in _RETRYABLE and attempt < self._max_retries:
                    wait = _get_backoff(attempt, response)
                    await asyncio.sleep(wait)
                    continue
                _raise_for_status(response)
                if response.status_code == 204:
                    return None
                return response.json()
            except (httpx.ConnectError, httpx.ReadTimeout) as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    await asyncio.sleep(0.5 * 2**attempt)
                    continue
                raise KanonivError(f"Connection failed: {exc}") from exc
        raise KanonivError(f"Request failed after {self._max_retries + 1} attempts") from last_exc

    async def close(self) -> None:
        await self._client.aclose()


def _clean_params(params: dict[str, Any] | None) -> dict[str, Any] | None:
    if params is None:
        return None
    return {k: v for k, v in params.items() if v is not None}


def _get_backoff(attempt: int, response: httpx.Response) -> float:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return float(retry_after)
        except ValueError:
            pass
    return 0.5 * 2**attempt


def _backoff(attempt: int, response: httpx.Response) -> None:
    time.sleep(_get_backoff(attempt, response))
