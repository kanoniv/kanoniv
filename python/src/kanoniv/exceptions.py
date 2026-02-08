"""Typed exceptions for Kanoniv API errors."""

from __future__ import annotations

from typing import Any


class KanonivError(Exception):
    """Base exception for all Kanoniv SDK errors."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        body: Any = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class AuthenticationError(KanonivError):
    """401 Unauthorized — invalid or missing credentials."""


class ForbiddenError(KanonivError):
    """403 Forbidden — insufficient permissions."""


class NotFoundError(KanonivError):
    """404 Not Found — resource does not exist."""


class ValidationError(KanonivError):
    """400 Bad Request — invalid input."""


class ConflictError(KanonivError):
    """409 Conflict — resource already exists or state conflict."""


class RateLimitError(KanonivError):
    """429 Too Many Requests — rate limit exceeded."""

    def __init__(
        self,
        message: str,
        *,
        retry_after: float | None = None,
        status_code: int | None = 429,
        body: Any = None,
    ) -> None:
        super().__init__(message, status_code=status_code, body=body)
        self.retry_after = retry_after


class ServerError(KanonivError):
    """5xx Server Error — something went wrong on the server."""
