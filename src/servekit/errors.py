"""Typed failures.

Every error carries the HTTP status a gateway should return, so adapters stay thin
and the degradation ladder stays honest: reject early, never time out silently.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


class PlatformError(Exception):
    status = 500
    code = "internal"

    def __init__(
        self,
        message: str,
        *,
        retry_after: Optional[float] = None,
        detail: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.retry_after = retry_after
        self.detail = detail or {}

    def to_dict(self) -> Dict[str, Any]:
        body: Dict[str, Any] = {"error": {"code": self.code, "message": self.message}}
        if self.retry_after is not None:
            body["error"]["retry_after"] = self.retry_after
        if self.detail:
            body["error"]["detail"] = self.detail
        return body


class InvalidRequest(PlatformError):
    status = 400
    code = "invalid_request"


class Unauthorized(PlatformError):
    status = 401
    code = "unauthorized"


class PolicyDenied(PlatformError):
    status = 403
    code = "denied"


class RateLimited(PlatformError):
    status = 429
    code = "rate_limited"


class NoCapacity(PlatformError):
    status = 503
    code = "no_capacity"


class Overloaded(PlatformError):
    status = 503
    code = "overloaded"


class DeadlineExceeded(PlatformError):
    status = 504
    code = "deadline_exceeded"


class UpstreamError(PlatformError):
    status = 502
    code = "upstream_error"
