"""Policies: the operational concerns that are identical for every model.

Add a capability (TTS, ASR, embeddings, video) and these keep working unchanged.
That is the point of the framework: one place to fix quota, auth and audit.
"""

from __future__ import annotations

import json
import time
from typing import Dict, Iterable, List, Set

from .contracts import Context, Request, Response
from .errors import PolicyDenied, RateLimited, Unauthorized

# --------------------------------------------------------------------------- #
# Auth
# --------------------------------------------------------------------------- #


class ApiKeyPolicy:
    """Bearer-style key check before anything expensive happens."""

    name = "api_key"

    def __init__(self, valid_keys: Iterable[str], header: str = "api_key") -> None:
        self.valid_keys: Set[str] = set(valid_keys)
        self.header = header

    async def before(self, request: Request, ctx: Context) -> Request:
        key = request.metadata.get(self.header)
        if key not in self.valid_keys:
            raise Unauthorized("missing or invalid api key")
        return request

    async def after(self, response: Response, ctx: Context) -> Response:
        return response


# --------------------------------------------------------------------------- #
# Rate limiting
# --------------------------------------------------------------------------- #


class _Bucket:
    __slots__ = ("tokens", "updated_at")

    def __init__(self, tokens: float) -> None:
        self.tokens = tokens
        self.updated_at = time.monotonic()


class TenantQuotaPolicy:
    """Token bucket per tenant. Protects the platform from one noisy client."""

    name = "tenant_quota"

    def __init__(self, rate_per_s: float = 5.0, burst: float = 10.0) -> None:
        self.rate = rate_per_s
        self.burst = burst
        self._buckets: Dict[str, _Bucket] = {}

    def _allow(self, tenant: str) -> bool:
        bucket = self._buckets.setdefault(tenant, _Bucket(self.burst))
        now = time.monotonic()
        bucket.tokens = min(self.burst, bucket.tokens + (now - bucket.updated_at) * self.rate)
        bucket.updated_at = now
        if bucket.tokens >= 1.0:
            bucket.tokens -= 1.0
            return True
        return False

    async def before(self, request: Request, ctx: Context) -> Request:
        if not self._allow(request.tenant):
            raise RateLimited("tenant rate limit exceeded", retry_after=1.0)
        return request

    async def after(self, response: Response, ctx: Context) -> Response:
        return response


# --------------------------------------------------------------------------- #
# Input hygiene
# --------------------------------------------------------------------------- #


class PayloadLimitPolicy:
    """Rejects oversized inputs before they reach a GPU."""

    name = "payload_limit"

    def __init__(self, max_bytes: int = 1_000_000) -> None:
        self.max_bytes = max_bytes

    async def before(self, request: Request, ctx: Context) -> Request:
        size = len(json.dumps(request.payload, default=str).encode("utf-8"))
        if size > self.max_bytes:
            raise PolicyDenied(
                "payload too large",
                detail={"bytes": size, "max_bytes": self.max_bytes},
            )
        ctx.extras["payload_bytes"] = size
        return request

    async def after(self, response: Response, ctx: Context) -> Response:
        return response


# --------------------------------------------------------------------------- #
# Audit
# --------------------------------------------------------------------------- #


class AuditPolicy:
    """Keeps a rolling record of who asked for what. Redact before persisting."""

    name = "audit"

    def __init__(self, keep: int = 100) -> None:
        self.keep = keep
        self.records: List[dict] = []

    async def before(self, request: Request, ctx: Context) -> Request:
        return request

    async def after(self, response: Response, ctx: Context) -> Response:
        self.records.append(
            {
                "request_id": response.request_id,
                "tenant": ctx.tenant,
                "model": response.model,
                "ms": round(response.elapsed_ms, 2),
            }
        )
        del self.records[:-self.keep]
        return response
