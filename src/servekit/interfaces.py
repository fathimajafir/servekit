"""The five protocols every service implements or consumes.

If a new model runtime cannot be expressed as a ``Backend``, the abstraction is wrong.
If a new operational concern needs to change the pipeline, it belongs in a ``Policy``,
not in a fork of the gateway.
"""

from __future__ import annotations

from typing import AsyncIterator, Optional, Protocol, Sequence, runtime_checkable

from .contracts import (
    Candidate,
    Context,
    Health,
    ModelSpec,
    Request,
    Response,
    StreamEvent,
)


@runtime_checkable
class Backend(Protocol):
    """A runnable model endpoint: local process, Ray Serve deployment, HTTP box, or SaaS."""

    spec: ModelSpec

    async def health(self) -> Health:
        """Cheap probe. Must not block on inference."""

    async def warmup(self) -> None:
        """Load weights / open connections so the first request is not the cold start."""

    async def invoke(self, request: Request) -> Response:
        """Single-shot call."""

    def stream(self, request: Request) -> AsyncIterator[StreamEvent]:
        """Chunked call. Implementations are async generators."""

    async def drain(self) -> None:
        """Stop accepting new work and let in-flight work finish (SIGTERM path)."""


@runtime_checkable
class Cache(Protocol):
    async def get(self, key: str) -> Optional[Response]:
        ...

    async def set(self, key: str, value: Response, ttl_s: float) -> None:
        ...


@runtime_checkable
class Router(Protocol):
    """Pure decision function: given healthy candidates, pick exactly one."""

    def choose(self, request: Request, candidates: Sequence[Candidate]) -> Candidate:
        ...


@runtime_checkable
class Policy(Protocol):
    """Cross-cutting concern: auth, quota, safety, audit, redaction.

    ``before`` may rewrite the request (e.g. inject tenant) or raise to deny.
    ``after`` may rewrite the response (e.g. redact) and runs in reverse order.
    """

    name: str

    async def before(self, request: Request, ctx: Context) -> Request:
        ...

    async def after(self, response: Response, ctx: Context) -> Response:
        ...


@runtime_checkable
class Telemetry(Protocol):
    """Non-blocking observer. Implementations must never raise."""

    def on_start(self, request: Request, ctx: Context) -> None:
        ...

    def on_finish(self, response: Response, ctx: Context) -> None:
        ...

    def on_error(self, request: Request, ctx: Context, error: BaseException) -> None:
        ...


@runtime_checkable
class Scaler(Protocol):
    """Decides replica count. The control plane applies it through ``Backend.scale``."""

    async def desired_replicas(self, spec: ModelSpec, health: Health, pressure: float) -> int:
        ...
