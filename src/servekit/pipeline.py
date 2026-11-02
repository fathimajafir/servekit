"""The pipeline: one ordered path that every request takes.

    policy.before -> cache lookup -> route -> admit -> backend -> policy.after -> cache set

The gateway owns protocol translation. The pipeline owns behaviour. Keeping those
apart is what makes ten services cost the same to operate as one.
"""

from __future__ import annotations

import asyncio
import time
from typing import AsyncIterator, List, Optional, Sequence

from .admission import AdmissionController, NullAdmission
from .cache import NullCache
from .contracts import Context, Request, Response, StreamEvent, Usage
from .errors import DeadlineExceeded, PlatformError, UpstreamError
from .interfaces import Backend, Cache, Policy, Router, Telemetry
from .registry import Registry
from .routing import LeastLoadedRouter
from .telemetry import NullTelemetry


class ServingPipeline:
    def __init__(
        self,
        registry: Registry,
        router: Optional[Router] = None,
        cache: Optional[Cache] = None,
        admission=None,
        policies: Sequence[Policy] = (),
        telemetry: Optional[Telemetry] = None,
        queue_timeout_s: float = 30.0,
        cache_ttl_s: float = 300.0,
    ) -> None:
        self.registry = registry
        self.router = router or LeastLoadedRouter()
        self.cache = cache or NullCache()
        self.admission = admission or NullAdmission()
        self.policies: List[Policy] = list(policies)
        self.telemetry = telemetry or NullTelemetry()
        self.queue_timeout_s = queue_timeout_s
        self.cache_ttl_s = cache_ttl_s

    # -- unary -------------------------------------------------------------

    async def invoke(self, request: Request) -> Response:
        ctx = Context(request_id=request.request_id, tenant=request.tenant)
        started = time.monotonic()
        try:
            request = await self._before(request, ctx)
            self.telemetry.on_start(request, ctx)

            key = request.cache_key()
            if key is not None:
                hit = await self.cache.get(key)
                if hit is not None:
                    hit.request_id = request.request_id
                    hit.cache_hit = True
                    hit.elapsed_ms = (time.monotonic() - started) * 1000.0
                    ctx.cache_hit = True
                    ctx.model = hit.model
                    self.telemetry.on_finish(hit, ctx)
                    return hit

            candidate = self.router.choose(request, self.registry.resolve(request))
            ctx.model = candidate.spec.id

            budget = self._remaining(request)
            async with self.admission.slot(request.priority, budget):
                if request.deadline_s is None:
                    response = await candidate.backend.invoke(request)
                else:
                    response = await asyncio.wait_for(
                        candidate.backend.invoke(request), timeout=self._remaining(request)
                    )

            response.request_id = request.request_id
            response.elapsed_ms = (time.monotonic() - started) * 1000.0
            response = await self._after(response, ctx)

            if key is not None:
                await self.cache.set(key, response, self.cache_ttl_s)

            self.telemetry.on_finish(response, ctx)
            return response

        except PlatformError as exc:
            self.telemetry.on_error(request, ctx, exc)
            raise
        except asyncio.TimeoutError as exc:
            err = DeadlineExceeded("upstream exceeded its deadline")
            self.telemetry.on_error(request, ctx, err)
            raise err from exc
        except Exception as exc:  # defensive: never leak a bare exception to the gateway
            err = UpstreamError(f"backend failed: {exc}")
            self.telemetry.on_error(request, ctx, err)
            raise err from exc

    # -- streaming ---------------------------------------------------------

    async def stream(self, request: Request) -> AsyncIterator[StreamEvent]:
        """Yields events. The admission slot is held for the whole stream, because a
        stream consumes a replica connection from first byte to last."""
        ctx = Context(request_id=request.request_id, tenant=request.tenant)
        started = time.monotonic()
        usage: Optional[Usage] = None
        model = "unknown"
        try:
            request = await self._before(request, ctx)
            self.telemetry.on_start(request, ctx)

            candidate = self.router.choose(request, self.registry.resolve(request))
            model = candidate.spec.id
            ctx.model = model

            async with self.admission.slot(request.priority, self._remaining(request)):
                async for event in candidate.backend.stream(request):
                    if event.usage is not None:
                        usage = event.usage
                    yield event

            summary = Response(
                request_id=request.request_id,
                model=model,
                usage=usage or Usage(),
                elapsed_ms=(time.monotonic() - started) * 1000.0,
            )
            self.telemetry.on_finish(summary, ctx)

        except PlatformError as exc:
            self.telemetry.on_error(request, ctx, exc)
            yield StreamEvent(request_id=request.request_id, done=True, error=exc.message)
        except Exception as exc:  # pragma: no cover - defensive
            err = UpstreamError(f"stream failed: {exc}")
            self.telemetry.on_error(request, ctx, err)
            yield StreamEvent(request_id=request.request_id, done=True, error=err.message)

    # -- hooks -------------------------------------------------------------

    def _remaining(self, request: Request) -> float:
        """Budget left for this request, falling back to the queue timeout."""
        if request.deadline_s is None:
            return self.queue_timeout_s
        left = request.deadline_s - (time.monotonic() - request.created_at)
        return max(0.01, left)

    async def _before(self, request: Request, ctx: Context) -> Request:
        for policy in self.policies:
            request = await policy.before(request, ctx)
        return request

    async def _after(self, response: Response, ctx: Context) -> Response:
        for policy in reversed(self.policies):
            response = await policy.after(response, ctx)
        return response


async def shutdown(pipeline: ServingPipeline) -> None:
    """Drain every backend, then stop accepting work. Call from SIGTERM."""
    for candidate in pipeline.registry.all():
        backend: Backend = candidate.backend
        try:
            await backend.drain()
        except Exception:
            pass
    if isinstance(pipeline.admission, AdmissionController):
        await pipeline.admission.close()
