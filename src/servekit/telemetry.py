"""Telemetry: cheap observers, never in the request path's critical failure mode.

Everything here must be non-blocking and must never raise. A metrics bug that takes
down the gateway is the classic self-inflicted outage.
"""

from __future__ import annotations

import logging
from collections import Counter, deque
from typing import Deque, Dict, List

from .contracts import Context, Request, Response

log = logging.getLogger("servekit")


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[index]


class NullTelemetry:
    def on_start(self, request: Request, ctx: Context) -> None:
        return None

    def on_finish(self, response: Response, ctx: Context) -> None:
        return None

    def on_error(self, request: Request, ctx: Context, error: BaseException) -> None:
        return None

    def snapshot(self) -> Dict:
        return {}


class MemoryTelemetry:
    """In-process counters for local runs, tests and a /metrics endpoint."""

    def __init__(self, recent: int = 50) -> None:
        self.requests = 0
        self.cache_hits = 0
        self.errors = 0
        self.by_model: Counter = Counter()
        self.error_codes: Counter = Counter()
        self.latencies_ms: List[float] = []
        self.recent: Deque[Dict] = deque(maxlen=recent)

    def on_start(self, request: Request, ctx: Context) -> None:
        self.requests += 1

    def on_finish(self, response: Response, ctx: Context) -> None:
        if response.cache_hit:
            self.cache_hits += 1
        self.by_model[response.model] += 1
        self.latencies_ms.append(response.elapsed_ms)
        self.recent.append(
            {
                "request_id": response.request_id,
                "model": response.model,
                "cache_hit": response.cache_hit,
                "ms": round(response.elapsed_ms, 2),
            }
        )

    def on_error(self, request: Request, ctx: Context, error: BaseException) -> None:
        self.errors += 1
        self.error_codes[type(error).__name__] += 1

    def snapshot(self) -> Dict:
        return {
            "requests": self.requests,
            "cache_hits": self.cache_hits,
            "cache_hit_rate": round(self.cache_hits / self.requests, 3) if self.requests else 0.0,
            "errors": self.errors,
            "error_codes": dict(self.error_codes),
            "by_model": dict(self.by_model),
            "latency_ms": {
                "p50": round(_percentile(self.latencies_ms, 50), 2),
                "p95": round(_percentile(self.latencies_ms, 95), 2),
                "p99": round(_percentile(self.latencies_ms, 99), 2),
            },
        }


class LoggingTelemetry:
    """Structured-ish logs, one line per request. Good enough until OTLP is wired."""

    def on_start(self, request: Request, ctx: Context) -> None:
        log.info("start id=%s cap=%s tenant=%s prio=%s",
                 request.request_id, request.capability.value, request.tenant, request.priority.name)

    def on_finish(self, response: Response, ctx: Context) -> None:
        log.info("finish id=%s model=%s cache=%s ms=%.2f",
                 response.request_id, response.model, response.cache_hit, response.elapsed_ms)

    def on_error(self, request: Request, ctx: Context, error: BaseException) -> None:
        log.warning("error id=%s type=%s msg=%s",
                    request.request_id, type(error).__name__, error)

    def snapshot(self) -> Dict:
        return {}


class Fanout:
    """Send the same event to several sinks (memory + logs + OTLP)."""

    def __init__(self, *sinks) -> None:
        self.sinks = sinks

    def on_start(self, request: Request, ctx: Context) -> None:
        for sink in self.sinks:
            try:
                sink.on_start(request, ctx)
            except Exception:
                pass

    def on_finish(self, response: Response, ctx: Context) -> None:
        for sink in self.sinks:
            try:
                sink.on_finish(response, ctx)
            except Exception:
                pass

    def on_error(self, request: Request, ctx: Context, error: BaseException) -> None:
        for sink in self.sinks:
            try:
                sink.on_error(request, ctx, error)
            except Exception:
                pass

    def snapshot(self) -> Dict:
        return {i: sink.snapshot() for i, sink in enumerate(self.sinks)}
