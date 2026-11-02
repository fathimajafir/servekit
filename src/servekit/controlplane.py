"""Control plane: the part that keeps the fleet honest.

It never serves traffic. It watches health, pushes replica counts, and shifts
traffic between versions. In a small deployment this runs in-process; at scale it is
its own service and speaks to Kubernetes or Ray instead of calling ``Backend.scale``
directly. The interface does not change.
"""

from __future__ import annotations

import asyncio
import time
from typing import Dict, List, Optional

from .contracts import Health, ModelSpec
from .interfaces import Scaler
from .registry import Registry


class HealthMonitor:
    """Polls every registered backend and writes results into the registry.

    ``ok`` is intentionally about *usefulness*, not liveness: a replica that is
    alive but saturated should not receive more traffic.
    """

    def __init__(self, registry: Registry, interval_s: float = 5.0) -> None:
        self.registry = registry
        self.interval_s = interval_s
        self._task: Optional[asyncio.Task] = None
        self.last_run: float = 0.0

    async def tick(self) -> List[dict]:
        results = []
        for candidate in self.registry.all():
            try:
                health = await candidate.backend.health()
            except Exception as exc:  # a dead backend must not stop the sweep
                health = Health(ok=False, detail=f"health check failed: {exc}")
            self.registry.update_health(candidate.spec.id, health)
            results.append(
                {"model": candidate.spec.id, "ok": health.ok,
                 "load": round(health.load(candidate.spec.max_concurrency), 3)}
            )
        self.last_run = time.monotonic()
        return results

    async def run(self) -> None:
        while True:
            await self.tick()
            await asyncio.sleep(self.interval_s)

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.ensure_future(self.run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None


class QueueDepthScaler:
    """Reference scaler: add replicas when sustained load is above target.

    Deliberately dumb. Real scalers add a rate-of-change term (scale before the
    queue grows) and a scale-down delay longer than cold start, so replicas do not
    flap in and out of existence.
    """

    def __init__(
        self,
        target_load: float = 0.7,
        min_replicas: int = 1,
        max_replicas: int = 8,
        scale_down_delay_s: float = 120.0,
    ) -> None:
        self.target_load = target_load
        self.min_replicas = min_replicas
        self.max_replicas = max_replicas
        self.scale_down_delay_s = scale_down_delay_s
        self._last_change: Dict[str, float] = {}

    async def desired_replicas(self, spec: ModelSpec, health: Health, pressure: float) -> int:
        load = health.load(spec.max_concurrency)
        if load > self.target_load:
            desired = max(1, int(round(load / self.target_load)))
            self._last_change[spec.id] = time.monotonic()
        elif load < self.target_load / 2:
            last = self._last_change.get(spec.id, 0.0)
            if time.monotonic() - last < self.scale_down_delay_s:
                return self.min_replicas if load == 0 else max(1, self.min_replicas)
            desired = self.min_replicas
        else:
            desired = self.min_replicas
        return max(self.min_replicas, min(self.max_replicas, desired))


class Reconciler:
    """Applies desired replica counts to backends that can scale themselves.

    Backends expose ``async def scale(n)`` when they own their own replicas (Ray
    Serve does). Backends that cannot - a single container, a SaaS provider - are
    simply skipped, and the platform scales them by adding registry entries instead.
    """

    def __init__(
        self,
        registry: Registry,
        scaler: Scaler,
        interval_s: float = 10.0,
        min_pressure: float = 0.0,
    ) -> None:
        self.registry = registry
        self.scaler = scaler
        self.interval_s = interval_s
        self._task: Optional[asyncio.Task] = None
        self.last_applied: Dict[str, int] = {}

    async def tick(self) -> Dict[str, int]:
        applied: Dict[str, int] = {}
        for candidate in self.registry.all():
            backend = candidate.backend
            scale = getattr(backend, "scale", None)
            if scale is None:
                continue
            pressure = candidate.health.load(candidate.spec.max_concurrency)
            desired = await self.scaler.desired_replicas(
                candidate.spec, candidate.health, pressure
            )
            if self.last_applied.get(candidate.spec.id) != desired:
                await scale(desired)
                self.last_applied[candidate.spec.id] = desired
            applied[candidate.spec.id] = desired
        return applied

    async def run(self) -> None:
        while True:
            await self.tick()
            await asyncio.sleep(self.interval_s)

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.ensure_future(self.run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None


class Canary:
    """Traffic-shift helper for version rollouts.

    Starts at 0%, moves in steps, and can be aborted by the caller. The caller owns
    the decision (error rate, eval score); this only owns the arithmetic.
    """

    def __init__(self, steps: tuple = (1, 5, 25, 50, 100)) -> None:
        self.steps = steps
        self.index = 0

    @property
    def percent(self) -> int:
        return self.steps[self.index]

    def weights(self, stable: str, candidate: str) -> Dict[str, int]:
        n = self.percent
        return {stable: 100 - n, candidate: n}

    def advance(self) -> int:
        self.index = min(self.index + 1, len(self.steps) - 1)
        return self.percent

    def abort(self) -> Dict[str, int]:
        self.index = 0
        return {}


class Rollout:
    """Tracks a canary plus its gate. Kept tiny on purpose."""

    def __init__(self, canary: Canary, error_budget: float = 0.02) -> None:
        self.canary = canary
        self.error_budget = error_budget
        self.history: List[dict] = []

    def observe(self, percent: int, error_rate: float) -> str:
        verdict = "continue" if error_rate <= self.error_budget else "abort"
        self.history.append({"percent": percent, "error_rate": error_rate, "verdict": verdict})
        return verdict
