"""Routing strategies.

A router answers one question: *of the models that could serve this request, which
one should?* Keep them pure and small; composition beats configuration.

The strategies are ordered by the maturity stage where they become useful:

    stage 3 (poc/prod)   PinnedRouter, LeastLoadedRouter
    stage 4 (scale)      CheapestRouter, WeightedRouter (canary)
    stage 5 (platform)   CostAwareRouter + tenant tiers, FallbackChain
"""

from __future__ import annotations

import hashlib
from typing import Dict, List, Sequence

from .contracts import Candidate, Priority, Request
from .errors import NoCapacity


def _live(candidates: Sequence[Candidate]) -> List[Candidate]:
    healthy = [c for c in candidates if c.available]
    # Fall back to anything that is at least configured and reported up: it is
    # better to queue on a full replica than to reject while capacity exists.
    return healthy or [c for c in candidates if c.health.ok and c.spec.enabled]


class PinnedRouter:
    """Honours ``request.model``; used for evals, migrations and customer pinning."""

    name = "pinned"

    def __init__(self, fallback: str = "least_loaded") -> None:
        self.fallback = fallback

    def choose(self, request: Request, candidates: Sequence[Candidate]) -> Candidate:
        if request.model:
            for c in candidates:
                if c.spec.id == request.model:
                    return c
            raise NoCapacity(
                f"model '{request.model}' is not available",
                detail={"requested": request.model},
            )
        return LeastLoadedRouter().choose(request, candidates)


class CheapestRouter:
    """Cheapest adequate model wins. The single largest cost lever on a platform."""

    name = "cheapest"

    def choose(self, request: Request, candidates: Sequence[Candidate]) -> Candidate:
        if not candidates:
            raise NoCapacity(f"no model serves '{request.capability.value}'")
        pool = _live(candidates)
        return min(
            pool,
            key=lambda c: (
                c.spec.cost_tier,
                c.health.load(c.spec.max_concurrency),
            ),
        )


class LeastLoadedRouter:
    """Spreads load; the safe default behind a plain load balancer."""

    name = "least_loaded"

    def choose(self, request: Request, candidates: Sequence[Candidate]) -> Candidate:
        if not candidates:
            raise NoCapacity(f"no model serves '{request.capability.value}'")
        pool = _live(candidates)
        return min(
            pool,
            key=lambda c: (
                c.health.load(c.spec.max_concurrency),
                c.spec.cost_tier,
            ),
        )


class LatencyAwareRouter:
    """Matches the caller's urgency to a model's latency class before anything else."""

    name = "latency_aware"

    def choose(self, request: Request, candidates: Sequence[Candidate]) -> Candidate:
        if not candidates:
            raise NoCapacity(f"no model serves '{request.capability.value}'")
        pool = _live(candidates)
        if request.priority >= Priority.PREMIUM:
            fast = [c for c in pool if c.spec.latency_class.value != "batch"]
            pool = fast or pool
        return min(
            pool,
            key=lambda c: (c.health.load(c.spec.max_concurrency), c.spec.cost_tier),
        )


class WeightedRouter:
    """Deterministic traffic split, the primitive behind canaries and A/B tests.

    Weights are mutable at runtime so the control plane can shift traffic without
    redeploying the gateway.
    """

    name = "weighted"

    def __init__(self, weights: Dict[str, int], salt: str = "canary") -> None:
        self.weights = dict(weights)
        self.salt = salt

    def set_weights(self, weights: Dict[str, int]) -> None:
        self.weights = dict(weights)

    def choose(self, request: Request, candidates: Sequence[Candidate]) -> Candidate:
        if not candidates:
            raise NoCapacity(f"no model serves '{request.capability.value}'")
        pool = _live(candidates)
        by_id = {c.spec.id: c for c in pool}
        buckets: List[tuple] = []
        for model_id, weight in self.weights.items():
            if model_id in by_id and weight > 0:
                buckets.append((model_id, weight))
        if not buckets:
            return LeastLoadedRouter().choose(request, pool)

        digest = hashlib.blake2b(
            f"{self.salt}:{request.request_id}".encode("utf-8"), digest_size=8
        ).digest()
        point = int.from_bytes(digest, "big") % sum(w for _, w in buckets)
        upto = 0
        for model_id, weight in buckets:
            upto += weight
            if point < upto:
                return by_id[model_id]
        return by_id[buckets[-1][0]]


class FallbackChain:
    """Tries each router in order; first one that can answer wins.

    Use for "prefer cheap, never fail" behaviour. Note this is *routing* fallback;
    retrying after an upstream error is the pipeline's job.
    """

    name = "fallback_chain"

    def __init__(self, *routers) -> None:
        self.routers = routers

    def choose(self, request: Request, candidates: Sequence[Candidate]) -> Candidate:
        last: Exception = NoCapacity("no router could place the request")
        for router in self.routers:
            try:
                return router.choose(request, candidates)
            except NoCapacity as exc:  # pragma: no cover - defensive
                last = exc
        raise last
