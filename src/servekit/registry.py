"""The registry: what exists right now.

Separating "what models exist" (registry) from "which one should answer"
(router) and "should we serve at all" (admission) is what lets each piece stay
small as the platform grows.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Dict, Iterable, List, Optional

from .contracts import Candidate, Capability, Health, Request


class Registry:
    """In-memory view of the model fleet.

    In production this is a cache over a database or a control-plane watch stream:
    the API stays the same, only ``_reload`` changes.
    """

    def __init__(self) -> None:
        self._models: Dict[str, Candidate] = {}

    # -- writes ------------------------------------------------------------

    def register(self, spec: ModelSpec, backend, health: Optional[Health] = None) -> Candidate:
        candidate = Candidate(spec=spec, backend=backend, health=health or Health())
        self._models[spec.id] = candidate
        return candidate

    def deregister(self, model_id: str) -> None:
        self._models.pop(model_id, None)

    def set_enabled(self, model_id: str, enabled: bool) -> None:
        current = self._models.get(model_id)
        if current is None:
            return
        self._models[model_id] = Candidate(
            spec=replace(current.spec, enabled=enabled),
            backend=current.backend,
            health=current.health,
        )

    def update_health(self, model_id: str, health: Health) -> None:
        current = self._models.get(model_id)
        if current is not None:
            current.health = health

    # -- reads -------------------------------------------------------------

    def get(self, model_id: str) -> Optional[Candidate]:
        return self._models.get(model_id)

    def candidates(self, capability: Capability) -> List[Candidate]:
        capability = Capability(capability)
        return [c for c in self._models.values() if c.spec.capability is capability]

    def resolve(self, request: Request) -> List[Candidate]:
        """All models that could serve this request, including a disabled pin."""
        if request.model:
            current = self._models.get(request.model)
            if current is None:
                return []
            return [current] if current.spec.enabled else []
        return [c for c in self.candidates(request.capability) if c.spec.enabled]

    def all(self) -> List[Candidate]:
        return list(self._models.values())

    def snapshot(self) -> List[dict]:
        return [
            {
                "id": c.spec.id,
                "capability": c.spec.capability.value,
                "version": c.spec.version,
                "backend": c.spec.backend,
                "cost_tier": c.spec.cost_tier,
                "enabled": c.spec.enabled,
                "health": {
                    "ok": c.health.ok,
                    "inflight": c.health.inflight,
                    "queue_depth": c.health.queue_depth,
                },
            }
            for c in self._models.values()
        ]


def filter_capability(candidates: Iterable[Candidate], capability: Capability) -> List[Candidate]:
    capability = Capability(capability)
    return [c for c in candidates if c.spec.capability is capability]
