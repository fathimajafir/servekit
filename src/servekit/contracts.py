"""Wire contracts shared by every service on the platform.

Nothing here knows about a specific model. A service is just an implementation of
``Capability`` plus a ``ModelSpec`` that describes its cost, latency and capacity.
That is the seam: add a new provider by declaring a new ``ModelSpec`` and a backend,
not by changing the gateway.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any, Dict, Mapping, Optional


class Capability(str, Enum):
    """What a service can do.

    Capabilities are deliberately coarse. ``text.generate`` covers every chat model
    from a 4B router to a frontier model; ``audio.speech`` covers every TTS provider
    including a SaaS one sitting behind the same API.
    """

    TEXT_GENERATE = "text.generate"
    TEXT_EMBED = "text.embed"
    TEXT_RERANK = "text.rerank"
    AUDIO_SPEECH = "audio.speech"
    AUDIO_TRANSCRIBE = "audio.transcribe"
    IMAGE_GENERATE = "image.generate"
    VIDEO_GENERATE = "video.generate"


class Priority(IntEnum):
    """Admission priority. Higher wins when the platform is saturated.

    Strict priority is safe here because callers always carry a deadline: a batch
    job that waits too long is expired rather than starved forever.
    """

    BATCH = 0
    FREE = 10
    STANDARD = 20
    PREMIUM = 30
    REALTIME = 40


class LatencyClass(str, Enum):
    REALTIME = "realtime"          # first byte < 300 ms, streaming mandatory
    INTERACTIVE = "interactive"    # seconds
    BATCH = "batch"                # minutes to hours


class CacheScope(str, Enum):
    NONE = "none"       # never cache (non-deterministic sampling)
    GLOBAL = "global"   # identical request for any tenant may reuse the answer
    TENANT = "tenant"   # only reuse within the same tenant


@dataclass(frozen=True)
class ModelSpec:
    """Everything the platform needs to route to, bill and scale one model.

    This is the only metadata an operator writes to add a service.
    """

    id: str
    capability: Capability
    version: str = "1"
    backend: str = "local"                     # adapter family: local|ray|http|slurm|saas
    endpoint: Optional[str] = None             # address for remote adapters
    cost_tier: int = 5                         # 1 = cheapest, 10 = most expensive
    latency_class: LatencyClass = LatencyClass.INTERACTIVE
    max_concurrency: int = 8                   # in-flight requests one replica accepts
    streaming: bool = True
    unit: str = "token"                        # billing / usage unit
    tags: Mapping[str, str] = field(default_factory=dict)
    enabled: bool = True


@dataclass
class Request:
    """A normalized request. Every protocol is translated into this before routing."""

    capability: Capability
    payload: Dict[str, Any] = field(default_factory=dict)
    model: Optional[str] = None                # pin a model; None = let the router choose
    tenant: str = "anonymous"
    priority: Priority = Priority.STANDARD
    deadline_s: Optional[float] = None         # total budget; enforced at admission
    cache_scope: CacheScope = CacheScope.NONE
    idempotency_key: Optional[str] = None
    request_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    created_at: float = field(default_factory=time.monotonic)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.capability, Capability):
            self.capability = Capability(self.capability)
        if isinstance(self.cache_scope, str):
            self.cache_scope = CacheScope(self.cache_scope)
        if not isinstance(self.priority, Priority):
            self.priority = Priority[self.priority.upper()] if isinstance(
                self.priority, str
            ) else Priority(self.priority)

    def cache_key(self) -> Optional[str]:
        """Stable key for an exact-match cache, or None when caching is off."""
        if self.cache_scope is CacheScope.NONE:
            return None
        scope_tenant = self.tenant if self.cache_scope is CacheScope.TENANT else None
        material = {
            "capability": self.capability.value,
            "model": self.model or "*",
            "tenant": scope_tenant,
            "payload": self.payload,
        }
        blob = json.dumps(material, sort_keys=True, default=str).encode("utf-8")
        return f"{self.capability.value}:{hashlib.blake2b(blob, digest_size=16).hexdigest()}"

    def elapsed_ms(self) -> float:
        return (time.monotonic() - self.created_at) * 1000.0


@dataclass
class Usage:
    input_units: int = 0
    output_units: int = 0
    unit: str = "token"

    def total(self) -> int:
        return self.input_units + self.output_units


@dataclass
class Response:
    request_id: str
    model: str
    output: Any = None
    usage: Usage = field(default_factory=Usage)
    cache_hit: bool = False
    elapsed_ms: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StreamEvent:
    """One chunk of a streaming answer. The terminal event carries ``done=True``."""

    request_id: str
    delta: Any = None
    usage: Optional[Usage] = None
    done: bool = False
    error: Optional[str] = None


@dataclass
class Health:
    """Reported state of one model endpoint. Drives routing and autoscaling."""

    ok: bool = True
    inflight: int = 0
    queue_depth: int = 0
    detail: str = ""
    checked_at: float = field(default_factory=time.monotonic)

    def load(self, capacity: int) -> float:
        """0.0 = idle, 1.0 = at capacity, >1.0 = saturated."""
        return (self.inflight + self.queue_depth) / float(max(capacity, 1))


@dataclass
class Candidate:
    """A routable pairing of spec + live backend + last known health."""

    spec: ModelSpec
    backend: Any          # servekit.interfaces.Backend (kept loose to avoid a cycle)
    health: Health = field(default_factory=Health)

    @property
    def available(self) -> bool:
        if not self.spec.enabled or not self.health.ok:
            return False
        return self.health.load(self.spec.max_concurrency) < 1.0


@dataclass
class Context:
    """Per-request baggage handed to policies, telemetry and the control plane."""

    request_id: str
    tenant: str
    started_at: float = field(default_factory=time.monotonic)
    model: Optional[str] = None
    cache_hit: bool = False
    extras: Dict[str, Any] = field(default_factory=dict)

    def elapsed_ms(self) -> float:
        return (time.monotonic() - self.started_at) * 1000.0
