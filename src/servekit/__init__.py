"""servekit - a reusable framework for serving many models behind one API.

The two things to import are almost always:

    from servekit import Registry, ServingPipeline, ModelSpec, Capability

Everything else is an extension point you reach for as the platform grows.
"""

from .contracts import (
    CacheScope,
    Candidate,
    Capability,
    Context,
    Health,
    LatencyClass,
    ModelSpec,
    Priority,
    Request,
    Response,
    StreamEvent,
    Usage,
)
from .errors import (
    DeadlineExceeded,
    InvalidRequest,
    NoCapacity,
    Overloaded,
    PlatformError,
    PolicyDenied,
    RateLimited,
    Unauthorized,
    UpstreamError,
)
from .interfaces import Backend, Cache, Policy, Router, Scaler, Telemetry
from .admission import AdmissionController, NullAdmission
from .cache import MemoryCache, NullCache
from .policy import ApiKeyPolicy, AuditPolicy, PayloadLimitPolicy, TenantQuotaPolicy
from .registry import Registry
from .routing import (
    CheapestRouter,
    FallbackChain,
    LatencyAwareRouter,
    LeastLoadedRouter,
    PinnedRouter,
    WeightedRouter,
)
from .pipeline import ServingPipeline, shutdown
from .controlplane import (
    Canary,
    HealthMonitor,
    QueueDepthScaler,
    Reconciler,
    Rollout,
)
from .telemetry import Fanout, LoggingTelemetry, MemoryTelemetry, NullTelemetry

__version__ = "0.1.0"

__all__ = [
    "AdmissionController",
    "ApiKeyPolicy",
    "AuditPolicy",
    "Backend",
    "Cache",
    "CacheScope",
    "Canary",
    "Candidate",
    "Capability",
    "CheapestRouter",
    "Context",
    "DeadlineExceeded",
    "FallbackChain",
    "Fanout",
    "Health",
    "HealthMonitor",
    "InvalidRequest",
    "LatencyAwareRouter",
    "LatencyClass",
    "LeastLoadedRouter",
    "LoggingTelemetry",
    "MemoryCache",
    "MemoryTelemetry",
    "ModelSpec",
    "NoCapacity",
    "NullAdmission",
    "NullCache",
    "NullTelemetry",
    "Overloaded",
    "PayloadLimitPolicy",
    "PinnedRouter",
    "PlatformError",
    "Policy",
    "PolicyDenied",
    "Priority",
    "QueueDepthScaler",
    "RateLimited",
    "Reconciler",
    "Registry",
    "Request",
    "Response",
    "Rollout",
    "Router",
    "Scaler",
    "ServingPipeline",
    "StreamEvent",
    "Telemetry",
    "TenantQuotaPolicy",
    "Unauthorized",
    "UpstreamError",
    "Usage",
    "WeightedRouter",
    "shutdown",
]
