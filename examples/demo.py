"""A guided tour of the framework, no GPU and no network required.

    PYTHONPATH=src python3 examples/demo.py

Each section is one behaviour the platform gives you for free once a service is
registered: routing, caching, streaming, admission, policies, telemetry.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from backends import large_llm, small_llm, tts_engine  # noqa: E402

from servekit import (  # noqa: E402
    AdmissionController,
    ApiKeyPolicy,
    Capability,
    CheapestRouter,
    MemoryCache,
    MemoryTelemetry,
    Priority,
    Registry,
    Request,
    ServingPipeline,
    TenantQuotaPolicy,
)
from servekit.errors import PlatformError  # noqa: E402


def banner(title: str) -> None:
    print(f"\n{'=' * 68}\n{title}\n{'=' * 68}")


def chat(text: str, **kwargs) -> Request:
    return Request(
        capability=Capability.TEXT_GENERATE,
        payload={"messages": [{"role": "user", "content": text}]},
        metadata={"api_key": "dev-key"},
        **kwargs,
    )


async def main() -> None:
    registry = Registry()
    for backend in (small_llm(), large_llm(), tts_engine()):
        registry.register(backend.spec, backend)

    telemetry = MemoryTelemetry()
    admission = AdmissionController(max_inflight=16, max_queue=32, queue_timeout_s=2.0)
    cache = MemoryCache(max_entries=128, default_ttl_s=60)
    pipeline = ServingPipeline(
        registry=registry,
        router=CheapestRouter(),
        cache=cache,
        admission=admission,
        policies=[
            ApiKeyPolicy({"dev-key"}),
            TenantQuotaPolicy(rate_per_s=50, burst=100),
        ],
        telemetry=telemetry,
    )

    banner("1. The fleet - one registry entry per service")
    for row in registry.snapshot():
        print(f"  {row['id']:<12} {row['capability']:<14} cost={row['cost_tier']} "
              f"concurrency={next(c.spec.max_concurrency for c in registry.all() if c.spec.id == row['id'])}")

    banner("2. Routing: the cheap model answers, the expensive one stays idle")
    response = await pipeline.invoke(chat("what is a token"))
    print(f"  model={response.model}  cache={response.cache_hit}")
    print(f"  answer: {response.output['text'][:80]}...")
    print("  -> CheapestRouter picked 'chat-small'. You did not write routing code.")

    banner("3. Pinning a model: same request, explicit choice")
    response = await pipeline.invoke(chat("draft a contract clause", model="chat-large"))
    print(f"  model={response.model}  (pinned by the caller)")

    banner("4. Caching: identical request, zero GPU")
    cached = chat("what is a token", cache_scope="global")
    first = await pipeline.invoke(cached)
    second = await pipeline.invoke(cached)
    print(f"  first : cache={first.cache_hit}  {first.elapsed_ms:.1f} ms")
    print(f"  second: cache={second.cache_hit}  {second.elapsed_ms:.1f} ms")

    banner("5. Streaming: the slot is held until the last token")
    print("  ", end="")
    async for event in pipeline.stream(chat("explain photosynthesis")):
        if event.error:
            print(f"\n  error: {event.error}")
        elif event.delta:
            token = event.delta.get("text", event.delta)
            print(token, end="", flush=True)
    print()

    banner("6. A second capability: TTS rides the same pipeline")
    speech = await pipeline.invoke(
        Request(
            capability=Capability.AUDIO_SPEECH,
            payload={"input": "hello from the platform"},
            metadata={"api_key": "dev-key"},
        )
    )
    print(f"  model={speech.model}  bytes={len(speech.output)}  unit={speech.usage.unit}")

    banner("7. Policies: no valid key, no GPU")
    try:
        await pipeline.invoke(
            Request(capability=Capability.TEXT_GENERATE, payload={"input": "hi"})
        )
    except PlatformError as exc:
        print(f"  {type(exc).__name__}: {exc.message} (http {exc.status})")

    banner("8. Admission: a bounded queue says no instead of collapsing")
    tight = ServingPipeline(
        registry=registry,
        router=CheapestRouter(),
        admission=AdmissionController(max_inflight=1, max_queue=1, queue_timeout_s=0.2),
        telemetry=MemoryTelemetry(),
    )
    results = await asyncio.gather(
        *[tight.invoke(chat(f"slow question {i}")) for i in range(4)],
        return_exceptions=True,
    )
    for i, result in enumerate(results):
        label = type(result).__name__ if isinstance(result, Exception) else "ok"
        print(f"  request {i}: {label}")

    banner("9. What the platform recorded")
    print(f"  admission: {admission.stats()}")
    print(f"  cache    : {cache.stats()}")
    print(f"  telemetry: {telemetry.snapshot()}")

    banner("10. Priority is a real lever, not a label")
    print("  REALTIME requests jump the queue ahead of BATCH requests.")
    print("  Both carry deadlines, so nothing starves: it expires and gets a 503.")
    print(f"  priorities: {[p.name for p in Priority]}")


if __name__ == "__main__":
    asyncio.run(main())
