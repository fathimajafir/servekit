"""The smallest set of tests that proves the framework's contracts hold.

Plain asyncio + pytest, no plugins. Run with:  PYTHONPATH=src python3 -m pytest -q
"""

from __future__ import annotations

import asyncio

import pytest
from backends import SimulatedLLM, SimulatedTTS, large_llm, small_llm, tts_engine

from servekit import (
    AdmissionController,
    ApiKeyPolicy,
    Capability,
    CheapestRouter,
    MemoryCache,
    MemoryTelemetry,
    ModelSpec,
    PinnedRouter,
    Registry,
    Request,
    Response,
    ServingPipeline,
)
from servekit.errors import NoCapacity, PolicyDenied


def run(coro):
    return asyncio.run(coro)


def build(**kwargs):
    registry = Registry()
    for backend in (small_llm(), large_llm(), tts_engine()):
        registry.register(backend.spec, backend)
    pipeline = ServingPipeline(registry=registry, telemetry=MemoryTelemetry(), **kwargs)
    return registry, pipeline


def chat(text="hello", **kwargs):
    return Request(
        capability=Capability.TEXT_GENERATE,
        payload={"messages": [{"role": "user", "content": text}]},
        metadata={"api_key": "dev-key"},
        **kwargs,
    )


def test_routes_to_cheapest_model():
    registry, pipeline = build(router=CheapestRouter())

    async def go():
        return await pipeline.invoke(chat())

    response = run(go())
    assert response.model == "chat-small"
    assert response.output["text"].startswith("[chat-small]")


def test_pinned_model_is_honoured():
    registry, pipeline = build(router=PinnedRouter())

    async def go():
        return await pipeline.invoke(chat(model="chat-large"))

    assert run(go()).model == "chat-large"


def test_unknown_pin_is_no_capacity():
    _, pipeline = build(router=PinnedRouter())

    async def go():
        await pipeline.invoke(chat(model="does-not-exist"))

    with pytest.raises(NoCapacity):
        run(go())


def test_cache_hit_skips_the_backend():
    _, pipeline = build(cache=MemoryCache())

    async def go():
        request = chat(cache_scope="global")
        first = await pipeline.invoke(request)
        second = await pipeline.invoke(request)
        return first, second

    first, second = run(go())
    assert first.cache_hit is False
    assert second.cache_hit is True
    assert second.output == first.output


def test_policy_denies_before_the_backend():
    _, pipeline = build(policies=[ApiKeyPolicy({"good"})])

    async def go():
        await pipeline.invoke(
            Request(capability=Capability.TEXT_GENERATE, payload={"input": "hi"})
        )

    with pytest.raises(Exception) as exc:
        run(go())
    assert exc.value.status == 401


def test_second_capability_uses_the_same_pipeline():
    _, pipeline = build()

    async def go():
        return await pipeline.invoke(
            Request(capability=Capability.AUDIO_SPEECH, payload={"input": "speak this"})
        )

    response = run(go())
    assert response.model == "tts-v1"
    assert isinstance(response.output, bytes)


def test_admission_rejects_instead_of_queueing_forever():
    registry = Registry()
    spec = ModelSpec(id="slow", capability=Capability.TEXT_GENERATE, max_concurrency=1)
    backend = SimulatedLLM(spec, latency_s=0.3)
    registry.register(spec, backend)

    pipeline = ServingPipeline(
        registry=registry,
        router=CheapestRouter(),
        admission=AdmissionController(max_inflight=1, max_queue=1, queue_timeout_s=0.05),
    )

    async def go():
        return await asyncio.gather(
            *[pipeline.invoke(chat(f"q{i}")) for i in range(5)],
            return_exceptions=True,
        )

    results = run(go())
    failures = [r for r in results if isinstance(r, Exception)]
    assert failures, "a bounded queue must reject under overload"
    assert all(hasattr(f, "status") for f in failures)


def test_stream_yields_deltas_then_done_with_usage():
    _, pipeline = build()

    async def go():
        events = []
        async for event in pipeline.stream(chat("stream me")):
            events.append(event)
        return events

    events = run(go())
    assert events[-1].done is True
    assert events[-1].usage is not None
    assert events[-1].usage.output_units > 0
    assert any(e.delta for e in events[:-1])
