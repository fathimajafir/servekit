"""An OpenAI-shaped HTTP surface over the framework.

    PYTHONPATH=src uvicorn app:app --app-dir examples --port 8100

This file is intentionally thin. It translates protocol, and nothing else: no
routing, no retries, no caching, no quota logic lives here.
"""

from __future__ import annotations

import json
import sys
from contextlib import asynccontextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fastapi import Body, FastAPI, Header  # noqa: E402
from fastapi.responses import JSONResponse, Response as HttpResponse, StreamingResponse  # noqa: E402

from backends import large_llm, small_llm, tts_engine  # noqa: E402

from servekit import (  # noqa: E402
    AdmissionController,
    ApiKeyPolicy,
    Capability,
    CheapestRouter,
    HealthMonitor,
    MemoryCache,
    MemoryTelemetry,
    PlatformError,
    Registry,
    Request,
    ServingPipeline,
    TenantQuotaPolicy,
    shutdown,
)

registry = Registry()
for backend in (small_llm(), large_llm(), tts_engine()):
    registry.register(backend.spec, backend)

telemetry = MemoryTelemetry()
cache = MemoryCache(max_entries=2048)
admission = AdmissionController(max_inflight=64, max_queue=512, queue_timeout_s=20.0)
pipeline = ServingPipeline(
    registry=registry,
    router=CheapestRouter(),
    cache=cache,
    admission=admission,
    policies=[ApiKeyPolicy({"dev-key"}), TenantQuotaPolicy(rate_per_s=20, burst=40)],
    telemetry=telemetry,
)
health = HealthMonitor(registry, interval_s=5.0)


@asynccontextmanager
async def lifespan(_: FastAPI):
    for candidate in registry.all():
        await candidate.backend.warmup()
    health.start()
    yield
    await health.stop()
    await shutdown(pipeline)


app = FastAPI(title="servekit", version="0.1.0", lifespan=lifespan)


@app.exception_handler(PlatformError)
async def platform_error_handler(_, exc: PlatformError) -> JSONResponse:
    headers = {}
    if exc.retry_after is not None:
        headers["Retry-After"] = str(int(exc.retry_after) or 1)
    return JSONResponse(exc.to_dict(), status_code=exc.status, headers=headers)


def _tenant(header: str | None) -> str:
    return header or "anonymous"


@app.post("/v1/chat/completions")
async def chat_completions(
    payload: dict = Body(...),
    x_tenant: str | None = Header(default=None, alias="X-Tenant"),
    x_api_key: str | None = Header(default=None, alias="X-Api-Key"),
):
    request = Request(
        capability=Capability.TEXT_GENERATE,
        payload=payload,
        model=payload.get("model"),
        tenant=_tenant(x_tenant),
        metadata={"api_key": x_api_key},
    )

    if not payload.get("stream"):
        response = await pipeline.invoke(request)
        return JSONResponse(
            {
                "id": response.request_id,
                "model": response.model,
                "choices": [{"message": {"role": "assistant",
                                         "content": response.output["text"]}}],
                "usage": {
                    "input_tokens": response.usage.input_units,
                    "output_tokens": response.usage.output_units,
                },
            },
            headers={"X-Request-Id": response.request_id,
                     "X-Cache": "hit" if response.cache_hit else "miss"},
        )

    async def events():
        async for event in pipeline.stream(request):
            if event.error:
                yield f"data: {json.dumps({'error': event.error})}\n\n"
                return
            if event.done:
                yield "data: [DONE]\n\n"
                return
            chunk = {"choices": [{"delta": event.delta}]}
            yield f"data: {json.dumps(chunk)}\n\n"

    return StreamingResponse(events(), media_type="text/event-stream")


@app.post("/v1/audio/speech")
async def audio_speech(
    payload: dict = Body(...),
    x_tenant: str | None = Header(default=None, alias="X-Tenant"),
    x_api_key: str | None = Header(default=None, alias="X-Api-Key"),
):
    request = Request(
        capability=Capability.AUDIO_SPEECH,
        payload=payload,
        model=payload.get("model"),
        tenant=_tenant(x_tenant),
        metadata={"api_key": x_api_key},
    )
    result = await pipeline.invoke(request)
    return HttpResponse(
        content=result.output,
        media_type=result.metadata.get("content_type", "audio/wav"),
        headers={"X-Request-Id": result.request_id, "X-Model": result.model},
    )


@app.get("/v1/models")
async def list_models():
    return {"data": registry.snapshot()}


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    candidates = registry.all()
    ready = [c for c in candidates if c.health.ok]
    status = 200 if ready else 503
    return JSONResponse(
        {"ready": len(ready), "total": len(candidates)},
        status_code=status,
    )


@app.get("/metrics")
async def metrics():
    return {
        "telemetry": telemetry.snapshot(),
        "admission": admission.stats(),
        "cache": cache.stats(),
    }
