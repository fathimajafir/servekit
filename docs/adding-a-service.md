# Adding a service

A new model service is three steps. None of them touch the gateway.

## 1. Declare the model

```python
from servekit import Capability, LatencyClass, ModelSpec

SPEC = ModelSpec(
    id="voice-pro",
    capability=Capability.AUDIO_SPEECH,
    version="2",
    backend="http",              # free-form label, used by ops dashboards
    endpoint="http://10.0.0.14:8100",
    cost_tier=4,                 # 1 = cheapest
    latency_class=LatencyClass.REALTIME,
    max_concurrency=8,           # what one replica can hold at once
    streaming=True,
    unit="second",               # billing unit
    tags={"team": "speech", "gpu": "a10g"},
)
```

`cost_tier` and `latency_class` are the only fields the router reads. Everything else is
for operators, billing and dashboards.

## 2. Implement five methods

```python
import httpx
from servekit import Health, Request, Response, StreamEvent, Usage
from servekit.errors import UpstreamError


class VoiceProBackend:
    def __init__(self, spec, endpoint: str) -> None:
        self.spec = spec
        self._client = httpx.AsyncClient(base_url=endpoint, timeout=120.0)
        self._inflight = 0

    async def health(self) -> Health:
        try:
            r = await self._client.get("/healthz", timeout=1.0)
            return Health(ok=r.status_code == 200, inflight=self._inflight)
        except Exception as exc:
            return Health(ok=False, detail=str(exc))

    async def warmup(self) -> None:
        await self._client.get("/warmup")

    async def invoke(self, request: Request) -> Response:
        self._inflight += 1
        try:
            r = await self._client.post("/synthesize", json=request.payload)
            if r.status_code >= 500:
                raise UpstreamError(f"voice-pro returned {r.status_code}")
            return Response(
                request_id=request.request_id,
                model=self.spec.id,
                output=r.content,
                usage=Usage(output_units=int(r.headers.get("x-audio-seconds", 0)), unit="second"),
                metadata={"content_type": r.headers.get("content-type", "audio/wav")},
            )
        finally:
            self._inflight -= 1

    async def stream(self, request: Request) -> AsyncIterator[StreamEvent]:
        # If the upstream cannot stream, wrap the unary call rather than inventing chunks.
        response = await self.invoke(request)
        yield StreamEvent(request_id=request.request_id, delta=response.output)
        yield StreamEvent(request_id=request.request_id, done=True, usage=response.usage)

    async def drain(self) -> None:
        while self._inflight > 0:
            await asyncio.sleep(0.05)
        await self._client.aclose()
```

### The three rules

1. **`health()` must be fast and must not queue behind inference.** If it blocks, the health
   sweep takes as long as your slowest request and the router flies blind.
2. **`health()` reports load, not liveness.** `inflight` is what lets the router avoid a
   saturated replica that is technically alive.
3. **`drain()` must terminate.** A drain that waits forever turns every deploy into an
   outage.

## 3. Register it

```python
registry.register(SPEC, VoiceProBackend(SPEC, SPEC.endpoint))
```

That's it. `POST /v1/audio/speech` now routes to it when `model="voice-pro"` is pinned, and
`CheapestRouter` will pick it automatically if it beats the alternatives on `cost_tier`.

## Wrapping Ray Serve

Ray Serve already owns replicas, batching and its own router. Wrap the *handle*, not the
deployment:

```python
from ray import serve

class RayBackend:
    def __init__(self, spec, handle) -> None:
        self.spec = spec
        self._handle = handle

    async def health(self) -> Health:
        try:
            stats = await self._handle.get_stats.remote()   # your own method
            return Health(ok=True, **stats)
        except Exception as exc:
            return Health(ok=False, detail=str(exc))

    async def invoke(self, request: Request) -> Response:
        return await self._handle.invoke.remote(request)

    async def stream(self, request: Request):
        async for chunk in self._handle.stream.remote(request):
            yield chunk

    async def scale(self, replicas: int) -> None:
        # Serve's autoscaler owns this in practice; this hook is for manual overrides.
        await self._handle.scale.remote(replicas)
```

Because `scale` exists, `Reconciler` will call it. Because it is optional, every backend
that cannot scale is simply skipped.

## Checklist before you ship a service

- [ ] `health()` returns in well under the health sweep interval
- [ ] `health()` reports `inflight`, and `max_concurrency` matches reality
- [ ] `drain()` exits within the pod's termination grace period
- [ ] `usage` is in a unit your billing understands
- [ ] streaming path releases the slot on client disconnect (do not catch `GeneratorExit`)
- [ ] errors raise `PlatformError` subclasses, not bare `Exception`
- [ ] `cost_tier` reflects actual GPU-seconds per request, not a guess
- [ ] the service answers `/v1/models`-style introspection if it is discoverable
- [ ] a test registers it in a `Registry` and calls `pipeline.invoke` once

## Anti-patterns

| Don't | Do |
|---|---|
| `if capability == "audio.speech"` inside the pipeline | put the difference in the backend |
| return `{"error": ...}` with HTTP 200 | raise `UpstreamError` |
| cache streaming responses | cache only deterministic unary calls (`cache_scope="global"`) |
| set `max_concurrency` to the GPU's happy number | set it to what the *service* can hold, including queueing |
| let the backend own retries | router picks a different model; the pipeline owns deadlines |
