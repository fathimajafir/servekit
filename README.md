# servekit

A small, dependency-free framework for running **many model services behind one API**.

Every model service — a chat model, a TTS engine, an embedding model, a third-party
provider — needs the same nine things done around it:

> authentication · quota · admission control · routing · caching · retries/deadlines ·
> streaming · telemetry · rollout

Writing those nine things per service is how a platform turns into fifteen inconsistent
services. `servekit` writes them once, and a service becomes **one class with five methods
and one line of metadata**.

```python
registry.register(ModelSpec(id="tts-v1", capability=Capability.AUDIO_SPEECH, cost_tier=3), TTSBackend())
```

That is the whole integration. The gateway, quota, queue, cache, metrics and health checks
are already there.

---

## Quickstart

No dependencies beyond the standard library for the core.

```bash
cd servekit
PYTHONPATH=src python3 -m pytest -q          # 8 tests, ~2s
PYTHONPATH=src python3 examples/demo.py      # guided tour, no GPU
```

With the optional HTTP surface:

```bash
pip install -e ".[api]"
PYTHONPATH=src uvicorn app:app --app-dir examples --port 8100
```

```bash
curl -s localhost:8100/v1/chat/completions \
  -H 'content-type: application/json' -H 'x-api-key: dev-key' \
  -d '{"messages":[{"role":"user","content":"hello"}],"stream":true}'
```

---

## The shape of a request

```
                 ┌──────────────────────────────────────────────────────┐
   protocol      │  gateway (examples/app.py)                           │
   translation   │  OpenAI JSON  ->  servekit.Request                    │
                 └───────────────────────────┬──────────────────────────┘
                                             │
                 ┌───────────────────────────▼──────────────────────────┐
                 │  ServingPipeline                                     │
                 │                                                      │
                 │   1. policies.before    auth, quota, input limits     │
                 │   2. cache lookup       exact-match response cache    │
                 │   3. registry.resolve   which models can do this?     │
                 │   4. router.choose      which one should?             │
                 │   5. admission.slot     bounded queue, priority       │
                 │   6. backend.invoke     the actual model              │
                 │   7. policies.after     redaction, audit              │
                 │   8. cache.set                                        │
                 │   9. telemetry.on_finish                              │
                 └───────────────────────────┬──────────────────────────┘
                                             │
        ┌────────────────────────────────────┼────────────────────────────────────┐
        ▼                                    ▼                                    ▼
  chat-small (cost 1)                 chat-large (cost 8)                   tts-v1
  Ray Serve / vLLM / HTTP             Ray Serve / vLLM / HTTP               any TTS provider
```

The gateway file is thin on purpose. If you find yourself adding `if capability == ...`
there, the abstraction is leaking.

---

## The five protocols

Everything is an implementation of one of these. Nothing else is required to add a service.

| Protocol | Question it answers | Shipped implementations |
|---|---|---|
| `Backend` | how do I actually run inference? | your model, plus simulated ones in `examples/` |
| `Router` | which model should answer? | `Cheapest`, `LeastLoaded`, `Pinned`, `LatencyAware`, `Weighted` (canary), `FallbackChain` |
| `Policy` | who is allowed, and what must happen around the call? | `ApiKey`, `TenantQuota`, `PayloadLimit`, `Audit` |
| `Cache` | can we avoid the work entirely? | `MemoryCache`, `NullCache` |
| `Scaler` | how many replicas does this need? | `QueueDepthScaler` |

---

## Repository map

| Path | What lives there |
|---|---|
| `src/servekit/contracts.py` | `ModelSpec`, `Request`, `Response`, `Health`, `Capability` — the wire format |
| `src/servekit/interfaces.py` | the five protocols |
| `src/servekit/pipeline.py` | the ordered path every request takes |
| `src/servekit/admission.py` | bounded queue + priorities |
| `src/servekit/routing.py` | routing strategies |
| `src/servekit/policy.py` | auth, quota, limits, audit |
| `src/servekit/cache.py` | exact-match cache |
| `src/servekit/registry.py` | what models exist right now |
| `src/servekit/controlplane.py` | health sweeps, scaling, canary/rollout |
| `src/servekit/telemetry.py` | counters, logs, fanout |
| `examples/backends.py` | fake LLM and TTS, no GPU needed |
| `examples/app.py` | OpenAI-shaped FastAPI surface |
| `examples/demo.py` | guided tour of every behaviour |
| `docs/architecture.md` | why the layers are cut this way |
| `docs/adding-a-service.md` | step-by-step for a new model |

---

## What this deliberately does not do

- **No model code.** No weights, no tokenizers, no inference engine. That is the backend's job.
- **No distributed state.** `MemoryCache` and `Registry` are per-process. Swapping in Redis
  or Postgres means implementing the same three-method interface.
- **No scheduler.** Replica placement is Kubernetes' or Ray's job; `servekit` only decides
  *desired* counts and canary weights.
- **No prompt management.** Prompts are payload.

Those are choices, not gaps: each one is a database, a cluster, or a product decision that
does not belong inside a library.

---

## Where it fits the maturity ladder

Model deployment usually matures in stages: a notebook, then a container, then replicas
behind a load balancer, then autoscaling, then a multi-tenant platform. This framework is
the code that carries you from **stage 2 (single node) to stage 5 (platform)** without
rewriting the gateway:

| Stage | What you add to `servekit` |
|---|---|
| 2 — single node | `Registry` + `LeastLoadedRouter` + `NullAdmission` |
| 3 — replicated | `AdmissionController`, health loop, `MemoryCache` → Redis |
| 4 — autoscaled | `QueueDepthScaler` + a `Backend` that wraps Ray Serve |
| 5 — platform | `CheapestRouter` + `TenantQuotaPolicy` + `WeightedRouter` canary + a real registry |
