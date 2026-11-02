# Architecture

## 1. The problem this solves

Ten model services, each with its own FastAPI app, means ten copies of:

| Concern | Cost of getting it wrong once |
|---|---|
| API key check | an open endpoint |
| Tenant quota | one customer starves everyone |
| Admission control | a latency collapse during peak |
| Routing | you pay for the frontier model on "hi" |
| Cache | you pay for the same answer 10,000× |
| Deadlines | requests pile up behind a dead backend |
| Streaming | timeouts that look like random 504s |
| Telemetry | you cannot say which model is expensive |
| Drain on SIGTERM | every deploy drops in-flight work |

Nine concerns × ten services = ninety places to be wrong. The framework makes it
**nine places, once**, and each service stays a thin adapter.

## 2. Principles

1. **A service is a capability plus metadata, not a repository.** Adding TTS should not
   require a new gateway route; it should require a `ModelSpec`.
2. **Separate *what exists* from *which one answers* from *should we serve at all*.**
   Registry / Router / Admission. Each stays small because each answers one question.
3. **The queue is the product.** Throughput is a batching problem; survival is a queue
   problem. Bound it, prioritise it, expire it.
4. **Fail with a status code.** Every failure maps to one of 400/401/403/429/502/503/504.
   A request that neither succeeds nor fails is the only unacceptable outcome.
5. **Stateless gateway, stateful edges.** The gateway holds nothing that cannot be rebuilt;
   state lives in the cache, the registry and the backends.

## 3. Layers

```
L7  Edge            TLS, routing, rate limit at the perimeter (nginx / LB / CDN)
L6  Gateway         protocol translation, streaming, error mapping        [stateless, scales on RPS]
L5  Pipeline        policies -> cache -> route -> admit -> call -> observe [stateless]
L4  Routing         registry view + strategy                              [stateless]
L3  Admission       bounded queue, priorities, deadlines                  [in-process or shared]
L2  Backends        Ray Serve | vLLM/HTTP | SLURM job | SaaS proxy        [stateful, scales on GPU]
L1  Control plane   health sweep, scaling, canary, drain                  [singleton per cluster]
L0  Substrate       Kubernetes / SLURM / Ray, storage, networking
```

**Deployable units.** In a POC all of L5–L3 live in one process. As load grows they split
in this order, and only when the previous split is the actual bottleneck:

| Split | Trigger |
|---|---|
| L6 gateway from L2 backends | you need to deploy a model without touching the API |
| L1 control plane into its own process | more than one gateway replica needs consistent health |
| L3 admission into a shared store | replicas must share one queue (rare; usually you shard by tenant) |
| L4 routing into a service | cache-affinity routing across hundreds of replicas |

Anything split earlier buys you network hops and no capacity.

## 4. Contracts

The contracts are small because they are the part you cannot change later.

- **`ModelSpec`** — the operator's entire job. Cost tier, latency class, concurrency and
  capability are enough to route, bill and scale.
- **`Request`** — normalised across protocols. `capability` + `payload` + `tenant` +
  `priority` + `deadline`. Protocol-specific shapes never reach the router.
- **`Health`** — `inflight`, `queue_depth`, `ok`. Deliberately *not* CPU/GPU utilisation:
  those are the scaler's business, not the router's.
- **`Candidate`** — spec + backend + health. What a router actually chooses between.
- **`Response` / `StreamEvent`** — one shape for every modality. A TTS byte stream and a
  chat token stream are the same contract with a different `delta`.

## 5. Lifecycles

### Unary

```
policy.before ──► cache.get ──hit──► return (0 ms, 0 GPU)
       │                │
       │               miss
       │                ▼
       └──► resolve ──► route ──► admit ──► backend.invoke ──► policy.after ──► cache.set
                                                                   │
                                                            telemetry.on_finish
```

### Streaming — it is a different machine, not a flag

| | Unary | Streaming |
|---|---|---|
| Slot held for | the call | **the whole stream** |
| Admission unit | RPS | concurrent connections |
| Cache | full response | usually none (partial answers are rarely reusable) |
| Failure after first byte | clean 5xx | cannot change status → must emit an in-band error event |
| Client disconnect | irrelevant | must release the slot via `GeneratorExit` |
| Load balancer | seconds of timeout | minutes; must not buffer |

This is why `pipeline.stream()` holds the admission slot inside a `try/finally` around the
`async for`. A leaked slot is indistinguishable from a dead replica within minutes.

## 6. Where state lives

| State | In-process (POC) | Shared (production) | If it goes wrong |
|---|---|---|---|
| Registry | dict | DB + watch | stale routes, 503s |
| Health | polled dict | same, pushed by backends | traffic to dead replicas |
| Cache | LRU | Redis | only a hit-rate dip |
| Quota | token bucket | Redis counters | over-serving one tenant |
| Admission | semaphore | per-replica (sharded by LB) | imbalance, not corruption |

The admission queue is intentionally **per replica**, not global. A global queue is a
single point of coordination and a single point of failure; the load balancer already
spreads requests, so per-replica queues approximate a fair global one at a fraction of
the cost.

## 7. Scaling: three independent loops

1. **Gateway** — stateless, scales on request rate. Add replicas freely.
2. **Backends** — scale on queue depth / concurrency, with a scale-down delay longer than
   cold start (otherwise replicas flap). `QueueDepthScaler` is the reference; Ray Serve
   and KEDA do this natively.
3. **Control plane** — one leader per cluster. Polls, reconciles, shifts canary weights.
   Never in the request path.

Confusing these is the most common scaling bug: people autoscale the gateway when the
queue is in the backend.

## 8. Degradation ladder

When capacity runs out, degrade in this order — never silently:

```
1. serve from cache              (free)
2. route to a cheaper/smaller model   (quality drops, latency improves)
3. queue with a deadline         (latency drops, no errors)
4. reject with 429/503/504 + Retry-After   (some users fail fast)
5. shed load by priority         (free tier fails, paid tier survives)
```

Every step is observable. A platform that jumps from 1 to 4 without 2 and 3 is leaving
capacity on the table; one that skips 4 is lying to itself.

## 9. What changes when you add a capability

| Capability | Backend | Unit | Routing | Extra concern |
|---|---|---|---|---|
| `text.generate` | Ray Serve / vLLM | token | cheapest adequate model | prefix-cache affinity |
| `text.embed` | HTTP / batch | token | least loaded | batching is mandatory |
| `audio.speech` | TTS engine or SaaS | second of audio | cheapest + latency class | long streams, shared segment state |
| `audio.transcribe` | Whisper-style | second of audio | least loaded | upload size limits |
| `image.generate` | diffusion server | image | cost tier | long jobs, async queue |
| `video.generate` | diffusion cluster | second of video | **batch only** | job API, not request/response |

Note the pattern: **the pipeline does not change.** Only the backend and the routing
policy do. That is the test of whether an abstraction is real.

## 10. Service boundaries, honestly

`servekit` is a **library plus a thin gateway**, not a microservice mesh. Start with one
process. Split when a specific number forces it:

| Symptom | Split |
|---|---|
| Deploying a model restarts the API | gateway ↔ backend |
| Two gateways disagree about health | control plane |
| One tenant's traffic starves another | per-tenant queue or dedicated pool |
| Cache hit rate is low across replicas | shared cache |
| Canary needs instant rollback | weighted router + control plane |

Everything else is premature.

## 11. Non-goals

- Model loading, tokenisation, batching algorithms — the runtime owns these.
- Prompt/tool orchestration — that is an application concern.
- Billing — the framework emits `Usage`; a billing service turns it into money.
- Exactly-once semantics — idempotency keys are carried, not enforced.
