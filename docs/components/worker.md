# Worker

`pyjobkit.Worker` is the consumer side. It claims jobs from a backend,
runs the matching executor under `asyncio.TaskGroup`, renews leases,
applies retry policy, fires webhooks on terminal states, and runs a
watchdog that reaps abandoned leases from crashed peers.

## Construction

```python
from pyjobkit import Worker

worker = Worker(
    engine,
    max_concurrency=16,            # simultaneous coroutines
    batch=4,                       # jobs claimed per poll
    poll_interval=0.5,             # idle poll cadence (s)
    lease_ttl=30,                  # lease lifetime (s)
    queue_capacity=10_000,         # backpressure threshold for /healthz
    stop_timeout=60,               # max seconds to wait on drain
    retry_policy="exponential:1:2",
    watchdog_interval_s=30,        # how often _reap_loop runs (default: lease_ttl)
    rate_limits={"http": {"max_per_second": 5, "burst": 10}},
    kinds=["email", "sms"],        # only claim these kinds (None = all)
    tags=["high-priority"],        # only claim jobs that carry at least one
    heartbeat_interval_s=10,       # emit worker.heartbeat events
    on_heartbeat=async_callback,   # optional
    on_lease_lost=async_callback,  # optional
)
```

`Worker` is a **single-shot** object: once `run()` (or `run(once=True)`)
has returned, calling it again on the same instance raises
`RuntimeError`. Create a fresh `Worker` for the next invocation.

## Running

```python
# Production daemon - loops forever until stop_event / SIGTERM / SIGINT.
await worker.run()

# Drain mode - claim every ready job, run it, exit.
await worker.run(once=True)
```

As an async context manager:

```python
async with Worker(engine) as worker:
    task = asyncio.create_task(worker.run())
    ...
    # __aexit__ calls request_stop() + wait_stopped()
```

## Filters

`kinds` and `tags` are independent. When both are set a job must
match *both* (its `kind` is in the allowlist **and** its tag set
intersects `tags`). Jobs that fail the filter are immediately
released back to the queue (`retry(delay=0)`) so another worker can
pick them up.

```python
# Two dedicated worker pools backed by the same database.
mail_worker = Worker(engine, kinds=["email", "sms"])
heavy_worker = Worker(engine, kinds=["transcode"], max_concurrency=4)
```

## Retry policies

Pass a `RetryPolicy` instance, a spec string, or `None` (defaults to
`ExponentialBackoff(base=1, factor=2)`):

```python
from pyjobkit import JitteredExponentialBackoff

worker = Worker(
    engine,
    retry_policy=JitteredExponentialBackoff(
        base=1, factor=2, max_delay_s=60, jitter=0.1,
        give_up_after_age_s=24 * 3600,
    ),
)
```

Per-job overrides flow through the payload marker
`__pjk_retry_policy` (set by `Engine.enqueue(..., retry_policy=...)`).

## Lease lifecycle

1. Worker calls `backend.claim_batch(worker_id, limit=batch)`. Each
   row arrives with a `version` (used for optimistic locking).
2. `_extend_loop` runs in the background, refreshing the lease every
   `lease_ttl / 2` seconds. Lost lease (rare) raises
   `LeaseLostError`; the worker emits the `job.lease_lost` event and
   invokes `on_lease_lost(job_id, row)` if set.
3. On terminal state the worker calls
   `engine.succeed / fail / timeout` with `expected_version`. Optimistic
   conflicts (someone else finalised it) are logged as
   `job.lock_conflict` and dropped.

## Health

```python
health = await worker.check_health()
# {"status": "healthy", "queue_depth": 12, "queue_overflow": False}
```

`status` flips to `unhealthy` when:

- `backend.check_connection()` raises, or
- `queue_capacity` is configured and depth `>= queue_capacity`.

Pair this with an HTTP `/livez` endpoint in your application if you
need richer signals than the TCP probe in the Helm chart.

## Heartbeats

```python
async def record(worker_id):
    await redis.set(f"worker:{worker_id}", time.time(), ex=60)

worker = Worker(engine, heartbeat_interval_s=10, on_heartbeat=record)
```

The loop emits a structured `worker.heartbeat` log event on the
configured cadence regardless of the callback, so even without
`on_heartbeat` you get a liveness trace in your log aggregator.

## Watchdog

A background `_reap_loop` runs every `watchdog_interval_s` and calls
`engine.reap_expired()`. When at least one lease is reclaimed the
worker logs:

```json
{"event": "watchdog.reaped", "worker_id": "...", "count": 3}
```

The default interval is `lease_ttl`. In production with a small
fleet you can tighten it (e.g. `watchdog_interval_s=5`) so abandoned
jobs come back into rotation faster.

## State-change events

The worker emits structured log events on every status transition.
They are visible in both `text` and `json` log formats; the JSON
formatter surfaces them in this order:

| `event` | When |
| --- | --- |
| `job.started` | After claim + before executor runs. |
| `job.succeeded` | Normal completion. |
| `job.shadow_succeeded` | Shadow run finished. |
| `job.timeout` | Hit `timeout_s`, no more retries. |
| `job.retry` | Failed but will be retried. |
| `job.failed` | Max attempts exhausted. |
| `job.cancelled` | `Engine.cancel` observed or `CancelledError`. |
| `job.lease_lost` | Lease expired while job was in flight. |
| `job.lock_conflict` | Optimistic concurrency conflict during finalise. |
| `watchdog.reaped` | Lease reaper finalised one or more jobs. |
| `worker.heartbeat` | Periodic liveness ping (when enabled). |
| `chain.broken` | Tail enqueue failed after head succeeded. |
| `phase.started` / `phase.ended` | From `ctx.profile_phase(...)`. |

Each event carries `job_id` / `worker_id` / `status` / `duration_ms`
where applicable - see `pyjobkit.logging.JsonFormatter`.
