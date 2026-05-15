# Engine

`pyjobkit.Engine` is the producer-side facade. It validates inputs,
applies routing and per-job markers (shadow / tags / retry policy /
webhooks / trace context), waits for queue capacity, and hands the
work off to the configured `QueueBackend`. It is also the side that
worker code uses internally to register state transitions back to the
backend.

## Construction

```python
from pyjobkit import Engine, MemoryBackend
from pyjobkit.executors import HttpExecutor, SubprocessExecutor

engine = Engine(
    backend=MemoryBackend(),
    executors=[SubprocessExecutor(), HttpExecutor()],
    default_max_attempts=5,           # used when enqueue() omits it
    max_queue_size=10_000,            # optional backpressure cap
    enqueue_timeout_s=30,             # how long enqueue() will wait
    enqueue_check_interval_s=0.1,     # how often it re-polls depth
)
```

`Engine` is an async context manager so you can hand it off to a
framework that controls its lifetime:

```python
async with Engine(backend=backend, executors=[Hello()]) as engine:
    await engine.enqueue(kind="hello", payload={"name": "Ada"})
```

`__aexit__` calls `engine.close()`, which in turn cleans up backend /
log sink / event bus resources that implement a `close()` method.

## Enqueueing a single job

```python
job_id = await engine.enqueue(
    kind="send_email",
    payload={"to": "user@example.com", "template": "welcome"},
    priority=50,                       # lower wins
    max_attempts=5,                    # None -> Engine.default_max_attempts
    timeout_s=120,                     # per-job wall-clock cap
    idempotency_key="welcome-42",      # dedup across enqueue calls
    scheduled_for=None,                # see enqueue_at / enqueue_in
)
```

Optional job-level markers:

| Argument | Effect |
| --- | --- |
| `shadow=True` | Dry-run; executor runs, result is discarded. |
| `tags=["user=42", "priority=high"]` | Filterable on the worker (`Worker(tags=...)`). |
| `retry_policy="fixed:5"` | Per-job retry override; spec string only. |
| `webhooks={"complete": "https://..."}` | Fire HTTP callback on terminal state. |

## Delayed jobs

```python
from datetime import datetime, timedelta, timezone

# Absolute time (timezone-aware required).
await engine.enqueue_at(
    when=datetime(2030, 1, 1, 12, tzinfo=timezone.utc),
    kind="newyear",
    payload={},
)

# Relative offset - seconds or timedelta.
await engine.enqueue_in(60, kind="reminder", payload={})
await engine.enqueue_in(timedelta(hours=1), kind="reminder", payload={})
```

## Bulk enqueue

`Engine.enqueue_many` accepts a list of kwargs dicts. When no per-job
markers are used and no router is registered, it delegates to the
backend's bulk insert (single round trip on SQL).

```python
ids = await engine.enqueue_many([
    {"kind": "ping", "payload": {"i": 0}},
    {"kind": "ping", "payload": {"i": 1}, "priority": 10},
    {"kind": "ping", "payload": {"i": 2}, "tags": ["urgent"]},  # falls back to per-row
])
```

## Chains

`Engine.chain(step_a, step_b, step_c)` enqueues the first step
immediately. When the worker successfully finishes a step it enqueues
the next one, threading the previous result into the new payload as
`previous_result`.

```python
head_id = await engine.chain(
    {"kind": "fetch", "payload": {"url": "https://api.example.com"}},
    {"kind": "transform", "payload": {}},   # gets payload["previous_result"]
    {"kind": "store", "payload": {}},
)
```

Failure / timeout / cancellation aborts the rest of the chain. If the
tail enqueue itself fails (e.g. the database is down) the worker emits
a structured `chain.broken` log event and bumps
`pyjobkit_chain_broken_total`.

## Dynamic routing

```python
def router(kind: str, payload: dict) -> str | None:
    if payload.get("type") == "email":
        return "mail-worker"   # rewrite the kind
    return None                # keep original

engine.set_router(router)
```

The callable may be sync **or** async (`Callable[..., Awaitable[str | None]]`).
Returning `None` keeps the caller-supplied kind. The router runs
before `kind` validation, so it can map arbitrary domain values into
the strict `[A-Za-z0-9_.-]+` pattern.

## Read-side

```python
record = await engine.get(job_id)
print(record["status"], record["result"])   # JobRecord TypedDict

await engine.cancel(job_id)                  # cooperative; see docs/cancellation.md

depth = await engine.queue_depth()
await engine.check_connection()              # raises on backend trouble
```

## Retention

```python
from datetime import timedelta

removed = await engine.purge_finished(
    older_than=timedelta(days=30),
    statuses=("success", "cancelled"),
)
```

Backends without `purge_finished` raise `NotImplementedError`. The
shipped `MemoryBackend` and `SQLBackend` both implement it; the
`pyjobkit-prune` console script wraps this method.

## Plugin discovery

```python
new = engine.register_plugins()              # all
new = engine.register_plugins(only=["s3"])   # by entry-point name
```

Loads executor factories from the `pyjobkit.executors` entry-point
group. Already-registered `kind` values are skipped with a warning so
applications can call `register_plugins()` defensively at startup.

## Payload markers

The Engine reserves a few payload keys starting with `__pjk_`. They
are stripped before the executor sees the payload. The set is
exported as `pyjobkit.engine.INTERNAL_PAYLOAD_KEYS` plus a
`strip_internal_payload(payload)` helper that you can call when
serialising a `JobRecord` for an external API.
