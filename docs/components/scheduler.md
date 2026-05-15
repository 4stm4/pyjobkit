# Scheduler

`pyjobkit.scheduler.Scheduler` periodically enqueues jobs against an
`Engine`. Combine it with a `LeaderLock` for HA setups so only one
worker process actually fires the schedule.

## Basic usage

```python
import asyncio
from pyjobkit import Engine, MemoryBackend
from pyjobkit.scheduler import Scheduler

engine = Engine(backend=MemoryBackend(), executors=[...])
sched = Scheduler(engine)

sched.every("5m", name="health-check", kind="probe", payload={})
sched.every("1h", name="cleanup",     kind="prune",  payload={"older_than": "30d"})
sched.every("30s", name="warmup",     kind="warmup", payload={}, run_immediately=True)

stop = asyncio.Event()
await sched.run(stop_event=stop)
```

Interval forms accepted by `every` (and `parse_interval`):

| Form | Example |
| --- | --- |
| seconds (int / float) | `30`, `7.5` |
| `Ns` / `Nm` / `Nh` / `Nd` | `"30s"`, `"5m"`, `"2h"`, `"1d"` |
| `datetime.timedelta` | `timedelta(hours=2)` |

`run_immediately=True` fires the entry on the first tick instead of
waiting one full interval. Useful for warmup tasks that should also
run on process start.

## Idempotency under HA

Every tick stamps the enqueue with
`idempotency_key=f"scheduler:{name}:{slot}"` where `slot` is the
integer time bucket. Two schedulers racing on the same slot
(typically right after a leader handover) collapse to a single
backend row via the UNIQUE constraint on `idempotency_key`.

## Failure handling

If `engine.enqueue` raises, the worker:

1. Bumps `pyjobkit_scheduler_enqueue_failures_total`.
2. Logs a WARNING with the exception.
3. **Does not** advance `next_run`, so the next tick retries the
   same slot. The idempotency key prevents duplicates if the
   previous attempt actually landed.

## Combining with leader election

```python
from pyjobkit.leader import MemoryLeaderLock

# In production: subclass LeaderLock to back this with Postgres or Redis.
lock = MemoryLeaderLock(name="scheduler")
await sched.run(stop_event=stop, leader_lock=lock, lock_ttl_s=30)
```

While `lock.try_acquire()` returns `False` the loop sleeps and
re-tries; once it returns `True` the scheduler fires until the lock
is lost or `stop_event` is set.

## API surface

| Method | Purpose |
| --- | --- |
| `every(interval, *, name, kind, payload=None, run_immediately=False, **enqueue_kwargs)` | Register / replace an entry. |
| `remove(name)` | Drop an entry. |
| `entries()` | List registered names (sorted). |
| `tick()` | Run one pass; returns the number of enqueues. |
| `run(stop_event, leader_lock=None, lock_ttl_s=30, sleep_resolution_s=1)` | Loop until stopped. |

`Scheduler` does **not** persist `last_run` to a shared store. After
a process restart every entry treats `now` as the baseline; the
idempotency key keeps you safe from duplicate enqueues on the same
slot but you may miss one slot during the restart window.

## Cron expressions

Not supported out of the box. If you need cron syntax, install
`croniter` and write a thin wrapper:

```python
from croniter import croniter
import time

def add_cron(sched, *, name, cron, **kwargs):
    base = time.time()
    delay = croniter(cron, base).get_next() - base
    sched.every(delay, name=name, **kwargs)
```

Subsequent ticks will use the original interval; if you need true
cron semantics with per-firing recomputation, drive the scheduler
from a coroutine that calls `Engine.enqueue` directly.
