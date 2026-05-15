# Leader election

`pyjobkit.leader` provides a small cooperative leader-election
primitive for "only one worker does X" tasks - typically the
`Scheduler` or any singleton maintenance loop.

```python
from pyjobkit.leader import LeaderLock, MemoryLeaderLock, leader_loop
```

## The contract

```python
class LeaderLock(ABC):
    async def try_acquire(self, *, ttl_s: float) -> bool: ...
    async def release(self) -> None: ...
```

`try_acquire` must be atomic: two callers cannot both observe `True`
before the previous holder's `ttl_s` lapses. Renewals are encoded as
repeated `try_acquire` calls.

## Built-in: MemoryLeaderLock

In-process implementation. Different instances created with the
same `name` compete for the same logical lock (state is held at
class level so multiple `Scheduler` instances inside one Python
process behave like distributed callers).

```python
lock_a = MemoryLeaderLock(name="scheduler")
lock_b = MemoryLeaderLock(name="scheduler")

await lock_a.try_acquire(ttl_s=30)    # True
await lock_b.try_acquire(ttl_s=30)    # False - someone else holds it
await lock_a.release()
await lock_b.try_acquire(ttl_s=30)    # True now
```

`MemoryLeaderLock.reset_state()` wipes shared state; tests use it
between cases.

## Writing a Postgres lock

The recommended production layout is one row per logical lock plus
`expires_at`. The simplest skeleton:

```python
import time
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from uuid import uuid4

from pyjobkit.leader import LeaderLock


class PostgresLock(LeaderLock):
    def __init__(self, engine: AsyncEngine, name: str):
        self.engine = engine
        self.name = name
        self.holder = str(uuid4())

    async def try_acquire(self, *, ttl_s: float) -> bool:
        async with self.engine.begin() as conn:
            row = (await conn.execute(text("""
                INSERT INTO pyjobkit_leases(name, holder, expires_at)
                     VALUES (:n, :h, NOW() + :t * INTERVAL '1 second')
                ON CONFLICT (name) DO UPDATE
                       SET holder = EXCLUDED.holder,
                           expires_at = EXCLUDED.expires_at
                     WHERE pyjobkit_leases.expires_at < NOW()
                        OR pyjobkit_leases.holder = EXCLUDED.holder
                 RETURNING holder
            """), {"n": self.name, "h": self.holder, "t": ttl_s})).first()
        return row is not None and row[0] == self.holder

    async def release(self) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(text(
                "DELETE FROM pyjobkit_leases WHERE name=:n AND holder=:h"
            ), {"n": self.name, "h": self.holder})
```

Bring your own table; the library doesn't ship it because no two
deployments agree on what columns belong on a coordination row.

## leader_loop

Convenience helper that drives a coroutine while the lock is held
and re-acquires on every renewal cycle:

```python
from pyjobkit.leader import leader_loop, MemoryLeaderLock

async def only_leader_does_this() -> None:
    while True:
        await do_something()
        await asyncio.sleep(1)

lock = MemoryLeaderLock(name="singleton")
stop = asyncio.Event()

await leader_loop(
    lock,
    ttl_s=30,
    renew_every_s=10,       # defaults to ttl_s / 2
    on_leader=only_leader_does_this,
    stop_event=stop,
    cancel_grace_s=5,       # how long to wait for on_leader to honour cancel
)
```

Semantics:

- The function blocks until `stop_event` is set.
- While the lock is held it runs `on_leader()` concurrently and
  re-acquires the lock every `renew_every_s` seconds.
- If a renewal returns `False`, the `on_leader` task is cancelled
  (with at most `cancel_grace_s` to honour the cancel) and the loop
  goes back to polling for the lock.
- If `on_leader` raises, the loop logs the exception and restarts a
  fresh task on the next renewal cycle.

`cancel_grace_s` exists so a misbehaving `on_leader` that swallows
`CancelledError` cannot block the shutdown path forever - the lock
is released after the grace window even if the task is still
running.
