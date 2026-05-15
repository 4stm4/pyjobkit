# Backends

A backend stores jobs and answers `claim_batch` / state-transition
queries on behalf of the `Worker`. All backends conform to the
`pyjobkit.contracts.QueueBackend` ABC (see
`pyjobkit/contracts.py`). Three are shipped today.

## MemoryBackend

In-process, no external dependencies. Suitable for tests,
prototyping, and tutorials.

```python
from pyjobkit import MemoryBackend

backend = MemoryBackend(lease_ttl_s=30)

job_id = await backend.enqueue(
    kind="ping",
    payload={"x": 1},
    priority=100,
    scheduled_for=None,
    max_attempts=3,
    idempotency_key="ping-1",
    timeout_s=None,
)
print(await backend.queue_depth())
print(await backend.count(status="queued"))
print(await backend.all_jobs(status="success", limit=50))

await backend.clear()           # drop everything (test helper)
```

State lives in a `dict[UUID, _Job]` guarded by a single
`asyncio.Lock`. Do not use it for durable workloads - everything is
lost when the process exits. It is, however, fully
contract-compliant: `succeed` / `fail` / `extend_lease` honour
optimistic-locking semantics with `expected_version`.

## SQLBackend

SQLAlchemy-backed, async-friendly. Targets Postgres, MySQL, and
SQLite.

```python
from sqlalchemy.ext.asyncio import create_async_engine
from pyjobkit.backends.sql import SQLBackend

db = create_async_engine("postgresql+asyncpg://user:pass@host/db")
backend = SQLBackend(
    db,
    prefer_pg_skip_locked=True,   # use FOR UPDATE SKIP LOCKED on PG
    lease_ttl_s=30,
)
```

Schema is created by `pyjobkit-migrate up` (see [CLI tools](cli.md)).
The `job_tasks` table is described inline in
`pyjobkit/backends/sql/schema.py`; it is intentionally one wide row
so backups / replicas / point-in-time restore all work without
external coordination.

### Bulk enqueue

```python
ids = await backend.enqueue_many([
    {"kind": "ping", "payload": {"i": i}}
    for i in range(100)
])
```

A single `INSERT ... VALUES (...), (...)` round trip. Duplicate
`idempotency_key` inside the batch raises `ValueError` before any
row is inserted; cross-batch collisions surface as the underlying
driver's `IntegrityError`.

### `SKIP LOCKED`

When the backend is Postgres and `prefer_pg_skip_locked` is True the
claim path uses:

```sql
WITH picked AS (
  SELECT id FROM job_tasks
   WHERE status='queued' AND scheduled_for <= NOW()
   ORDER BY priority ASC, created_at ASC
   LIMIT :limit
   FOR UPDATE SKIP LOCKED
)
UPDATE job_tasks SET leased_by=:worker, lease_until=NOW() + ..., version=version+1
  FROM picked WHERE jt.id = picked.id;
```

On other dialects the backend uses a per-row `SELECT ... LIMIT 1 +
UPDATE WHERE version=...` loop and logs a one-shot warning. Set
`prefer_pg_skip_locked=False` (or `--disable-skip-locked`) to force
the generic path even on Postgres.

### Retention

```python
from datetime import timedelta

await backend.purge_finished(
    older_than=timedelta(days=30),
    statuses=("success", "cancelled"),
)
```

The `pyjobkit-prune` console script wraps this.

### Internal retry

`_retry_with_backoff` wraps every state-transition statement so
transient `DBAPIError` / `ConnectionError` failures retry up to three
times with exponential backoff before giving up. Optimistic-lock
conflicts surface immediately as `OptimisticLockError` - they are
not retried (because by then someone else has finalised the row).

## RedisBackend (preview)

Optional. Install with `pyjobkit[redis]` and pass
`redis://host:port/db` as the URL.

```python
from pyjobkit.backends import RedisBackend

backend = RedisBackend("redis://localhost:6379/0", lease_ttl_s=30)
```

Layout:

- `pyjobkit:job:<id>` - HASH per job.
- `pyjobkit:queue:ready` - ZSET scored by `priority * 1e12 + scheduled_for`.
- `pyjobkit:leased` - ZSET scored by `lease_until` epoch seconds.
- `pyjobkit:idempotency:<key>` - string -> job_id.
- `pyjobkit:cancel:<id>` - boolean flag.

Atomic claim and reap are implemented with small Lua scripts invoked
via `EVAL` directly (no `EVALSHA` cache - this keeps the backend
compatible with `fakeredis` and similar test doubles).

`RedisBackend` is **preview** until we have load-test data and at
least one production user. The schema and key prefixes may change
between minor releases; pin the version in production if you depend
on it.

## Writing a custom backend

Subclass `QueueBackend` and implement every abstract method. Use
`MemoryBackend` as the reference implementation - it is the smallest
fully compliant backend in the tree. The most important contracts:

- `enqueue` returns a `UUID`; if `idempotency_key` is set and an
  existing job carries it, return that job's id instead of inserting.
- `claim_batch` must atomically transition the picked row(s) out of
  the candidate pool. Concurrent workers must not double-claim.
- `extend_lease` with a mismatched `expected_version` raises
  `OptimisticLockError`. Mismatched `worker_id` silently no-ops.
- `succeed` / `fail` / `timeout` accept `expected_version` and raise
  `OptimisticLockError` on mismatch.
- `retry` resets `status='queued'`, clears the lease, bumps version,
  and sets `scheduled_for = now + delay`.

Optional but recommended:

- `purge_finished(older_than, statuses)` for retention support.
- `all_jobs(status=, limit=)` for the REST list endpoint and the
  bundled dashboard.

Register the new backend via the `pyjobkit.executors` entry-point
group is **not** how this works - backends are wired up directly when
constructing `Engine(backend=...)`. There is no plugin auto-discovery
for backends today.
