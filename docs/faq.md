# FAQ

### Do I need a Redis or RabbitMQ?

No. Pyjobkit's primary transport is your SQL database. Run
`alembic upgrade head` once to create the `job_tasks` table and you are
done. If you prefer Redis or another broker, implement the
`QueueBackend` ABC - see **Extending Pyjobkit**.

### What happens if a worker crashes mid-job?

The worker holds a lease on the job (`lease_until` column). The lease
is extended every `lease_ttl / 2` seconds. If the worker dies, the
watchdog on any other worker reaps the expired lease and marks the job
`failed` with `{"error": "lease_expired"}`, after which the retry
policy decides whether to requeue.

### Are jobs guaranteed to run exactly once?

No - Pyjobkit is at-least-once. After a lease expiry the same job may
be claimed by another worker. Use `idempotency_key` to deduplicate at
enqueue time, and make your executors idempotent.

### How do I run a one-off job from a shell?

```bash
pyjobkit-simulate jobs.json
```

The `jobs.json` file lists kinds + payloads; the simulator runs them
against the in-memory backend and exits non-zero if anything failed.

### How do I see what's in the queue without running a worker?

```python
backend = MemoryBackend()
# or SQLBackend(...) - the introspection helpers below are MemoryBackend-only.
print(await backend.count(status="queued"))
print(await backend.all_jobs())
```

For SQL deployments, query the `job_tasks` table directly. The schema
is stable and documented in `pyjobkit/backends/sql/schema.py`.

### How do I add metrics?

Install `prometheus_client` and the library will register counters and
histograms automatically (see `pyjobkit/metrics.py`). Use
`ctx.profile_phase("name")` inside your executors to record sub-phase
durations under `pyjobkit_phase_duration_seconds`.

### Can I run jobs synchronously for tests?

Yes - use `MemoryBackend` and `Worker.run(once=True)`. See the
example in **Getting started**.
