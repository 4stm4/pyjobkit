# Pyjobkit 1.0 - announcement

We are excited to announce **Pyjobkit 1.0**: a backend-agnostic, async-first
job processing toolkit for Python. The 1.0 release marks the point where
the public API surface is frozen and follows Semantic Versioning.

## What is Pyjobkit?

Pyjobkit lets you queue and run background jobs in Python without
deploying a broker. Its primary transport is your SQL database
(Postgres / MySQL / SQLite via SQLAlchemy); jobs are claimed with
`SELECT ... FOR UPDATE SKIP LOCKED` on Postgres, leases are renewed
automatically, and workers compete safely thanks to per-row optimistic
locking.

```python
from pyjobkit import Engine, MemoryBackend, Worker
from pyjobkit.contracts import ExecContext, Executor


class Hello(Executor):
    kind = "hello"

    async def run(self, *, job_id, payload, ctx: ExecContext) -> dict:
        await ctx.log(f"running with {payload}")
        return {"greeting": f"hi {payload['name']}"}


engine = Engine(backend=MemoryBackend(), executors=[Hello()])
worker = Worker(engine)

await engine.enqueue(kind="hello", payload={"name": "world"})
await worker.run(once=True)
```

## What's in the box

The 1.0 release is the culmination of a focused sprint that closed
out the v0.x roadmap. The headline features:

- **SQL-first queue** with `SKIP LOCKED`, optimistic locking, and
  versioned leases.
- **Async executors** with cooperative cancellation, progress hooks,
  and phase profiling (`ctx.profile_phase(...)`).
- **Pluggable retry policies** (`FixedDelay`, `ExponentialBackoff`,
  `JitteredExponentialBackoff`) configurable per worker or per job.
- **Shadow / dry-run** mode for safe staging of jobs (`enqueue(...,
  shadow=True)`); the executor still runs, the result is discarded.
- **Per-kind rate limiting** with token buckets, configurable from
  TOML / CLI / env.
- **Tags + filtering**: enqueue jobs with arbitrary tags; workers can
  claim subsets via `Worker(tags=[...])` / `Worker(kinds=[...])`.
- **Dynamic routing** via `Engine.set_router(callable)` to rewrite the
  dispatch kind based on payload.
- **Webhook notifications** on terminal states (`complete`, `fail`,
  `timeout`).
- **Delayed scheduling** via `enqueue_at(when=...)` /
  `enqueue_in(delay)`.
- **PEP 561 typed surface** - `py.typed` shipped, plus `JobRecord`,
  `JobResult`, `FailureReason`, `JobStatus`, `LogStream` TypedDicts and
  Literals.
- **Structured JSON logs** with `event` / `job_id` / `worker_id` /
  `status` / `duration_ms` fields.
- **Plugin discovery**: third-party executors via the
  `pyjobkit.executors` entry-point group.
- **Worker tooling**: `--once` drain mode, configurable watchdog,
  `on_lease_lost` callback, `pyjobkit-simulate` for local debugging.
- **Operations**: Dockerfile for the worker, GitHub Actions example
  pipeline, mkdocs-material documentation deployed to GitHub Pages.

## Who is it for?

Pyjobkit is a good fit when:

- You already run Postgres and would rather not deploy Redis / RabbitMQ
  just for background work.
- Your tasks are I/O-bound and naturally `async def`.
- You want first-class typed APIs and observability built in.
- You prefer explicit, inspectable retry semantics over opaque kwargs.

If you need million-jobs-per-minute throughput on a single instance,
or rich workflow primitives like Celery's canvas, other libraries will
serve you better. See `docs/comparison.md` for a head-to-head with
Celery, RQ, and Dramatiq.

## What 1.0 means

Starting now, the symbols exported from `pyjobkit/__init__.py` and the
CLI flags / config keys are covered by SemVer. Removals or breaking
behaviour changes require a 2.0 release; new optional parameters and
new helpers ship in 1.x. The full policy is documented in
`docs/stability.md`.

## Getting started

```bash
pip install pyjobkit
pip install "pyjobkit[pg]"      # asyncpg for Postgres
pip install "pyjobkit[sqlite]"  # aiosqlite for local prototyping
```

Then read `docs/getting-started.md`. For a tighter onboarding loop:

```bash
echo '{"jobs": [{"kind": "subprocess", "payload": {"cmd": "echo hi"}}]}' > demo.json
pyjobkit-simulate demo.json
```

## Acknowledgements

Thanks to everyone who filed issues during the v0.x cycle - this
release closes 37 of them. Feedback, bug reports, and feature requests
are very welcome at https://github.com/4stm4/pyjobkit/issues.
