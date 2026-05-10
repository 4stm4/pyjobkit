# Pyjobkit vs Celery, RQ, Dramatiq

This document positions Pyjobkit against three of the most established
Python job-processing libraries. The goal is not to crown a "best"
toolkit - each of them is an excellent fit for some workloads - but to
help you decide whether Pyjobkit's trade-offs match your project.

## At a glance

| Dimension | **Pyjobkit** | Celery | RQ | Dramatiq |
|---|---|---|---|---|
| Primary transport | **Your SQL database** (Postgres / SQLite / MySQL via SQLAlchemy) | RabbitMQ, Redis, SQS, ... | Redis | RabbitMQ, Redis |
| External broker required | **No** | Yes | Yes (Redis) | Yes |
| Async (`async def`) executors | **First-class** (`async def run(...)`) | Limited (Celery 5 added asyncio support, but the worker model is sync) | Sync only | Sync only (asyncio via plugins) |
| Public type surface | **PEP 561 (`py.typed`) + TypedDicts** | Partial | Partial | Partial |
| Optimistic concurrency / lease versioning | **Yes** (per-job `version`) | No | No (atomic Redis ops) | No |
| Lease / heartbeat for long jobs | **Yes** (`extend_lease`) | Visibility timeout only | Optional `Job.refresh_ttl` | `Worker` heartbeats |
| Built-in scheduler | `scheduled_for`, `enqueue_at`, `enqueue_in` | Celery Beat | rq-scheduler (separate) | dramatiq-periodiq / actors `with_delay` |
| Cron-style recurring jobs | Roadmap | Celery Beat | rq-scheduler | dramatiq-cron |
| Rate limiting | Per-kind (token bucket) | Per-task `rate_limit` | No | Built-in rate limiter |
| Retry policies | **Pluggable** (fixed / exponential / jittered, or custom) | Built-in (`autoretry_for`) | Per-job | Built-in (`Retries` middleware) |
| Dry-run / shadow mode | **Yes** (`enqueue(..., shadow=True)`) | No | No | No |
| Per-job idempotency key | **Yes** | Manual | Manual | Manual |
| Structured logging | **JSON formatter built in** | Standard logging | Standard logging | Standard logging |
| Prometheus metrics | Built-in counters/histograms | celery-exporter (3rd party) | rq-exporter (3rd party) | dramatiq-prometheus |
| Pluggable executors | **Yes** (`Executor` ABC, entry points) | Tasks are functions; "executors" map to pools | Jobs are callables | Actors |
| Wire format | JSON in your SQL row | Pickle / JSON / msgpack | Pickle | JSON / Pickle |
| Production maturity | Early / focused (v0.2) | Battle-tested, very large surface area | Battle-tested, simple | Mature, smaller community |

## When Pyjobkit is a good fit

* You already run Postgres and would rather not deploy a broker just to
  run background jobs.
* Your jobs are naturally `async` (HTTP I/O, database I/O, streaming
  APIs) and you want to avoid the cost of dispatching them through a
  sync worker.
* You want first-class observability without bolting on third-party
  exporters - JSON logs and Prometheus metrics ship with the library.
* You want strong typing on the producer side - `JobRecord`, `JobResult`,
  `FailureReason` are TypedDicts, the package is marked `py.typed`.
* You want explicit, inspectable retry semantics (`RetryPolicy`
  instances, not opaque kwargs).

## When something else is probably better

* **Throughput in the millions of jobs/minute on a single instance.**
  Redis-backed Celery / RQ / Dramatiq scale further on commodity
  hardware than a single SQL backend can without sharding.
* **Workflows / canvases / chained chords.** Celery's canvas (groups,
  chains, chords, links) is far richer than anything Pyjobkit offers
  today.
* **Sync-only codebase.** If none of your tasks are async, Celery and
  Dramatiq's sync-first execution models will feel more natural.
* **You want a huge ecosystem of community-maintained tasks.** Celery
  has by far the largest plugin surface; Pyjobkit is intentionally
  small.

## Feature matrix details

### Transport and durability

Pyjobkit treats your SQL database as the queue. The default schema is
versioned via Alembic migrations and uses `SELECT ... FOR UPDATE SKIP
LOCKED` on Postgres to avoid contention. There is no separate "broker"
or "result backend" - jobs and their results live in the same row, so
operational tooling (backups, replicas, audit trails) just works.

### Async-first execution

`Executor.run` is an `async def`. The worker's concurrency model is
`asyncio` - the `--concurrency` CLI flag bounds the number of
simultaneously-running coroutines, not OS threads or processes. This
makes Pyjobkit cheap for I/O-bound workloads (HTTP callouts, downstream
APIs, streaming uploads).

### Lease + optimistic locking

Every claim grants the worker a versioned lease. The worker extends the
lease periodically; if the worker crashes, the watchdog reclaims the job
after the lease expires. State transitions (`succeed`, `fail`, `retry`)
check the expected `version` so racing workers can never both finalize
the same row.

### Pluggable retry policies

```python
from pyjobkit import Worker, ExponentialBackoff, JitteredExponentialBackoff

Worker(engine, retry_policy=JitteredExponentialBackoff(1, 2, max_delay_s=60))
# or
Worker(engine, retry_policy="exponential_jitter:1:2:60:0.1")
```

Per-job overrides are supported via `Engine.enqueue(...,
retry_policy="fixed:5")`.

### Shadow / dry-run mode

`Engine.enqueue(..., shadow=True)` lets you exercise the full pipeline
(claim, lease, executor invocation, logs, progress updates) without
recording the executor's return value. Executors can branch on
`ctx.is_shadow` to skip irreversible side effects.

## Versioning

Pyjobkit currently sits at `0.x`. Public API surface is annotated and
re-exported from the package root; breaking changes will be called out
in the changelog. A `1.0` release will lock down the surface per semver.
