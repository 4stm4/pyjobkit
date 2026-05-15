# Pyjobkit

[![Version](https://img.shields.io/badge/version-1.0.0b1-blue.svg)](https://pypi.org/project/pyjobkit/)
[![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/4stm4/99574c3e4a5b6f3890c375bde7e1f0cd/raw/coverage-summary.json)](https://github.com/4stm4/pyjobkit/actions)
[![Python Version](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/release/python-3130/)

Pyjobkit is a backend-agnostic toolkit for building reliable asynchronous job processing systems. It provides an `Engine` facade for enqueueing work, a cooperative asyncio `Worker`, a set of executor contracts, and pluggable queue backends so you can adapt the runtime to your infrastructure with minimal glue code.

## Features
- **Backends** - SQL (`SQLBackend`, Postgres / MySQL / SQLite via SQLAlchemy) for production, `MemoryBackend` for tests, optional `RedisBackend` (preview). All implement the same `QueueBackend` ABC.
- **Async worker** - `Worker` built on `asyncio.TaskGroup` with concurrency limits, batch polling, lease extension, optimistic locking, configurable watchdog, optional heartbeat callback, and SIGTERM / SIGINT graceful shutdown.
- **Executors** - `SubprocessExecutor` (with optional command allowlist), `HttpExecutor`, and an optional `DockerExecutor`. Custom executors implement `Executor`; third-party packages can register via the `pyjobkit.executors` entry-point group.
- **Retry policies** - `FixedDelay` / `ExponentialBackoff` / `JitteredExponentialBackoff`, configurable per worker or per job, with an optional wall-clock `give_up_after_age_s` cap.
- **Scheduling** - `Engine.enqueue_at(when)` / `enqueue_in(delay)` for delayed jobs, `pyjobkit.scheduler.Scheduler` for periodic enqueues, plus job-level `tags`, `shadow` (dry-run), and per-job `retry_policy` overrides.
- **Workflows** - `Engine.chain(step_a, step_b, ...)` runs steps sequentially and threads the previous result through `payload["previous_result"]`; `Engine.enqueue_many(...)` bulk-inserts a batch in one round trip.
- **Routing** - `Engine.set_router(callable)` (sync or async) rewrites kinds based on payload; `Worker(kinds=..., tags=...)` filters claimed jobs.
- **Rate limiting** - per-kind token bucket configured via the worker, the CLI, or TOML.
- **Observability** - JSON log formatter (`pyjobkit.logging.JsonFormatter`), `ctx.profile_phase(...)`, Prometheus `/metrics` exporter, optional OpenTelemetry spans on enqueue / execute (with W3C trace context propagation through the payload), and webhook notifications on terminal states (HMAC-signed when `PYJOBKIT_WEBHOOK_SECRET` is set).
- **CLI** - `pyjobkit` (worker with `--once`, `--kind`, `--config`, `--log-format`, etc.), `pyjobkit-simulate` (run a JSON job file against the in-memory backend), `pyjobkit-migrate` (Alembic migrations), and `pyjobkit-prune` (retention).
- **REST + dashboard** - optional FastAPI router (`pyjobkit.integrations.fastapi.make_router` with a `dependencies=[Depends(...)]` auth hook) plus a bundled HTML dashboard and a TypeScript client (`ts/`).
- **Typed API** - `py.typed` marker, public `TypedDict`s (`JobRecord`, `JobResult`, `FailureReason`), `JobStatus` / `LogStream` literals.
- **Deployment assets** - Dockerfile, Helm chart (`deploy/helm/pyjobkit`), Grafana dashboard (`deploy/grafana/`).


## Installation
```bash
pip install pyjobkit
```

Optional extras:

```bash
pip install "pyjobkit[pg]"        # asyncpg (PostgreSQL)
pip install "pyjobkit[mysql]"     # aiomysql
pip install "pyjobkit[sqlite]"    # aiosqlite
pip install "pyjobkit[redis]"     # RedisBackend (preview)
pip install "pyjobkit[docker]"    # DockerExecutor
pip install "pyjobkit[fastapi]"   # REST router + dashboard
pip install "pyjobkit[metrics]"   # Prometheus /metrics exporter
pip install "pyjobkit[otel]"      # OpenTelemetry spans
```

## Getting started
The core building blocks are the `Engine`, a `QueueBackend`, at least one executor, and the `Worker` loop.

```python
import asyncio
from pyjobkit import Engine, Worker
from pyjobkit.backends.memory import MemoryBackend
from pyjobkit.contracts import ExecContext, Executor

class HelloExecutor:
    kind = "hello"

    async def run(self, *, job_id, payload, ctx: ExecContext):
        await ctx.log(f"processing {job_id}")
        name = payload.get("name", "world")
        return {"message": f"Hello, {name}!"}

async def main():
    backend = MemoryBackend()
    engine = Engine(backend=backend, executors=[HelloExecutor()])
    worker = Worker(engine, max_concurrency=2)

    # enqueue a job
    job_id = await engine.enqueue(kind="hello", payload={"name": "Ada"})
    print("enqueued", job_id)

    # run the worker loop (typically done in a dedicated process)
    await worker.run()

asyncio.run(main())
```

The memory backend keeps jobs in-process, making it ideal for unit tests and experimentation. For production you can switch to the SQL backend without changing the worker or executors:

```python
from sqlalchemy.ext.asyncio import create_async_engine
from pyjobkit.backends.sql import SQLBackend

engine = create_async_engine("postgresql+asyncpg://user:pass@host/db")
backend = SQLBackend(engine, prefer_pg_skip_locked=True, lease_ttl_s=60)
```

## SQL schema and migrations
The SQL backend uses the `job_tasks` table defined in `pyjobkit.backends.sql.schema`. The bundled `pyjobkit-migrate` console script runs Alembic against the migrations shipped with the package:

```bash
pyjobkit-migrate --dsn postgresql+asyncpg://user:pass@host/db up
pyjobkit-migrate --dsn postgresql+asyncpg://user:pass@host/db current
```

It accepts the async DSN forms (`postgresql+asyncpg`, `mysql+aiomysql`, `sqlite+aiosqlite`) and rewrites them to the matching sync drivers internally. The DSN may also come from `PYJOBKIT_DSN` or `.pyjobkit.toml`. Re-run `pyjobkit-migrate up` on every deploy; each minor release that touches the schema ships an Alembic revision.

For quick prototyping you can still bootstrap the table via SQLAlchemy directly:

```python
from sqlalchemy.ext.asyncio import create_async_engine
from pyjobkit.backends.sql.schema import metadata

engine = create_async_engine("postgresql+asyncpg://user:pass@host/db")
async with engine.begin() as conn:
    await conn.run_sync(metadata.create_all)
```

## Running the bundled worker CLI
Once the schema exists, you can run the provided worker process:

```bash
pyjobkit --dsn postgresql+asyncpg://user:pass@host/db \
    --concurrency 8 \
    --batch 4 \
    --lease-ttl 30 \
    --poll-interval 0.5
```

Use `--disable-skip-locked` when targeting databases that do not support the Postgres-specific optimization. The CLI wires in the SQL backend plus the HTTP and subprocess executors, so jobs with `kind="http"` or `kind="subprocess"` will run out of the box.

The worker installs SIGTERM / SIGINT handlers that call `worker.request_stop()`, so container runtimes (Kubernetes, Docker, systemd) get a clean drain on shutdown. Pass `--once` for a one-shot drain (useful for cron-style invocations) or `--kind` to restrict the worker to specific job kinds.

### Retention

The SQL backend keeps finished jobs forever unless you remove them. Run `pyjobkit-prune` on a schedule:

```bash
pyjobkit-prune --older-than 30d --statuses success,cancelled
pyjobkit-prune --older-than 90d --statuses failed,timeout
```

`--older-than` accepts compact durations (`30d`, `24h`, `90m`, `60s`). Without it every terminal job in the chosen statuses is deleted.

### Configuration via TOML / environment

CLI flags can be replaced (or supplemented) by a `.pyjobkit.toml` file in the working directory or by `PYJOBKIT_*` environment variables. Resolution order is **CLI flags -> environment -> TOML file -> defaults**.

```toml
# .pyjobkit.toml
[pyjobkit]
dsn = "postgresql+asyncpg://user:pass@host/db"   # alias: db_url
poll_interval = 0.5
max_attempts = 3
default_executor = "myapp.executors:make_redis"
concurrency = 8
batch = 1
lease_ttl = 30
log_level = "INFO"
disable_skip_locked = false
extra_executors = ["myapp.executors:make_celery"]
log_format = "json"  # 'text' (default) or 'json' for structured logs
retry_policy = "exponential_jitter:1:2:30:0.1"  # or "fixed:5", "exponential:1:2"
```

```bash
export PYJOBKIT_DSN=postgresql+asyncpg://user:pass@host/db
export PYJOBKIT_POLL_INTERVAL=1.0
pyjobkit               # uses env + ./.pyjobkit.toml
pyjobkit --config /etc/pyjobkit.toml
```

The same loader is exposed programmatically as `pyjobkit.load_config()` / `pyjobkit.Config`.

### In-memory backend (testing / debug)

For unit tests, prototyping, and tutorials the library ships with a
fully-featured in-memory implementation of `QueueBackend`:

```python
from pyjobkit import Engine, MemoryBackend
from pyjobkit.executors import SubprocessExecutor

backend = MemoryBackend()
engine = Engine(backend=backend, executors=[SubprocessExecutor()])
job_id = await engine.enqueue(kind="subprocess", payload={"cmd": "echo hi"})
# debug helpers
print(await backend.count())                 # 1
print(await backend.count(status="queued"))  # 1
await backend.clear()
```

All state lives in process memory and is dropped when the process
exits - do not use this backend for durable workloads.

## Docker and Kubernetes

A minimal worker image is provided in [`Dockerfile`](Dockerfile):

```bash
docker build -t pyjobkit/worker:dev .
docker run --rm \
  -e PYJOBKIT_DSN=postgresql+asyncpg://user:pass@host/db \
  pyjobkit/worker:dev
```

The image installs `pyjobkit` with the `asyncpg` and `aiosqlite`
drivers; configuration is read from `PYJOBKIT_*` environment variables
or a TOML file mounted into the container.

For Kubernetes a Helm chart is shipped in [`deploy/helm/pyjobkit`](deploy/helm/pyjobkit). It renders the Deployment, the DSN Secret, a pre-install migration Job (`pyjobkit-migrate up`), a `metrics` Service, and an optional `ServiceMonitor` for kube-prometheus-stack. See [`deploy/README.md`](deploy/README.md) for installation instructions and [`deploy/grafana/pyjobkit-dashboard.json`](deploy/grafana/pyjobkit-dashboard.json) for a ready-to-import Grafana dashboard.

## Production deployment

See [`docs/production.md`](docs/production.md) for guidance on schema migrations, graceful shutdown, retention, observability, rate limits, and Postgres tuning. [`docs/cancellation.md`](docs/cancellation.md) documents the cooperative cancellation contract. The full stability policy is in [`docs/stability.md`](docs/stability.md). Vulnerabilities should be reported per [`SECURITY.md`](SECURITY.md).

## Comparison with other Python job libraries

See [docs/comparison.md](docs/comparison.md) for a head-to-head
positioning against Celery, RQ, and Dramatiq.

## Extending Pyjobkit
- **Custom executors** - Implement the `Executor` protocol, register instances when constructing the `Engine`, and leverage the `ExecContext` helpers (`log`, `set_progress`, `is_cancelled`).
- **Alternate backends** - Implement the `QueueBackend` protocol to target message brokers or proprietary queues while reusing the worker and executor layers.
- **Logging & events** - Swap the memory log sink or event bus with your own implementations (e.g., stream to Loki or publish over Redis) by passing them to the `Engine` constructor.



## Examples
- [`examples/taskboard`](examples/taskboard) - A single-page FastAPI dashboard that enqueues demo jobs which sleep for a random duration using the in-memory backend. Includes a Dockerfile for quick demos.

### Demo dashboard screenshot

![Taskboard demo screenshot](pyjobkit_demo_dashboard.png)


### Running the Taskboard demo with Docker Compose
The repository ships with a minimal `docker-compose.yml` that runs the taskboard FastAPI app using the official Python image and a runtime install script:

```bash
docker compose up taskboard
# open http://localhost:8000 to view the UI
```

> **Troubleshooting BuildKit errors**
>
> Some environments (including Portainer-managed hosts) route Docker daemon traffic through an HTTP proxy. When that proxy does not understand HTTP/2, `docker compose` can fail during the build phase with an error similar to:
>
> ```
> Failed to deploy a stack: compose build operation failed: listing workers for Build: failed to list workers: Unavailable: connection error: desc = "error reading server preface: http2: failed reading the frame payload: http2: frame too large, note that the frame header looked like an HTTP/1.1 header"
> ```
>
> BuildKit (used by `docker compose` by default) communicates with the daemon over HTTP/2. To bypass the proxy limitation, temporarily disable BuildKit for the build command:
>
> ```bash
> DOCKER_BUILDKIT=0 docker compose up --build taskboard
> ```
>
> The legacy builder falls back to HTTP/1.1 and succeeds in environments where BuildKit cannot establish its HTTP/2 connection. Note: the **Environment variables** form in Portainer only passes values to the Compose file itself, so setting `DOCKER_BUILDKIT` there will not disable BuildKit. For Portainer, use one of the approaches below.

### Running the Taskboard demo in Portainer

Below is a proven scenario for running the `examples/taskboard` demo stack on a host managed by Portainer.

1. Make sure your Docker host (where Portainer agent or standalone daemon runs) has internet access for git and pip.
2. In Portainer, go to **Stacks -> Add stack -> Web editor**.
3. Copy the contents of your updated `docker-compose.yml` (from this repository) into the editor. Make sure only the `taskboard` service is present in the YAML.
4. Click **Deploy the stack**. Portainer will pull the official `python:3.13-slim` image, install dependencies, clone the repository, and start the FastAPI app.

> All jobs and data are stored in memory. Restarting the container will reset the job history. For production, consider building and publishing your own image to a registry.

#### Local run (alternative)

```bash
docker compose up taskboard
```

#### Notes
- All jobs and demo data are stored in memory; restarting the container clears the history.
- For production, build and push your own image to a registry.
- If you need to access a private repository, use environment variables to pass a token or SSH key.

## Requirements
- Python 3.13+
- An asyncio-compatible event loop (the worker uses `asyncio.TaskGroup`)
- SQL backend users need SQLAlchemy 2.x plus an async driver for their database

## License
Pyjobkit is distributed under the MIT License. See [`LICENSE`](LICENSE) for the full text.
