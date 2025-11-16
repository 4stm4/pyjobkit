# Pyjobkit

[![Version](https://img.shields.io/badge/version-0.1.0-blue.svg)](https://pypi.org/project/pyjobkit/)
[![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/4stm4/99574c3e4a5b6f3890c375bde7e1f0cd/raw/coverage-summary.json)](https://github.com/4stm4/pyjobkit/actions)
[![Python Version](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/release/python-3130/)

Pyjobkit is a backend-agnostic toolkit for building reliable asynchronous job processing systems. It provides an `Engine` facade for enqueueing work, a cooperative asyncio `Worker`, a set of executor contracts, and pluggable queue backends so you can adapt the runtime to your infrastructure with minimal glue code.

## Features
- **Backend abstraction** – Use the production-ready SQL backend (`pyjobkit.backends.sql.SQLBackend`) or the lightweight in-memory backend for tests and demos. Both implement the same `QueueBackend` protocol exposed in `pyjobkit.contracts`.
- **Async worker loop** – `pyjobkit.worker.Worker` is built on `asyncio.TaskGroup`, supports concurrency limits, batch polling, lease extension, and jittered polling to minimize thundering herds.
- **Composable executors** – Executors implement a simple `run(job_id, payload, ctx)` coroutine. The package ships with an HTTP executor for calling webhooks and a subprocess executor for shell jobs, and you can add your own by conforming to the `Executor` protocol.
- **Observability hooks** – A memory log sink and an in-process event bus are included so executors can emit structured logs and progress updates without depending on a specific logging stack.
- **CLI worker** – The `pyjobkit` console script wires together the SQL backend with built-in executors and exposes flags for DSN, concurrency, lease TTL, polling interval, and Postgres `SKIP LOCKED` tuning.


## Installation
```bash
# install from source
pip install -e .
```

Optional extras for async database drivers (install manually):

```bash
pip install asyncpg      # for PostgreSQL
pip install aiomysql     # for MySQL
pip install aiosqlite    # for SQLite
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
The SQL backend uses the `job_tasks` table defined in `pyjobkit.backends.sql.schema`. You can create it via SQLAlchemy:

```python
from sqlalchemy.ext.asyncio import create_async_engine
from pyjobkit.backends.sql.schema import metadata

engine = create_async_engine("postgresql+asyncpg://user:pass@host/db")
async with engine.begin() as conn:
    await conn.run_sync(metadata.create_all)
```

Manage migrations however you prefer (Alembic, plain SQL files, etc.)—only the schema in `schema.py` is required by Pyjobkit.

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

## Extending Pyjobkit
- **Custom executors** – Implement the `Executor` protocol, register instances when constructing the `Engine`, and leverage the `ExecContext` helpers (`log`, `set_progress`, `is_cancelled`).
- **Alternate backends** – Implement the `QueueBackend` protocol to target message brokers or proprietary queues while reusing the worker and executor layers.
- **Logging & events** – Swap the memory log sink or event bus with your own implementations (e.g., stream to Loki or publish over Redis) by passing them to the `Engine` constructor.



## Examples
- [`examples/taskboard`](examples/taskboard) – A single-page FastAPI dashboard that enqueues demo jobs which sleep for a random duration using the in-memory backend. Includes a Dockerfile for quick demos.

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
2. In Portainer, go to **Stacks → Add stack → Web editor**.
3. Copy the contents of your updated `docker-compose.yml` (from this repository) into the editor. Make sure only the `taskboard` service is present in the YAML.
4. Click **Deploy the stack**. Portainer will pull the official `python:3.13-slim` image, install dependencies, clone the repository, and start the FastAPI app.

> 💡 All jobs and data are stored in memory. Restarting the container will reset the job history. For production, consider building and publishing your own image to a registry.

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
