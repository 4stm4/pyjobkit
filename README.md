# Jobkit

[![Version](https://img.shields.io/badge/version-0.1.0-blue.svg)](https://pypi.org/project/jobkit/)
[![Test Coverage](https://img.shields.io/badge/coverage-95%25-success.svg)](https://github.com/your-org/pyjobkit/actions)
[![Python Version](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/release/python-3130/)

Jobkit is a backend-agnostic toolkit for building reliable asynchronous job processing systems. It provides an `Engine` facade for enqueueing work, a cooperative asyncio `Worker`, a set of executor contracts, and pluggable queue backends so you can adapt the runtime to your infrastructure with minimal glue code.

## Features
- **Backend abstraction** – Use the production-ready SQL backend (`jobkit.backends.sql.SQLBackend`) or the lightweight in-memory backend for tests and demos. Both implement the same `QueueBackend` protocol exposed in `jobkit.contracts`.
- **Async worker loop** – `jobkit.worker.Worker` is built on `asyncio.TaskGroup`, supports concurrency limits, batch polling, lease extension, and jittered polling to minimize thundering herds.
- **Composable executors** – Executors implement a simple `run(job_id, payload, ctx)` coroutine. The package ships with an HTTP executor for calling webhooks and a subprocess executor for shell jobs, and you can add your own by conforming to the `Executor` protocol.
- **Observability hooks** – A memory log sink and an in-process event bus are included so executors can emit structured logs and progress updates without depending on a specific logging stack.
- **CLI worker** – The `jobkit-worker` console script wires together the SQL backend with built-in executors and exposes flags for DSN, concurrency, lease TTL, polling interval, and Postgres `SKIP LOCKED` tuning.

## Installation
```bash
pip install jobkit
# or install from source
pip install -e .
```

Optional extras bring in async database drivers for different SQL engines:

```bash
pip install "jobkit[pg]"      # asyncpg for PostgreSQL
pip install "jobkit[mysql]"   # aiomysql for MySQL
pip install "jobkit[sqlite]"  # aiosqlite for SQLite
```

## Getting started
The core building blocks are the `Engine`, a `QueueBackend`, at least one executor, and the `Worker` loop.

```python
import asyncio
from jobkit import Engine, Worker
from jobkit.backends.memory import MemoryBackend
from jobkit.contracts import ExecContext, Executor

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
from jobkit.backends.sql import SQLBackend

engine = create_async_engine("postgresql+asyncpg://user:pass@host/db")
backend = SQLBackend(engine, prefer_pg_skip_locked=True, lease_ttl_s=60)
```

## SQL schema and migrations
The SQL backend uses the `job_tasks` table defined in `jobkit.backends.sql.schema`. You can create it via SQLAlchemy:

```python
from sqlalchemy.ext.asyncio import create_async_engine
from jobkit.backends.sql.schema import metadata

engine = create_async_engine("postgresql+asyncpg://user:pass@host/db")
async with engine.begin() as conn:
    await conn.run_sync(metadata.create_all)
```

Manage migrations however you prefer (Alembic, plain SQL files, etc.)—only the schema in `schema.py` is required by Jobkit.

## Running the bundled worker CLI
Once the schema exists, you can run the provided worker process:

```bash
jobkit-worker --dsn postgresql+asyncpg://user:pass@host/db \
    --concurrency 8 \
    --batch 4 \
    --lease-ttl 30 \
    --poll-interval 0.5
```

Use `--disable-skip-locked` when targeting databases that do not support the Postgres-specific optimization. The CLI wires in the SQL backend plus the HTTP and subprocess executors, so jobs with `kind="http"` or `kind="subprocess"` will run out of the box.

## Extending Jobkit
- **Custom executors** – Implement the `Executor` protocol, register instances when constructing the `Engine`, and leverage the `ExecContext` helpers (`log`, `set_progress`, `is_cancelled`).
- **Alternate backends** – Implement the `QueueBackend` protocol to target message brokers or proprietary queues while reusing the worker and executor layers.
- **Logging & events** – Swap the memory log sink or event bus with your own implementations (e.g., stream to Loki or publish over Redis) by passing them to the `Engine` constructor.

## Requirements
- Python 3.13+
- An asyncio-compatible event loop (the worker uses `asyncio.TaskGroup`)
- SQL backend users need SQLAlchemy 2.x plus an async driver for their database

## License
Jobkit is distributed under the MIT License. See [`LICENSE`](LICENSE) for the full text.
