# Getting started

## Install

```bash
pip install pyjobkit
# Optional driver extras
pip install "pyjobkit[pg]"      # asyncpg for Postgres
pip install "pyjobkit[sqlite]"  # aiosqlite for local prototyping
```

Python 3.13 or newer is required.

## A 30-second walkthrough

```python
import asyncio
from uuid import UUID

from pyjobkit import Engine, MemoryBackend, Worker
from pyjobkit.contracts import ExecContext, Executor


class Hello(Executor):
    kind = "hello"

    async def run(self, *, job_id: UUID, payload: dict, ctx: ExecContext) -> dict:
        await ctx.log(f"running with {payload}")
        return {"greeting": f"hi {payload['name']}"}


async def main() -> None:
    backend = MemoryBackend()
    engine = Engine(backend=backend, executors=[Hello()])
    worker = Worker(engine, poll_interval=0.05)

    job_id = await engine.enqueue(kind="hello", payload={"name": "world"})
    await worker.run(once=True)

    record = await backend.get(job_id)
    print(record["status"], record["result"])  # "success {'greeting': 'hi world'}"


asyncio.run(main())
```

`Worker.run(once=True)` drains the queue and exits, which is the easiest
way to play with Pyjobkit in a notebook or REPL. For a real deployment
you would target the SQL backend and let `pyjobkit` (the console script)
own the process lifecycle.

## Production-style worker

```bash
# Create the schema once
alembic -c pyjobkit/backends/sql/alembic.ini upgrade head

# Then run the worker
pyjobkit --dsn postgresql+asyncpg://user:pass@host/db \
         --concurrency 16 \
         --batch 4 \
         --lease-ttl 30 \
         --log-format json
```

Settings can come from `--flag`, `PYJOBKIT_*` environment variables, or
a `.pyjobkit.toml` file - see **Configuration** for the full surface.

## Next steps

- Configure the worker via TOML / env variables in **Configuration**.
- Add custom executors or backends in **Extending Pyjobkit**.
- Browse the public API in **API reference**.
