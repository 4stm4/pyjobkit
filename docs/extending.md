# Extending Pyjobkit

## Custom executors

Implement the `Executor` ABC:

```python
from uuid import UUID
from pyjobkit.contracts import ExecContext, Executor


class S3UploadExecutor(Executor):
    kind = "s3.upload"

    async def run(self, *, job_id: UUID, payload: dict, ctx: ExecContext) -> dict:
        async with ctx.profile_phase("download"):
            data = await fetch(payload["url"])
        async with ctx.profile_phase("upload"):
            await upload_to_s3(payload["bucket"], data)
        await ctx.set_progress(1.0)
        return {"size": len(data)}
```

Register it explicitly:

```python
engine = Engine(
    backend=backend,
    executors=[S3UploadExecutor()],
)
```

Or distribute it as a plugin via `pyproject.toml`:

```toml
[project.entry-points."pyjobkit.executors"]
s3 = "myplugin.executors:S3UploadExecutor"
```

…and have the worker auto-discover it with `--enable-plugins` /
`enable_plugins = true` in config.

## Custom backends

Implement the `QueueBackend` ABC (`pyjobkit/contracts.py`). The
`SQLBackend` and `MemoryBackend` source files are the recommended
reference implementations.

## Pluggable retry strategy

```python
from pyjobkit.retry import RetryPolicy

class DegradedFastBackoff(RetryPolicy):
    def delay(self, attempt: int) -> float:
        return 0.1 if attempt < 3 else 5.0

worker = Worker(engine, retry_policy=DegradedFastBackoff())
```

## Lease-loss callback

When a worker loses its lease (e.g. because the network partition
hid it from the database for longer than `lease_ttl`), the affected
task is aborted. Hook into that transition with:

```python
async def alert(job_id, row):
    await pager.notify(f"lost lease on {job_id} ({row['kind']})")

worker = Worker(engine, on_lease_lost=alert)
```

## Logging

Pyjobkit emits events with a small structured shape:

```text
{"ts": "...", "level": "INFO", "logger": "pyjobkit.worker",
 "msg": "job state changed", "event": "job.succeeded",
 "job_id": "...", "worker_id": "...", "status": "success",
 "duration_ms": 12.5}
```

Pipe the worker's stderr to your aggregator of choice. The formatter is
available as `pyjobkit.logging.JsonFormatter` for use in other
processes.
