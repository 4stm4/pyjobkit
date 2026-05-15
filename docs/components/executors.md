# Executors

An executor implements `pyjobkit.contracts.Executor`. Its `run`
coroutine is what actually does the work for a given `kind`. The
worker hands every claimed job to the matching executor along with an
`ExecContext` that exposes `log`, `set_progress`, `is_cancelled`, and
`profile_phase`.

```python
from uuid import UUID
from pyjobkit.contracts import ExecContext, Executor


class Hello(Executor):
    kind = "hello"

    async def run(self, *, job_id: UUID, payload: dict, ctx: ExecContext) -> dict:
        await ctx.log(f"running for {payload['name']}")
        if await ctx.is_cancelled():
            return {"cancelled": True}
        async with ctx.profile_phase("network"):
            data = await fetch(payload["url"])
        await ctx.set_progress(1.0)
        return {"size": len(data)}
```

`kind` must match `[A-Za-z0-9_.-]+`. The same value is what callers
pass to `Engine.enqueue(kind=...)`. Register executors at
`Engine` construction time:

```python
engine = Engine(backend=backend, executors=[Hello(), Another()])
```

`Engine.register_executor(executor)` adds one at runtime.
`Engine.register_plugins()` discovers them via entry points (see
below).

## Built-in executors

### SubprocessExecutor

Runs an OS-level subprocess.

```python
from pyjobkit.executors import SubprocessExecutor

executor = SubprocessExecutor(
    allowed_commands=["python3", "pdf2text", "ffmpeg"],   # strongly recommended
)
```

Payload schema:

| Key | Required | Description |
| --- | --- | --- |
| `cmd` | yes | List `argv` (preferred) or shell string |
| `env` | no | Dict of environment variables |
| `cwd` | no | Working directory |

Returns `{"returncode": int, "duration_ms": int}`. Stdout / stderr
are streamed line-by-line into `ctx.log` under `"stdout"` /
`"stderr"` streams. Non-zero exit codes are logged but **do not**
raise - the worker's retry policy only kicks in on a Python
exception. If you want non-zero exits to retry, raise from the
executor:

```python
class StrictSubprocess(SubprocessExecutor):
    async def run(self, **kw):
        result = await super().run(**kw)
        if result["returncode"] != 0:
            raise RuntimeError(f"exit {result['returncode']}")
        return result
```

#### Allowlist

`SubprocessExecutor()` without `allowed_commands` logs a one-shot
warning at first run; production deployments should always set the
list when the producer surface is exposed to untrusted callers. When
the allowlist is set:

- The payload `cmd` must be a list (shell strings are rejected with
  `PermissionError` because `bash -c '...'` would otherwise bypass the
  filter).
- The first element (`argv[0]`) must be in the allowlist.

### HttpExecutor

```python
from pyjobkit.executors import HttpExecutor

engine = Engine(backend=backend, executors=[HttpExecutor()])
```

Payload schema:

```python
{
    "method": "POST",
    "url": "https://example.com/api",
    "headers": {"Authorization": "Bearer ..."},
    "json": {"hello": "world"},
    "timeout": 15,                  # optional override
}
```

Returns `{"status_code": int, "body": <decoded body>}`. Non-2xx
responses raise `httpx.HTTPStatusError` and trigger retry per the
worker's policy.

### DockerExecutor

Optional. Install with `pyjobkit[docker]`.

```python
from pyjobkit.executors import DockerExecutor

engine = Engine(
    backend=backend,
    executors=[DockerExecutor(default_timeout_s=600)],
)
```

Payload:

```python
{
    "image": "alpine:3.20",
    "command": ["echo", "hello"],   # list or string
    "env": {"FOO": "bar"},
    "timeout_s": 120,
    "pull": True,
}
```

Returns `{"exit_code", "stdout", "stderr", "container_id"}`. Non-zero
exit codes raise `DockerExecutionError`. The container is always
deleted in the executor's `finally` block; on timeout it is killed
before deletion.

## Plugin discovery

Third-party packages can ship executors via the
`pyjobkit.executors` entry-point group. In your plugin's
`pyproject.toml`:

```toml
[project.entry-points."pyjobkit.executors"]
s3 = "myplugin.executors:make_s3_executor"
```

`make_s3_executor` is any zero-arg callable returning an `Executor`
instance.

```python
# Auto-discover everything.
new = engine.register_plugins()

# Or pick a subset by entry-point name.
new = engine.register_plugins(only=["s3"])
```

Already-registered `kind` values are skipped with a warning;
import / instantiation failures are logged and the loader continues.

CLI / Helm equivalent: pass `--enable-plugins` to the `pyjobkit`
worker, or set `enable_plugins = true` in `.pyjobkit.toml`.

## Batch executors

Some workloads benefit from processing many jobs of the same kind
together (one network round trip per batch instead of per row,
batched GPU inference, vectorised DB writes). The `BatchExecutor`
ABC plus `dispatch_batch` helper let you do that without subclassing
`Worker`:

```python
from pyjobkit.batch import BatchExecutor, BatchJob, dispatch_batch


class EmbedExecutor(BatchExecutor):
    kind = "embed"
    max_batch_size = 32

    async def run_batch(self, jobs):
        payloads = [j.payload for j in jobs]
        embeddings = await embedding_model.embed_many(payloads)
        return [{"embedding": e} for e in embeddings]


claimed = await backend.claim_batch(worker_id, limit=32)
await dispatch_batch(engine, EmbedExecutor(), claimed, worker_id=worker_id)
```

`dispatch_batch` calls `engine.mark_running` for each row, then hands
the whole list to `run_batch`. If `run_batch` succeeds, every job is
finalised with its corresponding result via
`engine.succeed(expected_version=...)`. If `run_batch` raises, every
job is failed with the exception's `repr()`. Per-row retry semantics
are not supported - that is what the regular `Worker` is for.
