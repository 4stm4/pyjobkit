# Public types

The library ships a `py.typed` marker (PEP 561) and re-exports the
`TypedDict`s / `Literal`s that cross the API boundary. Import them
straight from the package root:

```python
from pyjobkit import (
    JobRecord,
    JobResult,
    FailureReason,
    JobStatus,
    LogStream,
)
```

The shapes are intentionally permissive (`total=False`) so backends
may include extra implementation-specific keys without breaking
typed callers.

## JobStatus

```python
from typing import Literal, get_args

JobStatus = Literal[
    "queued",
    "running",
    "success",
    "failed",
    "cancelled",
    "timeout",
]

assert set(get_args(JobStatus)) == {
    "queued", "running", "success", "failed", "cancelled", "timeout"
}
```

Use it on dashboards, switch statements, or anywhere you reason
about transitions.

```python
def alert_color(status: JobStatus) -> str:
    return {
        "success": "green",
        "queued": "blue",
        "running": "blue",
    }.get(status, "red")
```

## LogStream

```python
LogStream = Literal["stdout", "stderr"]
```

Accepted by `ExecContext.log(message, stream=...)`.

## JobRecord

The shape returned by `Engine.get` / `QueueBackend.get` /
`Backend.all_jobs`. Every field is optional - individual backends
may omit fields that are not relevant to their storage model.

```python
class JobRecord(TypedDict, total=False):
    id: UUID | str
    kind: str
    status: JobStatus
    payload: dict[str, Any]
    result: dict[str, Any] | None
    priority: int
    attempts: int
    max_attempts: int
    timeout_s: int | None
    scheduled_for: datetime | None
    created_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    lease_until: datetime | None
    leased_by: UUID | None
    version: int | None
    idempotency_key: str | None
    cancel_requested: bool
```

The REST integration's `JobRecordResponse` mirrors this shape; the
FastAPI router strips internal `__pjk_*` markers from `payload`
before returning it.

## JobResult

The structured value an executor returns. All keys are optional
(`total=False`); pyjobkit does not interpret the contents, it just
persists them in the backend's `result` column.

```python
class JobResult(TypedDict, total=False):
    ok: bool
    error: str
    data: Any
```

You are free to ignore this type and return any JSON-serialisable
dict; the only thing that has to be true is that the dict is
serialisable by the backend's storage layer (SQLAlchemy's `JSON` /
`JSONB` types for the SQL backend, Python `json.dumps` for the
Redis backend).

## FailureReason

Structured payload accepted by `Engine.fail`. The worker uses it
internally when an executor raises; you can use it when failing a
job by hand:

```python
class FailureReason(TypedDict, total=False):
    error: str
    exception_type: str
    message: str
    details: dict[str, Any]
```

```python
await engine.fail(
    job_id,
    {
        "error": "downstream_429",
        "message": "rate-limited by SendGrid",
        "details": {"retry_after": 60},
    },
)
```

`error` is the conventional short identifier
(`"timeout"`, `"cancelled"`, `"exception"`, ...). The worker fills
in `exception_type` and `message` automatically when an unhandled
exception propagates from the executor.

## Other public names

The full re-exported surface lives in
[`pyjobkit/__init__.py`](https://github.com/4stm4/pyjobkit/blob/main/pyjobkit/__init__.py):

| Symbol | Module | Purpose |
| --- | --- | --- |
| `Engine`, `Worker` | `pyjobkit` | Producer + consumer facades |
| `MemoryBackend` | `pyjobkit.backends` | Tests / debug backend |
| `Executor`, `ExecContext`, `QueueBackend` | `pyjobkit.contracts` | ABCs |
| `Config`, `ConfigError`, `load_config` | `pyjobkit.config` | TOML / env config loader |
| `RetryPolicy`, `FixedDelay`, `ExponentialBackoff`, `JitteredExponentialBackoff`, `parse_policy` | `pyjobkit.retry` | Retry policies |

Anything imported via dotted paths under `pyjobkit.*` that is **not**
re-exported from the package root is internal and may change in any
release.
