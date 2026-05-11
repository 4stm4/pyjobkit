# API reference

The package re-exports the most commonly used types from
`pyjobkit/__init__.py`:

```python
from pyjobkit import (
    Engine,
    Worker,
    MemoryBackend,
    Executor,
    ExecContext,
    QueueBackend,
    Config,
    ConfigError,
    load_config,
    # Types
    JobRecord,
    JobResult,
    JobStatus,
    LogStream,
    FailureReason,
    # Retry policies
    RetryPolicy,
    FixedDelay,
    ExponentialBackoff,
    JitteredExponentialBackoff,
    parse_policy,
)
```

A standalone API reference site (`mkdocstrings`) can be wired into this
mkdocs project; for now, refer to the inline docstrings in the source
tree. They are the canonical reference and are honored by type checkers
thanks to the `py.typed` marker.

## Top-level helpers

| Symbol | Purpose |
|---|---|
| `Engine.enqueue` | Persist a new job |
| `Engine.enqueue_at` / `enqueue_in` | Schedule a job for the future |
| `Engine.get` | Fetch a `JobRecord` |
| `Engine.cancel` | Request cancellation |
| `Engine.register_plugins` | Load executors from entry points |
| `Worker.run` / `Worker.run(once=True)` | Worker main loop / drain |
| `MemoryBackend.count / all_jobs / clear` | Test helpers |
| `ctx.profile_phase` | Measure executor sub-phases |

## Stability

The library is at `0.x`; public symbols are intentionally re-exported so
that breaking changes show up as `__init__.py` deletions on every PR.
Versioned release notes live in [CHANGELOG.md](https://github.com/4stm4/pyjobkit/blob/main/CHANGELOG.md).
