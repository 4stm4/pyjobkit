# Components

Per-component reference for the public surface of Pyjobkit. Pick
the page that matches what you are wiring up:

| Layer | Page |
| --- | --- |
| Producer-side facade | [Engine](engine.md) |
| Consumer-side daemon | [Worker](worker.md) |
| Storage drivers | [Backends](backends.md) |
| Executor protocol + built-ins | [Executors](executors.md) |
| Retry policies | [Retry](retry.md) |
| Periodic enqueueing | [Scheduler](scheduler.md) |
| HA leader election | [Leader](leader.md) |
| Webhook notifications | [Webhooks](webhooks.md) |
| Per-kind throttling | [Rate limiting](ratelimit.md) |
| Structured logs + events | [Logging](logging.md) |
| OpenTelemetry tracing | [Tracing](tracing.md) |
| Prometheus metrics | [Metrics](metrics.md) |
| Public TypedDicts / literals | [Types](types.md) |
| Console scripts | [CLI tools](cli.md) |
| FastAPI router + dashboard + TS client | [FastAPI](fastapi.md) |

Each page is self-contained and lists examples for the common
operations. The canonical reference is still the docstrings in
`pyjobkit/`, surfaced via the `py.typed` marker and visible to your
IDE / type checker.
