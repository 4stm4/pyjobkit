# OpenTelemetry tracing

Pyjobkit emits OpenTelemetry spans around `Engine.enqueue` and
`Worker._execute_row` when the `opentelemetry-api` package is
installed. When OTel is missing the helpers degrade to no-ops, so
the rest of the library never has to guard the import.

## Install

```bash
pip install "pyjobkit[otel]"
```

…or pull the bits you need yourself (`opentelemetry-api` is the
runtime requirement; `opentelemetry-sdk` plus an exporter pair the
producer with a real backend).

## Wire up an exporter

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter,
)

provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(provider)
```

After that every `engine.enqueue` runs inside a `pyjobkit.enqueue`
span and every worker dispatch runs inside `pyjobkit.execute`.

## Trace context propagation

Pyjobkit propagates the W3C `traceparent` (and optional
`tracestate`) through the job payload itself. The flow:

1. Producer code calls `engine.enqueue` from inside its own span.
2. `pyjobkit.tracing.inject_trace_context(payload)` snapshots the
   current span's context into the payload as
   `__pjk_trace_context: {traceparent, tracestate}`.
3. Worker reads the marker via `restore_trace_context(payload)`
   when handling the job, so its `pyjobkit.execute` span is
   re-parented to the producer's trace.

The trace-context marker is one of the
`INTERNAL_PAYLOAD_KEYS`, so it never reaches the executor's
`payload` and is stripped before the payload is rendered into a
REST response.

```python
from opentelemetry import trace

tracer = trace.get_tracer("myapp")
with tracer.start_as_current_span("submit_email"):
    await engine.enqueue(kind="email", payload={"to": "..."})
# Worker's pyjobkit.execute span becomes a child of "submit_email".
```

## Programmatic span helpers

```python
from pyjobkit.tracing import span

with span("myapp.do-work", tenant="acme"):
    ...
```

`span(name, **attributes)` is a context manager that:

- yields a real `Span` when OTel is installed,
- yields `None` (no-op) when OTel is missing.

Attributes are forwarded via `Span.set_attribute`; `None` values are
ignored.

## Verifying it works

```python
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

exporter = InMemorySpanExporter()
provider = TracerProvider()
provider.add_span_processor(SimpleSpanProcessor(exporter))
trace.set_tracer_provider(provider)

# ... run a workflow ...

names = [s.name for s in exporter.get_finished_spans()]
assert "pyjobkit.enqueue" in names
assert "pyjobkit.execute" in names
```

`tests/test_tracing.py` runs exactly this pattern.

## What is **not** traced today

- Backend operations (`claim_batch`, `succeed`, `retry`, etc.) are
  not wrapped in their own spans. The SQLAlchemy instrumentation
  package handles this for the SQL backend if you install
  `opentelemetry-instrumentation-sqlalchemy`.
- `ctx.profile_phase` blocks log structured events with
  `duration_ms` but do **not** start child spans. The Prometheus
  histogram is the canonical signal for those.
