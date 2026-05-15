"""Tests for the optional OpenTelemetry hooks."""

from __future__ import annotations

import asyncio
import importlib.util
from uuid import UUID

import pytest

from pyjobkit import Engine, MemoryBackend
from pyjobkit.contracts import Executor
from pyjobkit.tracing import (
    TRACE_CONTEXT_PAYLOAD_KEY,
    inject_trace_context,
    span,
    strip_trace_context,
)


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        return {}


def test_inject_is_noop_without_otel_or_outside_span() -> None:
    # Outside an active span (or without OTel installed) inject is a
    # no-op: the payload is returned unchanged.
    out = inject_trace_context({"x": 1})
    assert out == {"x": 1}


def test_strip_trace_context_drops_marker() -> None:
    payload = {"x": 1, TRACE_CONTEXT_PAYLOAD_KEY: {"traceparent": "...."}}
    assert strip_trace_context(payload) == {"x": 1}


def test_span_context_manager_is_safe_when_no_otel() -> None:
    with span("pyjobkit.test", kind="x") as sp:
        # sp is None when OTel is not installed; when it is installed
        # it is a real Span and the block runs to completion.
        pass


_OTEL_AVAILABLE = importlib.util.find_spec("opentelemetry") is not None


@pytest.mark.skipif(not _OTEL_AVAILABLE, reason="opentelemetry not installed")
def test_enqueue_injects_trace_context_when_inside_span() -> None:
    async def _run() -> None:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        provider = TracerProvider()
        exporter = InMemorySpanExporter()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        trace.set_tracer_provider(provider)

        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])

        tracer = trace.get_tracer("pyjobkit.test")
        with tracer.start_as_current_span("producer"):
            job_id = await engine.enqueue(kind="noop", payload={"x": 1})

        rec = await backend.get(job_id)
        assert TRACE_CONTEXT_PAYLOAD_KEY in rec["payload"]
        # And a pyjobkit.enqueue span was emitted.
        spans = [s.name for s in exporter.get_finished_spans()]
        assert "pyjobkit.enqueue" in spans

    asyncio.run(_run())
