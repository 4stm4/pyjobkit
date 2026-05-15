"""Optional OpenTelemetry tracing hooks.

When the ``opentelemetry-api`` package is installed, the helpers in
this module emit spans around the high-traffic codepaths
(``Engine.enqueue``, ``Worker._execute_row``, ``Executor.run``) and
propagate W3C trace context across the enqueue/run boundary via a
payload marker (so spans on the worker side can re-parent themselves
to the producer's trace).

When OpenTelemetry is not installed the helpers degrade to no-ops, so
the rest of the library never has to guard the import.
"""

from __future__ import annotations

import contextlib
import logging
from typing import Any, Iterator

logger = logging.getLogger(__name__)

TRACE_CONTEXT_PAYLOAD_KEY = "__pjk_trace_context"
"""Payload key carrying the W3C ``traceparent`` (and optional ``tracestate``)."""

try:  # pragma: no cover - exercised only when opentelemetry is installed
    from opentelemetry import context as _otel_context
    from opentelemetry import propagate as _otel_propagate
    from opentelemetry import trace as _otel_trace

    _OTEL_AVAILABLE = True
except Exception:  # pragma: no cover - defensive import guard
    _otel_trace = None  # type: ignore[assignment]
    _otel_context = None  # type: ignore[assignment]
    _otel_propagate = None  # type: ignore[assignment]
    _OTEL_AVAILABLE = False


def _tracer():  # type: ignore[no-untyped-def]
    if not _OTEL_AVAILABLE:  # pragma: no cover - guarded by callers
        return None
    return _otel_trace.get_tracer("pyjobkit")


def inject_trace_context(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a payload copy with the current span's W3C context attached.

    No-op when OpenTelemetry is not installed.
    """

    if not _OTEL_AVAILABLE:
        return payload
    carrier: dict[str, str] = {}
    _otel_propagate.inject(carrier)
    if not carrier:
        return payload
    return {**payload, TRACE_CONTEXT_PAYLOAD_KEY: carrier}


@contextlib.contextmanager
def restore_trace_context(payload: dict[str, Any]) -> Iterator[None]:
    """Activate the parent context stored in ``payload`` (if any)."""

    if not _OTEL_AVAILABLE:
        yield
        return
    carrier = payload.get(TRACE_CONTEXT_PAYLOAD_KEY)
    if not carrier:
        yield
        return
    ctx = _otel_propagate.extract(carrier)
    token = _otel_context.attach(ctx)
    try:
        yield
    finally:
        _otel_context.detach(token)


@contextlib.contextmanager
def span(name: str, **attributes: Any) -> Iterator[Any]:
    """Yield a span named ``name`` (or a no-op when OTel is missing)."""

    if not _OTEL_AVAILABLE:
        yield None
        return
    tracer = _tracer()
    if tracer is None:  # pragma: no cover - defensive
        yield None
        return
    with tracer.start_as_current_span(name) as sp:
        for key, value in attributes.items():
            if value is None:
                continue
            try:
                sp.set_attribute(key, value)
            except Exception:  # pragma: no cover - defensive
                pass
        yield sp


def strip_trace_context(payload: dict[str, Any]) -> dict[str, Any]:
    """Return ``payload`` without the trace-context marker."""

    if TRACE_CONTEXT_PAYLOAD_KEY not in payload:
        return payload
    return {k: v for k, v in payload.items() if k != TRACE_CONTEXT_PAYLOAD_KEY}
