"""Lightweight optional metrics instrumentation.

The module exposes Prometheus-compatible counters and histograms when the
``prometheus_client`` dependency is available. When it is not installed the
objects gracefully degrade to no-op stubs so callers can instrument codepaths
without taking a hard dependency.

A small helper :func:`start_metrics_server` boots a background HTTP server
that exposes the Prometheus text format at ``/metrics`` (issue #44).
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

try:  # pragma: no cover - exercised only when prometheus_client is installed
    from prometheus_client import Counter, Histogram, start_http_server

    _PROMETHEUS_AVAILABLE = True
except Exception:  # pragma: no cover - defensive import guard

    class _NoOpMetric:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def inc(self, *args: Any, **kwargs: Any) -> None:
            return None

        def observe(self, *args: Any, **kwargs: Any) -> None:
            return None

    Counter = Histogram = _NoOpMetric  # type: ignore[misc]
    start_http_server = None  # type: ignore[assignment]
    _PROMETHEUS_AVAILABLE = False


def start_metrics_server(port: int = 9000, host: str = "0.0.0.0") -> bool:
    """Start a Prometheus exporter HTTP server in the background.

    Returns ``True`` when the server was started, ``False`` when the
    ``prometheus_client`` package is not installed. The server runs in a
    daemon thread and shuts down with the process.
    """

    if not _PROMETHEUS_AVAILABLE:
        logger.warning(
            "prometheus_client not installed; cannot start /metrics server"
        )
        return False
    start_http_server(port, addr=host)
    logger.info("Prometheus /metrics server listening on %s:%d", host, port)
    return True


extend_lease_conflicts = Counter(
    "pyjobkit_extend_lease_conflicts_total",
    "Count of lease extensions that failed due to optimistic lock conflicts.",
)

extend_lease_latency = Histogram(
    "pyjobkit_extend_lease_latency_seconds",
    "Latency distribution for lease extension operations.",
)

lease_ttl_seconds = Histogram(
    "pyjobkit_lease_ttl_seconds",
    "Observed TTL values requested when extending leases.",
)


phase_duration_seconds = Histogram(
    "pyjobkit_phase_duration_seconds",
    "Duration of executor phases instrumented via ctx.profile_phase().",
)

webhook_failures = Counter(
    "pyjobkit_webhook_failures_total",
    "Count of webhook deliveries that failed (per attempt, before retry).",
)

