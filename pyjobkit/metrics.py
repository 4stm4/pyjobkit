"""Lightweight optional metrics instrumentation.

The module exposes Prometheus-compatible counters and histograms when the
``prometheus_client`` dependency is available. When it is not installed the
objects gracefully degrade to no-op stubs so callers can instrument codepaths
without taking a hard dependency.
"""

from __future__ import annotations

from typing import Any

try:  # pragma: no cover - exercised only when prometheus_client is installed
    from prometheus_client import Counter, Histogram
except Exception:  # pragma: no cover - defensive import guard

    class _NoOpMetric:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def inc(self, *args: Any, **kwargs: Any) -> None:
            return None

        def observe(self, *args: Any, **kwargs: Any) -> None:
            return None

    Counter = Histogram = _NoOpMetric  # type: ignore[misc]


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

