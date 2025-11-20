"""Tests for metrics fallbacks when Prometheus client is unavailable."""

from __future__ import annotations

import importlib
import sys


def test_metrics_noop_metrics_expose_expected_api(monkeypatch) -> None:
    """Ensure metrics gracefully degrade to no-op implementations."""

    # Force a clean import so the fallback path is exercised regardless of
    # previous imports during the test session.
    sys.modules.pop("pyjobkit.metrics", None)
    module = importlib.import_module("pyjobkit.metrics")

    for metric in (
        module.extend_lease_conflicts,
        module.extend_lease_latency,
        module.lease_ttl_seconds,
    ):
        assert metric.__class__.__name__ == "_NoOpMetric"
        assert metric.inc() is None
        assert metric.observe(1.23) is None
