"""Tests for metrics fallbacks when Prometheus client is unavailable."""

from __future__ import annotations

import importlib
import sys


def test_metrics_noop_metrics_expose_expected_api(monkeypatch) -> None:
    """Ensure metrics gracefully degrade to no-op implementations.

    The test reimports :mod:`pyjobkit.metrics` while masking the
    optional ``prometheus_client`` dependency, then restores the real
    module so other tests that rely on it (the OpenTelemetry roundtrip,
    the FastAPI dashboard) keep working.
    """

    # Mask prometheus_client so the fallback branch is exercised.
    for name in ("prometheus_client", "prometheus_client.core"):
        monkeypatch.setitem(sys.modules, name, None)

    original = sys.modules.pop("pyjobkit.metrics", None)
    try:
        module = importlib.import_module("pyjobkit.metrics")

        for metric in (
            module.extend_lease_conflicts,
            module.extend_lease_latency,
            module.lease_ttl_seconds,
        ):
            assert metric.__class__.__name__ == "_NoOpMetric"
            assert metric.inc() is None
            assert metric.observe(1.23) is None
    finally:
        sys.modules.pop("pyjobkit.metrics", None)
        if original is not None:
            sys.modules["pyjobkit.metrics"] = original
