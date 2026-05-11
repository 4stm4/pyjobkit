"""Tests for the Prometheus /metrics helper (#44)."""

from __future__ import annotations

import pytest

from pyjobkit import metrics


def test_start_metrics_server_returns_false_without_prometheus(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(metrics, "_PROMETHEUS_AVAILABLE", False)
    assert metrics.start_metrics_server(port=12345) is False


def test_start_metrics_server_invokes_prometheus_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, str]] = []

    def fake_start(port: int, addr: str = "0.0.0.0") -> None:
        calls.append((port, addr))

    monkeypatch.setattr(metrics, "_PROMETHEUS_AVAILABLE", True)
    monkeypatch.setattr(metrics, "start_http_server", fake_start)

    assert metrics.start_metrics_server(port=12345, host="127.0.0.1") is True
    assert calls == [(12345, "127.0.0.1")]
