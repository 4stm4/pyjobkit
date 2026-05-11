"""Tests for the bundled HTML dashboard (#55, #71)."""

from __future__ import annotations

import importlib.util

import pytest

from pyjobkit.integrations.ui import DASHBOARD_HTML

_FASTAPI_AVAILABLE = (
    importlib.util.find_spec("fastapi") is not None
    and importlib.util.find_spec("pydantic") is not None
)


def test_dashboard_html_contains_expected_anchors() -> None:
    assert "<title>Pyjobkit dashboard</title>" in DASHBOARD_HTML
    assert "__API_PREFIX__" in DASHBOARD_HTML  # template placeholder
    for s in ("queued", "running", "success", "failed", "timeout", "cancelled"):
        assert s in DASHBOARD_HTML


@pytest.mark.skipif(not _FASTAPI_AVAILABLE, reason="fastapi/pydantic not installed")
def test_mount_dashboard_serves_html() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from pyjobkit.integrations.fastapi import make_router
    from pyjobkit.integrations.ui import mount_dashboard
    from pyjobkit import Engine, MemoryBackend

    engine = Engine(backend=MemoryBackend(), executors=[])
    app = FastAPI()
    app.include_router(make_router(engine), prefix="/api/v1")
    mount_dashboard(app, api_prefix="/api/v1", ui_path="/ui")

    with TestClient(app) as client:
        r = client.get("/ui")
        assert r.status_code == 200
        assert "Pyjobkit dashboard" in r.text
        # Placeholder must be substituted by mount_dashboard.
        assert "__API_PREFIX__" not in r.text
        assert "/api/v1" in r.text


def test_ts_client_files_present() -> None:
    from pathlib import Path

    repo = Path(__file__).resolve().parent.parent
    assert (repo / "ts" / "pyjobkit.d.ts").is_file()
    assert (repo / "ts" / "pyjobkit.js").is_file()
