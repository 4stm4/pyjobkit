"""Tests for the FastAPI integration (#58, #64)."""

from __future__ import annotations

import importlib.util
import sys

import pytest

from pyjobkit import MemoryBackend
from pyjobkit.engine import Engine
from pyjobkit.integrations.fastapi import (
    FastAPIDependencyMissing,
    make_router,
)


_FASTAPI_AVAILABLE = (
    importlib.util.find_spec("fastapi") is not None
    and importlib.util.find_spec("pydantic") is not None
)


def test_make_router_raises_when_fastapi_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "fastapi", None)  # type: ignore[arg-type]
    engine = Engine(backend=MemoryBackend(), executors=[])
    with pytest.raises(FastAPIDependencyMissing):
        make_router(engine)


@pytest.mark.skipif(not _FASTAPI_AVAILABLE, reason="fastapi/pydantic not installed")
def test_router_endpoints_round_trip() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from pyjobkit.contracts import Executor

    class _Echo(Executor):
        kind = "echo"

        async def run(self, *, job_id, payload, ctx):  # pragma: no cover
            return {"echo": payload}

    backend = MemoryBackend()
    engine = Engine(backend=backend, executors=[_Echo()])
    app = FastAPI()
    app.include_router(make_router(engine), prefix="/api/v1")

    with TestClient(app) as client:
        r = client.post("/api/v1/jobs", json={"kind": "echo", "payload": {"x": 1}})
        assert r.status_code == 202
        jid = r.json()["job_id"]

        r = client.get(f"/api/v1/jobs/{jid}")
        assert r.status_code == 200
        assert r.json()["status"] == "queued"

        r = client.post(f"/api/v1/jobs/{jid}/cancel")
        assert r.status_code == 202

        r = client.get("/api/v1/healthz")
        assert r.status_code == 200 and r.json() == {"status": "ok"}
