"""Coverage for FastAPI router endpoints not exercised elsewhere."""

from __future__ import annotations

import asyncio
import importlib.util

import pytest

from pyjobkit import Engine, MemoryBackend
from pyjobkit.contracts import Executor


_FASTAPI = (
    importlib.util.find_spec("fastapi") is not None
    and importlib.util.find_spec("pydantic") is not None
)

pytestmark = pytest.mark.skipif(not _FASTAPI, reason="fastapi/pydantic not installed")


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id, payload, ctx):
        return {}


def _make_client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from pyjobkit.integrations.fastapi import make_router

    backend = MemoryBackend()
    engine = Engine(backend=backend, executors=[_Noop()])
    app = FastAPI()
    app.include_router(make_router(engine), prefix="/api/v1")
    return TestClient(app), engine, backend


def test_list_jobs_endpoint_filters_and_limits() -> None:
    client, engine, backend = _make_client()

    async def seed():
        for _ in range(3):
            await engine.enqueue(kind="noop", payload={})
        success_id = await engine.enqueue(kind="noop", payload={})
        await backend.succeed(success_id, {"ok": True})

    asyncio.run(seed())

    with client:
        r = client.get("/api/v1/jobs")
        assert r.status_code == 200
        body = r.json()
        assert len(body) == 4

        r = client.get("/api/v1/jobs?status=success")
        assert r.status_code == 200
        assert {j["status"] for j in r.json()} == {"success"}

        r = client.get("/api/v1/jobs?limit=2")
        assert len(r.json()) == 2


def test_list_jobs_returns_501_when_backend_lacks_all_jobs() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from pyjobkit.integrations.fastapi import make_router
    from pyjobkit.contracts import QueueBackend

    class _NoListBackend(QueueBackend):
        async def enqueue(self, **kwargs):
            return None

        async def get(self, job_id):
            raise KeyError

        async def cancel(self, job_id):
            pass

        async def is_cancelled(self, job_id):
            return False

        async def claim_batch(self, worker_id, *, limit=1):
            return []

        async def mark_running(self, job_id, worker_id):
            pass

        async def extend_lease(self, job_id, worker_id, ttl_s, *, expected_version=None):
            pass

        async def succeed(self, job_id, result, *, expected_version=None):
            pass

        async def fail(self, job_id, reason, *, expected_version=None):
            pass

        async def timeout(self, job_id, *, expected_version=None):
            pass

        async def retry(self, job_id, *, delay):
            pass

        async def reap_expired(self):
            return 0

        async def queue_depth(self):
            return 0

        async def check_connection(self):
            pass

    engine = Engine(backend=_NoListBackend(), executors=[_Noop()])
    app = FastAPI()
    app.include_router(make_router(engine), prefix="/api/v1")
    with TestClient(app) as client:
        assert client.get("/api/v1/jobs").status_code == 501


def test_list_jobs_falls_back_when_backend_signature_is_old() -> None:
    """Backends predating the status/limit kwargs should still work."""

    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from pyjobkit.integrations.fastapi import make_router

    backend = MemoryBackend()

    async def all_jobs_no_kwargs():
        return await MemoryBackend.all_jobs(backend)

    # Replace with a no-kwarg version so the TypeError fallback fires.
    backend.all_jobs = all_jobs_no_kwargs  # type: ignore[method-assign]

    engine = Engine(backend=backend, executors=[_Noop()])

    async def seed():
        await engine.enqueue(kind="noop", payload={})

    asyncio.run(seed())

    app = FastAPI()
    app.include_router(make_router(engine), prefix="/api/v1")
    with TestClient(app) as client:
        r = client.get("/api/v1/jobs?status=queued&limit=10")
        assert r.status_code == 200
        assert len(r.json()) == 1


def test_get_job_returns_404_for_missing() -> None:
    from uuid import uuid4

    client, _engine, _backend = _make_client()
    with client:
        assert client.get(f"/api/v1/jobs/{uuid4()}").status_code == 404


def test_healthz_returns_503_when_backend_is_down() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from pyjobkit.integrations.fastapi import make_router

    backend = MemoryBackend()

    async def boom():
        raise RuntimeError("db gone")

    backend.check_connection = boom  # type: ignore[method-assign]
    engine = Engine(backend=backend, executors=[_Noop()])
    app = FastAPI()
    app.include_router(make_router(engine), prefix="/api/v1")
    with TestClient(app) as client:
        assert client.get("/api/v1/healthz").status_code == 503
