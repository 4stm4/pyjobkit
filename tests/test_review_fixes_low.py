"""Regression tests for the low-severity code-review fixes."""

from __future__ import annotations

import asyncio
import importlib.util
import time
from uuid import UUID, uuid4

import httpx
import pytest

from pyjobkit import Engine, MemoryBackend
from pyjobkit.contracts import Executor
from pyjobkit.scheduler import Scheduler
from pyjobkit.webhooks import fire, verify_signature


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id, payload, ctx):
        return {}


# #17 run_immediately fires on the first tick ------------------------------


def test_scheduler_run_immediately_fires_on_first_tick() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])

        fake = {"now": 1000.0}
        sched = Scheduler(engine, clock=lambda: fake["now"])
        sched.every("60s", name="warmup", kind="noop", run_immediately=True)

        # First tick at t = 1000 must enqueue without waiting for the
        # 60-second interval.
        assert await sched.tick() == 1
        assert await backend.count() == 1

    asyncio.run(_run())


def test_scheduler_default_waits_for_interval() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])

        fake = {"now": 1000.0}
        sched = Scheduler(engine, clock=lambda: fake["now"])
        sched.every("60s", name="warmup", kind="noop")

        assert await sched.tick() == 0
        fake["now"] = 1061.0
        assert await sched.tick() == 1

    asyncio.run(_run())


# #16 utcnow removed -------------------------------------------------------


def test_scheduler_module_no_longer_exports_utcnow() -> None:
    from pyjobkit import scheduler

    assert not hasattr(scheduler, "utcnow")


# #20 webhook timestamp + verify_signature ---------------------------------


class _CapturingTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(200)


def test_webhook_emits_timestamp_header_and_verify_round_trip() -> None:
    async def _run() -> None:
        transport = _CapturingTransport()
        client = httpx.AsyncClient(transport=transport)
        await fire(
            webhooks={"complete": "https://example.com"},
            status="success",
            job_id=UUID("00000000-0000-0000-0000-000000000001"),
            kind="x",
            attempts=1,
            duration_ms=1.0,
            result=None,
            client=client,
            secret="topsecret",
        )
        await client.aclose()
        req = transport.requests[0]
        ts = req.headers["X-Pyjobkit-Timestamp"]
        sig = req.headers["X-Pyjobkit-Signature"]
        assert int(ts) <= int(time.time())

        assert verify_signature(
            body=req.content,
            secret="topsecret",
            signature_header=sig,
            timestamp_header=ts,
        )

        # Wrong secret must fail.
        assert not verify_signature(
            body=req.content,
            secret="other",
            signature_header=sig,
            timestamp_header=ts,
        )

        # Timestamp outside the replay window must fail.
        assert not verify_signature(
            body=req.content,
            secret="topsecret",
            signature_header=sig,
            timestamp_header=ts,
            now=int(ts) + 3600,
            replay_window_s=60,
        )

        # Tampered body must fail.
        assert not verify_signature(
            body=req.content + b"x",
            secret="topsecret",
            signature_header=sig,
            timestamp_header=ts,
        )

    asyncio.run(_run())


def test_verify_signature_rejects_malformed_input() -> None:
    assert not verify_signature(
        body=b"hi", secret="s", signature_header=None, timestamp_header="123"
    )
    assert not verify_signature(
        body=b"hi", secret="s", signature_header="sha256=zz", timestamp_header="abc"
    )
    assert not verify_signature(
        body=b"hi", secret="s", signature_header="md5=zz", timestamp_header="123"
    )


# #22 cancel_job returns 404 on missing job --------------------------------


_FASTAPI_AVAILABLE = (
    importlib.util.find_spec("fastapi") is not None
    and importlib.util.find_spec("pydantic") is not None
)


@pytest.mark.skipif(not _FASTAPI_AVAILABLE, reason="fastapi/pydantic not installed")
def test_rest_cancel_returns_404_for_unknown_job() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from pyjobkit.integrations.fastapi import make_router

    engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
    app = FastAPI()
    app.include_router(make_router(engine), prefix="/api/v1")

    with TestClient(app) as client:
        r = client.post(f"/api/v1/jobs/{uuid4()}/cancel")
        assert r.status_code == 404


@pytest.mark.skipif(not _FASTAPI_AVAILABLE, reason="fastapi/pydantic not installed")
def test_rest_cancel_returns_202_for_known_job() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from pyjobkit.integrations.fastapi import make_router

    backend = MemoryBackend()
    engine = Engine(backend=backend, executors=[_Noop()])

    async def _seed() -> UUID:
        return await engine.enqueue(kind="noop", payload={})

    jid = asyncio.run(_seed())
    app = FastAPI()
    app.include_router(make_router(engine), prefix="/api/v1")

    with TestClient(app) as client:
        r = client.post(f"/api/v1/jobs/{jid}/cancel")
        assert r.status_code == 202
