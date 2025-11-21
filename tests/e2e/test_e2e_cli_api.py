"""End-to-end check: enqueue a job, process it with the worker, and read the result."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any

import httpx
import pytest

from pyjobkit.backends.memory import MemoryBackend
from pyjobkit.engine import Engine
from pyjobkit.executors.http import HttpExecutor
from pyjobkit.worker import Worker


class _FakeClient:
    def __init__(self, *_, **__) -> None:
        self.requests: list[tuple[str, str]] = []

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *exc) -> None:
        return None

    async def request(self, method: str, url: str, **_: Any) -> httpx.Response:
        self.requests.append((method, url))
        return httpx.Response(
            status_code=200,
            json={"ok": True},
            headers={"content-type": "application/json"},
            request=httpx.Request(method, url),
        )


@pytest.mark.e2e
def test_enqueue_and_fetch_result(monkeypatch) -> None:
    async def _run() -> None:
        monkeypatch.setattr("pyjobkit.executors.http.httpx.AsyncClient", _FakeClient)

        backend = MemoryBackend(lease_ttl_s=1)
        engine = Engine(backend=backend, executors=[HttpExecutor()])
        worker = Worker(
            engine,
            max_concurrency=1,
            batch=1,
            poll_interval=0.05,
            lease_ttl=backend.lease_ttl_s,
        )

        job_id = await engine.enqueue(kind="http", payload={"url": "https://example"})

        worker_task = asyncio.create_task(worker.run())
        try:
            for _ in range(100):
                row = await engine.get(job_id)
                if row["status"] == "success":
                    break
                await asyncio.sleep(0.05)
            else:  # pragma: no cover - defensive
                pytest.fail("job not processed by worker")
        finally:
            worker.request_stop()
            await worker.wait_stopped()
            worker_task.cancel()
            with suppress(asyncio.CancelledError):
                await worker_task

        result = row["result"]
        assert result["status"] == 200
        assert result["body"] == {"ok": True}

        logs = await engine.log_sink.get(job_id)
        assert any("https://example" in entry.message for entry in logs)

    asyncio.run(_run())
