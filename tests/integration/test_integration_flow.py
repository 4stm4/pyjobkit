"""Integration scenarios for the worker lifecycle with MemoryBackend."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from uuid import UUID, uuid4

import pytest

from pyjobkit.backends.memory import MemoryBackend
from pyjobkit.contracts import ExecContext, Executor
from pyjobkit.engine import Engine
from pyjobkit.worker import Worker


class _EchoExecutor(Executor):
    kind = "echo"

    def __init__(self) -> None:
        self.calls: list[UUID] = []

    async def run(self, *, job_id: UUID, payload: dict, ctx: ExecContext) -> dict:
        await ctx.log(f"echo {payload['value']}")
        self.calls.append(job_id)
        await asyncio.sleep(payload.get("sleep", 0))
        return {"echo": payload["value"]}


class _CountingBackend(MemoryBackend):
    def __init__(self, *, lease_ttl_s: int) -> None:
        super().__init__(lease_ttl_s=lease_ttl_s)
        self.extends: list[tuple[UUID, int]] = []

    async def extend_lease(
        self,
        job_id: UUID,
        worker_id: UUID,
        ttl_s: int,
        *,
        expected_version: int | None = None,
    ) -> None:
        self.extends.append((job_id, ttl_s))
        await super().extend_lease(
            job_id, worker_id, ttl_s, expected_version=expected_version
        )


POLL_INTERVAL = 0.05


@pytest.mark.integration
def test_enqueue_to_success_flow() -> None:
    async def _run() -> None:
        backend = MemoryBackend(lease_ttl_s=1)
        executor = _EchoExecutor()
        engine = Engine(backend=backend, executors=[executor])
        worker = Worker(
            engine,
            max_concurrency=1,
            batch=1,
            poll_interval=POLL_INTERVAL,
            lease_ttl=backend.lease_ttl_s,
        )

        job_id = await engine.enqueue(
            kind=executor.kind, payload={"value": "ok"}, priority=10, timeout_s=5
        )

        worker_task = asyncio.create_task(worker.run())
        try:
            for _ in range(100):
                row = await engine.get(job_id)
                if row["status"] == "success":
                    result = row["result"]
                    break
                await asyncio.sleep(POLL_INTERVAL)
            else:  # pragma: no cover - defensive
                pytest.fail("job did not finish in time")
        finally:
            worker.request_stop()
            await worker.wait_stopped()
            worker_task.cancel()
            with suppress(asyncio.CancelledError):
                await worker_task

        assert executor.calls == [job_id]
        assert result == {"echo": "ok"}
        assert row["attempts"] == 1
        assert row["lease_until"] is None

    asyncio.run(_run())


@pytest.mark.integration
def test_worker_extends_leases_for_long_jobs() -> None:
    async def _run() -> None:
        backend = _CountingBackend(lease_ttl_s=0.2)
        executor = _EchoExecutor()
        engine = Engine(backend=backend, executors=[executor])
        worker = Worker(
            engine,
            max_concurrency=1,
            batch=1,
            poll_interval=POLL_INTERVAL,
            lease_ttl=backend.lease_ttl_s,
        )

        job_id = await engine.enqueue(
            kind=executor.kind, payload={"value": "slow", "sleep": 0.35}, timeout_s=5
        )

        worker_task = asyncio.create_task(worker.run())
        try:
            for _ in range(120):
                row = await engine.get(job_id)
                if row["status"] == "success":
                    break
                await asyncio.sleep(POLL_INTERVAL)
            else:  # pragma: no cover - defensive
                pytest.fail("job never succeeded")
        finally:
            worker.request_stop()
            await worker.wait_stopped()
            worker_task.cancel()
            with suppress(asyncio.CancelledError):
                await worker_task

        assert row["status"] == "success"
        assert executor.calls == [job_id]
        assert len(backend.extends) >= 2
        assert all(call[0] == job_id for call in backend.extends)

    asyncio.run(_run())
