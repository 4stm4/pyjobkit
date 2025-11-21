"""Lightweight load scenario to verify worker stability under pressure."""

from __future__ import annotations

import asyncio
import time
from contextlib import suppress
from uuid import UUID

import pytest

from pyjobkit.backends.memory import MemoryBackend
from pyjobkit.contracts import ExecContext, Executor
from pyjobkit.engine import Engine
from pyjobkit.worker import Worker


class _FastExecutor(Executor):
    kind = "fast"

    def __init__(self) -> None:
        self.completed: list[UUID] = []

    async def run(self, *, job_id: UUID, payload: dict, ctx: ExecContext) -> dict:
        await asyncio.sleep(0.01)
        self.completed.append(job_id)
        return {"ok": True}


@pytest.mark.load
@pytest.mark.asyncio
async def test_worker_handles_dozen_parallel_jobs() -> None:
    backend = MemoryBackend(lease_ttl_s=2)
    executor = _FastExecutor()
    engine = Engine(backend=backend, executors=[executor])
    worker = Worker(
        engine,
        max_concurrency=10,
        batch=5,
        poll_interval=0.01,
        lease_ttl=backend.lease_ttl_s,
    )

    job_ids = [
        await engine.enqueue(kind=executor.kind, payload={"n": i}, priority=50)
        for i in range(40)
    ]

    started = time.perf_counter()
    worker_task = asyncio.create_task(worker.run())
    try:
        for _ in range(200):
            statuses = [await engine.get(job_id) for job_id in job_ids]
            if all(row["status"] == "success" for row in statuses):
                break
            await asyncio.sleep(0.02)
        else:  # pragma: no cover - defensive
            pytest.fail(
                f"Only {len(executor.completed)} of {len(job_ids)} jobs completed"
            )
    finally:
        worker.request_stop()
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(worker.wait_stopped(), timeout=5)
        worker_task.cancel()
        with suppress(asyncio.CancelledError):
            await worker_task

    duration = time.perf_counter() - started
    print(f"Finished {len(job_ids)} jobs in {duration:.2f}s")

    assert len(executor.completed) == len(job_ids)
