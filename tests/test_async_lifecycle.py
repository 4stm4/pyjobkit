"""Tests for async context managers and run(once=True) reuse guard."""

from __future__ import annotations

import asyncio
from uuid import UUID

import pytest

from pyjobkit import Engine, MemoryBackend, Worker
from pyjobkit.contracts import Executor


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        return {}


def test_engine_async_context_manager_closes() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        closed: list[bool] = []

        async def fake_close() -> None:
            closed.append(True)

        backend.close = fake_close  # type: ignore[attr-defined]

        async with Engine(backend=backend, executors=[_Noop()]) as engine:
            await engine.enqueue(kind="noop", payload={})

        assert closed == [True]

    asyncio.run(_run())


def test_worker_async_context_manager_stops_inflight() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        async with Worker(engine, poll_interval=0.01) as worker:
            run_task = asyncio.create_task(worker.run())
            await asyncio.sleep(0.05)
        assert worker._stopped.is_set()
        run_task.cancel()
        try:
            await run_task
        except asyncio.CancelledError:
            pass

    asyncio.run(_run())


def test_worker_run_twice_raises() -> None:
    async def _run() -> None:
        engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
        worker = Worker(engine, poll_interval=0.01)
        await worker.run(once=True)
        with pytest.raises(RuntimeError, match="cannot be called twice"):
            await worker.run(once=True)

    asyncio.run(_run())
