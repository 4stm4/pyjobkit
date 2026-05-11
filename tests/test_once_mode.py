"""Tests for Worker(once=True) and kind filtering (#47)."""

from __future__ import annotations

import asyncio
from uuid import UUID

from pyjobkit import MemoryBackend
from pyjobkit.contracts import Executor
from pyjobkit.engine import Engine
from pyjobkit.worker import Worker


class _Echo(Executor):
    kind = "echo"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        return {"echo": payload}


class _Other(Executor):
    kind = "other"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        return {"ok": True}


def test_run_once_drains_and_exits() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Echo()])
        for i in range(3):
            await engine.enqueue(kind="echo", payload={"i": i})

        worker = Worker(engine, poll_interval=0.01)
        # run(once=True) must exit on its own when the queue is empty.
        await asyncio.wait_for(worker.run(once=True), timeout=5)

        assert await backend.count(status="success") == 3
        assert await backend.count(status="queued") == 0

    asyncio.run(_run())


def test_run_once_empty_queue_exits_immediately() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Echo()])
        worker = Worker(engine, poll_interval=0.01)
        await asyncio.wait_for(worker.run(once=True), timeout=2)

    asyncio.run(_run())


def test_kind_filter_skips_other_kinds_back_to_queue() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Echo(), _Other()])
        echo_id = await engine.enqueue(kind="echo", payload={"m": 1})
        other_id = await engine.enqueue(kind="other", payload={})

        worker = Worker(engine, kinds=["echo"], poll_interval=0.01)
        await asyncio.wait_for(worker.run(once=True), timeout=5)

        echo_rec = await backend.get(echo_id)
        other_rec = await backend.get(other_id)
        assert echo_rec["status"] == "success"
        # Rejected job is queued back, not processed.
        assert other_rec["status"] == "queued"

    asyncio.run(_run())
