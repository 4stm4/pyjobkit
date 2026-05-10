"""Tests for the shadow / dry-run execution mode (#75)."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID, uuid4

import pytest

from pyjobkit.backends.memory import MemoryBackend
from pyjobkit.contracts import Executor
from pyjobkit.engine import SHADOW_PAYLOAD_KEY, Engine
from pyjobkit.worker import Worker


class _RecordingExecutor(Executor):
    kind = "shadowable"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.shadow_flags: list[bool] = []

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        self.calls.append({"job_id": job_id, "payload": payload})
        self.shadow_flags.append(ctx.is_shadow)
        await ctx.log("shadowable executed")
        return {"answer": 42}


def test_enqueue_with_shadow_marks_payload() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_RecordingExecutor()])
        job_id = await engine.enqueue(
            kind="shadowable", payload={"x": 1}, shadow=True
        )
        record = await backend.get(job_id)
        assert record["payload"][SHADOW_PAYLOAD_KEY] is True
        assert record["payload"]["x"] == 1

    asyncio.run(_run())


def test_enqueue_does_not_mutate_caller_payload() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_RecordingExecutor()])
        caller_payload = {"x": 1}
        await engine.enqueue(kind="shadowable", payload=caller_payload, shadow=True)
        assert SHADOW_PAYLOAD_KEY not in caller_payload

    asyncio.run(_run())


def test_worker_runs_executor_with_is_shadow_and_discards_result() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        executor = _RecordingExecutor()
        engine = Engine(backend=backend, executors=[executor])
        job_id = await engine.enqueue(
            kind="shadowable", payload={"x": 1}, shadow=True
        )
        worker = Worker(engine)
        claimed = await backend.claim_batch(worker.worker_id, limit=1)
        assert claimed and claimed[0]["id"] == str(job_id) or claimed[0]["id"] == job_id

        await worker._execute_row(dict(claimed[0]))

        assert executor.shadow_flags == [True]
        assert executor.calls[0]["payload"] == {"x": 1}
        assert SHADOW_PAYLOAD_KEY not in executor.calls[0]["payload"]

        record = await backend.get(job_id)
        assert record["status"] == "success"
        assert record["result"] == {"shadow": True, "result_discarded": True}

    asyncio.run(_run())


def test_default_exec_context_is_shadow_defaults_false() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_RecordingExecutor()])
        ctx = engine.make_ctx(uuid4())
        assert ctx.is_shadow is False

        shadow_ctx = engine.make_ctx(uuid4(), is_shadow=True)
        assert shadow_ctx.is_shadow is True

    asyncio.run(_run())


def test_worker_records_normal_result_when_not_shadow() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        executor = _RecordingExecutor()
        engine = Engine(backend=backend, executors=[executor])
        job_id = await engine.enqueue(kind="shadowable", payload={"x": 1})
        worker = Worker(engine)
        claimed = await backend.claim_batch(worker.worker_id, limit=1)
        await worker._execute_row(dict(claimed[0]))

        assert executor.shadow_flags == [False]
        record = await backend.get(job_id)
        assert record["status"] == "success"
        assert record["result"] == {"answer": 42}

    asyncio.run(_run())
