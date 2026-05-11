"""Tests for the BatchExecutor / dispatch_batch helpers (#63)."""

from __future__ import annotations

import asyncio
from typing import Sequence
from uuid import UUID, uuid4

import pytest

from pyjobkit import MemoryBackend
from pyjobkit.batch import BatchExecutor, BatchJob, dispatch_batch
from pyjobkit.contracts import Executor
from pyjobkit.engine import Engine


class _StubExecutor(Executor):
    """Required so engine has a registered executor for the kind."""

    kind = "embed"

    async def run(self, *, job_id, payload, ctx):  # pragma: no cover
        return {}


class _EmbedBatch(BatchExecutor):
    kind = "embed"

    async def run_batch(self, jobs: Sequence[BatchJob]) -> list[dict]:
        return [{"len": len(j.payload.get("text", ""))} for j in jobs]


class _Raising(BatchExecutor):
    kind = "embed"

    async def run_batch(self, jobs):
        raise RuntimeError("kaboom")


def test_dispatch_batch_finalizes_each_job_with_its_result() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_StubExecutor()])
        ids = []
        for text in ("hi", "hello", "world!"):
            jid = await engine.enqueue(kind="embed", payload={"text": text})
            ids.append(jid)

        worker_id = uuid4()
        rows = await backend.claim_batch(worker_id, limit=3)
        await dispatch_batch(engine, _EmbedBatch(), rows, worker_id=worker_id)

        records = [await backend.get(j) for j in ids]
        statuses = [r["status"] for r in records]
        assert statuses == ["success", "success", "success"]
        lengths = [r["result"]["len"] for r in records]
        assert lengths == [2, 5, 6]

    asyncio.run(_run())


def test_dispatch_batch_fails_all_when_batch_raises() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_StubExecutor()])
        ids = [
            await engine.enqueue(kind="embed", payload={"text": "a"})
            for _ in range(3)
        ]
        worker_id = uuid4()
        rows = await backend.claim_batch(worker_id, limit=3)
        await dispatch_batch(engine, _Raising(), rows, worker_id=worker_id)
        for jid in ids:
            rec = await backend.get(jid)
            assert rec["status"] == "failed"
            assert "kaboom" in rec["result"]["error"]

    asyncio.run(_run())


def test_dispatch_batch_rejects_oversized_input() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_StubExecutor()])

        class _Small(_EmbedBatch):
            max_batch_size = 1

        for _ in range(2):
            await engine.enqueue(kind="embed", payload={"text": "a"})
        rows = await backend.claim_batch(uuid4(), limit=2)
        with pytest.raises(ValueError):
            await dispatch_batch(engine, _Small(), rows, worker_id=uuid4())

    asyncio.run(_run())


def test_dispatch_batch_returns_silently_for_empty_rows() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_StubExecutor()])
        await dispatch_batch(engine, _EmbedBatch(), [], worker_id=uuid4())

    asyncio.run(_run())
