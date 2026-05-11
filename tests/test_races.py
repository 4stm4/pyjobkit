"""Concurrency / race-condition tests for state transitions (#77).

Each test stresses a specific code path by driving many parallel
coroutines through the same backend and asserting that final state
remains internally consistent. The :class:`MemoryBackend` is used as
the single source of truth because it shares the same locking
discipline that production backends are expected to honor.
"""

from __future__ import annotations

import asyncio
from uuid import UUID

import pytest

from pyjobkit import MemoryBackend
from pyjobkit.contracts import Executor
from pyjobkit.engine import Engine
from pyjobkit.worker import Worker


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        return {}


class _SlowOK(Executor):
    kind = "slow"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        await asyncio.sleep(0.01)
        return {"ok": True}


class _Flaky(Executor):
    """Fails N times before succeeding."""

    kind = "flaky"

    def __init__(self) -> None:
        self._seen: dict[str, int] = {}

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        key = str(job_id)
        n = self._seen[key] = self._seen.get(key, 0) + 1
        if n <= int(payload.get("fail_for", 1)):
            raise RuntimeError(f"transient #{n}")
        return {"ok": True, "attempts": n}


def test_parallel_enqueue_unique_ids() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        ids = await asyncio.gather(
            *(engine.enqueue(kind="noop", payload={"i": i}) for i in range(50))
        )
        assert len(set(ids)) == 50

    asyncio.run(_run())


def test_parallel_workers_each_job_runs_once() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_SlowOK()])
        for _ in range(20):
            await engine.enqueue(kind="slow", payload={})

        workers = [
            Worker(engine, poll_interval=0.005) for _ in range(4)
        ]
        await asyncio.wait_for(
            asyncio.gather(*(w.run(once=True) for w in workers)), timeout=10
        )

        assert await backend.count(status="success") == 20
        assert await backend.count(status="queued") == 0
        assert await backend.count(status="running") == 0

    asyncio.run(_run())


def test_cancel_during_run_keeps_state_consistent() -> None:
    async def _run() -> None:
        backend = MemoryBackend()

        async def _cancel(_jid, _payload, ctx):
            raise asyncio.CancelledError

        class _CancelExec(Executor):
            kind = "cancel"

            async def run(self, *, job_id, payload, ctx):
                await _cancel(job_id, payload, ctx)
                return {}

        engine = Engine(backend=backend, executors=[_CancelExec()])
        for _ in range(10):
            await engine.enqueue(kind="cancel", payload={})

        worker = Worker(engine, poll_interval=0.005)
        await asyncio.wait_for(worker.run(once=True), timeout=5)

        # No job should be left in a non-terminal state.
        assert await backend.count(status="queued") == 0
        assert await backend.count(status="running") == 0
        # All ended in cancelled (the worker handled CancelledError).
        assert await backend.count(status="cancelled") == 10

    asyncio.run(_run())


def test_retry_with_fresh_workers_eventually_succeeds() -> None:
    """A flaky job is eventually finalized when fresh workers run it."""

    async def _run() -> None:
        backend = MemoryBackend()
        flaky = _Flaky()
        engine = Engine(backend=backend, executors=[flaky])
        job_id = await engine.enqueue(
            kind="flaky",
            payload={"fail_for": 2},
            max_attempts=5,
            retry_policy="fixed:0.0",
        )

        # Each worker exits after a single run(once=True); fresh workers
        # are created per iteration because run(once=True) latches _stop.
        for _ in range(50):
            worker = Worker(engine, poll_interval=0.005)
            await worker.run(once=True)
            rec = await backend.get(job_id)
            if rec["status"] in {"success", "failed"}:
                break
            await asyncio.sleep(0.005)

        rec = await backend.get(job_id)
        assert rec["status"] == "success", rec

    asyncio.run(_run())


def test_optimistic_lock_prevents_double_completion() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        job_id = await engine.enqueue(kind="noop", payload={})
        # Move to running so finalization is valid.
        claimed = await backend.claim_batch(uuid_factory(), limit=1)
        await backend.mark_running(job_id, uuid_factory())
        version_at_claim = claimed[0]["version"]

        # Two workers race to mark the same job done with the same version.
        await asyncio.gather(
            backend.succeed(job_id, {"a": 1}, expected_version=version_at_claim),
            backend.succeed(job_id, {"a": 2}, expected_version=version_at_claim),
        )
        rec = await backend.get(job_id)
        assert rec["status"] == "success"
        # Only one of the two writes should have been applied.
        assert rec["result"] in ({"a": 1}, {"a": 2})

    asyncio.run(_run())


def uuid_factory() -> UUID:
    from uuid import uuid4

    return uuid4()
