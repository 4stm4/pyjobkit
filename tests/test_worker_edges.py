"""Extra coverage for Worker error paths and helpers."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from pyjobkit import Engine, MemoryBackend, Worker
from pyjobkit.contracts import Executor, OptimisticLockError
from pyjobkit.worker import LeaseLostError


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id, payload, ctx):
        return {}


def test_age_seconds_handles_iso_string_and_none() -> None:
    w = Worker(Engine(backend=MemoryBackend(), executors=[_Noop()]))
    assert w._age_seconds({"created_at": None}) is None
    assert w._age_seconds({"created_at": "not-iso"}) is None
    iso = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    age = w._age_seconds({"created_at": iso})
    assert age is not None and 8 < age < 12
    # Naive datetime -> TypeError -> None.
    naive = datetime.utcnow()
    assert w._age_seconds({"created_at": naive}) is None


def test_check_health_reports_unhealthy_on_connection_failure() -> None:
    async def _go():
        backend = MemoryBackend()

        async def boom():
            raise RuntimeError("db down")

        backend.check_connection = boom  # type: ignore[method-assign]
        engine = Engine(backend=backend, executors=[_Noop()])
        worker = Worker(engine)
        health = await worker.check_health()
        assert health["status"] == "unhealthy"
        assert "db down" in health["reason"]

    asyncio.run(_go())


def test_check_health_overflow_when_depth_at_capacity() -> None:
    async def _go():
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        worker = Worker(engine, queue_capacity=1)
        await backend.enqueue(kind="noop", payload={})
        health = await worker.check_health()
        assert health["status"] == "unhealthy"
        assert health["queue_overflow"] is True

    asyncio.run(_go())


def test_lease_lost_path_invokes_callback() -> None:
    async def _go():
        backend = MemoryBackend()

        class _ExecLeaseLost(Executor):
            kind = "lease-lost"

            async def run(self, *, job_id, payload, ctx):
                raise LeaseLostError(job_id)

        engine = Engine(backend=backend, executors=[_ExecLeaseLost()])
        called: list = []

        async def cb(jid, row):
            called.append(jid)

        worker = Worker(engine, on_lease_lost=cb)
        jid = await engine.enqueue(kind="lease-lost", payload={})
        claimed = await backend.claim_batch(worker.worker_id, limit=1)
        await worker._execute_row(dict(claimed[0]))
        assert called == [jid]

    asyncio.run(_go())


def test_optimistic_lock_short_circuits(caplog: pytest.LogCaptureFixture) -> None:
    async def _go():
        backend = MemoryBackend()

        class _Exec(Executor):
            kind = "ok"

            async def run(self, *, job_id, payload, ctx):
                return {"ok": True}

        engine = Engine(backend=backend, executors=[_Exec()])

        async def boom(*_a, **_kw):
            raise OptimisticLockError("version conflict")

        engine.succeed = boom  # type: ignore[method-assign]
        worker = Worker(engine)
        await engine.enqueue(kind="ok", payload={})
        claimed = await backend.claim_batch(worker.worker_id, limit=1)
        with caplog.at_level(logging.INFO, logger="pyjobkit.worker"):
            await worker._execute_row(dict(claimed[0]))
        assert any(
            getattr(r, "event", None) == "job.lock_conflict" for r in caplog.records
        )

    asyncio.run(_go())


def test_wait_for_drain_cancels_outstanding_tasks_on_timeout() -> None:
    async def _go():
        engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
        worker = Worker(engine, stop_timeout=0.05)

        # The task must call _decrement_active when cancelled so the
        # drain's _active_jobs_zero event fires; mimic the real
        # execute_row dance.
        async def long_running():
            try:
                await asyncio.sleep(60)
            finally:
                worker._decrement_active()

        worker._increment_active()
        task = asyncio.create_task(long_running())
        worker._tasks.add(task)
        await worker._wait_for_drain()
        assert worker._stopped.is_set()
        assert task.cancelled() or task.done()

    asyncio.run(_go())


def test_reap_loop_logs_exception_and_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def _go():
        backend = MemoryBackend()

        boom_count = {"n": 0}

        async def flaky_reap():
            boom_count["n"] += 1
            if boom_count["n"] == 1:
                raise RuntimeError("transient")
            return 0

        engine = Engine(backend=backend, executors=[_Noop()])
        engine.reap_expired = flaky_reap  # type: ignore[method-assign]
        worker = Worker(engine, watchdog_interval_s=0.02)

        with caplog.at_level(logging.WARNING, logger="pyjobkit.worker"):
            task = asyncio.create_task(worker._reap_loop())
            await asyncio.sleep(0.1)
            worker._stop.set()
            await task

        assert any("reap_expired failed" in r.message for r in caplog.records)

    asyncio.run(_go())


def test_heartbeat_loop_swallows_callback_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def _go():
        engine = Engine(backend=MemoryBackend(), executors=[_Noop()])

        async def boom(_wid):
            raise RuntimeError("bad cb")

        worker = Worker(
            engine, heartbeat_interval_s=0.02, on_heartbeat=boom
        )
        with caplog.at_level(logging.WARNING, logger="pyjobkit.worker"):
            task = asyncio.create_task(worker._heartbeat_loop())
            await asyncio.sleep(0.06)
            worker._stop.set()
            await task

        assert any("on_heartbeat callback raised" in r.message for r in caplog.records)

    asyncio.run(_go())
