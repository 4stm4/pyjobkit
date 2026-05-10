"""Tests for the worker watchdog / reap loop (#72)."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from pyjobkit import MemoryBackend
from pyjobkit.contracts import Executor
from pyjobkit.engine import Engine
from pyjobkit.worker import Worker


class _NoopExecutor(Executor):
    kind = "noop"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        return {}


def test_watchdog_default_interval_matches_lease_ttl() -> None:
    engine = Engine(backend=MemoryBackend(), executors=[_NoopExecutor()])
    worker = Worker(engine, lease_ttl=7)
    assert worker.watchdog_interval_s == 7.0


def test_watchdog_explicit_interval_override() -> None:
    engine = Engine(backend=MemoryBackend(), executors=[_NoopExecutor()])
    worker = Worker(engine, lease_ttl=30, watchdog_interval_s=2.5)
    assert worker.watchdog_interval_s == 2.5


def test_watchdog_rejects_non_positive_interval() -> None:
    engine = Engine(backend=MemoryBackend(), executors=[_NoopExecutor()])
    with pytest.raises(ValueError):
        Worker(engine, watchdog_interval_s=0)


def test_reap_loop_runs_engine_reap_and_logs_event(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def _run() -> None:
        backend = MemoryBackend(lease_ttl_s=1)
        engine = Engine(backend=backend, executors=[_NoopExecutor()])
        worker = Worker(engine, watchdog_interval_s=0.05)

        # Manually create a job with an already-expired lease.
        job_id = await backend.enqueue(kind="noop", payload={})
        claimed = await backend.claim_batch(worker.worker_id, limit=1)
        assert claimed
        # Backdate the lease so the next reap finds it.
        async with backend._lock:  # type: ignore[attr-defined]
            job = backend._jobs[job_id]  # type: ignore[attr-defined]
            job.lease_until = datetime.now(timezone.utc) - timedelta(seconds=10)

        with caplog.at_level(logging.INFO, logger="pyjobkit.worker"):
            reap_task = asyncio.create_task(worker._reap_loop())
            try:
                await asyncio.sleep(0.2)
            finally:
                worker._stop.set()
                await reap_task

        rec = await backend.get(job_id)
        assert rec["status"] == "failed"
        assert rec["result"] == {"error": "lease_expired"}
        assert any(
            getattr(r, "event", None) == "watchdog.reaped"
            for r in caplog.records
        )

    asyncio.run(_run())
