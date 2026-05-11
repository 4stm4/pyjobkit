"""Tests for the worker heartbeat loop (#59)."""

from __future__ import annotations

import asyncio
import logging
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


def test_heartbeat_interval_validation() -> None:
    engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
    with pytest.raises(ValueError):
        Worker(engine, heartbeat_interval_s=0)
    with pytest.raises(ValueError):
        Worker(engine, heartbeat_interval_s=-1)


def test_heartbeat_loop_emits_events_and_callback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def _run() -> None:
        engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
        ticks: list[UUID] = []

        async def cb(worker_id: UUID) -> None:
            ticks.append(worker_id)

        worker = Worker(
            engine,
            heartbeat_interval_s=0.05,
            on_heartbeat=cb,
        )

        with caplog.at_level(logging.INFO, logger="pyjobkit.worker"):
            task = asyncio.create_task(worker._heartbeat_loop())
            await asyncio.sleep(0.18)
            worker._stop.set()
            await task

        # 0.18 / 0.05 ~= 3 ticks expected; tolerate one off-by-one.
        assert 2 <= len(ticks) <= 4
        assert all(tid == worker.worker_id for tid in ticks)
        heartbeats = [
            r for r in caplog.records if getattr(r, "event", None) == "worker.heartbeat"
        ]
        assert heartbeats

    asyncio.run(_run())


def test_heartbeat_loop_exits_on_stop() -> None:
    async def _run() -> None:
        engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
        worker = Worker(engine, heartbeat_interval_s=10.0)
        worker._stop.set()
        # With stop already set the loop must terminate immediately.
        await asyncio.wait_for(worker._heartbeat_loop(), timeout=1.0)

    asyncio.run(_run())
