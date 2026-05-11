"""Tests for the periodic Scheduler."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import UUID

import pytest

from pyjobkit import Engine, MemoryBackend
from pyjobkit.contracts import Executor
from pyjobkit.leader import MemoryLeaderLock
from pyjobkit.scheduler import Scheduler, parse_interval


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        return {}


def test_parse_interval_accepts_common_forms() -> None:
    assert parse_interval("5m") == timedelta(minutes=5)
    assert parse_interval("90") == timedelta(seconds=90)
    assert parse_interval(60) == timedelta(seconds=60)
    assert parse_interval(timedelta(hours=2)) == timedelta(hours=2)


def test_parse_interval_rejects_bad() -> None:
    with pytest.raises(ValueError):
        parse_interval("bogus")


def test_tick_enqueues_only_after_interval_elapses() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])

        fake = {"now": 1000.0}

        def clock() -> float:
            return fake["now"]

        sched = Scheduler(engine, clock=clock)
        sched.every("5s", name="probe", kind="noop")

        # Just registered, next_run = 1005; tick at 1003 enqueues nothing.
        fake["now"] = 1003.0
        assert await sched.tick() == 0

        # Tick at 1006: one enqueue happens, next_run advances to 1011.
        fake["now"] = 1006.0
        assert await sched.tick() == 1

        # Tick again at 1009 — still before 1011.
        fake["now"] = 1009.0
        assert await sched.tick() == 0

        # Confirm jobs landed in the backend.
        assert await backend.count(status="queued") == 1

    asyncio.run(_run())


def test_run_loop_respects_stop_event() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        sched = Scheduler(engine)
        sched.every("60s", name="probe", kind="noop")

        stop = asyncio.Event()

        async def stopper() -> None:
            await asyncio.sleep(0.05)
            stop.set()

        await asyncio.wait_for(
            asyncio.gather(
                sched.run(stop_event=stop, sleep_resolution_s=0.02),
                stopper(),
            ),
            timeout=2,
        )

    asyncio.run(_run())


def test_run_with_leader_lock_only_enqueues_for_holder() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        sched = Scheduler(engine)
        # Tiny interval so the tick fires immediately.
        sched.every("0.01s", name="probe", kind="noop")

        MemoryLeaderLock.reset_state()
        leader = MemoryLeaderLock(name="sched")
        # Another holder grabs the lock first.
        loser = MemoryLeaderLock(name="sched")
        await loser.try_acquire(ttl_s=10)

        stop = asyncio.Event()

        async def stopper() -> None:
            await asyncio.sleep(0.1)
            stop.set()

        await asyncio.gather(
            sched.run(
                stop_event=stop,
                leader_lock=leader,
                sleep_resolution_s=0.02,
            ),
            stopper(),
        )

        # The leader-not-held branch returns immediately on the next
        # stop poll, so nothing is enqueued.
        assert await backend.count(status="queued") == 0

    asyncio.run(_run())
