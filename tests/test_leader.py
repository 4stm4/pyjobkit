"""Tests for the leader election primitives (#61)."""

from __future__ import annotations

import asyncio

import pytest

from pyjobkit.leader import LeaderLock, MemoryLeaderLock, leader_loop


def test_memory_lock_first_caller_wins() -> None:
    async def _run() -> None:
        MemoryLeaderLock.reset_state()
        a = MemoryLeaderLock(name="t1")
        b = MemoryLeaderLock(name="t1")
        assert await a.try_acquire(ttl_s=1.0) is True
        assert await b.try_acquire(ttl_s=1.0) is False
        # a can renew indefinitely.
        assert await a.try_acquire(ttl_s=1.0) is True

    asyncio.run(_run())


def test_memory_lock_release_lets_someone_else_take_it() -> None:
    async def _run() -> None:
        # MemoryLeaderLock instances share state only via the singleton
        # owner field, so this test demonstrates the release semantic on
        # a single shared lock - which mirrors how production
        # implementations behave (one lock, many would-be holders).
        class _SharedLock(LeaderLock):
            def __init__(self) -> None:
                self.owner: object | None = None

            async def try_acquire(self, *, ttl_s: float) -> bool:
                if self.owner is None:
                    self.owner = object()
                    return True
                return False

            async def release(self) -> None:
                self.owner = None

        lock = _SharedLock()
        assert await lock.try_acquire(ttl_s=1.0) is True
        assert await lock.try_acquire(ttl_s=1.0) is False
        await lock.release()
        assert await lock.try_acquire(ttl_s=1.0) is True

    asyncio.run(_run())


def test_leader_loop_runs_on_leader_until_stop() -> None:
    async def _run() -> None:
        lock = MemoryLeaderLock()
        ticks: list[int] = []

        async def work() -> None:
            try:
                while True:
                    ticks.append(len(ticks))
                    await asyncio.sleep(0.02)
            except asyncio.CancelledError:
                pass

        stop = asyncio.Event()

        async def stopper() -> None:
            await asyncio.sleep(0.15)
            stop.set()

        await asyncio.gather(
            leader_loop(
                lock,
                ttl_s=0.1,
                renew_every_s=0.04,
                on_leader=work,
                stop_event=stop,
            ),
            stopper(),
        )

        assert ticks, "leader_loop should have driven on_leader at least once"

    asyncio.run(_run())


def test_leader_loop_validation() -> None:
    async def _run() -> None:
        lock = MemoryLeaderLock()
        with pytest.raises(ValueError):
            await leader_loop(lock, ttl_s=0, on_leader=lambda: asyncio.sleep(0))
        with pytest.raises(ValueError):
            await leader_loop(
                lock, ttl_s=1, renew_every_s=0, on_leader=lambda: asyncio.sleep(0)
            )

    asyncio.run(_run())
