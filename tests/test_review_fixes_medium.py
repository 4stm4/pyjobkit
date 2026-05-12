"""Regression tests for the medium-severity code-review fixes."""

from __future__ import annotations

import asyncio
import importlib.util
import logging
from uuid import UUID

import pytest

from pyjobkit import Engine, MemoryBackend
from pyjobkit.contracts import Executor
from pyjobkit.leader import MemoryLeaderLock, leader_loop
from pyjobkit.scheduler import Scheduler


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id, payload, ctx):
        return {}


# #8 scheduler entry is idempotent across leader churn ---------------------


def test_scheduler_uses_idempotency_key_per_slot() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])

        fake = {"now": 1000.0}
        sched = Scheduler(engine, clock=lambda: fake["now"])
        sched.every("10s", name="probe", kind="noop")

        # Two ticks at the same slot - the second is a no-op via
        # idempotency_key, so backend keeps only one row.
        fake["now"] = 1010.0
        await sched.tick()

        # Force the entry's next_run back, simulating a churn where a
        # second scheduler retried the same slot.
        sched._entries["probe"].next_run = 1010.0  # type: ignore[attr-defined]
        await sched.tick()

        assert await backend.count() == 1

    asyncio.run(_run())


# #9 scheduler failures emit metric and do not advance next_run -----------


def test_scheduler_failure_does_not_advance_next_run() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])

        async def boom(**kwargs):
            raise RuntimeError("nope")

        engine.enqueue = boom  # type: ignore[method-assign]

        fake = {"now": 1000.0}
        sched = Scheduler(engine, clock=lambda: fake["now"])
        sched.every("5s", name="probe", kind="noop")

        fake["now"] = 1010.0  # past next_run
        prev_next = sched._entries["probe"].next_run  # type: ignore[attr-defined]
        await sched.tick()
        # next_run unchanged so we re-attempt next tick.
        assert sched._entries["probe"].next_run == prev_next  # type: ignore[attr-defined]

    asyncio.run(_run())


# #10 / #11 bulk insert validates specs and rejects duplicates -------------


def test_memory_enqueue_many_rejects_missing_kind() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        with pytest.raises(ValueError, match="kind' is required"):
            await backend.enqueue_many([{"payload": {"x": 1}}])

    asyncio.run(_run())


def test_memory_enqueue_many_rejects_duplicate_idempotency_keys() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        with pytest.raises(ValueError, match="duplicate idempotency_key"):
            await backend.enqueue_many(
                [
                    {"kind": "noop", "idempotency_key": "k1"},
                    {"kind": "noop", "idempotency_key": "k1"},
                ]
            )

    asyncio.run(_run())


# #13 list_jobs filter pushed into backend ---------------------------------


def test_memory_all_jobs_supports_status_and_limit() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        for _ in range(3):
            await backend.enqueue(kind="noop", payload={})
        a = await backend.enqueue(kind="noop", payload={})
        await backend.succeed(a, {"ok": True})

        queued = await backend.all_jobs(status="queued")
        assert len(queued) == 3
        capped = await backend.all_jobs(limit=2)
        assert len(capped) == 2

    asyncio.run(_run())


# #15 leader_loop honours cancel_grace_s -----------------------------------


def test_leader_loop_bounded_by_cancel_grace_when_on_leader_is_slow_to_cancel() -> None:
    """The loop must return within ``cancel_grace_s`` even if on_leader is slow.

    A misbehaving on_leader that swallows ``CancelledError`` would hang
    the shutdown path forever; ``cancel_grace_s`` puts an upper bound on
    that wait. We simulate a misbehaving coroutine that *delays* its
    cancellation (rather than swallowing forever, which would make
    ``asyncio.run`` itself hang during teardown) and assert that
    ``leader_loop`` releases the lock well before the misbehaviour
    finishes.
    """

    import time

    async def _run() -> None:
        MemoryLeaderLock.reset_state()
        lock = MemoryLeaderLock(name="cg")
        stop = asyncio.Event()

        async def on_leader() -> None:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                # Take an extra moment before honouring the cancel.
                # This is the misbehaviour we want bounded.
                await asyncio.sleep(1.0)
                raise

        async def stopper() -> None:
            await asyncio.sleep(0.05)
            stop.set()

        t0 = time.perf_counter()
        await asyncio.gather(
            leader_loop(
                lock,
                ttl_s=1,
                renew_every_s=0.5,
                on_leader=on_leader,
                stop_event=stop,
                cancel_grace_s=0.1,
            ),
            stopper(),
        )
        elapsed = time.perf_counter() - t0

        # cancel_grace_s is 0.1; total path should be well under 1s
        # (the on_leader's swallow-then-honour delay).
        assert elapsed < 0.8, f"leader_loop did not honour cancel_grace_s ({elapsed=:.2f}s)"

        # The lock must have been released for a fresh holder.
        fresh = MemoryLeaderLock(name="cg")
        assert await fresh.try_acquire(ttl_s=1) is True

    asyncio.run(_run())


# #12 REST max_attempts default uses Engine.default_max_attempts -----------


_FASTAPI_AVAILABLE = (
    importlib.util.find_spec("fastapi") is not None
    and importlib.util.find_spec("pydantic") is not None
)


@pytest.mark.skipif(not _FASTAPI_AVAILABLE, reason="fastapi/pydantic not installed")
def test_rest_enqueue_uses_engine_default_max_attempts() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from pyjobkit.integrations.fastapi import make_router

    backend = MemoryBackend()
    engine = Engine(
        backend=backend, executors=[_Noop()], default_max_attempts=11
    )
    app = FastAPI()
    app.include_router(make_router(engine), prefix="/api/v1")

    with TestClient(app) as client:
        r = client.post("/api/v1/jobs", json={"kind": "noop", "payload": {}})
        assert r.status_code == 202
        jid = UUID(r.json()["job_id"])

    asyncio.run(_check_max_attempts(backend, jid, 11))


async def _check_max_attempts(backend, jid, expected) -> None:
    rec = await backend.get(jid)
    assert rec["max_attempts"] == expected
