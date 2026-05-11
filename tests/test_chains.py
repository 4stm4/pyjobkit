"""Tests for Engine.chain + worker follow-up enqueue."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

import pytest

from pyjobkit import Engine, MemoryBackend, Worker
from pyjobkit.contracts import Executor


class _Add(Executor):
    """Echoes payload['n'] and adds previous_result if present."""

    kind = "add"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        prev = (payload.get("previous_result") or {}).get("total", 0)
        return {"total": prev + int(payload.get("n", 0))}


def test_chain_runs_steps_sequentially_threading_results() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Add()])
        worker = Worker(engine, poll_interval=0.005)

        head = await engine.chain(
            {"kind": "add", "payload": {"n": 1}},
            {"kind": "add", "payload": {"n": 10}},
            {"kind": "add", "payload": {"n": 100}},
        )

        # Drain by spinning a fresh worker per cycle (chains queue
        # follow-ups during succeed, not before run(once=True) exits).
        for _ in range(8):
            w = Worker(engine, poll_interval=0.005)
            await asyncio.wait_for(w.run(once=True), timeout=2)
            if await backend.count(status="queued") == 0:
                break

        # Three jobs total: head + two follow-ups, all success.
        assert await backend.count(status="success") == 3
        # Tail job's result reflects the running total.
        all_jobs = await backend.all_jobs()
        totals = sorted(j["result"]["total"] for j in all_jobs)
        assert totals == [1, 11, 111]

    asyncio.run(_run())


def test_chain_aborts_when_first_step_fails() -> None:
    async def _run() -> None:
        backend = MemoryBackend()

        class _Boom(Executor):
            kind = "boom"

            async def run(self, *, job_id, payload, ctx):
                raise RuntimeError("nope")

        engine = Engine(backend=backend, executors=[_Boom(), _Add()])
        head = await engine.chain(
            {"kind": "boom", "payload": {}, "max_attempts": 1},
            {"kind": "add", "payload": {"n": 5}},
        )
        w = Worker(engine, poll_interval=0.005)
        await asyncio.wait_for(w.run(once=True), timeout=2)

        first = await backend.get(head)
        assert first["status"] == "failed"
        # No follow-up was queued.
        assert await backend.count() == 1

    asyncio.run(_run())


def test_chain_rejects_empty() -> None:
    async def _run() -> None:
        engine = Engine(backend=MemoryBackend(), executors=[_Add()])
        with pytest.raises(ValueError):
            await engine.chain()

    asyncio.run(_run())
