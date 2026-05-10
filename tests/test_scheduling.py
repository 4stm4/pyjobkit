"""Tests for delayed scheduling helpers (#57)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from pyjobkit import Engine, MemoryBackend
from pyjobkit.contracts import Executor


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        return {}


def test_enqueue_at_records_scheduled_for() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        when = datetime.now(timezone.utc) + timedelta(minutes=5)
        job_id = await engine.enqueue_at(when, kind="noop", payload={"x": 1})
        rec = await backend.get(job_id)
        assert rec["scheduled_for"] == when

    asyncio.run(_run())


def test_enqueue_at_rejects_naive_datetime() -> None:
    async def _run() -> None:
        engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
        with pytest.raises(ValueError, match="timezone-aware"):
            await engine.enqueue_at(
                datetime(2030, 1, 1, 12, 0, 0),
                kind="noop",
                payload={},
            )

    asyncio.run(_run())


def test_enqueue_in_seconds() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        before = datetime.now(timezone.utc)
        job_id = await engine.enqueue_in(10, kind="noop", payload={})
        rec = await backend.get(job_id)
        delay = (rec["scheduled_for"] - before).total_seconds()
        assert 9 <= delay <= 11

    asyncio.run(_run())


def test_enqueue_in_timedelta() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        before = datetime.now(timezone.utc)
        job_id = await engine.enqueue_in(
            timedelta(minutes=2), kind="noop", payload={}
        )
        rec = await backend.get(job_id)
        delay = (rec["scheduled_for"] - before).total_seconds()
        assert 119 <= delay <= 121

    asyncio.run(_run())


def test_enqueue_in_rejects_negative_delay() -> None:
    async def _run() -> None:
        engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
        with pytest.raises(ValueError, match="non-negative"):
            await engine.enqueue_in(-1, kind="noop", payload={})

    asyncio.run(_run())


def test_enqueue_at_forwards_shadow_and_retry_policy() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        when = datetime.now(timezone.utc) + timedelta(seconds=30)
        job_id = await engine.enqueue_at(
            when,
            kind="noop",
            payload={"a": 1},
            shadow=True,
            retry_policy="fixed:0.5",
        )
        rec = await backend.get(job_id)
        # Payload should carry both markers.
        assert rec["payload"]["__pjk_shadow"] is True
        assert rec["payload"]["__pjk_retry_policy"] == "fixed:0.5"

    asyncio.run(_run())
