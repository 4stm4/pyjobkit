"""Tests for purge_finished retention on memory backend."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID

from pyjobkit import Engine, MemoryBackend
from pyjobkit.contracts import Executor


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        return {}


def test_purge_finished_removes_terminal_jobs() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        a = await backend.enqueue(kind="noop", payload={})
        await backend.enqueue(kind="noop", payload={})
        # Mark first job as success.
        await backend.succeed(a, {"ok": True})
        removed = await backend.purge_finished()
        assert removed == 1
        assert await backend.count() == 1  # the queued one survives

    asyncio.run(_run())


def test_purge_finished_respects_older_than() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        a = await backend.enqueue(kind="noop", payload={})
        await backend.succeed(a, {"ok": True})
        # Backdate finished_at.
        async with backend._lock:  # type: ignore[attr-defined]
            backend._jobs[a].finished_at = (  # type: ignore[attr-defined]
                datetime.now(timezone.utc) - timedelta(days=10)
            )
        # 5-day cutoff should remove it; 30-day cutoff should not.
        kept_first = await backend.purge_finished(older_than=timedelta(days=30))
        assert kept_first == 0
        cleared = await backend.purge_finished(older_than=timedelta(days=5))
        assert cleared == 1

    asyncio.run(_run())


def test_engine_purge_finished_proxies_to_backend() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        a = await engine.enqueue(kind="noop", payload={})
        await backend.succeed(a, {"ok": True})
        removed = await engine.purge_finished()
        assert removed == 1

    asyncio.run(_run())
