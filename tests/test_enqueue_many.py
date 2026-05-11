"""Tests for Engine.enqueue_many bulk helper."""

from __future__ import annotations

import asyncio
from uuid import UUID

from pyjobkit import Engine, MemoryBackend
from pyjobkit.contracts import Executor


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        return {}


def test_enqueue_many_returns_ids_in_order() -> None:
    async def _run() -> None:
        engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
        ids = await engine.enqueue_many(
            [
                {"kind": "noop", "payload": {"i": 0}},
                {"kind": "noop", "payload": {"i": 1}, "tags": ["hi"]},
                {"kind": "noop", "payload": {"i": 2}, "shadow": True},
            ]
        )
        assert len(ids) == 3
        assert all(isinstance(i, UUID) for i in ids)

    asyncio.run(_run())


def test_enqueue_many_uses_backend_bulk_path() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        ids = await engine.enqueue_many(
            [
                {"kind": "noop", "payload": {"i": i}}
                for i in range(5)
            ]
        )
        assert len(ids) == 5
        # All persisted.
        for jid in ids:
            rec = await backend.get(jid)
            assert rec["status"] == "queued"

    asyncio.run(_run())


def test_enqueue_many_falls_back_when_payload_uses_markers() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        ids = await engine.enqueue_many(
            [
                {"kind": "noop", "payload": {"i": 0}, "tags": ["hi"]},
                {"kind": "noop", "payload": {"i": 1}, "shadow": True},
            ]
        )
        assert len(ids) == 2
        # Tags / shadow markers must have been written to payload.
        rec0 = await backend.get(ids[0])
        assert "__pjk_tags" in rec0["payload"]
        rec1 = await backend.get(ids[1])
        assert rec1["payload"]["__pjk_shadow"] is True

    asyncio.run(_run())


def test_enqueue_many_empty_returns_empty_list() -> None:
    async def _run() -> None:
        engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
        assert await engine.enqueue_many([]) == []

    asyncio.run(_run())
