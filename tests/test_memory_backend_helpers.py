"""Tests for the debug helpers on :class:`MemoryBackend` (#68)."""

from __future__ import annotations

import asyncio

import pytest

from pyjobkit import MemoryBackend


def test_count_all_clear() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        assert await backend.count() == 0
        assert await backend.all_jobs() == []

        await backend.enqueue(kind="x", payload={"a": 1})
        await backend.enqueue(kind="x", payload={"a": 2})

        assert await backend.count() == 2
        assert await backend.count(status="queued") == 2
        assert await backend.count(status="success") == 0

        jobs = await backend.all_jobs()
        assert {j["kind"] for j in jobs} == {"x"}

        await backend.clear()
        assert await backend.count() == 0

    asyncio.run(_run())


def test_memory_backend_reexported_at_top_level() -> None:
    import pyjobkit

    assert pyjobkit.MemoryBackend is MemoryBackend
