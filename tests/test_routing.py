"""Tests for Engine.set_router dynamic routing (#76)."""

from __future__ import annotations

import asyncio
from uuid import UUID

import pytest

from pyjobkit import MemoryBackend
from pyjobkit.contracts import Executor
from pyjobkit.engine import Engine


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        return {}


def test_router_overrides_kind() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])

        def router(kind: str, payload: dict) -> str | None:
            if payload.get("type") == "email":
                return "noop"  # route email to noop executor
            return None

        engine.set_router(router)
        job_id = await engine.enqueue(
            kind="generic", payload={"type": "email"}
        )
        rec = await backend.get(job_id)
        assert rec["kind"] == "noop"

    asyncio.run(_run())


def test_router_returning_none_keeps_kind() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        engine.set_router(lambda kind, payload: None)
        job_id = await engine.enqueue(kind="noop", payload={})
        rec = await backend.get(job_id)
        assert rec["kind"] == "noop"

    asyncio.run(_run())


def test_router_can_be_cleared() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        engine.set_router(lambda k, p: "noop")
        engine.set_router(None)
        job_id = await engine.enqueue(kind="noop", payload={})
        rec = await backend.get(job_id)
        assert rec["kind"] == "noop"

    asyncio.run(_run())


def test_router_must_be_callable() -> None:
    engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
    with pytest.raises(TypeError):
        engine.set_router("not callable")  # type: ignore[arg-type]


def test_router_result_still_validated() -> None:
    async def _run() -> None:
        engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
        engine.set_router(lambda k, p: "bad kind!")
        with pytest.raises(ValueError, match="kind must contain"):
            await engine.enqueue(kind="ok", payload={})

    asyncio.run(_run())
