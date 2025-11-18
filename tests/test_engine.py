"""Tests covering the high level Engine facade and execution context."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from pyjobkit.backends.memory import MemoryBackend
from pyjobkit.engine import Engine


class _RecorderExecutor:
    kind = "demo"

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def run(self, *, job_id, payload: dict, ctx):  # type: ignore[override]
        await ctx.log(f"running {payload['value']}")
        await ctx.set_progress(0.5, step="half")
        result = {"echo": payload, "job": str(job_id)}
        self.calls.append((job_id, payload, result))
        return result


async def _exercise_engine() -> None:
    backend = MemoryBackend()
    executor = _RecorderExecutor()
    engine = Engine(backend=backend, executors=[executor])

    job_id = await engine.enqueue(kind="demo", payload={"value": 1}, priority=10, timeout_s=15)
    job_row = await engine.get(job_id)
    assert job_row["payload"]["value"] == 1

    await engine.cancel(job_id)
    cancelled = await engine.get(job_id)
    assert cancelled["cancel_requested"] is True

    # Execution context plumbing uses the configured log sink and event bus.
    ctx = engine.make_ctx(job_id)
    progress_events: list[dict] = []

    async def _progress_handler(payload: dict) -> None:
        progress_events.append(payload)

    engine.event_bus.subscribe(f"job.{job_id}.progress", _progress_handler)

    await ctx.log("hello world")
    logs = await engine.log_sink.get(job_id)
    assert logs[0].message == "hello world"

    await ctx.set_progress(0.75, note="almost")
    assert progress_events == [{"value": 0.75, "note": "almost"}]
    assert await ctx.is_cancelled() is True

    result = await executor.run(job_id=job_id, payload={"value": 42}, ctx=ctx)
    assert result["echo"] == {"value": 42}
    assert engine.executor_for("demo") is executor
    assert engine.executor_for("missing") is None


def test_engine_end_to_end() -> None:
    asyncio.run(_exercise_engine())


def test_engine_make_ctx_is_isolated() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[])
        job_a = uuid4()
        job_b = uuid4()
        ctx_a = engine.make_ctx(job_a)
        ctx_b = engine.make_ctx(job_b)
        await ctx_a.log("a")
        await ctx_b.log("b")
        logs_a = await engine.log_sink.get(job_a)
        logs_b = await engine.log_sink.get(job_b)
        assert [entry.message for entry in logs_a] == ["a"]
        assert [entry.message for entry in logs_b] == ["b"]

    asyncio.run(_run())


def test_engine_rejects_duplicate_executor_kinds() -> None:
    class _Executor:
        kind = "duplicate"

    backend = MemoryBackend()
    with pytest.raises(ValueError):
        Engine(backend=backend, executors=[_Executor(), _Executor()])
