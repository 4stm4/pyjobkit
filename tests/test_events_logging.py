"""Tests for the local event bus and memory log sink."""

from __future__ import annotations

import asyncio
from uuid import uuid4

from pyjobkit.contracts import LogRecord
from pyjobkit.events.local import LocalEventBus
from pyjobkit.logging.memory import MemoryLogSink


def test_local_event_bus_handles_multiple_handlers() -> None:
    async def _run() -> None:
        bus = LocalEventBus()
        payloads: list[dict] = []

        async def handler_one(payload: dict) -> None:
            payloads.append({"handler": 1, **payload})

        async def handler_two(payload: dict) -> None:
            payloads.append({"handler": 2, **payload})
            raise RuntimeError("ignored thanks to return_exceptions")

        bus.subscribe("demo", handler_one)
        bus.subscribe("demo", handler_two)
        await bus.publish("demo", {"value": 3})
        assert payloads == [
            {"handler": 1, "value": 3},
            {"handler": 2, "value": 3},
        ]

    asyncio.run(_run())


def test_memory_log_sink_enforces_capacity() -> None:
    async def _run() -> None:
        sink = MemoryLogSink(max_items=2)
        job_id = uuid4()
        for idx in range(3):
            await sink.write(LogRecord(job_id, "stdout", f"line-{idx}"))
        logs = await sink.get(job_id)
        assert [entry.message for entry in logs] == ["line-1", "line-2"]
        assert await sink.get(uuid4()) == []

    asyncio.run(_run())


def test_memory_log_sink_handles_missing_event_loop(monkeypatch) -> None:
    async def _run() -> None:
        sink = MemoryLogSink(max_items=1)
        job_id = uuid4()
        await sink.write(LogRecord(job_id, "stdout", "offline"))

        def _raise() -> None:
            raise RuntimeError("no loop")

        monkeypatch.setattr("pyjobkit.logging.memory.asyncio.get_running_loop", _raise)

        logs = await sink.get(job_id)
        assert [entry.message for entry in logs] == ["offline"]

    asyncio.run(_run())
