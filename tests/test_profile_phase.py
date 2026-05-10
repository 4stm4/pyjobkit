"""Tests for ExecContext.profile_phase (#74)."""

from __future__ import annotations

import asyncio
import logging
from uuid import UUID, uuid4

import pytest

from pyjobkit import MemoryBackend
from pyjobkit.engine import DefaultExecContext, Engine
from pyjobkit.events.local import LocalEventBus
from pyjobkit.logging.memory import MemoryLogSink


def _make_ctx() -> DefaultExecContext:
    return DefaultExecContext(
        job_id=uuid4(),
        log_sink=MemoryLogSink(),
        event_bus=LocalEventBus(),
        backend=MemoryBackend(),
    )


def test_profile_phase_logs_started_and_ended(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def _run() -> None:
        ctx = _make_ctx()
        with caplog.at_level(logging.INFO, logger="pyjobkit.contracts"):
            async with ctx.profile_phase("setup", attempt=1):
                await asyncio.sleep(0.01)

        starts = [r for r in caplog.records if getattr(r, "event", None) == "phase.started"]
        ends = [r for r in caplog.records if getattr(r, "event", None) == "phase.ended"]
        assert len(starts) == 1 and starts[0].phase == "setup"
        assert len(ends) == 1
        end = ends[0]
        assert end.phase == "setup"
        assert end.duration_ms > 5  # ~10 ms
        assert end.attempt == 1

    asyncio.run(_run())


def test_profile_phase_records_error_on_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def _run() -> None:
        ctx = _make_ctx()
        with caplog.at_level(logging.INFO, logger="pyjobkit.contracts"):
            with pytest.raises(RuntimeError):
                async with ctx.profile_phase("execute"):
                    raise RuntimeError("boom")

        end = [r for r in caplog.records if getattr(r, "event", None) == "phase.ended"][0]
        assert end.error == "RuntimeError"

    asyncio.run(_run())


def test_profile_phase_writes_summary_to_log_sink() -> None:
    async def _run() -> None:
        ctx = _make_ctx()
        async with ctx.profile_phase("cleanup"):
            pass
        sink: MemoryLogSink = ctx.log_sink  # type: ignore[assignment]
        records = await sink.get(ctx.job_id)
        assert any("phase cleanup took" in r.message for r in records)

    asyncio.run(_run())
