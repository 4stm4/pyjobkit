"""Targeted unit tests for executor edge-cases."""

from __future__ import annotations

import asyncio
import sys
from typing import Any
from uuid import uuid4

import pytest

from pyjobkit.contracts import ExecContext
from pyjobkit.executors.subprocess import SubprocessExecutor


class _Ctx(ExecContext):
    def __init__(self) -> None:
        self.records: list[tuple[str, str]] = []

    async def log(self, message: str, /, *, stream: str = "stdout") -> None:
        self.records.append((stream, message.strip()))

    async def is_cancelled(self) -> bool:  # pragma: no cover - protocol hook
        return False

    async def set_progress(self, value: float, /, **meta: Any) -> None:  # pragma: no cover
        self.records.append(("progress", f"{value}:{meta}"))


def test_subprocess_executor_captures_non_zero_exit() -> None:
    ctx = _Ctx()
    executor = SubprocessExecutor()

    result = asyncio.run(
        executor.run(
            job_id=uuid4(),
            payload={"cmd": [sys.executable, "-c", "import sys; sys.exit(2)"]},
            ctx=ctx,
        )
    )

    assert result["returncode"] == 2
    assert any("process exited with code 2" in msg for _, msg in ctx.records)


def test_subprocess_executor_terminates_on_cancel() -> None:
    ctx = _Ctx()
    executor = SubprocessExecutor()

    async def _run_and_cancel() -> None:
        task = asyncio.create_task(
            executor.run(
                job_id=uuid4(),
                payload={"cmd": [sys.executable, "-c", "import time; time.sleep(10)"]},
                ctx=ctx,
            )
        )
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(_run_and_cancel())

    assert any("terminating process" in msg for _, msg in ctx.records)
