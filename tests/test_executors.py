"""Tests for HTTP and subprocess executors."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Any, List
from uuid import uuid4

import pytest

from pyjobkit.executors.http import HttpExecutor
from pyjobkit.executors.subprocess import SubprocessExecutor


class TestContext:
    __test__ = False

    def __init__(self) -> None:
        self.logs: list[tuple[str, str]] = []

    async def log(self, message: str, /, *, stream: str = "stdout") -> None:
        self.logs.append((stream, message.strip()))

    async def is_cancelled(self) -> bool:  # pragma: no cover - protocol requirement
        return False

    async def set_progress(self, value: float, /, **meta):  # pragma: no cover
        self.logs.append(("progress", f"{value}:{meta}"))


@dataclass
class _Response:
    status_code: int
    headers: dict[str, str]
    text: str
    data: Any

    def json(self) -> Any:
        return self.data


def _patch_http_client(monkeypatch, responses: List[object]) -> None:
    class _Client:
        def __init__(self, *_, **__):
            self._responses = list(responses)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def request(self, *args, **kwargs):
            resp = self._responses.pop(0)
            if isinstance(resp, Exception):
                raise resp
            return resp

    monkeypatch.setattr("pyjobkit.executors.http.httpx.AsyncClient", _Client)


def test_http_executor_success(monkeypatch) -> None:
    async def _run() -> None:
        resp = _Response(200, {"content-type": "application/json"}, "{}", {"ok": True})
        _patch_http_client(monkeypatch, [resp])
        ctx = TestContext()
        executor = HttpExecutor()
        result = await executor.run(job_id=uuid4(), payload={"url": "https://example"}, ctx=ctx)
        assert result["status"] == 200
        assert ctx.logs[0][0] == "stdout"

    asyncio.run(_run())


def test_http_executor_retries_and_fails(monkeypatch) -> None:
    async def _run() -> None:
        exc = RuntimeError("boom")
        success = _Response(204, {"content-type": "text/plain"}, "ok", "ignored")
        _patch_http_client(monkeypatch, [exc, success])
        calls: list[float] = []

        async def fake_sleep(duration: float) -> None:
            calls.append(duration)

        monkeypatch.setattr("pyjobkit.executors.http.asyncio.sleep", fake_sleep)
        ctx = TestContext()
        executor = HttpExecutor()
        result = await executor.run(
            job_id=uuid4(),
            payload={"url": "https://example", "retries": 1, "backoff": 0.1},
            ctx=ctx,
        )
        assert result["status"] == 204
        assert any(stream == "stderr" for stream, _ in ctx.logs)
        assert calls == [0.1]

        _patch_http_client(monkeypatch, [RuntimeError("nope")])
        with pytest.raises(RuntimeError):
            await executor.run(job_id=uuid4(), payload={"url": "https://example"}, ctx=ctx)

    asyncio.run(_run())


def test_subprocess_executor_exec_and_shell() -> None:
    async def _run() -> None:
        ctx = TestContext()
        executor = SubprocessExecutor()
        result = await executor.run(
            job_id=uuid4(),
            payload={"cmd": ["python3", "-c", "import sys;print('out');print('err', file=sys.stderr)"]},
            ctx=ctx,
        )
        assert result["returncode"] == 0
        assert any(stream == "stderr" for stream, _ in ctx.logs)

        ctx_shell = TestContext()
        result_shell = await executor.run(
            job_id=uuid4(),
            payload={"cmd": "python3 -c \"print('shell')\""},
            ctx=ctx_shell,
        )
        assert result_shell["returncode"] == 0
        assert any("shell" in message for _, message in ctx_shell.logs)

    asyncio.run(_run())


def test_subprocess_executor_cancels_process(tmp_path) -> None:
    async def _run() -> None:
        pid_file = tmp_path / "child.pid"
        ctx = TestContext()
        executor = SubprocessExecutor()

        task = asyncio.create_task(
            executor.run(
                job_id=uuid4(),
                payload={
                    "cmd": [
                        "python3",
                        "-c",
                        (
                            "import os, time, pathlib; "
                            f"pathlib.Path('{pid_file}').write_text(str(os.getpid())); "
                            "time.sleep(10)"
                        ),
                    ]
                },
                ctx=ctx,
            )
        )
        for _ in range(20):
            if pid_file.exists():
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("child pid file not created")

        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        pid = int(pid_file.read_text())
        # Allow a brief moment for the process to receive the signal.
        await asyncio.sleep(0.05)
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)

        assert any("terminating process" in message for _, message in ctx.logs)
        pump_tasks = [t for t in asyncio.all_tasks() if t.get_coro().__name__ == "_pump"]
        assert pump_tasks == []

    asyncio.run(_run())


def test_subprocess_executor_times_out_and_kills(tmp_path) -> None:
    async def _run() -> None:
        pid_file = tmp_path / "child_timeout.pid"
        ctx = TestContext()
        executor = SubprocessExecutor()

        async def _long_running() -> dict:
            return await executor.run(
                job_id=uuid4(),
                payload={
                    "cmd": [
                        "python3",
                        "-c",
                        (
                            "import os, time, pathlib; "
                            f"pathlib.Path('{pid_file}').write_text(str(os.getpid())); "
                            "time.sleep(10)"
                        ),
                    ]
                },
                ctx=ctx,
            )

        with pytest.raises(asyncio.TimeoutError):
            async with asyncio.timeout(0.2):
                await _long_running()

        for _ in range(20):
            if pid_file.exists():
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("child pid file not created")

        pid = int(pid_file.read_text())
        await asyncio.sleep(0.05)
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)

        assert any("terminating process" in message for _, message in ctx.logs)
        pump_tasks = [t for t in asyncio.all_tasks() if t.get_coro().__name__ == "_pump"]
        assert pump_tasks == []

    asyncio.run(_run())


def test_subprocess_executor_logs_nonzero_exit() -> None:
    async def _run() -> None:
        ctx = TestContext()
        executor = SubprocessExecutor()
        result = await executor.run(
            job_id=uuid4(),
            payload={"cmd": ["python3", "-c", "import sys; sys.exit(3)"]},
            ctx=ctx,
        )

        assert result["returncode"] == 3
        assert ("stderr", "process exited with code 3") in ctx.logs

    asyncio.run(_run())
