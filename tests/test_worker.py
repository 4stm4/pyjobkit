"""Extensive tests for the asynchronous worker implementation."""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import suppress
from datetime import datetime
from typing import Any, Awaitable, Callable
from uuid import UUID, uuid4

import pytest

from pyjobkit.worker import Worker
from pyjobkit.executors.subprocess import SubprocessExecutor
from pyjobkit.contracts import ExecContext, Executor, QueueBackend


class _DummyCtx(ExecContext):
    def __init__(self) -> None:
        self.logs: list[tuple[str, str]] = []

    async def log(self, message: str, /, *, stream: str = "stdout") -> None:
        self.logs.append((stream, message.strip()))

    async def is_cancelled(self) -> bool:  # pragma: no cover - protocol requirement
        return False

    async def set_progress(self, value: float, /, **meta: Any) -> None:  # pragma: no cover
        self.logs.append(("progress", f"{value}:{meta}"))


class _Executor(Executor):
    def __init__(self, *, kind: str, behavior: Callable[[UUID, dict, ExecContext], Awaitable[dict]]):
        self.kind = kind
        self.behavior = behavior
        self.calls: list[tuple[UUID, dict]] = []

    async def run(self, *, job_id: UUID, payload: dict, ctx: ExecContext) -> dict:
        self.calls.append((job_id, payload))
        return await self.behavior(job_id, payload, ctx)


class _Backend(QueueBackend):
    def __init__(self) -> None:
        self.rows: list[list[QueueBackend.ClaimedJob]] = []
        self.marked: list[UUID] = []
        self.succeeded: list[tuple[UUID, dict, int | None]] = []
        self.failed: list[tuple[UUID, dict, int | None]] = []
        self.timed_out: list[tuple[UUID, int | None]] = []
        self.retried: list[tuple[UUID, float]] = []
        self.extended: list[UUID] = []
        self.reaped: int = 0
        self.claim_limits: list[int] = []

    async def enqueue(
        self,
        *,
        kind: str,
        payload: dict,
        priority: int = 100,
        scheduled_for: datetime | None = None,
        max_attempts: int = 3,
        idempotency_key: str | None = None,
        timeout_s: int | None = None,
    ) -> UUID:
        raise NotImplementedError

    async def get(self, job_id: UUID) -> dict:
        raise NotImplementedError

    async def cancel(self, job_id: UUID) -> None:
        raise NotImplementedError

    async def is_cancelled(self, job_id: UUID) -> bool:
        return False

    async def claim_batch(
        self, worker_id: UUID, *, limit: int = 1
    ) -> list[QueueBackend.ClaimedJob]:
        self.claim_limits.append(limit)
        return self.rows.pop(0) if self.rows else []

    async def mark_running(self, job_id: UUID, worker_id: UUID) -> None:
        self.marked.append(job_id)

    async def extend_lease(
        self,
        job_id: UUID,
        worker_id: UUID,
        ttl_s: int,
        *,
        expected_version: int | None = None,
    ) -> None:
        self.extended.append((job_id, expected_version))

    async def succeed(
        self, job_id: UUID, result: dict, *, expected_version: int | None = None
    ) -> None:
        self.succeeded.append((job_id, result, expected_version))

    async def fail(
        self, job_id: UUID, reason: dict, *, expected_version: int | None = None
    ) -> None:
        self.failed.append((job_id, reason, expected_version))

    async def timeout(self, job_id: UUID, *, expected_version: int | None = None) -> None:
        self.timed_out.append((job_id, expected_version))

    async def retry(self, job_id: UUID, *, delay: float) -> None:
        self.retried.append((job_id, delay))

    async def reap_expired(self) -> int:
        self.reaped += 1
        return self.reaped

    async def check_connection(self) -> None:
        return None

    async def queue_depth(self) -> int:
        return sum(len(batch) for batch in self.rows)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_concurrency": 0},
        {"batch": 0},
        {"poll_interval": 0},
        {"lease_ttl": 0},
        {"queue_capacity": 0},
        {"stop_timeout": 0},
    ],
)
def test_worker_rejects_invalid_configuration(kwargs) -> None:
    with pytest.raises(ValueError):
        Worker(_Engine(_Backend(), []), **kwargs)


class _Engine:
    def __init__(self, backend: _Backend, executors: list[_Executor]):
        self.backend = backend
        self._executors = {exe.kind: exe for exe in executors}
        self.contexts: dict[UUID, _DummyCtx] = {}

    def executor_for(self, kind: str):
        return self._executors.get(kind)

    def make_ctx(self, job_id: UUID) -> _DummyCtx:
        ctx = _DummyCtx()
        self.contexts[job_id] = ctx
        return ctx

    async def claim_batch(
        self, worker_id: UUID, *, limit: int = 1
    ) -> list[QueueBackend.ClaimedJob]:
        return await self.backend.claim_batch(worker_id, limit=limit)

    async def mark_running(self, job_id: UUID, worker_id: UUID) -> None:
        await self.backend.mark_running(job_id, worker_id)

    async def extend_lease(
        self,
        job_id: UUID,
        worker_id: UUID,
        ttl_s: int,
        *,
        expected_version: int | None = None,
    ) -> None:
        await self.backend.extend_lease(
            job_id, worker_id, ttl_s, expected_version=expected_version
        )

    async def succeed(self, job_id: UUID, result: dict, *, expected_version: int | None = None) -> None:
        await self.backend.succeed(job_id, result, expected_version=expected_version)

    async def fail(self, job_id: UUID, reason: dict, *, expected_version: int | None = None) -> None:
        await self.backend.fail(job_id, reason, expected_version=expected_version)

    async def timeout(self, job_id: UUID, *, expected_version: int | None = None) -> None:
        await self.backend.timeout(job_id, expected_version=expected_version)

    async def retry(self, job_id: UUID, *, delay: float) -> None:
        await self.backend.retry(job_id, delay=delay)

    async def reap_expired(self) -> int:
        return await self.backend.reap_expired()

    async def check_connection(self) -> None:
        await self.backend.check_connection()

    async def queue_depth(self) -> int:
        return await self.backend.queue_depth()


def test_worker_run_processes_rows(monkeypatch) -> None:
    async def _run() -> None:
        backend = _Backend()
        worker = Worker(_Engine(backend, []), max_concurrency=1, batch=1, poll_interval=0.01, lease_ttl=0.02)
        job_id = uuid4()

        async def behavior(job_id, payload, ctx):
            await asyncio.sleep(0.03)
            await ctx.log("done")
            worker.request_stop()
            return {"ok": True}

        executor = _Executor(kind="demo", behavior=behavior)
        worker.engine._executors = {"demo": executor}
        backend.rows = [[], [{"id": str(job_id), "kind": "demo", "payload": {}}]]

        await asyncio.wait_for(worker.run(), timeout=1)
        assert backend.succeeded == [(job_id, {"ok": True}, None)]
        assert backend.extended  # lease loop kicked in

    monkeypatch.setattr("pyjobkit.worker.random.random", lambda: 0.0)
    asyncio.run(_run())


def test_worker_limits_claims_to_concurrency(monkeypatch) -> None:
    async def _run() -> None:
        backend = _Backend()
        worker = Worker(_Engine(backend, []), max_concurrency=2, batch=5, poll_interval=0.01)
        backend.rows = [[{"id": str(uuid4()), "kind": "demo", "payload": {}}]]

        async def behavior(job_id, payload, ctx):
            worker.request_stop()
            return {"ok": True}

        executor = _Executor(kind="demo", behavior=behavior)
        worker.engine._executors = {"demo": executor}

        monkeypatch.setattr("pyjobkit.worker.random.random", lambda: 0.0)
        await asyncio.wait_for(worker.run(), timeout=1)
        assert backend.claim_limits[0] == 2

    asyncio.run(_run())


def test_worker_execute_unknown_executor() -> None:
    async def _run() -> None:
        backend = _Backend()
        worker = Worker(_Engine(backend, []))
        job_id = uuid4()
        await worker._execute_row({"id": job_id, "kind": "nope", "payload": {}})
        assert backend.failed == [
            (job_id, {"error": "unknown_kind", "kind": "nope"}, None)
        ]

    asyncio.run(_run())


def test_worker_execute_timeout() -> None:
    async def _run() -> None:
        backend = _Backend()

        async def slow(job_id, payload, ctx):
            await asyncio.sleep(0.05)
            return {"ok": False}

        executor = _Executor(kind="slow", behavior=slow)
        worker = Worker(_Engine(backend, [executor]), lease_ttl=1)
        await worker._execute_row({"id": uuid4(), "kind": "slow", "payload": {}, "timeout_s": 0.01})
        assert backend.timed_out

    asyncio.run(_run())


def test_worker_kills_subprocess_on_timeout(tmp_path) -> None:
    async def _run() -> None:
        backend = _Backend()
        executor = SubprocessExecutor()
        worker = Worker(_Engine(backend, [executor]), lease_ttl=1)
        pid_file = tmp_path / "child.pid"

        await worker._execute_row(
            {
                "id": uuid4(),
                "kind": executor.kind,
                "payload": {
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
                    # Allow enough time for the child process to start and write the PID
                    # file before the worker times out the job.
                    "timeout_s": 0.2,
                }
            )

        if not pid_file.exists():
            pytest.fail("child pid file not created")

        pid = int(pid_file.read_text())
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)

    asyncio.run(_run())


def test_worker_execute_cancelled_error() -> None:
    async def _run() -> None:
        backend = _Backend()

        async def cancelled(job_id, payload, ctx):
            raise asyncio.CancelledError

        executor = _Executor(kind="boom", behavior=cancelled)
        worker = Worker(_Engine(backend, [executor]))
        await worker._execute_row({"id": uuid4(), "kind": "boom", "payload": {}})
        assert backend.failed[0][1] == {"error": "cancelled"}

    asyncio.run(_run())


def test_worker_execute_generic_exception() -> None:
    async def _run() -> None:
        backend = _Backend()

        async def boom(job_id, payload, ctx):
            raise RuntimeError("boom")

        executor = _Executor(kind="boom", behavior=boom)
        worker = Worker(_Engine(backend, [executor]))
        await worker._execute_row(
            {"id": uuid4(), "kind": "boom", "payload": {}, "attempts": 2, "max_attempts": 2}
        )
        assert backend.failed[-1][1]["error"].startswith("RuntimeError")

    asyncio.run(_run())


def test_worker_run_row_releases_semaphore_on_error() -> None:
    async def _run() -> None:
        backend = _Backend()
        worker = Worker(_Engine(backend, []), max_concurrency=1)

        async def _boom(row):
            raise RuntimeError("boom")

        worker._execute_row = _boom  # type: ignore[assignment]
        await worker._sem.acquire()
        with pytest.raises(RuntimeError):
            await worker._run_row({})
        assert worker._sem._value == 1  # type: ignore[attr-defined]

    asyncio.run(_run())


def test_worker_extend_loop_can_be_cancelled() -> None:
    async def _run() -> None:
        backend = _Backend()
        worker = Worker(_Engine(backend, []), lease_ttl=0.01)
        task = asyncio.create_task(worker._extend_loop(uuid4(), None))
        await asyncio.sleep(0.03)
        task.cancel()
        await task

    asyncio.run(_run())


def test_worker_jitter_range() -> None:
    value = Worker._jitter(10.0)
    assert 8.0 <= value <= 12.0


def test_worker_wait_stopped_signals(monkeypatch) -> None:
    async def _run() -> None:
        backend = _Backend()
        worker = Worker(_Engine(backend, []), poll_interval=0.01)

        worker_task = asyncio.create_task(worker.run())
        worker.request_stop()

        await asyncio.wait_for(worker_task, timeout=1)
        await asyncio.wait_for(worker.wait_stopped(), timeout=1)

    monkeypatch.setattr("pyjobkit.worker.random.random", lambda: 0.0)
    asyncio.run(_run())


def test_worker_wait_stopped_after_cancel(monkeypatch) -> None:
    async def _run() -> None:
        backend = _Backend()
        worker = Worker(_Engine(backend, []), poll_interval=0.01)

        worker_task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.02)

        worker_task.cancel()
        with suppress(asyncio.CancelledError):
            await worker_task

        await asyncio.wait_for(worker.wait_stopped(), timeout=1)

    monkeypatch.setattr("pyjobkit.worker.random.random", lambda: 0.0)
    asyncio.run(_run())


def test_worker_logs_claim_batch_errors(monkeypatch, caplog) -> None:
    class _FlakyBackend(_Backend):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        async def claim_batch(
            self, worker_id: UUID, *, limit: int = 1
        ) -> list[QueueBackend.ClaimedJob]:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("db down")
            return []

    async def _run() -> None:
        backend = _FlakyBackend()
        worker = Worker(_Engine(backend, []), poll_interval=0.01)

        caplog.set_level(logging.WARNING, logger="pyjobkit.worker")

        worker_task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.05)
        worker.request_stop()
        await asyncio.wait_for(worker_task, timeout=1)

    monkeypatch.setattr("pyjobkit.worker.random.random", lambda: 0.0)
    asyncio.run(_run())

    warnings = [rec for rec in caplog.records if rec.levelno == logging.WARNING]
    assert any("claim_batch failed" in rec.getMessage() for rec in warnings)
