"""Async worker implementation built on asyncio.TaskGroup."""

from __future__ import annotations

import asyncio
import random
from asyncio import Task
from contextlib import suppress
from typing import Any
from uuid import UUID, uuid4

from .engine import Engine


class Worker:
    def __init__(
        self,
        engine: Engine,
        *,
        max_concurrency: int = 8,
        batch: int = 1,
        poll_interval: float = 0.5,
        lease_ttl: int = 30,
    ) -> None:
        self.engine = engine
        self.max_concurrency = max_concurrency
        self.batch = batch
        self.poll_interval = poll_interval
        self.lease_ttl = lease_ttl
        self.worker_id = uuid4()
        self._stop = asyncio.Event()
        self._stopped = asyncio.Event()
        self._sem = asyncio.Semaphore(max_concurrency)
        self._tasks: set[Task[None]] = set()

    def request_stop(self) -> None:
        self._stop.set()

    async def wait_stopped(self) -> None:
        """Wait until the worker finishes processing current tasks."""

        await self._stopped.wait()

    async def run(self) -> None:
        backoff = self.poll_interval
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._reap_loop())
                while not self._stop.is_set():
                    try:
                        rows = await self.engine.backend.claim_batch(
                            self.worker_id, limit=self.batch
                        )
                        backoff = self.poll_interval
                    except Exception:
                        await asyncio.sleep(self._jitter(backoff))
                        backoff = min(backoff * 2, 30)
                        continue

                    if not rows:
                        await asyncio.sleep(self._jitter(self.poll_interval))
                        continue

                    for row in rows:
                        await self._sem.acquire()
                        task = tg.create_task(self._run_row(row))
                        self._tasks.add(task)
                        task.add_done_callback(self._tasks.discard)
                tg.cancel_scope.cancel()
        finally:
            await asyncio.wait_for(self._drain(), timeout=60)

    async def _run_row(self, row: dict[str, Any]) -> None:
        try:
            await self._execute_row(row)
        finally:
            self._sem.release()

    async def _execute_row(self, row: dict[str, Any]) -> None:
        job_id = UUID(row["id"]) if not isinstance(row["id"], UUID) else row["id"]
        executor = self.engine.executor_for(row["kind"])
        if executor is None:
            await self.engine.backend.fail(job_id, {"error": "unknown_kind", "kind": row["kind"]})
            return
        ctx = self.engine.make_ctx(job_id)
        lease_task = asyncio.create_task(self._extend_loop(job_id))
        try:
            await self.engine.backend.mark_running(job_id, self.worker_id)
            timeout = row.get("timeout_s") or 300
            async with asyncio.timeout(timeout):
                result = await executor.run(job_id=job_id, payload=row["payload"], ctx=ctx)
            await self.engine.backend.succeed(job_id, result)
        except asyncio.TimeoutError:
            await self.engine.backend.timeout(job_id)
        except asyncio.CancelledError:
            await self.engine.backend.fail(job_id, {"error": "cancelled"})
            raise
        except Exception as exc:  # pragma: no cover - defensive
            attempts = (row.get("attempts") or 0) + 1
            if attempts >= row.get("max_attempts", 3):
                await self.engine.backend.fail(job_id, {"error": repr(exc)})
            else:
                await self.engine.backend.retry(job_id, delay=2**(attempts - 1))
        finally:
            lease_task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.shield(lease_task)

    async def _extend_loop(self, job_id: UUID) -> None:
        try:
            while True:
                await asyncio.sleep(self.lease_ttl * 0.5)
                await self.engine.backend.extend_lease(job_id, self.worker_id, self.lease_ttl)
        except asyncio.CancelledError:
            return

    async def _reap_loop(self) -> None:
        try:
            while not self._stop.is_set():
                await asyncio.sleep(self.lease_ttl)
                with suppress(Exception):
                    await self.engine.backend.reap_expired()
        except asyncio.CancelledError:
            return

    async def _drain(self) -> None:
        self._stop.set()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            with suppress(Exception):
                await asyncio.gather(*self._tasks, return_exceptions=True)
        while self._sem._value < self.max_concurrency:  # type: ignore[attr-defined]
            await asyncio.sleep(0.05)

    @staticmethod
    def _jitter(value: float) -> float:
        return value * (0.8 + random.random() * 0.4)
