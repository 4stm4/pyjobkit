"""Async worker implementation built on asyncio.TaskGroup."""

from __future__ import annotations

import asyncio
import logging
import random
from asyncio import Task
from contextlib import suppress
from typing import Any
from uuid import UUID, uuid4

from .engine import Engine

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        engine: Engine,
        *,
        max_concurrency: int = 8,
        batch: int = 1,
        poll_interval: float = 0.5,
        lease_ttl: int = 30,
        queue_capacity: int | None = None,
    ) -> None:
        self.engine = engine
        self.max_concurrency = max_concurrency
        self.batch = batch
        self.poll_interval = poll_interval
        self.lease_ttl = lease_ttl
        self.queue_capacity = queue_capacity
        self.worker_id = uuid4()
        self._stop = asyncio.Event()
        self._stopped = asyncio.Event()
        self._sem = asyncio.Semaphore(max_concurrency)
        self._tasks: set[Task[None]] = set()
        self._active_jobs = 0
        self._active_jobs_zero = asyncio.Event()
        self._active_jobs_zero.set()

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return (
            f"Worker(id={self.worker_id}, concurrency={self.max_concurrency}, "
            f"batch={self.batch})"
        )

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
                        rows = await self.engine.claim_batch(self.worker_id, limit=self.batch)
                        backoff = self.poll_interval
                    except Exception as exc:
                        logger.warning(
                            "claim_batch failed, backing off for %.2fs: %s",
                            backoff,
                            exc,
                            exc_info=True,
                        )
                        await asyncio.sleep(self._jitter(backoff))
                        backoff = min(backoff * 2, 30)
                        continue

                    if not rows:
                        await asyncio.sleep(self._jitter(self.poll_interval))
                        continue

                    for row in rows:
                        await self._sem.acquire()
                        self._increment_active()
                        task = tg.create_task(self._run_row(row))
                        self._tasks.add(task)
                        task.add_done_callback(self._tasks.discard)
        except asyncio.CancelledError:
            self._stop.set()
            raise
        finally:
            await asyncio.wait_for(self._drain(), timeout=60)

    async def _run_row(self, row: dict[str, Any]) -> None:
        try:
            await self._execute_row(row)
        finally:
            self._sem.release()
            self._decrement_active()

    async def _execute_row(self, row: dict[str, Any]) -> None:
        job_id = UUID(row["id"]) if not isinstance(row["id"], UUID) else row["id"]
        executor = self.engine.executor_for(row["kind"])
        if executor is None:
            await self.engine.fail(job_id, {"error": "unknown_kind", "kind": row["kind"]})
            return
        ctx = self.engine.make_ctx(job_id)
        expected_version = row.get("version")
        lease_task = asyncio.create_task(self._extend_loop(job_id, expected_version))
        try:
            await self.engine.mark_running(job_id, self.worker_id)
            timeout = row.get("timeout_s") or 300
            async with asyncio.timeout(timeout):
                result = await executor.run(job_id=job_id, payload=row["payload"], ctx=ctx)
            await self.engine.succeed(job_id, result, expected_version=expected_version)
        except asyncio.TimeoutError:
            await self.engine.timeout(job_id, expected_version=expected_version)
        except asyncio.CancelledError:
            await self.engine.fail(job_id, {"error": "cancelled"}, expected_version=expected_version)
            raise
        except Exception as exc:  # pragma: no cover - defensive
            attempts = (row.get("attempts") or 0) + 1
            if attempts >= row.get("max_attempts", 3):
                await self.engine.fail(job_id, {"error": repr(exc)}, expected_version=expected_version)
            else:
                await self.engine.retry(job_id, delay=2**(attempts - 1))
        finally:
            lease_task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.shield(lease_task)

    async def _extend_loop(self, job_id: UUID, expected_version: int | None) -> None:
        try:
            while True:
                await asyncio.sleep(self.lease_ttl * 0.5)
                await self.engine.extend_lease(
                    job_id,
                    self.worker_id,
                    self.lease_ttl,
                    expected_version=expected_version,
                )
        except asyncio.CancelledError:
            return

    async def _reap_loop(self) -> None:
        try:
            while True:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self.lease_ttl)
                    return
                except asyncio.TimeoutError:
                    with suppress(Exception):
                        await self.engine.reap_expired()
        except asyncio.CancelledError:
            return

    async def _drain(self) -> None:
        self._stop.set()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            with suppress(Exception):
                await asyncio.gather(*self._tasks, return_exceptions=True)
        try:
            await self._active_jobs_zero.wait()
        finally:
            self._stopped.set()

    async def check_health(self) -> dict[str, Any]:
        try:
            await self.engine.check_connection()
        except Exception as exc:
            return {"status": "unhealthy", "reason": repr(exc)}

        depth = await self.engine.queue_depth()
        overflow = self.queue_capacity is not None and depth > self.queue_capacity
        status = "unhealthy" if overflow else "healthy"
        return {
            "status": status,
            "queue_depth": depth,
            "queue_overflow": overflow,
        }

    @staticmethod
    def _jitter(value: float) -> float:
        return value * (0.8 + random.random() * 0.4)

    def _increment_active(self) -> None:
        self._active_jobs += 1
        if self._active_jobs == 1:
            self._active_jobs_zero.clear()

    def _decrement_active(self) -> None:
        self._active_jobs -= 1
        if self._active_jobs == 0:
            self._active_jobs_zero.set()
