"""Async worker implementation built on :mod:`asyncio` primitives.

The worker coordinates claiming jobs from a backend and executing them with a
bounded degree of concurrency. It provides cooperative shutdown hooks to allow
callers to signal termination and await graceful teardown via
:meth:`request_stop` and :meth:`wait_stopped`.
"""

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
        stop_timeout: float | None = 60,
    ) -> None:
        self.engine = engine
        self.max_concurrency = max_concurrency
        self.batch = batch
        self.poll_interval = poll_interval
        self.lease_ttl = lease_ttl
        self.queue_capacity = queue_capacity
        self.stop_timeout = stop_timeout
        self.worker_id = uuid4()
        self._stop = asyncio.Event()
        self._stopped = asyncio.Event()
        self._sem = asyncio.Semaphore(max_concurrency)
        self._tasks: set[Task[None]] = set()
        self._active_jobs = 0
        self._active_jobs_zero = asyncio.Event()
        self._active_jobs_zero.set()
        self._validate_configuration()
        self._claim_limit = min(self.batch, self.max_concurrency)
        if self.batch > self.max_concurrency:
            logger.debug(
                "batch size %s exceeds concurrency %s; limiting claims per poll",
                self.batch,
                self.max_concurrency,
            )
        backend_lease_ttl = getattr(self.engine.backend, "lease_ttl_s", None)
        if backend_lease_ttl is not None and backend_lease_ttl != self.lease_ttl:
            logger.warning(
                "Worker lease_ttl=%s differs from backend lease_ttl_s=%s; renewals may drift",
                self.lease_ttl,
                backend_lease_ttl,
            )

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
                        rows = await self.engine.claim_batch(
                            self.worker_id, limit=self._claim_limit
                        )
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
            try:
                await self._wait_for_drain()
            finally:
                self._stopped.set()

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
            return
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
                    try:
                        await self.engine.reap_expired()
                    except Exception as exc:
                        logger.warning(
                            "reap_expired failed, continuing to retry: %s",
                            exc,
                            exc_info=True,
                        )
        except asyncio.CancelledError:
            return

    async def _drain(self) -> None:
        self._stop.set()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            results = await asyncio.gather(*self._tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception) and not isinstance(
                    result, asyncio.CancelledError
                ):
                    logger.warning("Task raised during shutdown: %s", result, exc_info=result)
        await self._active_jobs_zero.wait()

    async def _wait_for_drain(self) -> None:
        drain_task = asyncio.create_task(self._drain())
        try:
            if self.stop_timeout is None:
                await asyncio.shield(drain_task)
            else:
                await asyncio.wait_for(asyncio.shield(drain_task), timeout=self.stop_timeout)
        except asyncio.TimeoutError:
            logger.error(
                "Worker shutdown timed out after %.2fs; %d tasks may still be running",
                self.stop_timeout,
                len(self._tasks),
            )
        finally:
            drain_task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.shield(drain_task)

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

    def _validate_configuration(self) -> None:
        if self.max_concurrency <= 0:
            raise ValueError("max_concurrency must be greater than 0")
        if self.batch <= 0:
            raise ValueError("batch must be greater than 0")
        if self.poll_interval <= 0:
            raise ValueError("poll_interval must be greater than 0")
        if self.lease_ttl <= 0:
            raise ValueError("lease_ttl must be greater than 0")
        if self.queue_capacity is not None and self.queue_capacity <= 0:
            raise ValueError("queue_capacity must be greater than 0 when provided")
        if self.stop_timeout is not None and self.stop_timeout <= 0:
            raise ValueError("stop_timeout must be greater than 0 when provided")
