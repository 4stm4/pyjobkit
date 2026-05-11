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
from time import perf_counter
from typing import Any, Iterable
from uuid import UUID, uuid4

from typing import Awaitable, Callable

from .contracts import OptimisticLockError
from .engine import Engine, SHADOW_PAYLOAD_KEY, TAGS_PAYLOAD_KEY
from .ratelimit import RateLimitSpec, TokenBucket, parse_rate_limits
from .retry import (
    DEFAULT_RETRY_POLICY,
    RETRY_POLICY_PAYLOAD_KEY,
    RetryPolicy,
    parse_policy,
)
from .webhooks import WEBHOOK_PAYLOAD_KEY
from . import webhooks as _webhooks

LeaseLostCallback = Callable[[UUID, dict], Awaitable[None]]

logger = logging.getLogger(__name__)


class LeaseLostError(RuntimeError):
    """Raised when a worker loses its lease for a job."""


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
        retry_policy: RetryPolicy | str | None = None,
        watchdog_interval_s: float | None = None,
        rate_limits: dict | None = None,
        on_lease_lost: LeaseLostCallback | None = None,
        kinds: Iterable[str] | None = None,
        tags: Iterable[str] | None = None,
    ) -> None:
        self.engine = engine
        self.max_concurrency = max_concurrency
        self.batch = batch
        self.poll_interval = poll_interval
        self.lease_ttl = lease_ttl
        self.queue_capacity = queue_capacity
        self.stop_timeout = stop_timeout
        if watchdog_interval_s is not None and watchdog_interval_s <= 0:
            raise ValueError("watchdog_interval_s must be > 0")
        self.watchdog_interval_s = (
            float(watchdog_interval_s)
            if watchdog_interval_s is not None
            else float(lease_ttl)
        )
        if retry_policy is None:
            self.retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY
        elif isinstance(retry_policy, RetryPolicy):
            self.retry_policy = retry_policy
        else:
            self.retry_policy = parse_policy(retry_policy)
        normalized_limits = parse_rate_limits(rate_limits)
        self.rate_limits: dict[str, RateLimitSpec] = normalized_limits
        self._buckets: dict[str, TokenBucket] = {
            kind: TokenBucket(rate, burst)
            for kind, (rate, burst) in normalized_limits.items()
        }
        self.on_lease_lost = on_lease_lost
        self.kinds: frozenset[str] | None = (
            frozenset(kinds) if kinds is not None else None
        )
        self.tags: frozenset[str] | None = (
            frozenset(tags) if tags is not None else None
        )
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

    async def run(self, *, once: bool = False) -> None:
        """Run the worker loop.

        When ``once`` is set the worker drains the queue (claims jobs
        until ``claim_batch`` returns empty, processes them, repeats
        only while jobs were claimed) and then exits gracefully. This
        is useful for batch / cron-style invocations that should finish
        when there is nothing left to do.
        """

        backoff = self.poll_interval
        try:
            async with asyncio.TaskGroup() as tg:
                if not once:
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

                    if self.kinds is not None or self.tags is not None:
                        accepted, rejected = self._partition_by_filter(rows)
                        rows = accepted
                        for skipped in rejected:
                            try:
                                await self.engine.retry(
                                    skipped["id"]
                                    if isinstance(skipped["id"], UUID)
                                    else UUID(str(skipped["id"])),
                                    delay=0,
                                )
                            except Exception as exc:  # pragma: no cover - defensive
                                logger.warning(
                                    "failed to release %s back to queue: %s",
                                    skipped.get("id"),
                                    exc,
                                )

                    if not rows:
                        if once:
                            break
                        await asyncio.sleep(self._jitter(self.poll_interval))
                        continue

                    for row in rows:
                        await self._sem.acquire()
                        self._increment_active()
                        task = tg.create_task(self._run_row(row))
                        self._tasks.add(task)
                        task.add_done_callback(self._tasks.discard)
        except asyncio.CancelledError:
            raise
        finally:
            try:
                await self._wait_for_drain()
            finally:
                self._stopped.set()

    def _partition_by_filter(
        self, rows: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        accepted: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        for row in rows:
            if self.kinds is not None and row.get("kind") not in self.kinds:
                rejected.append(row)
                continue
            if self.tags is not None:
                payload = row.get("payload") or {}
                job_tags = set(payload.get(TAGS_PAYLOAD_KEY) or ())
                if not (job_tags & self.tags):
                    rejected.append(row)
                    continue
            accepted.append(row)
        return accepted, rejected

    async def _run_row(self, row: dict[str, Any]) -> None:
        try:
            await self._execute_row(row)
        finally:
            self._sem.release()
            self._decrement_active()

    async def _fire_webhook(
        self,
        webhooks_spec: dict[str, str] | None,
        *,
        status: str,
        job_id: UUID,
        kind: str,
        attempts: int,
        started_at: float | None,
        result: Any,
    ) -> None:
        if not webhooks_spec:
            return
        from time import perf_counter

        duration_ms = (
            round((perf_counter() - started_at) * 1000, 3)
            if started_at is not None
            else None
        )
        await _webhooks.fire(
            webhooks=webhooks_spec,
            status=status,
            job_id=job_id,
            kind=kind,
            attempts=attempts,
            duration_ms=duration_ms,
            result=result,
        )

    def _log_state(
        self,
        event: str,
        *,
        job_id: UUID,
        status: str,
        started_at: float | None,
        **extra: Any,
    ) -> None:
        payload: dict[str, Any] = {
            "event": event,
            "job_id": str(job_id),
            "worker_id": str(self.worker_id),
            "status": status,
        }
        if started_at is not None:
            payload["duration_ms"] = round((perf_counter() - started_at) * 1000, 3)
        payload.update(extra)
        logger.info("job state changed", extra=payload)

    async def _execute_row(self, row: dict[str, Any]) -> None:
        job_id = UUID(row["id"]) if not isinstance(row["id"], UUID) else row["id"]
        executor = self.engine.executor_for(row["kind"])
        if executor is None:
            await self.engine.fail(job_id, {"error": "unknown_kind", "kind": row["kind"]})
            self._log_state(
                "job.failed",
                job_id=job_id,
                status="failed",
                started_at=None,
                reason="unknown_kind",
                kind=row["kind"],
            )
            return
        raw_payload = row["payload"]
        is_shadow = bool(raw_payload.get(SHADOW_PAYLOAD_KEY))
        webhooks_spec = raw_payload.get(WEBHOOK_PAYLOAD_KEY) or None
        per_job_policy_spec = raw_payload.get(RETRY_POLICY_PAYLOAD_KEY)
        retry_policy = self.retry_policy
        if per_job_policy_spec:
            try:
                retry_policy = parse_policy(per_job_policy_spec)
            except ValueError:
                logger.warning(
                    "ignoring invalid per-job retry policy %r for job %s",
                    per_job_policy_spec,
                    job_id,
                )
        payload = {
            k: v
            for k, v in raw_payload.items()
            if k not in (
                SHADOW_PAYLOAD_KEY,
                RETRY_POLICY_PAYLOAD_KEY,
                WEBHOOK_PAYLOAD_KEY,
                TAGS_PAYLOAD_KEY,
            )
        }
        ctx = self.engine.make_ctx(job_id, is_shadow=is_shadow)
        expected_version = row.get("version")
        lease_lost = asyncio.Event()
        lease_task = asyncio.create_task(
            self._extend_loop(job_id, expected_version, lease_lost)
        )
        cancel_task: asyncio.Task[None] | None = None
        started_at = perf_counter()
        self._log_state(
            "job.started",
            job_id=job_id,
            status="running",
            started_at=None,
            kind=row["kind"],
            shadow=is_shadow,
        )
        bucket = self._buckets.get(row["kind"])
        if bucket is not None:
            await bucket.acquire()
        try:
            await self.engine.mark_running(job_id, self.worker_id)
            timeout = row.get("timeout_s") or 300
            exec_task = asyncio.create_task(
                executor.run(job_id=job_id, payload=payload, ctx=ctx)
            )

            lease_watch = asyncio.create_task(lease_lost.wait())

            async def _watch_cancel() -> None:
                while True:
                    await asyncio.sleep(self.poll_interval)
                    if await ctx.is_cancelled():
                        exec_task.cancel()
                        return

            cancel_task = asyncio.create_task(_watch_cancel())
            async with asyncio.timeout(timeout):
                done, _ = await asyncio.wait(
                    {exec_task, lease_watch}, return_when=asyncio.FIRST_COMPLETED
                )
            if lease_watch in done:
                exec_task.cancel()
                raise LeaseLostError(job_id)
            lease_watch.cancel()
            with suppress(asyncio.CancelledError):
                await lease_watch
            result = await exec_task
            if await ctx.is_cancelled():
                self._log_state(
                    "job.cancelled",
                    job_id=job_id,
                    status="cancelled",
                    started_at=started_at,
                    note="completed after cancellation request",
                )
                await self.engine.cancel(job_id)
                return
            if is_shadow:
                shadow_result = {"shadow": True, "result_discarded": True}
                await self.engine.succeed(
                    job_id, shadow_result, expected_version=expected_version
                )
                self._log_state(
                    "job.shadow_succeeded",
                    job_id=job_id,
                    status="success",
                    started_at=started_at,
                    shadow=True,
                )
                await self._fire_webhook(
                    webhooks_spec,
                    status="success",
                    job_id=job_id,
                    kind=row["kind"],
                    attempts=(row.get("attempts") or 0) + 1,
                    started_at=started_at,
                    result=shadow_result,
                )
            else:
                await self.engine.succeed(
                    job_id, result, expected_version=expected_version
                )
                self._log_state(
                    "job.succeeded",
                    job_id=job_id,
                    status="success",
                    started_at=started_at,
                )
                await self._fire_webhook(
                    webhooks_spec,
                    status="success",
                    job_id=job_id,
                    kind=row["kind"],
                    attempts=(row.get("attempts") or 0) + 1,
                    started_at=started_at,
                    result=result,
                )
        except asyncio.TimeoutError:
            exec_task.cancel()
            with suppress(asyncio.CancelledError):
                await exec_task
            attempts = (row.get("attempts") or 0) + 1
            max_attempts = row.get("max_attempts", 3)
            if attempts >= max_attempts:
                await self.engine.timeout(job_id, expected_version=expected_version)
                self._log_state(
                    "job.timeout",
                    job_id=job_id,
                    status="timeout",
                    started_at=started_at,
                    attempts=attempts,
                )
                await self._fire_webhook(
                    webhooks_spec,
                    status="timeout",
                    job_id=job_id,
                    kind=row["kind"],
                    attempts=attempts,
                    started_at=started_at,
                    result={"error": "timeout"},
                )
            else:
                delay = retry_policy.delay(attempts)
                await self.engine.retry(job_id, delay=delay)
                self._log_state(
                    "job.retry",
                    job_id=job_id,
                    status="queued",
                    started_at=started_at,
                    attempts=attempts,
                    reason="timeout",
                    retry_delay_s=delay,
                )
        except LeaseLostError:
            self._log_state(
                "job.lease_lost",
                job_id=job_id,
                status="running",
                started_at=started_at,
            )
            if self.on_lease_lost is not None:
                try:
                    await self.on_lease_lost(job_id, dict(row))
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning(
                        "on_lease_lost callback raised for job %s: %s",
                        job_id,
                        exc,
                        exc_info=True,
                    )
            return
        except asyncio.CancelledError:
            self._log_state(
                "job.cancelled",
                job_id=job_id,
                status="cancelled",
                started_at=started_at,
            )
            await self.engine.cancel(job_id)
            return
        except OptimisticLockError as exc:
            self._log_state(
                "job.lock_conflict",
                job_id=job_id,
                status="running",
                started_at=started_at,
                detail=str(exc),
            )
            return
        except Exception as exc:  # pragma: no cover - defensive
            attempts = (row.get("attempts") or 0) + 1
            if attempts >= row.get("max_attempts", 3):
                fail_result = {"error": repr(exc)}
                await self.engine.fail(job_id, fail_result, expected_version=expected_version)
                self._log_state(
                    "job.failed",
                    job_id=job_id,
                    status="failed",
                    started_at=started_at,
                    attempts=attempts,
                    exception_type=type(exc).__name__,
                )
                await self._fire_webhook(
                    webhooks_spec,
                    status="failed",
                    job_id=job_id,
                    kind=row["kind"],
                    attempts=attempts,
                    started_at=started_at,
                    result=fail_result,
                )
            else:
                delay = retry_policy.delay(attempts)
                await self.engine.retry(job_id, delay=delay)
                self._log_state(
                    "job.retry",
                    job_id=job_id,
                    status="queued",
                    started_at=started_at,
                    attempts=attempts,
                    reason="exception",
                    exception_type=type(exc).__name__,
                    retry_delay_s=delay,
                )
        finally:
            if "lease_watch" in locals():
                lease_watch.cancel()
                with suppress(asyncio.CancelledError):
                    await lease_watch
            if cancel_task:
                cancel_task.cancel()
                with suppress(asyncio.CancelledError):
                    await asyncio.shield(cancel_task)
            lease_task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.shield(lease_task)

    async def _extend_loop(
        self,
        job_id: UUID,
        expected_version: int | None,
        lease_lost: asyncio.Event | None = None,
    ) -> None:
        if lease_lost is None:
            lease_lost = asyncio.Event()
        interval = self.lease_ttl * 0.5
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    await self.engine.extend_lease(
                        job_id,
                        self.worker_id,
                        self.lease_ttl,
                        expected_version=expected_version,
                    )
                except asyncio.CancelledError:
                    raise
                except OptimisticLockError:
                    lease_lost.set()
                    logger.info("Lost lease while extending job %s; stopping", job_id)
                    return
                except Exception as exc:  # pragma: no cover - defensive logging
                    logger.warning(
                        "extend_lease failed for job %s; retrying", job_id, exc_info=exc
                    )
        except asyncio.CancelledError:
            return

    async def _reap_loop(self) -> None:
        try:
            while True:
                try:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=self.watchdog_interval_s
                    )
                    return
                except asyncio.TimeoutError:
                    try:
                        reaped = await self.engine.reap_expired()
                        if reaped:
                            logger.info(
                                "watchdog reaped expired jobs",
                                extra={
                                    "event": "watchdog.reaped",
                                    "worker_id": str(self.worker_id),
                                    "count": reaped,
                                },
                            )
                    except Exception as exc:
                        logger.warning(
                            "reap_expired failed, continuing to retry: %s",
                            exc,
                            exc_info=True,
                        )
        except asyncio.CancelledError:
            logger.info("reap loop cancelled; shutting down")
            raise

    async def _wait_for_drain(self) -> None:
        self._stop.set()
        try:
            if self.stop_timeout is None:
                await self._active_jobs_zero.wait()
                return
            await asyncio.wait_for(self._active_jobs_zero.wait(), timeout=self.stop_timeout)
        except asyncio.TimeoutError:
            logger.error(
                "Worker shutdown timed out after %.2fs; %d tasks may still be running",
                self.stop_timeout,
                len(self._tasks),
            )
            for task in list(self._tasks):
                task.cancel()
        finally:
            try:
                if self._tasks:
                    results = await asyncio.gather(
                        *self._tasks, return_exceptions=True
                    )
                    for result in results:
                        if isinstance(result, asyncio.CancelledError):
                            logger.info("Task cancelled during shutdown")
                            continue
                        if isinstance(result, Exception):
                            logger.warning(
                                "Task raised during shutdown: %s",
                                result,
                                exc_info=result,
                            )
            finally:
                if not self._active_jobs_zero.is_set():
                    await self._active_jobs_zero.wait()
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
