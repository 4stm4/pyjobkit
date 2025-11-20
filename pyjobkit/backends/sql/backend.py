"""SQL backend implemented with SQLAlchemy async sessions."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import logging
from time import perf_counter
from typing import Any, Awaitable, Callable, Iterable, List
from uuid import UUID, uuid4

from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlalchemy.exc import DBAPIError

from .schema import JobTasks
from ...contracts import OptimisticLockError, QueueBackend
from ... import metrics

UTC = timezone.utc
logger = logging.getLogger(__name__)


class SQLBackend(QueueBackend):
    def __init__(
        self,
        engine: AsyncEngine,
        *,
        prefer_pg_skip_locked: bool = True,
        lease_ttl_s: int = 30,
    ) -> None:
        self.engine = engine
        self.prefer_pg_skip_locked = prefer_pg_skip_locked
        self.lease_ttl_s = lease_ttl_s
        self.sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        self._warned_about_skip_locked = False

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return (
            "SQLBackend("
            f"engine={self.engine.url!s}, "
            f"prefer_pg_skip_locked={self.prefer_pg_skip_locked}, "
            f"lease_ttl_s={self.lease_ttl_s}"
            ")"
        )

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
        job_id = uuid4()
        now = datetime.now(UTC)
        values = dict(
            id=str(job_id),
            status="queued",
            created_at=now,
            scheduled_for=scheduled_for or now,
            max_attempts=max_attempts,
            priority=priority,
            kind=kind,
            payload=payload,
            idempotency_key=idempotency_key,
            timeout_s=timeout_s,
        )
        async with self.sessionmaker() as session:
            await session.execute(JobTasks.insert().values(**values))
            await session.commit()
        return job_id

    async def get(self, job_id: UUID) -> dict:
        async with self.sessionmaker() as session:
            row = (
                await session.execute(select(JobTasks).where(JobTasks.c.id == str(job_id)))
            ).mappings().first()
            if not row:
                raise KeyError(job_id)
            return self._row_to_dict(row)

    async def cancel(self, job_id: UUID) -> None:
        async with self.sessionmaker() as session:
            await session.execute(
                update(JobTasks).where(JobTasks.c.id == str(job_id)).values(cancel_requested=True)
            )
            await session.commit()

    async def is_cancelled(self, job_id: UUID) -> bool:
        async with self.sessionmaker() as session:
            row = (
                await session.execute(
                    select(JobTasks.c.cancel_requested).where(JobTasks.c.id == str(job_id))
                )
            ).first()
            return bool(row and row.cancel_requested)

    async def claim_batch(
        self, worker_id: UUID, *, limit: int = 1
    ) -> List[QueueBackend.ClaimedJob]:
        if self.engine.dialect.name == "postgresql" and self.prefer_pg_skip_locked:
            rows = await self._claim_pg(worker_id, limit)
        else:
            if not getattr(self, "_warned_about_skip_locked", False):
                logger.warning(
                    "Running without SKIP LOCKED; concurrent workers may double-claim jobs"
                )
                self._warned_about_skip_locked = True
            rows = await self._claim_generic(worker_id, limit)
        return [self._row_to_dict(row) for row in rows]

    async def _claim_pg(self, worker_id: UUID, limit: int) -> Iterable[dict]:
        sql = text(
            """
            WITH picked AS (
                SELECT id FROM job_tasks
                WHERE status='queued' AND scheduled_for <= NOW()
                ORDER BY priority ASC, created_at ASC
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
            )
            UPDATE job_tasks jt
               SET leased_by=:worker,
                   lease_until=NOW() + (:ttl * interval '1 second'),
                   version=jt.version+1
             FROM picked
            WHERE jt.id = picked.id
            RETURNING jt.*;
            """
        )
        async with self.sessionmaker() as session:
            rows = (
                await session.execute(
                    sql,
                    {"worker": str(worker_id), "ttl": self.lease_ttl_s, "limit": limit},
                )
            ).mappings().all()
            await session.commit()
            return rows

    async def _claim_generic(self, worker_id: UUID, limit: int) -> Iterable[dict]:
        picked: list[dict] = []
        async with self.sessionmaker() as session:
            for _ in range(limit):
                query = (
                    select(JobTasks)
                    .where(JobTasks.c.status == "queued")
                    .where(JobTasks.c.scheduled_for <= func.now())
                    .where(
                        (JobTasks.c.lease_until.is_(None))
                        | (JobTasks.c.lease_until <= func.now())
                    )
                    .order_by(JobTasks.c.priority.asc(), JobTasks.c.created_at.asc())
                    .limit(1)
                )
                row = (await session.execute(query)).mappings().first()
                if not row:
                    break
                job_id = row["id"]
                lease_until = datetime.now(UTC) + timedelta(seconds=self.lease_ttl_s)
                stmt = (
                    update(JobTasks)
                    .where(JobTasks.c.id == job_id)
                    .where(JobTasks.c.version == row["version"])
                    .values(
                        leased_by=str(worker_id),
                        lease_until=lease_until,
                        version=JobTasks.c.version + 1,
                    )
                )
                res = await session.execute(stmt)
                if res.rowcount:
                    await session.commit()
                    picked.append(row)
                else:
                    await session.rollback()
            return picked

    async def mark_running(self, job_id: UUID, worker_id: UUID) -> None:
        async with self.sessionmaker() as session:
            await session.execute(
                update(JobTasks)
                .where(JobTasks.c.id == str(job_id))
                .values(
                    status="running",
                    started_at=datetime.now(UTC),
                    attempts=JobTasks.c.attempts + 1,
                )
            )
            await session.commit()

    async def extend_lease(
        self,
        job_id: UUID,
        worker_id: UUID,
        ttl_s: int,
        *,
        expected_version: int | None = None,
    ) -> None:
        start = perf_counter()

        async def _op() -> None:
            async with self.sessionmaker() as session:
                try:
                    stmt = (
                        update(JobTasks)
                        .where(JobTasks.c.id == str(job_id))
                        .where(JobTasks.c.leased_by == str(worker_id))
                    )
                    if expected_version is not None:
                        stmt = stmt.where(JobTasks.c.version == expected_version)
                    res = await session.execute(
                        stmt.values(lease_until=datetime.now(UTC) + timedelta(seconds=ttl_s))
                    )
                    if expected_version is not None and res.rowcount == 0:
                        raise OptimisticLockError(
                            f"extend_lease failed for {job_id} due to version mismatch"
                        )
                    await session.commit()
                except Exception:
                    await session.rollback()
                    raise

        try:
            await self._retry_with_backoff(_op)
        except OptimisticLockError:
            metrics.extend_lease_conflicts.inc()
            logger.warning(
                "Version mismatch on extend_lease for job %s by worker %s (expected_version=%s)",
                job_id,
                worker_id,
                expected_version,
            )
            raise
        finally:
            metrics.extend_lease_latency.observe(perf_counter() - start)
            metrics.lease_ttl_seconds.observe(ttl_s)

    async def succeed(
        self, job_id: UUID, result: dict, *, expected_version: int | None = None
    ) -> None:
        await self._finish(job_id, "success", result, expected_version=expected_version)

    async def fail(
        self, job_id: UUID, reason: dict, *, expected_version: int | None = None
    ) -> None:
        await self._finish(job_id, "failed", reason, expected_version=expected_version)

    async def timeout(
        self, job_id: UUID, *, expected_version: int | None = None
    ) -> None:
        await self._finish(
            job_id, "timeout", {"error": "timeout"}, expected_version=expected_version
        )

    async def retry(self, job_id: UUID, *, delay: float) -> None:
        retry_at = datetime.now(UTC) + timedelta(seconds=delay)
        async with self.sessionmaker() as session:
            await session.execute(
                update(JobTasks)
                .where(JobTasks.c.id == str(job_id))
                .where(JobTasks.c.status == "failed")
                .values(
                    status="queued",
                    scheduled_for=retry_at,
                    lease_until=None,
                    leased_by=None,
                    version=JobTasks.c.version + 1,
                )
            )
            await session.commit()

    async def reap_expired(self) -> int:
        now = datetime.now(UTC)
        async with self.sessionmaker() as session:
            expired_rows = (
                await session.execute(
                    select(JobTasks.c.id, JobTasks.c.version)
                    .where(JobTasks.c.leased_by.is_not(None))
                    .where(JobTasks.c.lease_until.is_not(None))
                    .where(JobTasks.c.lease_until <= now)
                    .where(JobTasks.c.status.in_(["queued", "running"]))
                )
            ).mappings().all()

            updated = 0
            for row in expired_rows:
                stmt = (
                    update(JobTasks)
                    .where(JobTasks.c.id == row["id"])
                    .where(JobTasks.c.version == row["version"])
                    .values(
                        status="failed",
                        finished_at=now,
                        result={"error": "lease_expired"},
                        lease_until=None,
                        leased_by=None,
                        version=JobTasks.c.version + 1,
                    )
                )
                res = await session.execute(stmt)
                updated += res.rowcount
            await session.commit()
            return updated

    async def queue_depth(self) -> int:
        async with self.sessionmaker() as session:
            result = await session.execute(
                select(func.count()).select_from(
                    select(JobTasks.c.id)
                    .where(JobTasks.c.status == "queued")
                    .where(JobTasks.c.scheduled_for <= func.now())
                    .subquery()
                )
            )
            count = result.scalar()
            return int(count or 0)

    async def check_connection(self) -> None:
        async with self.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    async def _finish(
        self, job_id: UUID, status: str, result: dict, *, expected_version: int | None
    ) -> None:
        async def _op() -> None:
            async with self.sessionmaker() as session:
                try:
                    stmt = update(JobTasks).where(JobTasks.c.id == str(job_id))
                    if expected_version is not None:
                        stmt = stmt.where(JobTasks.c.version == expected_version)
                    res = await session.execute(
                        stmt.values(
                            status=status,
                            finished_at=datetime.now(UTC),
                            result=result,
                            lease_until=None,
                            leased_by=None,
                            version=JobTasks.c.version + 1,
                        )
                    )
                    if expected_version is not None and res.rowcount == 0:
                        raise OptimisticLockError(
                            f"finish failed for {job_id} due to version mismatch"
                        )
                    await session.commit()
                except Exception:
                    await session.rollback()
                    raise

        try:
            await self._retry_with_backoff(_op)
        except OptimisticLockError:
            logger.warning(
                "Version mismatch on finish for job %s (expected_version=%s)",
                job_id,
                expected_version,
            )
            raise

    async def _retry_with_backoff(
        self,
        func: Callable[[], Awaitable[Any]],
        *,
        attempts: int = 3,
        base_delay: float = 0.1,
        max_delay: float = 2.0,
    ) -> Any:
        delay = base_delay
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return await func()
            except (DBAPIError, ConnectionError) as exc:
                last_exc = exc
                if attempt == attempts:
                    raise
                logger.warning(
                    "Transient backend error on attempt %s/%s; retrying in %.2fs",
                    attempt,
                    attempts,
                    delay,
                    exc_info=exc,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)
        if last_exc:
            raise last_exc

    @staticmethod
    def _row_to_dict(row: Any) -> dict:
        data = dict(row)
        data["id"] = UUID(data["id"]) if isinstance(data["id"], str) else data["id"]
        return data
