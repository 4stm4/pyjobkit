"""SQL backend implemented with SQLAlchemy async sessions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List
from uuid import UUID, uuid4

from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from .schema import JobTasks

UTC = timezone.utc


class SQLBackend:
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

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return (
            "SQLBackend("
            f"engine={self.engine.url!s}, "
            f"prefer_pg_skip_locked={self.prefer_pg_skip_locked}, "
            f"lease_ttl_s={self.lease_ttl_s}"
            ")"
        )

    async def enqueue(self, **kwargs):  # type: ignore[override]
        job_id = uuid4()
        now = datetime.now(UTC)
        values = dict(
            id=str(job_id),
            status="queued",
            created_at=now,
            scheduled_for=kwargs.get("scheduled_for") or now,
            max_attempts=kwargs.get("max_attempts", 3),
            priority=kwargs.get("priority", 100),
            kind=kwargs["kind"],
            payload=kwargs.get("payload", {}),
            idempotency_key=kwargs.get("idempotency_key"),
            timeout_s=kwargs.get("timeout_s"),
        )
        async with self.sessionmaker() as session:
            await session.execute(JobTasks.insert().values(**values))
            await session.commit()
        return job_id

    async def get(self, job_id: UUID) -> dict:  # type: ignore[override]
        async with self.sessionmaker() as session:
            row = (
                await session.execute(select(JobTasks).where(JobTasks.c.id == str(job_id)))
            ).mappings().first()
            if not row:
                raise KeyError(job_id)
            return self._row_to_dict(row)

    async def cancel(self, job_id: UUID) -> None:  # type: ignore[override]
        async with self.sessionmaker() as session:
            await session.execute(
                update(JobTasks).where(JobTasks.c.id == str(job_id)).values(cancel_requested=True)
            )
            await session.commit()

    async def is_cancelled(self, job_id: UUID) -> bool:  # type: ignore[override]
        async with self.sessionmaker() as session:
            row = (
                await session.execute(
                    select(JobTasks.c.cancel_requested).where(JobTasks.c.id == str(job_id))
                )
            ).first()
            return bool(row and row.cancel_requested)

    async def claim_batch(self, worker_id: UUID, *, limit: int = 1) -> List[dict]:  # type: ignore[override]
        if self.engine.dialect.name == "postgresql" and self.prefer_pg_skip_locked:
            rows = await self._claim_pg(worker_id, limit)
        else:
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

    async def mark_running(self, job_id: UUID, worker_id: UUID) -> None:  # type: ignore[override]
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
    ) -> None:  # type: ignore[override]
        async with self.sessionmaker() as session:
            stmt = (
                update(JobTasks)
                .where(JobTasks.c.id == str(job_id))
                .where(JobTasks.c.leased_by == str(worker_id))
            )
            if expected_version is not None:
                stmt = stmt.where(JobTasks.c.version == expected_version)
            await session.execute(
                stmt.values(lease_until=datetime.now(UTC) + timedelta(seconds=ttl_s))
            )
            await session.commit()

    async def succeed(
        self, job_id: UUID, result: dict, *, expected_version: int | None = None
    ) -> None:  # type: ignore[override]
        await self._finish(job_id, "success", result, expected_version=expected_version)

    async def fail(
        self, job_id: UUID, reason: dict, *, expected_version: int | None = None
    ) -> None:  # type: ignore[override]
        await self._finish(job_id, "failed", reason, expected_version=expected_version)

    async def timeout(
        self, job_id: UUID, *, expected_version: int | None = None
    ) -> None:  # type: ignore[override]
        await self._finish(
            job_id, "timeout", {"error": "timeout"}, expected_version=expected_version
        )

    async def retry(self, job_id: UUID, *, delay: float) -> None:  # type: ignore[override]
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

    async def reap_expired(self) -> int:  # type: ignore[override]
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

    async def queue_depth(self) -> int:  # type: ignore[override]
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

    async def check_connection(self) -> None:  # type: ignore[override]
        async with self.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    async def _finish(
        self, job_id: UUID, status: str, result: dict, *, expected_version: int | None
    ) -> None:
        async with self.sessionmaker() as session:
            stmt = update(JobTasks).where(JobTasks.c.id == str(job_id))
            if expected_version is not None:
                stmt = stmt.where(JobTasks.c.version == expected_version)
            await session.execute(
                stmt.values(
                    status=status,
                    finished_at=datetime.now(UTC),
                    result=result,
                    lease_until=None,
                    leased_by=None,
                    version=JobTasks.c.version + 1,
                )
            )
            await session.commit()

    @staticmethod
    def _row_to_dict(row: Any) -> dict:
        data = dict(row)
        data["id"] = UUID(data["id"]) if isinstance(data["id"], str) else data["id"]
        return data
