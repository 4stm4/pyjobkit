"""Tests for the SQL backend using a synchronous SQLite engine wrapper."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pyjobkit.backends.sql.backend import SQLBackend
from pyjobkit.backends.sql.schema import metadata


class _AsyncSessionWrapper:
    def __init__(self, sync_session):
        self._session = sync_session

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._session.close()

    async def execute(self, statement, params=None):
        return self._session.execute(statement, params or {})

    async def commit(self) -> None:
        self._session.commit()

    async def rollback(self) -> None:
        self._session.rollback()


def _make_backend() -> SQLBackend:
    sync_engine = create_engine("sqlite:///:memory:", future=True)
    metadata.create_all(sync_engine)
    SyncSession = sessionmaker(sync_engine, future=True)
    backend = object.__new__(SQLBackend)
    backend.engine = SimpleNamespace(dialect=sync_engine.dialect)
    backend.prefer_pg_skip_locked = False
    backend.lease_ttl_s = 1
    backend.sessionmaker = lambda: _AsyncSessionWrapper(SyncSession())
    return backend


def test_sql_backend_end_to_end() -> None:
    async def _run() -> None:
        backend = _make_backend()
        when = datetime.now(timezone.utc) - timedelta(seconds=5)
        job_a = await backend.enqueue(kind="alpha", payload={"idx": 1}, priority=5, scheduled_for=when)
        job_b = await backend.enqueue(kind="alpha", payload={"idx": 2}, priority=1, scheduled_for=when)
        await backend.cancel(job_a)
        cancelled = await backend.get(job_a)
        assert cancelled["cancel_requested"] is True

        worker_id = uuid4()
        rows = await backend.claim_batch(worker_id, limit=2)
        assert [row["payload"]["idx"] for row in rows] == [2, 1]

        await backend.mark_running(rows[0]["id"], worker_id)
        await backend.extend_lease(rows[0]["id"], worker_id, ttl_s=2)
        await backend.succeed(rows[0]["id"], {"ok": True})
        await backend.fail(rows[1]["id"], {"error": "nope"})

        extra = await backend.enqueue(kind="alpha", payload={}, scheduled_for=when)
        await backend.timeout(extra)

        success = await backend.get(rows[0]["id"])
        failed = await backend.get(rows[1]["id"])
        timed = await backend.get(extra)
        assert success["status"] == "success"
        assert failed["status"] == "failed"
        assert timed["status"] == "timeout"

    asyncio.run(_run())


def test_sql_backend_claim_generic_handles_contention() -> None:
    async def _run() -> None:
        backend = _make_backend()
        when = datetime.now(timezone.utc) - timedelta(seconds=1)
        job = await backend.enqueue(kind="alpha", payload={}, priority=1, scheduled_for=when)
        worker = uuid4()
        rows = await backend.claim_batch(worker, limit=1)
        assert rows and rows[0]["id"] == job
        # No queued rows -> claim returns []
        assert await backend.claim_batch(worker, limit=1) == []

    asyncio.run(_run())


def test_sql_backend_row_to_dict() -> None:
    data = SQLBackend._row_to_dict({"id": str(uuid4()), "value": 3})
    assert isinstance(data["id"], UUID)


def test_sql_backend_get_missing() -> None:
    async def _run() -> None:
        backend = _make_backend()
        with pytest.raises(KeyError):
            await backend.get(uuid4())

    asyncio.run(_run())


def test_sql_backend_claim_batch_prefers_pg(monkeypatch) -> None:
    async def _run() -> None:
        backend = _make_backend()
        backend.engine.dialect = SimpleNamespace(name="postgresql")
        backend.prefer_pg_skip_locked = True
        called: list[tuple] = []

        async def fake_pg(worker_id: UUID, limit: int):
            called.append((worker_id, limit))
            return [{"id": uuid4(), "payload": {}}]

        backend._claim_pg = fake_pg  # type: ignore[assignment]
        rows = await backend.claim_batch(uuid4(), limit=2)
        assert len(rows) == 1
        assert called[0][1] == 2

    asyncio.run(_run())


def test_sql_backend_claim_pg_executes_sql(monkeypatch) -> None:
    async def _run() -> None:
        backend = _make_backend()
        backend.lease_ttl_s = 5
        rows = [{"id": str(uuid4())}]

        class DummyResult:
            def __init__(self, rows):
                self._rows = rows

            def mappings(self):
                return self

            def all(self):
                return self._rows

        class DummySession:
            def __init__(self):
                self.params = None
                self.commits = 0

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def execute(self, sql, params=None):
                self.params = params
                return DummyResult(rows)

            async def commit(self):
                self.commits += 1

        dummy_session = DummySession()
        backend.sessionmaker = lambda: dummy_session
        result = await backend._claim_pg(uuid4(), limit=1)
        assert result == rows
        assert dummy_session.commits == 1

    asyncio.run(_run())


def test_sql_backend_init_and_claim_generic_rollback(monkeypatch) -> None:
    async def _run() -> None:
        backend = _make_backend()

        class DummySelectResult:
            def mappings(self):
                return self

            def first(self):
                return {"id": "00000000-0000-0000-0000-000000000000", "version": 1}

        class DummyUpdateResult:
            rowcount = 0

        class DummySession:
            def __init__(self) -> None:
                self.stage = 0
                self.rolled = False

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def execute(self, stmt, params=None):
                if self.stage == 0:
                    self.stage += 1
                    return DummySelectResult()
                self.stage += 1
                return DummyUpdateResult()

            async def commit(self):
                pass

            async def rollback(self):
                self.rolled = True

        dummy_session = DummySession()
        backend.sessionmaker = lambda: dummy_session
        rows = await backend._claim_generic(uuid4(), limit=1)
        assert rows == []
        assert dummy_session.rolled is True

    asyncio.run(_run())

    created: dict[str, object] = {}

    def fake_sessionmaker(engine, expire_on_commit=False):
        created["engine"] = engine
        created["expire"] = expire_on_commit
        return "factory"

    monkeypatch.setattr("pyjobkit.backends.sql.backend.async_sessionmaker", fake_sessionmaker)
    engine = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))
    backend = SQLBackend(engine, prefer_pg_skip_locked=True, lease_ttl_s=9)
    assert backend.sessionmaker == "factory"
    assert backend.engine is engine
    assert backend.lease_ttl_s == 9
