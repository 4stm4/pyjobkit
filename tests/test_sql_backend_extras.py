"""Extra coverage for the SQL backend: retry/reap/purge/cancel/generic claim."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from pyjobkit.backends.sql import SQLBackend
from pyjobkit.backends.sql.schema import metadata


@pytest.fixture
async def backend(tmp_path):
    db_path = tmp_path / "pyjobkit.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    yield SQLBackend(engine, prefer_pg_skip_locked=False, lease_ttl_s=5)
    await engine.dispose()


def _run(coro):
    return asyncio.run(coro)


def test_generic_claim_warns_once_and_claims(tmp_path, caplog):
    async def _go():
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'x.db'}")
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        backend = SQLBackend(engine, prefer_pg_skip_locked=False, lease_ttl_s=5)
        from uuid import uuid4

        # SQLite's CURRENT_TIMESTAMP is second-granular, so backdating
        # the schedule keeps the generic-claim path deterministic.
        await backend.enqueue(
            kind="echo",
            payload={},
            scheduled_for=datetime.now(timezone.utc) - timedelta(seconds=2),
        )
        import logging

        with caplog.at_level(logging.WARNING, logger="pyjobkit.backends.sql.backend"):
            claimed = await backend.claim_batch(uuid4(), limit=1)
        assert len(claimed) == 1
        assert any("SKIP LOCKED" in r.message for r in caplog.records)
        # Subsequent claim does NOT re-warn.
        caplog.clear()
        await backend.claim_batch(uuid4(), limit=1)
        assert not [r for r in caplog.records if "SKIP LOCKED" in r.message]
        await engine.dispose()

    _run(_go())


def test_is_cancelled_reflects_flag(tmp_path):
    async def _go():
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'c.db'}")
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        backend = SQLBackend(engine, prefer_pg_skip_locked=False)
        jid = await backend.enqueue(kind="echo", payload={})
        assert await backend.is_cancelled(jid) is False
        await backend.cancel(jid)
        assert await backend.is_cancelled(jid) is True
        await engine.dispose()

    _run(_go())


def test_retry_resets_scheduled_for_and_status(tmp_path):
    async def _go():
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'r.db'}")
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        backend = SQLBackend(engine, prefer_pg_skip_locked=False)
        jid = await backend.enqueue(kind="echo", payload={})
        await backend.fail(jid, {"error": "boom"})
        await backend.retry(jid, delay=2.0)
        rec = await backend.get(jid)
        assert rec["status"] == "queued"
        # SQLite returns naive datetimes; compare against a naive baseline.
        sched = rec["scheduled_for"]
        if sched.tzinfo is not None:
            sched = sched.astimezone(timezone.utc).replace(tzinfo=None)
        assert sched >= datetime.utcnow() - timedelta(seconds=5)
        await engine.dispose()

    _run(_go())


def test_reap_expired_finalises_orphaned_leases(tmp_path):
    async def _go():
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'reap.db'}")
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        backend = SQLBackend(engine, prefer_pg_skip_locked=False, lease_ttl_s=1)
        from uuid import uuid4

        jid = await backend.enqueue(
            kind="echo",
            payload={},
            scheduled_for=datetime.now(timezone.utc) - timedelta(seconds=2),
        )
        await backend.claim_batch(uuid4(), limit=1)
        # Backdate the lease so reap_expired finds it.
        from sqlalchemy import update
        from pyjobkit.backends.sql.schema import JobTasks

        async with backend.sessionmaker() as session:
            await session.execute(
                update(JobTasks)
                .where(JobTasks.c.id == str(jid))
                .values(lease_until=datetime.now(timezone.utc) - timedelta(seconds=10))
            )
            await session.commit()
        n = await backend.reap_expired()
        assert n == 1
        rec = await backend.get(jid)
        assert rec["status"] == "failed"
        assert rec["result"] == {"error": "lease_expired"}
        await engine.dispose()

    _run(_go())


def test_queue_depth_counts_only_queued_and_due(tmp_path):
    async def _go():
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'q.db'}")
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        backend = SQLBackend(engine, prefer_pg_skip_locked=False)
        await backend.enqueue(
            kind="echo",
            payload={},
            scheduled_for=datetime.now(timezone.utc) - timedelta(seconds=2),
        )
        # Future-scheduled job should NOT count.
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        await backend.enqueue(kind="echo", payload={}, scheduled_for=future)
        depth = await backend.queue_depth()
        assert depth == 1
        await engine.dispose()

    _run(_go())


def test_check_connection_pings(tmp_path):
    async def _go():
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'p.db'}")
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        backend = SQLBackend(engine, prefer_pg_skip_locked=False)
        await backend.check_connection()
        await engine.dispose()

    _run(_go())


def test_purge_finished_with_and_without_cutoff(tmp_path):
    async def _go():
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'pf.db'}")
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        backend = SQLBackend(engine, prefer_pg_skip_locked=False)
        a = await backend.enqueue(kind="echo", payload={})
        await backend.succeed(a, {"ok": True})
        b = await backend.enqueue(kind="echo", payload={})
        await backend.fail(b, {"error": "x"})

        # cutoff far in the past: nothing matches.
        removed = await backend.purge_finished(older_than=timedelta(days=30))
        assert removed == 0

        # No cutoff: drops every terminal row.
        removed = await backend.purge_finished()
        assert removed == 2
        await engine.dispose()

    _run(_go())


def test_finish_optimistic_lock_mismatch(tmp_path):
    async def _go():
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'ol.db'}")
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        backend = SQLBackend(engine, prefer_pg_skip_locked=False)
        from pyjobkit.contracts import OptimisticLockError

        jid = await backend.enqueue(kind="echo", payload={})
        with pytest.raises(OptimisticLockError):
            await backend.succeed(jid, {"ok": True}, expected_version=99)
        await engine.dispose()

    _run(_go())


def test_enqueue_many_bulk_insert(tmp_path):
    async def _go():
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'b.db'}")
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        backend = SQLBackend(engine, prefer_pg_skip_locked=False)
        backdated = datetime.now(timezone.utc) - timedelta(seconds=2)
        ids = await backend.enqueue_many(
            [
                {"kind": "echo", "payload": {"x": 1}, "scheduled_for": backdated},
                {
                    "kind": "echo",
                    "payload": {"x": 2},
                    "priority": 50,
                    "scheduled_for": backdated,
                },
                {
                    "kind": "echo",
                    "payload": {"x": 3},
                    "idempotency_key": "k1",
                    "scheduled_for": backdated,
                },
            ]
        )
        assert len(ids) == 3
        assert await backend.queue_depth() == 3
        with pytest.raises(ValueError):
            await backend.enqueue_many([{"payload": {}}])  # missing kind
        # Empty batch returns empty list (no-op).
        assert await backend.enqueue_many([]) == []
        await engine.dispose()

    _run(_go())


def test_retry_with_backoff_eventually_raises(tmp_path):
    async def _go():
        from sqlalchemy.exc import DBAPIError

        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'rb.db'}")
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        backend = SQLBackend(engine, prefer_pg_skip_locked=False)

        attempts = {"n": 0}

        async def always_fails():
            attempts["n"] += 1
            raise DBAPIError("statement", {}, Exception("boom"))

        with pytest.raises(DBAPIError):
            await backend._retry_with_backoff(always_fails, attempts=3, base_delay=0.001)
        assert attempts["n"] == 3
        await engine.dispose()

    _run(_go())


def test_retry_with_backoff_retries_then_succeeds(tmp_path):
    async def _go():
        from sqlalchemy.exc import DBAPIError

        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'rb2.db'}")
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        backend = SQLBackend(engine, prefer_pg_skip_locked=False)

        calls = {"n": 0}

        async def flaky():
            calls["n"] += 1
            if calls["n"] < 2:
                raise DBAPIError("statement", {}, Exception("transient"))
            return "ok"

        result = await backend._retry_with_backoff(
            flaky, attempts=3, base_delay=0.001
        )
        assert result == "ok"
        assert calls["n"] == 2
        await engine.dispose()

    _run(_go())
