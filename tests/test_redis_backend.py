"""Unit tests for the Redis backend (#50, preview).

Two layers of coverage:

* A small in-process fake client that implements just the redis-py
  methods the backend touches. Lua scripts are inert here, but the
  non-script paths (enqueue / get / cancel / extend_lease / finalize /
  retry / queue_depth / check_connection / close) are exercised.
* An end-to-end round trip against a real Redis URL, gated on the
  ``PYJOBKIT_TEST_REDIS`` environment variable (skipped otherwise).
"""

from __future__ import annotations

import asyncio
import os
import sys
from collections import defaultdict
from typing import Any

import pytest

from pyjobkit.backends.redis import RedisBackend, RedisDependencyMissing


def test_redis_backend_requires_redis_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "redis", None)  # type: ignore[arg-type]
    monkeypatch.setitem(sys.modules, "redis.asyncio", None)  # type: ignore[arg-type]
    with pytest.raises(RedisDependencyMissing):
        RedisBackend("redis://localhost:6379/0")


class _FakeRedis:
    """In-process stub for the redis-py async client surface we use."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, Any]] = defaultdict(dict)
        self.zsets: dict[str, dict[str, float]] = defaultdict(dict)
        self.strings: dict[str, str] = {}
        self.closed = False
        self.evaluated: list[tuple[str, tuple, tuple]] = []

    async def hset(self, key: str, *args, mapping=None, **kwargs):
        if mapping:
            self.hashes[key].update({k: str(v) for k, v in mapping.items()})
        elif len(args) >= 2:
            self.hashes[key][args[0]] = str(args[1])
        return 1

    async def hget(self, key: str, field: str):
        return self.hashes.get(key, {}).get(field)

    async def hgetall(self, key: str):
        return dict(self.hashes.get(key, {}))

    async def hincrby(self, key: str, field: str, amount: int = 1):
        current = int(self.hashes.get(key, {}).get(field, 0))
        self.hashes[key][field] = str(current + amount)
        return current + amount

    async def zadd(self, key: str, mapping: dict):
        self.zsets[key].update({str(k): float(v) for k, v in mapping.items()})
        return len(mapping)

    async def zrem(self, key: str, *members):
        removed = 0
        for m in members:
            if str(m) in self.zsets.get(key, {}):
                del self.zsets[key][str(m)]
                removed += 1
        return removed

    async def zcard(self, key: str):
        return len(self.zsets.get(key, {}))

    async def set(self, key: str, value: str):
        self.strings[key] = value
        return True

    async def get(self, key: str):
        return self.strings.get(key)

    async def ping(self):
        return True

    async def close(self):
        self.closed = True

    async def eval(self, script: str, numkeys: int, *args):
        # Record evaluations; the Lua bodies in the production module
        # are not interpreted here. The number of keys distinguishes
        # the two scripts shipped by the backend: claim has 2 keys
        # (queue + leased), reap has 1 (leased). claim is expected to
        # return a list of job ids; reap returns an integer count.
        keys = args[:numkeys]
        argv = args[numkeys:]
        self.evaluated.append((numkeys, tuple(keys), tuple(argv)))
        if numkeys == 1:
            return 0
        return []


@pytest.fixture
def fake_client(monkeypatch: pytest.MonkeyPatch) -> _FakeRedis:
    from pyjobkit.backends import redis as redis_backend_module

    fake = _FakeRedis()

    class _FakeMod:
        @staticmethod
        def from_url(url: str, decode_responses: bool = True):
            return fake

    monkeypatch.setattr(redis_backend_module, "_require_redis", lambda: _FakeMod)
    return fake


def test_redis_backend_enqueue_and_get(fake_client: _FakeRedis) -> None:
    async def _run() -> None:
        backend = RedisBackend("redis://fake")
        job_id = await backend.enqueue(kind="echo", payload={"x": 1})
        rec = await backend.get(job_id)
        assert rec["kind"] == "echo"
        assert rec["status"] == "queued"

    asyncio.run(_run())


def test_redis_backend_idempotency_dedupes(fake_client: _FakeRedis) -> None:
    async def _run() -> None:
        backend = RedisBackend("redis://fake")
        a = await backend.enqueue(kind="echo", payload={}, idempotency_key="k1")
        b = await backend.enqueue(kind="echo", payload={}, idempotency_key="k1")
        assert a == b

    asyncio.run(_run())


def test_redis_backend_cancel_marks_status(fake_client: _FakeRedis) -> None:
    async def _run() -> None:
        backend = RedisBackend("redis://fake")
        job_id = await backend.enqueue(kind="echo", payload={})
        await backend.cancel(job_id)
        rec = await backend.get(job_id)
        assert rec["status"] == "cancelled"
        assert await backend.is_cancelled(job_id) is True

    asyncio.run(_run())


def test_redis_backend_finalize_paths(fake_client: _FakeRedis) -> None:
    async def _run() -> None:
        backend = RedisBackend("redis://fake")
        a = await backend.enqueue(kind="echo", payload={})
        b = await backend.enqueue(kind="echo", payload={})
        c = await backend.enqueue(kind="echo", payload={})
        await backend.succeed(a, {"ok": True})
        await backend.fail(b, {"error": "boom"})
        await backend.timeout(c)
        assert (await backend.get(a))["status"] == "success"
        assert (await backend.get(b))["status"] == "failed"
        assert (await backend.get(c))["status"] == "timeout"

    asyncio.run(_run())


def test_redis_backend_retry_reschedules(fake_client: _FakeRedis) -> None:
    async def _run() -> None:
        backend = RedisBackend("redis://fake")
        job_id = await backend.enqueue(kind="echo", payload={})
        await backend.retry(job_id, delay=5)
        rec = await backend.get(job_id)
        assert rec["status"] == "queued"

    asyncio.run(_run())


def test_redis_backend_extend_lease_and_version_check(fake_client: _FakeRedis) -> None:
    async def _run() -> None:
        from pyjobkit.contracts import OptimisticLockError
        from uuid import uuid4

        backend = RedisBackend("redis://fake")
        job_id = await backend.enqueue(kind="echo", payload={})
        # Manually bump the version to simulate a racing worker.
        fake_client.hashes[f"pyjobkit:job:{job_id}"]["version"] = "5"
        await backend.extend_lease(job_id, uuid4(), ttl_s=10, expected_version=5)
        with pytest.raises(OptimisticLockError):
            await backend.extend_lease(
                job_id, uuid4(), ttl_s=10, expected_version=99
            )

    asyncio.run(_run())


def test_redis_backend_queue_depth_and_close(fake_client: _FakeRedis) -> None:
    async def _run() -> None:
        backend = RedisBackend("redis://fake")
        await backend.enqueue(kind="echo", payload={})
        await backend.enqueue(kind="echo", payload={})
        assert await backend.queue_depth() == 2
        await backend.check_connection()
        await backend.close()
        assert fake_client.closed is True

    asyncio.run(_run())


def test_redis_backend_get_missing_raises_keyerror(fake_client: _FakeRedis) -> None:
    async def _run() -> None:
        from uuid import uuid4

        backend = RedisBackend("redis://fake")
        with pytest.raises(KeyError):
            await backend.get(uuid4())

    asyncio.run(_run())


def test_redis_backend_claim_and_reap_dispatch_eval(fake_client: _FakeRedis) -> None:
    async def _run() -> None:
        from uuid import uuid4

        backend = RedisBackend("redis://fake")
        await backend.claim_batch(uuid4(), limit=3)
        await backend.reap_expired()
        # Two EVAL invocations recorded against the fake.
        assert len(fake_client.evaluated) == 2

    asyncio.run(_run())


def test_redis_backend_claim_returns_existing_rows(fake_client: _FakeRedis) -> None:
    """When the eval script returns ids, the backend rehydrates them from hashes."""

    async def _run() -> None:
        from uuid import uuid4

        backend = RedisBackend("redis://fake")
        # Seed two jobs and patch eval to return their ids.
        a = await backend.enqueue(kind="echo", payload={"x": 1})
        b = await backend.enqueue(kind="echo", payload={"x": 2})

        async def fake_eval(script, numkeys, *args):
            if numkeys == 2:
                return [str(a), str(b)]
            return 0

        fake_client.eval = fake_eval  # type: ignore[assignment]
        claimed = await backend.claim_batch(uuid4(), limit=2)
        assert {c["id"] for c in claimed} == {a, b}

    asyncio.run(_run())


def test_redis_backend_claim_skips_unknown_ids(fake_client: _FakeRedis) -> None:
    """If eval reports an id with no backing hash, it's skipped."""

    async def _run() -> None:
        from uuid import uuid4

        backend = RedisBackend("redis://fake")

        async def fake_eval(script, numkeys, *args):
            return [str(uuid4())] if numkeys == 2 else 0

        fake_client.eval = fake_eval  # type: ignore[assignment]
        claimed = await backend.claim_batch(uuid4(), limit=1)
        assert claimed == []

    asyncio.run(_run())


def test_redis_backend_retry_for_unknown_job_is_noop(fake_client: _FakeRedis) -> None:
    async def _run() -> None:
        from uuid import uuid4

        backend = RedisBackend("redis://fake")
        await backend.retry(uuid4(), delay=1.0)  # no row -> early return

    asyncio.run(_run())


def test_redis_backend_finish_stale_version_short_circuits(fake_client: _FakeRedis) -> None:
    async def _run() -> None:
        backend = RedisBackend("redis://fake")
        jid = await backend.enqueue(kind="echo", payload={})
        fake_client.hashes[f"pyjobkit:job:{jid}"]["version"] = "10"
        # Wrong expected_version -> silent return; status stays queued.
        await backend.succeed(jid, {"ok": True}, expected_version=1)
        rec = await backend.get(jid)
        assert rec["status"] == "queued"

    asyncio.run(_run())


def test_redis_require_redis_returns_module() -> None:
    """The non-error branch returns the imported module reference."""

    from pyjobkit.backends.redis import _require_redis

    mod = _require_redis()
    assert hasattr(mod, "from_url")


_REAL_REDIS_URL = os.environ.get("PYJOBKIT_TEST_REDIS")


@pytest.mark.skipif(
    not _REAL_REDIS_URL, reason="PYJOBKIT_TEST_REDIS not set; skipping real-Redis round-trip"
)
def test_redis_backend_real_round_trip() -> None:
    async def _run() -> None:
        backend = RedisBackend(_REAL_REDIS_URL)
        await backend.check_connection()
        job_id = await backend.enqueue(kind="echo", payload={"x": 1})
        rec = await backend.get(job_id)
        assert rec["kind"] == "echo"
        await backend.close()

    asyncio.run(_run())
