"""Unit tests for the Redis backend (#50, preview).

The integration tests require ``fakeredis`` and are skipped otherwise.
The import-time tests verify the optional-dependency guard.
"""

from __future__ import annotations

import importlib.util
import sys

import pytest

from pyjobkit.backends.redis import RedisBackend, RedisDependencyMissing


def test_redis_backend_requires_redis_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "redis", None)  # type: ignore[arg-type]
    monkeypatch.setitem(sys.modules, "redis.asyncio", None)  # type: ignore[arg-type]
    with pytest.raises(RedisDependencyMissing):
        RedisBackend("redis://localhost:6379/0")


_FAKEREDIS_AVAILABLE = importlib.util.find_spec("fakeredis") is not None


@pytest.mark.skipif(not _FAKEREDIS_AVAILABLE, reason="fakeredis not installed")
def test_redis_backend_round_trip_against_fakeredis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    import fakeredis.aioredis  # type: ignore[import-not-found]

    from pyjobkit.backends import redis as redis_backend_module

    async def _run() -> None:
        client = fakeredis.aioredis.FakeRedis(decode_responses=True)

        # Patch the from_url call to return the fake client.
        monkeypatch.setattr(
            redis_backend_module,
            "_require_redis",
            lambda: type(
                "fakemod",
                (),
                {"from_url": staticmethod(lambda *_a, **_k: client)},
            ),
        )

        backend = RedisBackend("redis://fake")
        await backend.check_connection()

        job_id = await backend.enqueue(kind="echo", payload={"x": 1})
        rec = await backend.get(job_id)
        assert rec["kind"] == "echo"
        assert rec["status"] == "queued"

        claimed = await backend.claim_batch(job_id, limit=5)
        assert claimed and str(claimed[0]["id"]) == str(job_id)

        await backend.succeed(job_id, {"ok": True}, expected_version=claimed[0]["version"])
        rec = await backend.get(job_id)
        assert rec["status"] == "success"
        await backend.close()

    asyncio.run(_run())
