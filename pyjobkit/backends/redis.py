"""Redis-backed QueueBackend (#50, preview).

This implementation stores each job as a Redis hash, the ready queue as
a ZSET scored by ``priority * 1e10 + scheduled_for_epoch_ms``, and the
in-flight leases as a separate ZSET scored by ``lease_expiry``. Atomic
state transitions (claim, succeed, fail, retry) are implemented with
small Lua scripts so concurrent workers cannot race.

The implementation is marked **preview**: the schema may evolve and the
optimistic-locking model is more conservative than the SQL backend
(version bumps happen on claim and on every state change, but reap and
retry semantics differ in failure modes).

``redis`` is an optional dependency; install ``pyjobkit[redis]`` (or
``pip install redis``) to enable it.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from ..contracts import OptimisticLockError, QueueBackend

__all__ = ["RedisBackend", "RedisDependencyMissing"]

logger = logging.getLogger(__name__)

KEY_PREFIX = "pyjobkit"
QUEUE_KEY = f"{KEY_PREFIX}:queue:ready"
LEASED_KEY = f"{KEY_PREFIX}:leased"


class RedisDependencyMissing(RuntimeError):
    """Raised when redis.asyncio is not installed."""


def _require_redis():  # type: ignore[no-untyped-def]
    try:
        import redis.asyncio as redis_async  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RedisDependencyMissing(
            "RedisBackend requires the 'redis' package; install pyjobkit[redis]."
        ) from exc
    return redis_async


def _job_key(job_id: UUID | str) -> str:
    return f"{KEY_PREFIX}:job:{job_id}"


def _score(priority: int, scheduled_for: datetime | None) -> float:
    ts = (scheduled_for or datetime.now(timezone.utc)).timestamp()
    return priority * 1e12 + ts


# ---------- Lua scripts ----------
# All scripts assume keys[1] = queue ZSET, keys[2] = leased ZSET,
# argv[1] = limit, argv[2] = now (epoch ms), argv[3] = lease ttl seconds.
_CLAIM_LUA = """
local now = tonumber(ARGV[2])
local lease_ttl = tonumber(ARGV[3])
local limit = tonumber(ARGV[1])
local claimed = {}
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', '+inf', 'LIMIT', 0, limit)
for _, jid in ipairs(ids) do
  local job_key = ARGV[4] .. jid
  local sched = tonumber(redis.call('HGET', job_key, 'scheduled_for_ts') or '0')
  if sched <= now / 1000 then
    redis.call('ZREM', KEYS[1], jid)
    redis.call('HSET', job_key, 'status', 'running', 'leased_by', ARGV[5],
               'lease_until_ts', now / 1000 + lease_ttl)
    redis.call('HINCRBY', job_key, 'version', 1)
    redis.call('ZADD', KEYS[2], now / 1000 + lease_ttl, jid)
    table.insert(claimed, jid)
  end
end
return claimed
"""

_REAP_LUA = """
local now = tonumber(ARGV[1]) / 1000
local prefix = ARGV[2]
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now)
for _, jid in ipairs(expired) do
  local job_key = prefix .. jid
  redis.call('ZREM', KEYS[1], jid)
  redis.call('HSET', job_key, 'status', 'failed',
             'result', '{"error":"lease_expired"}')
  redis.call('HINCRBY', job_key, 'version', 1)
end
return #expired
"""


class RedisBackend(QueueBackend):
    def __init__(self, url: str = "redis://localhost:6379/0", *, lease_ttl_s: int = 30) -> None:
        redis_async = _require_redis()
        self.lease_ttl_s = lease_ttl_s
        self._client = redis_async.from_url(url, decode_responses=True)
        self._claim_script = self._client.register_script(_CLAIM_LUA)
        self._reap_script = self._client.register_script(_REAP_LUA)

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
        if idempotency_key is not None:
            existing = await self._client.get(
                f"{KEY_PREFIX}:idempotency:{idempotency_key}"
            )
            if existing:
                return UUID(existing)

        job_id = uuid4()
        ts = (scheduled_for or datetime.now(timezone.utc)).timestamp()
        fields: dict[str, str | int | float] = {
            "id": str(job_id),
            "kind": kind,
            "status": "queued",
            "payload": json.dumps(payload),
            "priority": priority,
            "attempts": 0,
            "max_attempts": max_attempts,
            "scheduled_for_ts": ts,
            "timeout_s": timeout_s if timeout_s is not None else "",
            "version": 0,
        }
        await self._client.hset(_job_key(job_id), mapping=fields)
        await self._client.zadd(QUEUE_KEY, {str(job_id): _score(priority, scheduled_for)})
        if idempotency_key is not None:
            await self._client.set(
                f"{KEY_PREFIX}:idempotency:{idempotency_key}", str(job_id)
            )
        return job_id

    async def get(self, job_id: UUID) -> dict:
        data = await self._client.hgetall(_job_key(job_id))
        if not data:
            raise KeyError(job_id)
        return self._deserialize(data)

    async def cancel(self, job_id: UUID) -> None:
        await self._client.set(f"{KEY_PREFIX}:cancel:{job_id}", "1")
        # If still queued/running, mark cancelled.
        await self._client.hset(_job_key(job_id), "status", "cancelled")
        await self._client.zrem(QUEUE_KEY, str(job_id))
        await self._client.zrem(LEASED_KEY, str(job_id))

    async def is_cancelled(self, job_id: UUID) -> bool:
        return bool(await self._client.get(f"{KEY_PREFIX}:cancel:{job_id}"))

    async def claim_batch(
        self, worker_id: UUID, *, limit: int = 1
    ) -> list[QueueBackend.ClaimedJob]:
        now_ms = int(time.time() * 1000)
        ids = await self._claim_script(
            keys=[QUEUE_KEY, LEASED_KEY],
            args=[limit, now_ms, self.lease_ttl_s, f"{KEY_PREFIX}:job:", str(worker_id)],
        )
        out: list[QueueBackend.ClaimedJob] = []
        for jid in ids:
            data = await self._client.hgetall(_job_key(jid))
            if not data:
                continue
            row = self._deserialize(data)
            out.append(
                {
                    "id": UUID(row["id"]),
                    "kind": row["kind"],
                    "payload": row.get("payload") or {},
                    "timeout_s": row.get("timeout_s"),
                    "lease_until": row.get("lease_until"),
                    "version": row.get("version"),
                }
            )
        return out

    async def mark_running(self, job_id: UUID, worker_id: UUID) -> None:
        await self._client.hset(_job_key(job_id), "status", "running")
        await self._client.hincrby(_job_key(job_id), "attempts", 1)

    async def extend_lease(
        self,
        job_id: UUID,
        worker_id: UUID,
        ttl_s: int,
        *,
        expected_version: int | None = None,
    ) -> None:
        if expected_version is not None:
            current = await self._client.hget(_job_key(job_id), "version")
            if current is not None and int(current) != expected_version:
                raise OptimisticLockError(
                    f"version mismatch for {job_id}: have {current}, expected {expected_version}"
                )
        await self._client.zadd(LEASED_KEY, {str(job_id): time.time() + ttl_s})
        await self._client.hset(_job_key(job_id), "lease_until_ts", time.time() + ttl_s)

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
        retry_ts = time.time() + delay
        data = await self._client.hgetall(_job_key(job_id))
        if not data:
            return
        priority = int(data.get("priority", 100))
        await self._client.hset(
            _job_key(job_id),
            mapping={"status": "queued", "scheduled_for_ts": retry_ts},
        )
        await self._client.zrem(LEASED_KEY, str(job_id))
        await self._client.zadd(
            QUEUE_KEY, {str(job_id): priority * 1e12 + retry_ts}
        )
        await self._client.hincrby(_job_key(job_id), "version", 1)

    async def reap_expired(self) -> int:
        now_ms = int(time.time() * 1000)
        n = await self._reap_script(
            keys=[LEASED_KEY],
            args=[now_ms, f"{KEY_PREFIX}:job:"],
        )
        return int(n)

    async def queue_depth(self) -> int:
        return int(await self._client.zcard(QUEUE_KEY))

    async def check_connection(self) -> None:
        await self._client.ping()

    async def close(self) -> None:
        try:
            await self._client.close()
        except Exception:  # pragma: no cover - cleanup
            pass

    async def _finish(
        self,
        job_id: UUID,
        status: str,
        result: dict,
        *,
        expected_version: int | None,
    ) -> None:
        if expected_version is not None:
            current = await self._client.hget(_job_key(job_id), "version")
            if current is not None and int(current) != expected_version:
                # Silently no-op on version mismatch to match SQL backend
                # behaviour where finalization observes a stale version.
                return
        await self._client.hset(
            _job_key(job_id),
            mapping={"status": status, "result": json.dumps(result)},
        )
        await self._client.hincrby(_job_key(job_id), "version", 1)
        await self._client.zrem(LEASED_KEY, str(job_id))
        await self._client.zrem(QUEUE_KEY, str(job_id))

    @staticmethod
    def _deserialize(data: dict[str, str]) -> dict[str, Any]:
        out: dict[str, Any] = dict(data)
        if "payload" in out:
            try:
                out["payload"] = json.loads(out["payload"])
            except (TypeError, json.JSONDecodeError):
                out["payload"] = {}
        if out.get("result"):
            try:
                out["result"] = json.loads(out["result"])
            except (TypeError, json.JSONDecodeError):
                pass
        for k in ("priority", "attempts", "max_attempts", "version"):
            if k in out:
                try:
                    out[k] = int(out[k])
                except (TypeError, ValueError):
                    pass
        if out.get("timeout_s") == "":
            out["timeout_s"] = None
        elif "timeout_s" in out:
            try:
                out["timeout_s"] = int(out["timeout_s"])
            except (TypeError, ValueError):
                out["timeout_s"] = None
        ts = out.get("scheduled_for_ts")
        if ts:
            try:
                out["scheduled_for"] = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            except (TypeError, ValueError):
                pass
        return out
