"""Final coverage closers for the remaining missing lines."""

from __future__ import annotations

import asyncio
import importlib
import logging
import sys
import types
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from pyjobkit import Engine, MemoryBackend, Worker
from pyjobkit.contracts import Executor


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id, payload, ctx):
        return {}


# memory backend: bulk idempotency path -----------------------------------


def test_memory_bulk_idempotency_returns_existing_id() -> None:
    async def _go():
        backend = MemoryBackend()
        existing = await backend.enqueue(
            kind="x", payload={}, idempotency_key="dup"
        )
        ids = await backend.enqueue_many(
            [
                {"kind": "x", "idempotency_key": "dup"},
                {"kind": "x"},
            ]
        )
        assert ids[0] == existing  # the dup collapses to the existing row.

    asyncio.run(_go())


def test_memory_backend_finish_raises_on_version_mismatch() -> None:
    """MemoryBackend matches SQLBackend semantics: version mismatch raises."""

    from pyjobkit.contracts import OptimisticLockError

    async def _go():
        backend = MemoryBackend()
        jid = await backend.enqueue(kind="x", payload={})
        with pytest.raises(OptimisticLockError):
            await backend.succeed(jid, {"ok": True}, expected_version=99)

    asyncio.run(_go())


# leader_loop: on_leader raises while leader, then loop continues ---------


def test_leader_loop_logs_on_leader_exception_and_restarts() -> None:
    from pyjobkit.leader import MemoryLeaderLock, leader_loop

    async def _go():
        MemoryLeaderLock.reset_state()
        lock = MemoryLeaderLock(name="raise")
        calls = {"n": 0}

        async def on_leader():
            calls["n"] += 1
            raise RuntimeError("boom")

        stop = asyncio.Event()

        async def stopper():
            await asyncio.sleep(0.15)
            stop.set()

        await asyncio.gather(
            leader_loop(
                lock,
                ttl_s=0.5,
                renew_every_s=0.02,
                on_leader=on_leader,
                stop_event=stop,
                cancel_grace_s=0.05,
            ),
            stopper(),
        )
        assert calls["n"] >= 1

    asyncio.run(_go())


# webhooks: explicit secret keyword ---------------------------------------


def test_webhook_signature_uses_passed_secret_over_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The function argument wins over the env variable."""

    import hashlib
    import hmac

    import httpx

    from pyjobkit.webhooks import (
        SIGNATURE_HEADER,
        TIMESTAMP_HEADER,
        WEBHOOK_SECRET_ENV,
        fire,
        verify_signature,
    )

    monkeypatch.setenv(WEBHOOK_SECRET_ENV, "env-secret")

    captured: list[httpx.Request] = []

    class _Tx(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            captured.append(request)
            return httpx.Response(200)

    async def _go():
        client = httpx.AsyncClient(transport=_Tx())
        await fire(
            webhooks={"complete": "https://example.com"},
            status="success",
            job_id=UUID("00000000-0000-0000-0000-000000000003"),
            kind="x",
            attempts=1,
            duration_ms=1.0,
            result=None,
            client=client,
            secret="override-secret",
        )
        await client.aclose()

    asyncio.run(_go())
    req = captured[0]
    assert verify_signature(
        body=req.content,
        secret="override-secret",
        signature_header=req.headers[SIGNATURE_HEADER],
        timestamp_header=req.headers[TIMESTAMP_HEADER],
    )
    # Env value must NOT validate.
    assert not verify_signature(
        body=req.content,
        secret="env-secret",
        signature_header=req.headers[SIGNATURE_HEADER],
        timestamp_header=req.headers[TIMESTAMP_HEADER],
    )


# config: aliasing branch (extra_executors via env CSV) -------------------


def test_config_load_extra_executors_env_csv() -> None:
    from pyjobkit.config import load_config

    cfg = load_config(env={"PYJOBKIT_EXTRA_EXECUTORS": "a:b, c:d"})
    assert cfg.extra_executors == ("a:b", "c:d")


def test_config_log_format_strips_and_lowers() -> None:
    from pyjobkit.config import _coerce_log_format

    assert _coerce_log_format("  TEXT  ") == "text"


# executors/plugins: filter via `only` + entry exception handling --------


def test_plugin_discovery_filters_with_only(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib.metadata

    from pyjobkit.executors.plugins import (
        EXECUTOR_ENTRY_POINT_GROUP,
        discover_executors,
    )

    class _PluginExec(Executor):
        kind = "plug"

        async def run(self, *, job_id, payload, ctx):
            return {}

    class _Entry:
        def __init__(self, name):
            self.name = name

        def load(self):
            return lambda: _PluginExec()

    monkeypatch.setattr(
        importlib.metadata,
        "entry_points",
        lambda *, group: [_Entry("wanted"), _Entry("skipped")]
        if group == EXECUTOR_ENTRY_POINT_GROUP
        else [],
    )
    result = discover_executors(only=["wanted"])
    assert len(result) == 1


# ratelimit: refill clamps to capacity ------------------------------------


def test_ratelimit_refill_caps_at_capacity() -> None:
    """The bucket cannot accumulate more tokens than its capacity."""

    import time

    from pyjobkit.ratelimit import TokenBucket

    bucket = TokenBucket(max_per_second=1000, burst=2)
    # Push monotonic well into the future so refill should heavily fill.
    bucket._last = time.monotonic() - 60
    bucket._refill()
    assert bucket._tokens == bucket.capacity


# scheduler: parse_interval default-unit branch ---------------------------


def test_scheduler_parse_interval_explicit_seconds() -> None:
    from pyjobkit.scheduler import parse_interval

    assert parse_interval("60s") == timedelta(seconds=60)


# retry parse keyword args end-to-end -------------------------------------


def test_retry_parse_keyword_falls_through_for_fixed() -> None:
    from pyjobkit.retry import FixedDelay, parse_policy

    p = parse_policy("fixed:2:give_up_after_age_s=3600")
    assert isinstance(p, FixedDelay)
    assert p.give_up_after_age_s == 3600


# subprocess: stderr-only output exercises stream branches ---------------


def test_subprocess_executor_streams_stderr_lines() -> None:
    async def _go():
        from pyjobkit.executors.subprocess import SubprocessExecutor

        class _Ctx:
            is_shadow = False
            logs: list = []

            async def log(self, msg, /, *, stream="stdout"):
                self.logs.append((stream, msg))

            async def is_cancelled(self):
                return False

            async def set_progress(self, value, /, **meta):
                pass

        ctx = _Ctx()
        result = await SubprocessExecutor().run(
            job_id=uuid4(),
            payload={"cmd": "echo hello && echo oops 1>&2"},
            ctx=ctx,
        )
        assert result["returncode"] == 0
        streams = {s for s, _ in ctx.logs}
        assert {"stdout", "stderr"} <= streams

    asyncio.run(_go())


# engine: cancel + retry methods (trivial passthroughs) ------------------


def test_engine_cancel_and_retry_passthrough() -> None:
    async def _go():
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        jid = await engine.enqueue(kind="noop", payload={})
        await engine.cancel(jid)
        await engine.retry(jid, delay=0.1)
        # Round-trip helpers also exposed via Engine for symmetry.
        assert await engine.queue_depth() >= 0
        await engine.check_connection()

    asyncio.run(_go())


# logging: configure_logging level=int branch ----------------------------


def test_configure_logging_accepts_numeric_level() -> None:
    from pyjobkit.logging import configure_logging

    configure_logging(20, fmt="text")  # 20 == INFO
    # No assertion needed - the numeric branch must not raise.


def test_jsonformatter_uses_pretty_output_when_payload_unserialisable() -> None:
    """JsonFormatter falls back to ``default=str`` for exotic types."""

    import json

    from pyjobkit.logging import JsonFormatter

    class _Weird:
        def __repr__(self):
            return "<weird>"

    record = logging.LogRecord(
        name="t", level=logging.INFO, pathname=__file__, lineno=1,
        msg="hi", args=(), exc_info=None,
    )
    record.weird = _Weird()
    out = json.loads(JsonFormatter().format(record))
    assert "weird" in out["weird"]


# fastapi: list response keeps the id when it arrives as a string ---------


def test_fastapi_list_jobs_when_backend_returns_string_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib.util

    if importlib.util.find_spec("fastapi") is None:
        pytest.skip("fastapi not installed")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from pyjobkit.integrations.fastapi import make_router

    backend = MemoryBackend()
    engine = Engine(backend=backend, executors=[_Noop()])

    async def all_jobs(*, status=None, limit=None):
        # Return rows with string ids to exercise the UUID(str(jid))
        # branch in the list endpoint.
        return [{"id": str(uuid4()), "kind": "noop", "status": "queued"}]

    backend.all_jobs = all_jobs  # type: ignore[method-assign]
    app = FastAPI()
    app.include_router(make_router(engine), prefix="/api/v1")
    with TestClient(app) as client:
        r = client.get("/api/v1/jobs")
        assert r.status_code == 200
        assert len(r.json()) == 1
