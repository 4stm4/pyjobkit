"""Tests for executor allowlists, webhook HMAC, and router auth hook."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from uuid import UUID, uuid4

import httpx
import pytest

from pyjobkit.executors.subprocess import SubprocessExecutor
from pyjobkit.webhooks import fire


class _NullCtx:
    is_shadow = False

    async def log(self, message, /, *, stream="stdout"):
        pass

    async def is_cancelled(self):
        return False

    async def set_progress(self, value, /, **meta):
        pass


def test_subprocess_allowlist_rejects_unknown_command() -> None:
    async def _run() -> None:
        executor = SubprocessExecutor(allowed_commands=["echo"])
        with pytest.raises(PermissionError):
            await executor.run(
                job_id=uuid4(),
                payload={"cmd": ["rm", "-rf", "/"]},
                ctx=_NullCtx(),  # type: ignore[arg-type]
            )

    asyncio.run(_run())


def test_subprocess_allowlist_accepts_known_command() -> None:
    async def _run() -> None:
        executor = SubprocessExecutor(allowed_commands=["echo"])
        result = await executor.run(
            job_id=uuid4(),
            payload={"cmd": ["echo", "hi"]},
            ctx=_NullCtx(),  # type: ignore[arg-type]
        )
        assert result["returncode"] == 0

    asyncio.run(_run())


def test_subprocess_no_allowlist_warns_once(caplog: pytest.LogCaptureFixture) -> None:
    async def _run() -> None:
        import logging

        executor = SubprocessExecutor()  # no allowlist - permissive but warns
        with caplog.at_level(logging.WARNING, logger="pyjobkit.executors.subprocess"):
            await executor.run(
                job_id=uuid4(),
                payload={"cmd": ["echo", "hi"]},
                ctx=_NullCtx(),  # type: ignore[arg-type]
            )
            # Second run must not warn again.
            await executor.run(
                job_id=uuid4(),
                payload={"cmd": ["echo", "hi"]},
                ctx=_NullCtx(),  # type: ignore[arg-type]
            )

        warns = [r for r in caplog.records if "without allowed_commands" in r.message]
        assert len(warns) == 1

    asyncio.run(_run())


class _CapturingTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(200)


def test_webhook_signature_header_when_secret_provided() -> None:
    async def _run() -> None:
        transport = _CapturingTransport()
        client = httpx.AsyncClient(transport=transport)
        await fire(
            webhooks={"complete": "https://example.com"},
            status="success",
            job_id=UUID("00000000-0000-0000-0000-000000000001"),
            kind="x",
            attempts=1,
            duration_ms=1.0,
            result={"ok": True},
            client=client,
            secret="topsecret",
        )
        await client.aclose()
        req = transport.requests[0]
        assert "X-Pyjobkit-Signature" in req.headers
        sig = req.headers["X-Pyjobkit-Signature"]
        expected = "sha256=" + hmac.new(
            b"topsecret", req.content, hashlib.sha256
        ).hexdigest()
        assert sig == expected
        # Ensure body parses cleanly.
        body = json.loads(req.content)
        assert body["status"] == "success"

    asyncio.run(_run())


def test_webhook_retries_on_5xx_then_succeeds() -> None:
    async def _run() -> None:
        attempts: list[int] = []

        class _RetryTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request):
                attempts.append(1)
                if len(attempts) < 3:
                    return httpx.Response(500)
                return httpx.Response(200)

        client = httpx.AsyncClient(transport=_RetryTransport())
        await fire(
            webhooks={"complete": "https://example.com"},
            status="success",
            job_id=UUID("00000000-0000-0000-0000-000000000001"),
            kind="x",
            attempts=1,
            duration_ms=1.0,
            result=None,
            client=client,
            max_attempts=5,
            initial_delay_s=0.001,
        )
        await client.aclose()
        assert len(attempts) == 3

    asyncio.run(_run())


def test_router_dependencies_apply_to_endpoints() -> None:
    import importlib.util

    if importlib.util.find_spec("fastapi") is None:
        pytest.skip("fastapi not installed")
    from fastapi import Depends, FastAPI, HTTPException
    from fastapi.testclient import TestClient

    from pyjobkit import Engine, MemoryBackend
    from pyjobkit.integrations.fastapi import make_router

    def require_token(authorization: str | None = None) -> None:
        if authorization != "Bearer s3cr3t":
            raise HTTPException(401, "unauthorized")

    engine = Engine(backend=MemoryBackend(), executors=[])
    app = FastAPI()
    app.include_router(
        make_router(engine, dependencies=[Depends(require_token)]),
        prefix="/api/v1",
    )
    with TestClient(app) as client:
        assert client.get("/api/v1/healthz").status_code == 401
        ok = client.get("/api/v1/healthz", headers={"Authorization": "Bearer s3cr3t"})
        assert ok.status_code == 200
