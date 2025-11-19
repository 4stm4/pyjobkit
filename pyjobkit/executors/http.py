"""HTTP executor built on httpx.AsyncClient."""

from __future__ import annotations

import asyncio
import time
from uuid import UUID

import httpx

from ..contracts import ExecContext, Executor


class HttpExecutor(Executor):
    kind = "http"

    async def run(self, *, job_id: UUID, payload: dict, ctx: ExecContext) -> dict:
        method = payload.get("method", "GET").upper()
        url = payload["url"]
        timeout = payload.get("timeout", 30)
        retries = payload.get("retries", 0)
        backoff = payload.get("backoff", 0.5)
        data = payload.get("data")
        json_payload = payload.get("json")
        headers = payload.get("headers")
        async with httpx.AsyncClient(timeout=timeout) as client:
            attempt = 0
            while True:
                try:
                    started = time.perf_counter()
                    resp = await client.request(
                        method,
                        url,
                        data=data,
                        json=json_payload,
                        headers=headers,
                    )
                    duration = int((time.perf_counter() - started) * 1000)
                    await ctx.log(f"{method} {url} -> {resp.status_code} in {duration}ms")
                    body: object
                    if "application/json" in resp.headers.get("content-type", ""):
                        body = resp.json()
                    else:
                        body = resp.text
                    return {
                        "status": resp.status_code,
                        "headers": dict(resp.headers),
                        "body": body,
                        "duration_ms": duration,
                    }
                except Exception as exc:
                    if attempt >= retries:
                        await ctx.log(f"http executor failed: {exc}", stream="stderr")
                        raise
                    await ctx.log(f"attempt {attempt+1} failed: {exc}", stream="stderr")
                    await asyncio.sleep(backoff * (2**attempt))
                    attempt += 1
