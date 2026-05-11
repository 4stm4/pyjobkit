"""Optional webhook notifications fired on terminal job states (#56).

Webhook URLs are attached to a job at enqueue time via
``Engine.enqueue(..., webhooks={"complete": "...", "fail": "...",
"timeout": "..."})``. They are persisted as a marker inside the payload
and consumed by the worker right after a terminal state transition.

The HTTP request is a JSON ``POST`` with the following body::

    {
      "job_id": "uuid-string",
      "kind": "job-kind",
      "status": "success|failed|timeout",
      "attempts": int,
      "duration_ms": float | null,
      "result": {...} | null
    }

If ``PYJOBKIT_WEBHOOK_SECRET`` is set in the environment, every request
is signed with HMAC-SHA256 over the raw JSON body and sent via the
``X-Pyjobkit-Signature: sha256=<hex>`` header so receivers can verify
the call originated from this worker.

Webhook failures are logged at WARNING level and retried with
exponential backoff up to ``max_attempts`` times (default 3). After the
final failure the worker continues - webhooks never affect a job's
stored state.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
from typing import Any, Mapping
from uuid import UUID

import httpx

from . import metrics

WEBHOOK_PAYLOAD_KEY = "__pjk_webhooks"
WEBHOOK_SECRET_ENV = "PYJOBKIT_WEBHOOK_SECRET"

# Logical event -> webhook key
_EVENT_KEYS = {
    "success": "complete",
    "failed": "fail",
    "timeout": "timeout",
}

logger = logging.getLogger(__name__)


def normalize_webhooks(webhooks: Mapping[str, str] | None) -> dict[str, str] | None:
    """Validate and lowercase the webhook map; return ``None`` when empty."""

    if not webhooks:
        return None
    normalized: dict[str, str] = {}
    for raw_key, url in webhooks.items():
        key = raw_key.strip().lower()
        if key not in {"complete", "fail", "timeout"}:
            raise ValueError(
                f"webhooks keys must be one of "
                f"{{'complete', 'fail', 'timeout'}}; got {raw_key!r}"
            )
        if not isinstance(url, str) or not url.strip():
            raise ValueError(
                f"webhook URL for {key!r} must be a non-empty string"
            )
        normalized[key] = url.strip()
    return normalized


def _signature_header(body: bytes, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


async def fire(
    *,
    webhooks: Mapping[str, str] | None,
    status: str,
    job_id: UUID,
    kind: str,
    attempts: int,
    duration_ms: float | None,
    result: Any,
    client: httpx.AsyncClient | None = None,
    max_attempts: int = 3,
    initial_delay_s: float = 0.5,
    secret: str | None = None,
) -> None:
    """Send the appropriate webhook for ``status`` if one is registered.

    The call is retried with exponential backoff (``initial_delay_s``,
    ``initial_delay_s * 2``, ...) up to ``max_attempts`` total tries. If
    a ``secret`` is provided (or ``PYJOBKIT_WEBHOOK_SECRET`` is set in
    the environment) the body is signed with HMAC-SHA256 and the digest
    is forwarded as the ``X-Pyjobkit-Signature`` header.
    """

    if not webhooks:
        return
    key = _EVENT_KEYS.get(status)
    if key is None:
        return
    url = webhooks.get(key)
    if not url:
        return

    body = {
        "job_id": str(job_id),
        "kind": kind,
        "status": status,
        "attempts": attempts,
        "duration_ms": duration_ms,
        "result": result,
    }
    raw = json.dumps(body, default=str).encode("utf-8")
    headers: dict[str, str] = {"Content-Type": "application/json"}
    effective_secret = secret if secret is not None else os.environ.get(WEBHOOK_SECRET_ENV)
    if effective_secret:
        headers["X-Pyjobkit-Signature"] = _signature_header(raw, effective_secret)

    async def _send_once(c: httpx.AsyncClient) -> httpx.Response:
        return await c.post(url, content=raw, headers=headers, timeout=5.0)

    delay = initial_delay_s
    last_exc: Exception | None = None
    for attempt in range(1, max(1, max_attempts) + 1):
        try:
            if client is not None:
                response = await _send_once(client)
            else:
                async with httpx.AsyncClient() as ad_hoc:
                    response = await _send_once(ad_hoc)
            response.raise_for_status()
            return
        except Exception as exc:
            last_exc = exc
            metrics.webhook_failures.inc()
            logger.warning(
                "webhook %s -> %s failed (attempt %d/%d) for job %s: %s",
                key,
                url,
                attempt,
                max_attempts,
                job_id,
                exc,
            )
            if attempt >= max_attempts:
                break
            await asyncio.sleep(delay)
            delay *= 2

    logger.warning(
        "webhook %s -> %s permanently failed for job %s: %s",
        key,
        url,
        job_id,
        last_exc,
    )
