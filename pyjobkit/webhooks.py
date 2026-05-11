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

Webhook failures are logged at WARNING level and otherwise silently
ignored - they never block the worker or affect the job's stored state.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping
from uuid import UUID

import httpx

WEBHOOK_PAYLOAD_KEY = "__pjk_webhooks"

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
) -> None:
    """Send the appropriate webhook for ``status`` if one is registered."""

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
    try:
        if client is not None:
            response = await client.post(url, json=body, timeout=5.0)
        else:
            async with httpx.AsyncClient(timeout=5.0) as ad_hoc:
                response = await ad_hoc.post(url, json=body)
        response.raise_for_status()
    except Exception as exc:
        logger.warning(
            "webhook %s -> %s failed for job %s: %s",
            key,
            url,
            job_id,
            exc,
            exc_info=True,
        )
