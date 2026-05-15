# Webhooks

Pyjobkit can POST a JSON callback to an external URL when a job
reaches a terminal state. Webhooks are declared at enqueue time and
fired by the worker immediately after the state transition.

## Subscribing

```python
await engine.enqueue(
    kind="ingest",
    payload={"file": "s3://..."},
    webhooks={
        "complete": "https://api.example.com/hooks/done",
        "fail":     "https://api.example.com/hooks/failed",
        "timeout":  "https://api.example.com/hooks/timeout",
    },
)
```

Any subset of the three keys is allowed; unknown keys are rejected.
Empty URLs are rejected too. The map is persisted as a payload
marker (`__pjk_webhooks`) and the worker reads it back when it
finalises the job.

## Request shape

```http
POST /hooks/done HTTP/1.1
Content-Type: application/json
X-Pyjobkit-Timestamp: 1715817600
X-Pyjobkit-Signature: sha256=<hex>      (only when a secret is set)

{
  "job_id": "00000000-0000-0000-0000-000000000001",
  "kind": "ingest",
  "status": "success",
  "attempts": 1,
  "duration_ms": 312.4,
  "result": {"bytes_ingested": 1234}
}
```

## Signing

Set the `PYJOBKIT_WEBHOOK_SECRET` environment variable on the worker
(or pass `secret="..."` to `fire`). The library will:

1. Include an `X-Pyjobkit-Timestamp: <epoch_seconds>` header.
2. Compute `HMAC-SHA256("{timestamp}.{raw_body}", secret)` and send
   it as `X-Pyjobkit-Signature: sha256=<hex>`.

Without a secret the headers are omitted; the receiver can decide to
accept unsigned traffic or reject it.

## Verifying on the receiver

```python
from pyjobkit.webhooks import verify_signature

ok = verify_signature(
    body=request.body,
    secret=settings.PYJOBKIT_WEBHOOK_SECRET,
    signature_header=request.headers.get("X-Pyjobkit-Signature"),
    timestamp_header=request.headers.get("X-Pyjobkit-Timestamp"),
    replay_window_s=300,    # reject anything older than 5 minutes
)
if not ok:
    return Response(401)
```

`verify_signature` returns `False` for any malformed input (missing
headers, non-numeric timestamp, wrong algorithm prefix, expired
timestamp, signature mismatch). The signature compare is constant
time (`hmac.compare_digest`).

`replay_window_s` defaults to 300 seconds; choose a value that fits
your retry / clock-skew tolerance.

## Retry behaviour

`fire(..., max_attempts=3, initial_delay_s=0.5)` is the default.
The retry loop distinguishes retryable from non-retryable failures:

- `2xx`: success, return.
- `5xx` and transport errors (connection refused, DNS, timeout):
  retried with exponential backoff (`initial_delay_s * 2^n`).
- `4xx`: not retried (the receiver's contract says "stop bothering
  me").

Every failed attempt bumps the
`pyjobkit_webhook_failures_total` Prometheus counter; the final
warning is logged at WARNING level with the URL and exception. The
worker never blocks on webhook delivery - failures are logged and
the job's stored status is unaffected.

## Programmatic firing

```python
import httpx
from pyjobkit.webhooks import fire

async with httpx.AsyncClient() as client:
    await fire(
        webhooks={"complete": "https://example.com/hook"},
        status="success",
        job_id=job_id,
        kind="ingest",
        attempts=1,
        duration_ms=12.3,
        result={"ok": True},
        client=client,
        secret="my-secret",
        max_attempts=3,
        initial_delay_s=0.5,
    )
```

You can pass a pre-built `httpx.AsyncClient` to reuse connection
pools across many calls; without it `fire` opens an ad-hoc client
per call.

## Common gotchas

- Webhooks fire **after** the database state changes. If the
  process dies between `engine.succeed` and `fire`, the receiver
  never hears about it. Design the consumer to be idempotent or
  pull job state via `GET /jobs/{id}` instead.
- The signature covers the *body bytes*, not a re-serialised JSON.
  Receivers must use the raw request body, not their JSON parser's
  re-encoded form.
- `replay_window_s` requires clocks within reasonable skew. NTP is
  enough; don't make the window so tight that a normal retry storm
  invalidates legitimate deliveries.
