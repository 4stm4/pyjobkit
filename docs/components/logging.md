# Logging

Pyjobkit emits state-change events through the standard `logging`
module. Two formatters ship out of the box:

- The default `text` formatter prints
  `2026-05-12 10:00:00 [INFO] pyjobkit.worker: job state changed`.
- `pyjobkit.logging.JsonFormatter` renders each record as a single
  JSON object with structured fields surfaced first.

## Switching to JSON

```bash
pyjobkit --log-format json
# or
export PYJOBKIT_LOG_FORMAT=json
```

Programmatically:

```python
from pyjobkit.logging import configure_logging

configure_logging("INFO", fmt="json")
```

`configure_logging` installs a single `StreamHandler` on the root
logger and tags it so repeated calls swap it in place. **Other
handlers (e.g. pytest's `caplog`, Sentry's `SentryHandler`) are not
removed.**

## Output shape

```json
{
  "ts": "2026-05-12T10:00:00+00:00",
  "level": "INFO",
  "logger": "pyjobkit.worker",
  "msg": "job state changed",
  "event": "job.succeeded",
  "job_id": "00000000-...",
  "worker_id": "11111111-...",
  "status": "success",
  "duration_ms": 12.5
}
```

The `priority keys` block (`ts` / `level` / `logger` / `msg` /
`event` / `job_id` / `worker_id` / `status` / `duration_ms`) is
emitted in this fixed order so log aggregators can grep them
easily. Anything else passed via `extra={...}` is appended in
insertion order.

## Event catalogue

| `event` | When |
| --- | --- |
| `job.started` | Worker claimed a job and is about to call the executor. |
| `job.succeeded` | Normal completion. |
| `job.shadow_succeeded` | Shadow run finished; executor's return value was discarded. |
| `job.timeout` | Hit `timeout_s`, no retries left (or age cap reached). |
| `job.retry` | Failed but will be retried. |
| `job.failed` | Max attempts exhausted (or age cap reached). |
| `job.cancelled` | `Engine.cancel` observed or `CancelledError` raised. |
| `job.lease_lost` | Worker lost its lease mid-job. |
| `job.lock_conflict` | Optimistic-concurrency conflict during finalise. |
| `watchdog.reaped` | Lease reaper finalised one or more abandoned jobs. |
| `worker.heartbeat` | Periodic liveness ping (only when `heartbeat_interval_s` is set). |
| `chain.broken` | Tail enqueue failed after the head succeeded. |
| `phase.started` / `phase.ended` | From `ctx.profile_phase(...)`. |

Filter on the `event` field in your aggregator to build dashboards;
filter on `worker_id` to track a single replica.

## Per-phase profiling

```python
async def run(self, *, job_id, payload, ctx):
    async with ctx.profile_phase("download", url=payload["url"]):
        data = await fetch(payload["url"])
    async with ctx.profile_phase("parse"):
        parsed = parse(data)
    return {"records": len(parsed)}
```

Each phase emits `phase.started` on entry and `phase.ended` on exit
with `duration_ms` plus any keyword arguments you pass in. The exit
event also gets `error: <ExceptionType>` if the block raised. A
human-readable summary line (`"phase download took 12.3ms"`) is
sent to the executor's `ctx.log` so it shows up alongside the job's
normal output.

The duration is also observed in the
`pyjobkit_phase_duration_seconds` Prometheus histogram.

## Adding fields to every record

`JsonFormatter` forwards every `extra` field that is not a stdlib
`LogRecord` attribute. Use a `LoggerAdapter` (or `contextvars`) to
attach request ids, tenant ids, etc.:

```python
import logging

logger = logging.getLogger("myapp.workflow")

class Tenanted(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        kwargs.setdefault("extra", {})["tenant_id"] = self.extra["tenant_id"]
        return msg, kwargs

log = Tenanted(logger, {"tenant_id": "acme"})
log.info("doing the thing", extra={"event": "demo"})
```

The JSON formatter will surface `tenant_id` and `event` next to the
priority keys.
