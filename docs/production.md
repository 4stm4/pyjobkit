# Production deployment guide

This page collects the configuration, monitoring, and operational
practices recommended when running Pyjobkit beyond local development.

## Schema

Run the bundled migrations against your production database before
starting workers:

```bash
pyjobkit-migrate --dsn "$PYJOBKIT_DSN" up
pyjobkit-migrate --dsn "$PYJOBKIT_DSN" current
```

`pyjobkit-migrate` understands the async DSN drivers
(`postgresql+asyncpg`, `mysql+aiomysql`, `sqlite+aiosqlite`) and runs
the migrations through the matching synchronous driver. Subsequent
releases ship new Alembic revisions; re-run `pyjobkit-migrate up` as
part of every deploy.

## Worker process layout

- One worker process per pod / container. Concurrency is configured
  via `--concurrency`; the worker is asyncio-based, so a single
  process with `--concurrency 32` handles thousands of I/O-bound jobs
  per minute.
- Set `--lease-ttl` to roughly 2-3x the longest expected job duration
  to absorb GC pauses and short network blips. The watchdog runs at
  `--watchdog-interval` (defaults to `lease_ttl`) and reaps abandoned
  jobs.
- For long-running CPU-bound jobs, prefer a dedicated worker pool
  with low `--concurrency` (1-2) so coroutines do not starve each
  other.

## Graceful shutdown

The CLI installs `SIGTERM` / `SIGINT` handlers that call
`worker.request_stop()`, allowing in-flight tasks to finish before the
process exits. Configure container managers (Kubernetes, Nomad,
systemd) to send `SIGTERM` and wait at least `stop_timeout` (default
60s) before escalating to `SIGKILL`. In Kubernetes:

```yaml
terminationGracePeriodSeconds: 90
```

## Backpressure and retention

The `job_tasks` table accumulates terminal jobs forever unless pruned.
Schedule `pyjobkit-prune` daily:

```cron
15 3 * * * pyjobkit-prune --older-than 30d --statuses success,cancelled
30 3 * * * pyjobkit-prune --older-than 90d --statuses failed,timeout
```

Add an index on `(status, finished_at)` if your retention sweeps grow
slow at scale; the bundled baseline migration covers `(status,
scheduled_for, priority)` which is sufficient for the dispatch path.

## Observability

- **Logs.** Set `--log-format json` (or `PYJOBKIT_LOG_FORMAT=json`)
  and pipe stderr to your aggregator. State transitions carry
  `event`, `job_id`, `worker_id`, `status`, and `duration_ms`.
- **Metrics.** Install `pyjobkit[metrics]` and pass `--metrics-port
  9000`. Scrape `/metrics` with Prometheus; the histograms include
  lease extension latency, lock conflicts, executor phase durations,
  and webhook failures.
- **Heartbeats.** Pass `--heartbeat-interval-s` (CLI: not yet exposed
  in 1.0; use the library API directly) to emit a `worker.heartbeat`
  event every N seconds for external liveness tracking.
- **Tracing.** Trace IDs are not propagated automatically; attach
  them to payloads and forward in your executors when needed.

## Security

- `SubprocessExecutor` runs arbitrary commands from the payload.
  Production deployments must pass `allowed_commands=[...]` if the
  enqueue surface is exposed to untrusted callers (e.g. the REST
  router without auth).
- `DockerExecutor` accepts an image + command from the payload; run
  it on dedicated workers and consider scrubbing fields you do not
  expect.
- The REST integration is unauthenticated by default. Pass
  `dependencies=[Depends(verify_token)]` to `make_router`.
- Webhook deliveries are unsigned unless `PYJOBKIT_WEBHOOK_SECRET` is
  set. Receivers verify via the `X-Pyjobkit-Signature: sha256=<hex>`
  header.

## Postgres tips

- `--disable-skip-locked` is for SQLite / older Postgres
  installations. Keep it off in production Postgres to avoid worker
  contention.
- The bundled migrations create `idx_jobs_status_scheduled` and
  `idx_jobs_leased`; verify with `\di job_tasks*`.
- Pool size: SQLAlchemy's default of 5 connections per process is
  usually enough at `--concurrency 32`. Bump `pool_size`/`max_overflow`
  on the async engine if you see pool exhaustion.

## Rate limits

Per-kind token buckets are configured via TOML / env / CLI:

```toml
[pyjobkit.rate_limits]
http  = { max_per_second = 5,  burst = 10 }
email = { max_per_second = 1 }
```

Excess jobs **wait** for a token rather than being rejected; pair
with a sensible `--lease-ttl` so the wait does not trip the
watchdog.

## Common operational tasks

- **Replay failed jobs.** Move `failed` rows back to `queued`:
  ```sql
  UPDATE job_tasks
     SET status = 'queued',
         attempts = 0,
         scheduled_for = now(),
         lease_until = NULL,
         leased_by = NULL,
         version = version + 1
   WHERE status = 'failed' AND finished_at > now() - interval '1 day';
  ```
- **Drain a worker before scaling down.** Send `SIGTERM`; the CLI
  finishes inflight jobs (up to `stop_timeout`) and exits.
- **Quickly drain a backlog.** Run `pyjobkit --once` on a one-shot
  pod to flush a queue then exit; use this from a `Job` instead of a
  `Deployment` in Kubernetes.
