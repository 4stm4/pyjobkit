# CLI tools

Pyjobkit ships four console scripts. All of them honour the same
configuration sources as the library (CLI flags > `PYJOBKIT_*` env
vars > `.pyjobkit.toml` > defaults).

| Command | Purpose |
| --- | --- |
| `pyjobkit` | Run a worker. |
| `pyjobkit-simulate` | Drive jobs from a JSON / YAML file against an in-memory backend. |
| `pyjobkit-migrate` | Apply / rollback Alembic schema migrations. |
| `pyjobkit-prune` | Delete terminal jobs older than a threshold. |

## `pyjobkit` (worker)

```bash
pyjobkit --dsn postgresql+asyncpg://user:pass@host/db \
         --concurrency 16 --batch 4 --lease-ttl 30 \
         --log-format json --metrics-port 9000
```

All flags are optional except `--dsn` (or `PYJOBKIT_DSN`, or `dsn`
in `.pyjobkit.toml`). The full set is documented in
[configuration](../configuration.md). Two operational flags worth
calling out:

```bash
pyjobkit --once               # drain queue and exit (cron-friendly)
pyjobkit --kind email --kind sms   # only handle these kinds
```

The process installs SIGTERM / SIGINT handlers that call
`worker.request_stop`. Container orchestrators that send SIGTERM
(Kubernetes, Docker, systemd) get a clean drain up to
`--stop-timeout` seconds (default 60).

## `pyjobkit-simulate`

```bash
pyjobkit-simulate jobs.json \
        --concurrency 2 --timeout 30 --log-format json
```

Input format (JSON):

```json
{
  "jobs": [
    {"kind": "subprocess", "payload": {"cmd": "echo hi"}},
    {"kind": "subprocess",
     "payload": {"cmd": ["python", "-c", "print(2+2)"]},
     "max_attempts": 1,
     "retry_policy": "fixed:0.1",
     "shadow": false}
  ]
}
```

YAML works too when `pyyaml` is installed.

The simulator boots a fresh `MemoryBackend`, registers
`SubprocessExecutor` + `HttpExecutor` by default (add more with
`--executor module:factory`), enqueues every job, runs the worker
with `run(once=True)`, and prints a JSON summary on stdout:

```json
{
  "summary": {
    "success":  [{"id": "...", "kind": "subprocess", "result": {...}}],
    "failed":   [],
    "timeout":  [],
    "cancelled": [],
    "queued":   [],
    "running":  []
  }
}
```

Exit code is 0 when there are no failures or timeouts, otherwise 1.
The tool is intended for CI smoke tests and local debugging - it
never touches a real database.

## `pyjobkit-migrate`

Wraps Alembic against the bundled migrations directory.

```bash
pyjobkit-migrate --dsn $PYJOBKIT_DSN up           # alembic upgrade head
pyjobkit-migrate --dsn $PYJOBKIT_DSN down 1       # alembic downgrade -1
pyjobkit-migrate --dsn $PYJOBKIT_DSN current
pyjobkit-migrate --dsn $PYJOBKIT_DSN history
```

The tool accepts the same async DSN forms the worker uses
(`postgresql+asyncpg`, `mysql+aiomysql`, `sqlite+aiosqlite`) and
rewrites them internally to their sync counterparts (`psycopg2`,
`pymysql`, `sqlite`). The corresponding sync driver is pulled in by
the `pg` / `mysql` / `sqlite` extras:

```bash
pip install "pyjobkit[pg]"      # asyncpg + psycopg2-binary
pip install "pyjobkit[mysql]"   # aiomysql + pymysql
pip install "pyjobkit[sqlite]"  # aiosqlite (sqlite3 is in stdlib)
```

The Helm chart wires this up as a pre-install `Job`; bare metal
deploys typically run it from a release script.

## `pyjobkit-prune`

Deletes terminal jobs older than a threshold.

```bash
pyjobkit-prune --older-than 30d --statuses success,cancelled
pyjobkit-prune --older-than 90d --statuses failed,timeout
pyjobkit-prune                                       # deletes ALL terminal jobs
```

`--older-than` accepts compact durations:

| Suffix | Meaning |
| --- | --- |
| `Ns` / `N` | seconds |
| `Nm` | minutes |
| `Nh` | hours |
| `Nd` | days |

The script prints the deleted count to stdout (suitable for logging
via cron). Exit code is 0 on success.

A typical cron entry:

```cron
15 3 * * * /usr/local/bin/pyjobkit-prune --older-than 30d --statuses success,cancelled
30 3 * * * /usr/local/bin/pyjobkit-prune --older-than 90d --statuses failed,timeout
```

Or as a Kubernetes `CronJob` reusing the worker's Secret for DSN.

## Common gotchas

- `pyjobkit --dsn ... --once` returns when the queue is empty *as
  the worker observes it*. Jobs added concurrently after the last
  successful claim may be left behind. Re-run the command, or use
  the daemon mode for steady-state.
- `pyjobkit-migrate down 0` is a no-op; `down 1` rolls back the
  most recent revision; `down -2` rolls back two. The CLI accepts
  both forms; positive integers are normalised to the negative
  Alembic spelling.
- `pyjobkit-prune` deletes rows hard. There is no soft-delete; if
  you need an audit trail, dump the rows to cold storage first or
  add a trigger.
