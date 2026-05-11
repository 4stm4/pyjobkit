# Configuration

Worker settings can be supplied in three ways, with the following
priority (highest wins):

1. **CLI flags** passed to the `pyjobkit` command.
2. **Environment variables** prefixed with `PYJOBKIT_`.
3. **TOML file** - `.pyjobkit.toml` in the working directory, or the
   path passed to `--config`.

Defaults are baked into [`pyjobkit/config.py`](https://github.com/4stm4/pyjobkit/blob/main/pyjobkit/config.py).

## Available settings

| TOML key | Env var | CLI flag | Default | Notes |
|---|---|---|---|---|
| `dsn` (alias `db_url`) | `PYJOBKIT_DSN` | `--dsn` | _(required)_ | SQLAlchemy async DSN |
| `concurrency` | `PYJOBKIT_CONCURRENCY` | `--concurrency` | `8` | Max simultaneous coroutines |
| `batch` | `PYJOBKIT_BATCH` | `--batch` | `1` | Jobs claimed per poll |
| `lease_ttl` | `PYJOBKIT_LEASE_TTL` | `--lease-ttl` | `30` | Seconds before lease expires |
| `poll_interval` | `PYJOBKIT_POLL_INTERVAL` | `--poll-interval` | `0.5` | Idle poll cadence (s) |
| `watchdog_interval_s` | `PYJOBKIT_WATCHDOG_INTERVAL_S` | `--watchdog-interval` | `lease_ttl` | Lease reaper cadence (s) |
| `log_level` | `PYJOBKIT_LOG_LEVEL` | `--log-level` | `INFO` | Root logger level |
| `log_format` | `PYJOBKIT_LOG_FORMAT` | `--log-format` | `text` | `text` or `json` |
| `retry_policy` | `PYJOBKIT_RETRY_POLICY` | `--retry-policy` | `exponential:1:2` | Default policy spec |
| `disable_skip_locked` | `PYJOBKIT_DISABLE_SKIP_LOCKED` | `--disable-skip-locked` | `false` | For non-Postgres backends |
| `enable_plugins` | `PYJOBKIT_ENABLE_PLUGINS` | `--enable-plugins` | `false` | Auto-discover entry points |
| `extra_executors` | `PYJOBKIT_EXTRA_EXECUTORS` | `--executor` (repeatable) | `[]` | Dotted-path factories |
| `default_executor` | `PYJOBKIT_DEFAULT_EXECUTOR` | `--default-executor` | _none_ | Single primary executor |
| `rate_limits` | `PYJOBKIT_RATE_LIMITS` | `--rate-limit` (repeatable) | `{}` | See below |

## Example `.pyjobkit.toml`

```toml
[pyjobkit]
dsn = "postgresql+asyncpg://user:pass@host/db"
concurrency = 16
batch = 4
lease_ttl = 30
poll_interval = 0.5
log_format = "json"
retry_policy = "exponential_jitter:1:2:30:0.1"

[pyjobkit.rate_limits]
http  = { max_per_second = 5,  burst = 10 }
email = { max_per_second = 1 }
```

Equivalent environment / CLI invocation:

```bash
export PYJOBKIT_DSN="postgresql+asyncpg://user:pass@host/db"
export PYJOBKIT_LOG_FORMAT=json
pyjobkit --concurrency 16 --batch 4 \
         --retry-policy "exponential_jitter:1:2:30:0.1" \
         --rate-limit http:5:10 --rate-limit email:1
```

## Retry policies

Specs accepted by `--retry-policy` / `retry_policy`:

- `fixed:1.5` - constant 1.5 s delay
- `exponential:1:2` - 1 s base, factor 2 (1, 2, 4, ...)
- `exponential:1:2:30` - same, capped at 30 s
- `exponential_jitter:1:2:30:0.1` - 10% jitter
- Custom Python objects can be passed when constructing `Worker(retry_policy=...)`.

Per-job overrides flow through `Engine.enqueue(kind=..., payload=...,
retry_policy="fixed:5")`; the policy spec rides as a payload marker.

## Shadow / dry-run

`Engine.enqueue(..., shadow=True)` runs the executor normally but
discards its result, storing `{"shadow": true, "result_discarded":
true}` instead. Executors can branch on `ctx.is_shadow` to skip
irreversible side effects.
