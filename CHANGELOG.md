
## Unreleased

* **CI integration example** (closes #70)
  Added `.github/workflows/example-integration.yml` plus
  `examples/ci_integration.py` - a self-contained script that exercises
  the full enqueue / claim / retry / shadow path against the in-memory
  backend. The main pipeline now also runs this script as a smoke test
  on every push.

* **Closes #54** - duplicate of #53 ("Поддержка тегов и фильтрации");
  the tagging implementation in #53 covers both issues.

* **Lease-loss handling hook** (closes #62)
  Worker already raised `LeaseLostError` when its lease evaporated and
  awaited the `lease_lost.wait()` task; expose this transition to
  callers via an optional `on_lease_lost(job_id, row)` async callback so
  applications can record metrics, page operators, or hand off to a
  cleanup queue.

* **Per-kind rate limiting** (closes #73)
  Worker accepts a `rate_limits` mapping of `{kind: {max_per_second,
  burst}}`; each entry installs an async token-bucket consulted before
  the executor runs. Configurable via `[pyjobkit.rate_limits.<kind>]`
  TOML tables (or `PYJOBKIT_RATE_LIMITS="http:5:10,email:2"` /
  `--rate-limit http:5:10`). Excess jobs wait until tokens refill.

* **Executor phase profiling** (closes #74)
  `ExecContext.profile_phase("setup")` is an async context manager that
  measures the wall-clock duration of an executor phase, observes it in
  the new `pyjobkit_phase_duration_seconds` Prometheus histogram, and
  emits structured `phase.started` / `phase.ended` log events with
  `duration_ms`. A human-readable summary is also forwarded to the
  job's log sink. Exceptions inside the block propagate; the phase is
  still recorded with an `error` annotation.

* **Entry-point plugin discovery** (closes #51)
  Third-party packages may register executor factories under the
  `pyjobkit.executors` entry-point group. `Engine.register_plugins()`
  loads them on demand; the worker CLI exposes `--enable-plugins` (and
  config `enable_plugins`) for opt-in automatic discovery. Plugins that
  fail to import or return a non-Executor are skipped with a warning.

* **Delayed scheduling helpers** (closes #57)
  Added `Engine.enqueue_at(when=...)` and `Engine.enqueue_in(delay)` as
  ergonomic wrappers over the existing `scheduled_for` parameter.
  `enqueue_at` requires a timezone-aware `datetime`; `enqueue_in`
  accepts either a number of seconds or a `timedelta`. Cron-style
  recurring scheduling (`enqueue_every`) is not yet covered.

* **Comparison documentation** (closes #65)
  Added `docs/comparison.md` positioning Pyjobkit against Celery, RQ,
  and Dramatiq with a feature matrix and "when to pick which" guidance.

* **Configurable watchdog interval** (closes #72)
  `Worker` now accepts `watchdog_interval_s` (CLI `--watchdog-interval`,
  config `watchdog_interval_s`); when omitted it falls back to the
  previous behaviour of running every `lease_ttl` seconds. The reap loop
  emits a structured `watchdog.reaped` event whenever it reclaims one or
  more expired leases.

* **In-memory backend polish** (closes #68)
  `MemoryBackend` is now re-exported as `pyjobkit.MemoryBackend` with a
  documented "tests / prototyping / debug" status. Added `count(status=...)`,
  `all_jobs()`, and `clear()` introspection helpers.

* **TOML / environment configuration** (closes #46)
  New `pyjobkit.config` module loads settings from `.pyjobkit.toml` and
  `PYJOBKIT_*` environment variables. The worker CLI now accepts `--config`,
  `--max-attempts`, and `--default-executor`; `--dsn` may be omitted when
  provided via config or env. Resolution order: CLI → env → TOML → defaults.

* **VSCode workspace** (closes #67)
  Added `.vscode/` with snippets (enqueue, Engine, custom Executor,
  Worker loop, `load_config`, `.pyjobkit.toml`), pytest settings, debug
  launch configs, and recommended extensions.

* **Public typed API** (closes #48)
  New `pyjobkit.types` module exposes `JobStatus` / `LogStream` literals
  plus `JobRecord` / `JobResult` / `FailureReason` TypedDicts. A `py.typed`
  marker ships in the wheel so external type checkers honor the inline
  annotations (PEP 561).

* **Pluggable retry policies** (closes #52)
  New `pyjobkit.retry` module with `RetryPolicy` ABC and built-in
  `FixedDelay`, `ExponentialBackoff`, `JitteredExponentialBackoff`.
  Worker accepts a policy via its constructor or as a spec string
  (`"exponential:1:2"`, `"exponential_jitter:1:2:30:0.1"`, `"fixed:5"`).
  Per-job override available via `Engine.enqueue(..., retry_policy=...)`.
  CLI gains `--retry-policy`; config gains `retry_policy`.

* **Shadow / dry-run mode** (closes #75)
  `Engine.enqueue(..., shadow=True)` marks a job as dry-run. The worker
  exposes `ctx.is_shadow = True` so executors can short-circuit side
  effects; logs and progress updates are still delivered. The executor's
  return value is discarded and the job completes with the marker result
  `{"shadow": True, "result_discarded": True}`.

* **Structured JSON logs** (closes #45)
  New `pyjobkit.logging.JsonFormatter` plus `configure_logging(level, fmt=...)`
  helper. The worker CLI gains `--log-format` (and config key `log_format`)
  to switch between `text` and `json`. The worker emits structured
  state-change events (`job.started`, `job.succeeded`, `job.failed`,
  `job.timeout`, `job.retry`, `job.cancelled`, `job.lease_lost`,
  `job.lock_conflict`) with `job_id`, `worker_id`, `status`, and
  `duration_ms` fields.

##  Pyjobkit 0.2.0 – Stable Production Release

###  Major Improvements

* **Reliable Job Cancellation**
  Jobs cancelled during execution (`Engine.cancel()`) no longer reach the `succeeded` state. Cancellation is now enforced consistently.

* **Timeout Handling Respects max_attempts**
  Jobs that exceed `timeout_s` are now retried (if `max_attempts` allows), using exponential backoff. Previously, such jobs were marked as permanently failed.

* **Graceful Worker Shutdown**
  `Worker.wait_stopped()` now properly signals after all tasks are drained, enabling clean process termination.

* **Subprocess Cleanup on Cancel or Timeout**
  `SubprocessExecutor` now terminates or force-kills subprocesses if they outlive their job timeout or are cancelled, preventing zombie processes.

### Internal Fixes

* Executors now correctly propagate `CancelledError`, avoiding retries after cancellation.
* Worker loop is more robust to backend failures and transient errors.
* Improved fallback behavior for queueing backends without `SKIP LOCKED`.

---
