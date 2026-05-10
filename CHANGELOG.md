
## Unreleased

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
