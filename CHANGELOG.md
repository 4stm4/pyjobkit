
## Unreleased

* **DockerExecutor** (closes #49)
  New `pyjobkit.executors.docker.DockerExecutor` runs one-shot Docker
  containers per job using the optional `aiodocker` package. Payload
  shape: `{image, command, env, timeout_s, pull}`; non-zero exit codes
  raise `DockerExecutionError`. Install as `pyjobkit[docker]`. The
  module is lazy-imported so the rest of the package keeps working
  without aiodocker installed.

* **Batch executor API** (closes #63)
  New `pyjobkit.batch` module ships `BatchExecutor` (abstract `run_batch`
  taking a list of `BatchJob`) plus a `dispatch_batch(engine, executor,
  rows, worker_id)` helper that handles `mark_running` for each row and
  per-job `succeed` / `fail` with the right `expected_version`. Allows
  callers to amortise expensive operations across many similar jobs
  without subclassing `Worker`.

* **Leader election primitives** (closes #61)
  New `pyjobkit.leader` module exposes a `LeaderLock` ABC, an
  in-process `MemoryLeaderLock` reference implementation that shares
  state across instances by name, and a `leader_loop(...)` helper that
  drives a coroutine while the lock is held with automatic renewal.
  Production deployments back the lock with a SQL row or Redis SETNX.

* **Prometheus `/metrics` server** (closes #44)
  New `pyjobkit.metrics.start_metrics_server(port, host)` helper boots
  the `prometheus_client` HTTP exporter in a daemon thread; the worker
  CLI accepts `--metrics-port` / `--metrics-host`. Added a
  `pyjobkit[metrics]` install extra that pulls in `prometheus_client`.
  Without the extra the helper logs a warning and returns False.

* **Worker heartbeat loop** (closes #59)
  `Worker(heartbeat_interval_s=..., on_heartbeat=async_fn)` runs a
  background loop that emits structured `worker.heartbeat` log events
  on a configurable cadence and invokes an optional async callback
  with the worker's UUID, so applications can record worker liveness
  into their own store (Redis, Postgres heartbeats table, etc.).

## 1.0.0 - API freeze

This is the first stable release of Pyjobkit. The public surface
re-exported from `pyjobkit/__init__.py` is now covered by Semantic
Versioning - see `docs/stability.md` for the policy.

* **Release pipeline** (closes #79)
  Added `.github/workflows/release.yml` triggered on every `v*` tag:
  runs the full test suite, builds sdist + wheel with `python -m build`,
  uploads to PyPI through the project-scoped token, and creates a
  GitHub Release with the CHANGELOG body and built artifacts.

* **Release announcement** (closes #80)
  `docs/release-1.0.md` introduces 1.0 to readers: what's in the box,
  who it's for, how to install, and what the stability guarantee
  covers.

* **Frozen public API** (closes #78)
  Bumped `__version__` to `1.0.0`. Added `docs/stability.md`
  documenting what is public, what is internal, and the deprecation /
  schema-migration rules.

* **Race / concurrency test coverage** (closes #77)
  Added `tests/test_races.py` with focused stress tests for parallel
  enqueue uniqueness, multi-worker single-execution invariants,
  cancellation under load, retry convergence, and optimistic-lock
  protection against double-completion.

* **Job tags + worker tag filter** (closes #53)
  `Engine.enqueue(..., tags=["high-priority", "user=42"])` attaches a
  normalised, deduplicated tag list to the job (carried in a payload
  marker so no schema change is needed). `Worker(..., tags=[...])`
  claims only jobs whose tag set intersects the worker's filter; the
  rest are released back to the queue with zero delay. The marker is
  stripped from the payload before reaching the executor.

* **Webhook notifications on terminal states** (closes #56)
  `Engine.enqueue(..., webhooks={"complete": url, "fail": url,
  "timeout": url})` attaches per-job webhook URLs that the worker POSTs
  to with a JSON body (`job_id`, `kind`, `status`, `attempts`,
  `duration_ms`, `result`) when the matching terminal state is reached.
  Webhook failures are logged but never affect job state.

* **Dynamic routing hook** (closes #76)
  `Engine.set_router(callable)` installs a synchronous function called
  during `enqueue(kind, payload)`; returning a non-None string overrides
  the dispatch kind. Useful for picking specialized executors based on
  payload shape or tags. The returned kind is still validated against
  the executor registry's naming rules.

* **mkdocs documentation scaffolding** (closes #66)
  Added `mkdocs.yml` plus structured pages (`docs/index.md`,
  `getting-started.md`, `configuration.md`, `extending.md`, `api.md`,
  `faq.md`). A `.github/workflows/docs.yml` workflow builds the site
  with mkdocs-material and deploys to GitHub Pages on every `main`
  merge.

* **`pyjobkit-simulate` console script** (closes #69)
  Run a list of jobs described in JSON (or YAML, when PyYAML is
  installed) against the in-memory backend, then print a per-status
  summary. Exits non-zero if any job ended in `failed` or `timeout`.
  Useful for local debugging and CI smoke tests without a database.

* **`--once` mode and `--kind` filter** (closes #47)
  `Worker.run(once=True)` drains the queue then exits, suitable for
  cron-style invocations. `Worker(..., kinds=[...])` restricts the
  worker to specific job kinds; mismatched claims are released back to
  the queue with zero delay. The CLI exposes both as `--once` and a
  repeatable `--kind` flag.

* **Worker Docker image** (closes #60)
  Added a two-stage `Dockerfile` that builds a wheel and installs it
  into a slim Python image as the `pyjobkit` user. `ENTRYPOINT
  ["pyjobkit"]` so containers can be configured purely through
  `PYJOBKIT_*` environment variables; `asyncpg` and `aiosqlite` ship in
  the runtime layer.

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
