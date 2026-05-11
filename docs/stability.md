# Stability policy (1.0)

Starting with **Pyjobkit 1.0.0** the public API surface is frozen
according to [Semantic Versioning](https://semver.org).

## What is public

Everything re-exported from `pyjobkit/__init__.py` is part of the
stable API:

- `Engine`, `Worker`, `MemoryBackend`
- `Executor`, `ExecContext`, `QueueBackend` (the protocols you extend)
- `Config`, `ConfigError`, `load_config`
- Types: `JobRecord`, `JobResult`, `JobStatus`, `LogStream`,
  `FailureReason`
- Retry primitives: `RetryPolicy`, `FixedDelay`, `ExponentialBackoff`,
  `JitteredExponentialBackoff`, `parse_policy`

The console scripts `pyjobkit` and `pyjobkit-simulate` are also part of
the public surface; their flags and config keys are covered by semver.

## What is internal

Anything imported via dotted paths under `pyjobkit.*` that is **not**
re-exported from the package root is internal and may change in any
release. Examples include:

- `pyjobkit.worker.LeaseLostError` - re-export pending
- `pyjobkit.backends.sql.*` schema details (table names, migration ids)
- `pyjobkit.cli._*` private helpers
- The structure of payload marker keys (`__pjk_*`)

If you find yourself depending on something internal, open an issue so
we can promote it.

## Versioning rules

- **MAJOR** (`2.0`) - removal of a public symbol, removal of a CLI
  flag, schema change that breaks running deployments, change in
  behaviour that would surprise an existing caller.
- **MINOR** (`1.x`) - new optional parameters, new helpers, new
  executors / backends, new CLI flags. Existing code keeps working.
- **PATCH** (`1.0.x`) - bug fixes, documentation, internal cleanups.

## Deprecations

Deprecated APIs emit a `DeprecationWarning` for at least one minor
release before removal. The deprecation note is added to
`CHANGELOG.md` and pinned in the docs until the removing major
release.

## Schema migrations

The SQL backend ships Alembic migrations under
`pyjobkit/backends/sql/migrations/`. Each minor release is expected to
include a no-op or backward-compatible migration that older workers
can keep running against; breaking schema changes require a MAJOR bump
and a documented downtime / rollout plan.
