# Pyjobkit

**Pyjobkit** is a backend-agnostic job processing toolkit for Python.
Its primary transport is your SQL database (Postgres, MySQL, SQLite via
SQLAlchemy) - no broker required - and its executor model is async-first.

## Highlights

- SQL-first queue with optimistic locking and `SKIP LOCKED` on Postgres
- `async def` executors with lease, cancellation, and progress hooks
- Pluggable retry policies (fixed / exponential / jittered)
- Shadow / dry-run mode, per-kind rate limiting, structured JSON logs
- PEP 561 typed public API, Prometheus-compatible metrics
- In-memory backend for tests, debug helpers, and `pyjobkit-simulate`

## Pick your next page

- New here? Read **Getting started**.
- Already comfortable with the basics? Jump to **Configuration**.
- Writing custom executors / backends? See **Extending Pyjobkit**.
- Comparing to Celery / RQ / Dramatiq? Read **Comparison**.

The documentation lives in this repository under `docs/`; it is built
with [mkdocs-material](https://squidfunk.github.io/mkdocs-material/) and
deployed to GitHub Pages on every merge to `main`.
