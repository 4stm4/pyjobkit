# Contributing to Pyjobkit

Bug reports, feature requests, and pull requests are welcome. The
project follows Semantic Versioning starting with `1.0` (see
`docs/stability.md`).

## Development setup

```bash
git clone https://github.com/4stm4/pyjobkit
cd pyjobkit
python -m venv .venv && source .venv/bin/activate
pip install -e ".[pg,sqlite,fastapi,metrics,docker,redis]"
pip install pytest pytest-cov alembic
```

Python 3.13 is required.

## Running the test suite

```bash
pytest -q
```

The default run uses SQLite. To exercise the Postgres-only code paths
(`SKIP LOCKED`, advisory locks, JSONB) set `PYJOBKIT_TEST_DSN`:

```bash
docker run --rm -d --name pyjobkit-pg -p 5432:5432 \
  -e POSTGRES_PASSWORD=pyjobkit -e POSTGRES_DB=pyjobkit postgres:16-alpine
export PYJOBKIT_TEST_DSN=postgresql+asyncpg://postgres:pyjobkit@localhost:5432/pyjobkit
pyjobkit-migrate --dsn "$PYJOBKIT_TEST_DSN" up
pytest tests/test_sql_backend.py tests/integration -v
```

## Pull request checklist

1. Tests for new behaviour (`tests/test_*.py`).
2. Public API changes: update the re-exports in `pyjobkit/__init__.py`
   and the module docstrings.
3. Add a CHANGELOG.md entry under the next planned version.
4. `pytest` must stay green; CI runs the suite on every PR.
5. Migrations: any schema change comes with an Alembic revision under
   `pyjobkit/backends/sql/migrations/versions/`.

## Commit style

- One logical change per commit.
- Subject line under 72 chars, imperative mood (e.g. `add foo`,
  `fix bar`).
- Reference the issue with `closes #N` when applicable.
- Co-author the assistant (`Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>`)
  if the change came out of an AI-assisted session.

## Code style

- 100-column soft limit, four-space indent.
- Type-hint all new public functions; `pyjobkit` ships `py.typed`.
- Avoid emojis in docstrings, error messages, or commit messages.
- Comments only when the *why* is non-obvious.

## Reporting bugs

Open an issue at https://github.com/4stm4/pyjobkit/issues with:

- Pyjobkit version and Python version
- Backend in use (Postgres / SQLite / Memory / Redis)
- Minimal reproducer (10–30 lines is ideal)
- Expected vs. actual behaviour
- Relevant log output (`--log-format json` is easiest to share)

## Security issues

See [SECURITY.md](SECURITY.md). Do not open a public issue.
