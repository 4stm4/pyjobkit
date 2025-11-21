# Test Suite Overview

This repository uses **pytest** for all automated checks. The suite is split
into logical layers to mirror the runtime architecture of `pyjobkit`:

- `tests/unit/` – fast and isolated tests for individual components such as
  executors and the engine facade.
- `tests/integration/` – multi-component workflows using real backends or
  worker loops. These are skipped by default because they require external
  services (PostgreSQL/Redis, Docker daemon, HTTP endpoints).
- `tests/e2e/` – black-box flows that drive the CLI and API together. These
  are also skipped unless explicitly enabled via markers.
- `tests/load/` – stress and soak-style tests that validate throughput,
  backoff, and lease behaviour under pressure. They are opt-in because they
  take longer to run.

## Running the suite

```bash
# Quick feedback (unit only)
pytest tests/unit

# Full integration and E2E coverage when dependencies are available
pytest -m "integration or e2e"

# Execute all tests including load/stress (may take several minutes)
pytest -m "load"
```

### Markers

The following markers gate slower or environment-dependent checks:

- `integration` – requires external services such as PostgreSQL/Redis and
  sometimes Docker or HTTP endpoints.
- `e2e` – runs CLI/API flows end-to-end; expects a running API server and
  worker processes.
- `load` – executes high-volume or long-running stress scenarios.

To enable a marker, pass `-m "<marker>"` to `pytest` as shown above. The
smoke-level unit tests remain fast and environment-independent so they can be
run in CI by default.

### Current coverage highlights

- Integration scenarios in `tests/integration/test_integration_flow.py` run a
  worker against `MemoryBackend`, covering the full enqueue → run → success
  cycle and lease renewal for long-running jobs.
- The E2E test `tests/e2e/test_e2e_cli_api.py` validates the worker using
  `HttpExecutor` and reads results/logs through the engine's public API.
- The load test `tests/load/test_load_stress.py` creates a burst of jobs and
  ensures the worker remains stable under high parallelism.
