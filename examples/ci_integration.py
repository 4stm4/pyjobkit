"""End-to-end CI integration example for Pyjobkit (issue #70).

The script:

1. Builds a small in-process Engine with a custom ``echo`` executor.
2. Enqueues three jobs, including one configured to fail twice before
   succeeding so the retry path is exercised.
3. Starts a worker, waits for the queue to drain, and asserts the final
   state of each job.

It is intentionally hermetic - no database, no Redis, no Docker - so it
can run on any GitHub Actions runner without extra services.
"""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

from pyjobkit import Engine, MemoryBackend, Worker
from pyjobkit.contracts import ExecContext, Executor


class EchoExecutor(Executor):
    """Returns the payload back as the result, optionally failing N times."""

    kind = "echo"

    def __init__(self) -> None:
        self._attempts: dict[str, int] = {}

    async def run(self, *, job_id: UUID, payload: dict, ctx: ExecContext) -> dict:
        fail_for = int(payload.get("fail_for", 0))
        key = str(job_id)
        self._attempts[key] = self._attempts.get(key, 0) + 1
        if self._attempts[key] <= fail_for:
            raise RuntimeError(f"transient failure #{self._attempts[key]}")
        return {"echo": payload}


async def _drain(worker: Worker, backend: MemoryBackend, timeout_s: float = 10.0) -> None:
    """Run the worker until every job in the backend is in a terminal state."""

    task = asyncio.create_task(worker.run())
    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.05)
        outstanding = await backend.count(status="queued") + await backend.count(
            status="running"
        )
        if outstanding == 0:
            break
    else:
        worker.request_stop()
        await worker.wait_stopped()
        raise AssertionError("worker did not drain within timeout")
    worker.request_stop()
    await worker.wait_stopped()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def main() -> None:
    backend = MemoryBackend()
    engine = Engine(backend=backend, executors=[EchoExecutor()])
    worker = Worker(engine, poll_interval=0.05, watchdog_interval_s=0.2)

    ok_id = await engine.enqueue(kind="echo", payload={"msg": "hello"})
    retry_id = await engine.enqueue(
        kind="echo",
        payload={"msg": "retry", "fail_for": 2},
        max_attempts=5,
        retry_policy="fixed:0.05",
    )
    shadow_id = await engine.enqueue(
        kind="echo", payload={"msg": "shadow"}, shadow=True
    )

    await _drain(worker, backend)

    ok_record = await backend.get(ok_id)
    assert ok_record["status"] == "success", ok_record
    assert ok_record["result"] == {"echo": {"msg": "hello"}}, ok_record

    retry_record = await backend.get(retry_id)
    assert retry_record["status"] == "success", retry_record
    assert retry_record["attempts"] >= 3, retry_record

    shadow_record = await backend.get(shadow_id)
    assert shadow_record["status"] == "success", shadow_record
    assert shadow_record["result"] == {
        "shadow": True,
        "result_discarded": True,
    }, shadow_record

    print("OK: %d jobs processed" % await backend.count())


if __name__ == "__main__":
    asyncio.run(main())
