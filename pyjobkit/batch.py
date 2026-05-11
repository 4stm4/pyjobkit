"""Batch executor API (#63).

Some workloads benefit from processing many small jobs together - one
network round trip per batch instead of per job, a single GPU inference
pass for N rows of features, etc. ``BatchExecutor`` lets a kind expose
that shape:

.. code-block:: python

    class EmbedExecutor(BatchExecutor):
        kind = "embed"
        max_batch_size = 32

        async def run_batch(self, jobs):
            payloads = [j.payload for j in jobs]
            embeddings = await my_embedding_model.embed_many(payloads)
            return [{"embedding": e} for e in embeddings]

The library ships :func:`dispatch_batch` so applications can drive a
batch from a list of claimed rows without subclassing :class:`Worker`.
The function handles ``mark_running`` and per-job ``succeed`` /
``fail`` calls with the correct ``expected_version`` for each row.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Sequence
from uuid import UUID

from .engine import Engine, SHADOW_PAYLOAD_KEY

__all__ = ["BatchExecutor", "BatchJob", "dispatch_batch"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchJob:
    """One job inside a batch passed to :meth:`BatchExecutor.run_batch`."""

    job_id: UUID
    payload: dict[str, Any]
    attempts: int
    version: int | None


class BatchExecutor(ABC):
    """Executor that processes many jobs of the same kind together.

    The worker still claims jobs individually, but executor authors can
    aggregate them before invoking expensive operations. Implementations
    should return a list of results in the same order as ``jobs``.
    """

    #: Identifier used for dispatch routing (matches `Executor.kind`).
    kind: str

    #: Maximum number of jobs to pass to :meth:`run_batch` at once.
    max_batch_size: int = 32

    @abstractmethod
    async def run_batch(self, jobs: Sequence[BatchJob]) -> list[dict]:
        """Process ``jobs`` and return one result dict per input."""


async def dispatch_batch(
    engine: Engine,
    executor: BatchExecutor,
    rows: Sequence[dict],
    *,
    worker_id: UUID,
) -> None:
    """Run ``executor`` over ``rows`` and finalize each job in the backend.

    Each row is moved to ``running`` first; the entire batch is then
    handed to :meth:`BatchExecutor.run_batch`. If the call succeeds
    every job is finalized with its corresponding result via
    :meth:`Engine.succeed`. If the call raises, every job in the batch
    is failed with the exception's repr (callers wanting per-row retry
    semantics should use the regular :class:`Worker` loop instead).
    """

    if not rows:
        return
    if len(rows) > executor.max_batch_size:
        raise ValueError(
            f"batch of {len(rows)} exceeds executor.max_batch_size="
            f"{executor.max_batch_size}"
        )

    jobs: list[BatchJob] = []
    for row in rows:
        rid = row["id"]
        if not isinstance(rid, UUID):
            rid = UUID(str(rid))
        await engine.mark_running(rid, worker_id)
        payload = {
            k: v for k, v in (row.get("payload") or {}).items()
            if k != SHADOW_PAYLOAD_KEY
        }
        jobs.append(
            BatchJob(
                job_id=rid,
                payload=payload,
                attempts=row.get("attempts", 0) or 0,
                version=row.get("version"),
            )
        )

    try:
        results = await executor.run_batch(jobs)
    except Exception as exc:
        logger.warning(
            "BatchExecutor.run_batch raised; failing %d job(s): %s",
            len(jobs),
            exc,
        )
        reason = {"error": repr(exc), "batch": True}
        for job in jobs:
            await engine.fail(job.job_id, reason, expected_version=job.version)
        return

    if len(results) != len(jobs):
        raise RuntimeError(
            f"run_batch returned {len(results)} results for {len(jobs)} jobs"
        )

    for job, result in zip(jobs, results):
        await engine.succeed(job.job_id, result, expected_version=job.version)
