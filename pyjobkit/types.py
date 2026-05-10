"""Public TypedDict / Literal types for the Pyjobkit API surface.

These types pin the shapes of values that cross the library boundary
(``Engine.get``, ``Engine.succeed``, ``Engine.fail``, executor results,
etc.). They are intentionally permissive (``total=False``) so that
backends may include additional implementation-specific fields without
breaking existing callers.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, TypedDict
from uuid import UUID

__all__ = [
    "JobStatus",
    "LogStream",
    "JobRecord",
    "JobResult",
    "FailureReason",
]


JobStatus = Literal[
    "queued",
    "running",
    "success",
    "failed",
    "cancelled",
    "timeout",
]
"""Terminal and in-flight job states used by all built-in backends."""


LogStream = Literal["stdout", "stderr"]
"""Stream identifier accepted by :meth:`pyjobkit.contracts.ExecContext.log`."""


class JobRecord(TypedDict, total=False):
    """Shape of the mapping returned by ``Engine.get`` / ``QueueBackend.get``.

    All keys are optional at the type level: individual backends may omit
    fields that are not relevant to their storage model. Additional
    backend-specific keys are allowed and simply ignored by typed callers.
    """

    id: UUID | str
    kind: str
    status: JobStatus
    payload: dict[str, Any]
    result: dict[str, Any] | None
    priority: int
    attempts: int
    max_attempts: int
    timeout_s: int | None
    scheduled_for: datetime | None
    created_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    lease_until: datetime | None
    leased_by: UUID | None
    version: int | None
    idempotency_key: str | None
    cancel_requested: bool


class JobResult(TypedDict, total=False):
    """Structured result returned by an executor's ``run`` coroutine.

    All keys are optional. Callers may set ``ok`` to convey a boolean
    summary; ``error`` is reserved for failure annotations even when the
    job ultimately succeeded (for example after a degraded run).
    """

    ok: bool
    error: str
    data: Any


class FailureReason(TypedDict, total=False):
    """Structured payload accepted by :meth:`Engine.fail`.

    ``error`` is the conventional short identifier (for example
    ``"timeout"``, ``"cancelled"`` or ``"exception"``); ``exception_type``
    and ``message`` are filled in by the worker for raised exceptions.
    """

    error: str
    exception_type: str
    message: str
    details: dict[str, Any]
