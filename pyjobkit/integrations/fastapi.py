"""FastAPI integration providing a REST API for jobs (#58, #64).

The :func:`make_router` factory returns an ``APIRouter`` you can mount
into any FastAPI app::

    from fastapi import FastAPI
    from pyjobkit import Engine, MemoryBackend
    from pyjobkit.integrations.fastapi import make_router

    engine = Engine(backend=MemoryBackend(), executors=[...])
    app = FastAPI()
    app.include_router(make_router(engine), prefix="/api/v1")

Endpoints:

* ``POST /jobs`` - enqueue a new job. Body: ``{kind, payload, priority,
  max_attempts, scheduled_for, timeout_s, idempotency_key, tags,
  shadow, retry_policy, webhooks}``.
* ``GET /jobs/{job_id}`` - return the JobRecord for ``job_id``.
* ``POST /jobs/{job_id}/cancel`` - request cancellation.
* ``GET /healthz`` - backend connection check.

FastAPI / Pydantic are optional; install via ``pyjobkit[fastapi]``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from ..engine import Engine

__all__ = ["make_router", "FastAPIDependencyMissing"]


class FastAPIDependencyMissing(RuntimeError):
    """Raised when fastapi / pydantic are not installed."""


def _import_fastapi():  # type: ignore[no-untyped-def]
    try:
        import fastapi  # type: ignore[import-not-found]
        import pydantic  # type: ignore[import-not-found]
    except ImportError as exc:
        raise FastAPIDependencyMissing(
            "FastAPI integration requires 'fastapi' and 'pydantic'; "
            "install pyjobkit[fastapi]."
        ) from exc
    return fastapi, pydantic


def make_router(engine: Engine, *, prefix: str = ""):  # type: ignore[no-untyped-def]
    """Build a FastAPI ``APIRouter`` exposing job-management endpoints."""

    fastapi, pydantic = _import_fastapi()
    APIRouter = fastapi.APIRouter
    HTTPException = fastapi.HTTPException
    Body = fastapi.Body
    BaseModel = pydantic.BaseModel
    Field = pydantic.Field

    class EnqueueRequest(BaseModel):
        kind: str
        payload: dict[str, Any] = Field(default_factory=dict)
        priority: int = 100
        max_attempts: int = 3
        scheduled_for: datetime | None = None
        timeout_s: int | None = None
        idempotency_key: str | None = None
        tags: list[str] | None = None
        shadow: bool = False
        retry_policy: str | None = None
        webhooks: dict[str, str] | None = None

    class EnqueueResponse(BaseModel):
        job_id: UUID

    class JobRecordResponse(BaseModel):
        id: UUID
        kind: str | None = None
        status: str | None = None
        payload: dict[str, Any] | None = None
        result: dict[str, Any] | None = None
        attempts: int | None = None
        max_attempts: int | None = None
        priority: int | None = None
        scheduled_for: datetime | None = None
        created_at: datetime | None = None
        started_at: datetime | None = None
        finished_at: datetime | None = None

    router = APIRouter(prefix=prefix)

    @router.post("/jobs", response_model=EnqueueResponse, status_code=202)
    async def enqueue_job(req: EnqueueRequest = Body(...)) -> EnqueueResponse:
        job_id = await engine.enqueue(
            kind=req.kind,
            payload=req.payload,
            priority=req.priority,
            max_attempts=req.max_attempts,
            scheduled_for=req.scheduled_for,
            timeout_s=req.timeout_s,
            idempotency_key=req.idempotency_key,
            shadow=req.shadow,
            retry_policy=req.retry_policy,
            webhooks=req.webhooks,
            tags=req.tags,
        )
        return EnqueueResponse(job_id=job_id)

    @router.get("/jobs", response_model=list[JobRecordResponse])
    async def list_jobs(status: str | None = None, limit: int = 100) -> list[JobRecordResponse]:
        all_jobs = getattr(engine.backend, "all_jobs", None)
        if all_jobs is None:
            raise HTTPException(
                status_code=501,
                detail="Listing is only available on backends that implement all_jobs()",
            )
        records = await all_jobs()
        if status:
            records = [r for r in records if r.get("status") == status]
        records = records[:limit]
        out: list[JobRecordResponse] = []
        for r in records:
            jid = r.get("id")
            if not isinstance(jid, UUID):
                jid = UUID(str(jid))
            out.append(
                JobRecordResponse(id=jid, **{k: v for k, v in r.items() if k != "id"})
            )
        return out

    @router.get("/jobs/{job_id}", response_model=JobRecordResponse)
    async def get_job(job_id: UUID) -> JobRecordResponse:
        try:
            rec = await engine.get(job_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return JobRecordResponse(id=job_id, **{k: v for k, v in rec.items() if k != "id"})

    @router.post("/jobs/{job_id}/cancel", status_code=202)
    async def cancel_job(job_id: UUID) -> dict[str, str]:
        await engine.cancel(job_id)
        return {"status": "cancellation_requested"}

    @router.get("/healthz")
    async def healthz() -> dict[str, str]:
        try:
            await engine.check_connection()
        except Exception as exc:
            raise HTTPException(status_code=503, detail=str(exc))
        return {"status": "ok"}

    return router
