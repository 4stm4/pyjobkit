# FastAPI integration

The library ships an optional REST router, a bundled HTML
dashboard, and a TypeScript / JavaScript client. Install with
`pyjobkit[fastapi]`.

## REST router

```python
from fastapi import FastAPI
from pyjobkit import Engine, MemoryBackend
from pyjobkit.integrations.fastapi import make_router

engine = Engine(backend=MemoryBackend(), executors=[...])

app = FastAPI()
app.include_router(make_router(engine), prefix="/api/v1")
```

### Endpoints

| Method | Path | Body / Query | Response |
| --- | --- | --- | --- |
| `POST` | `/jobs` | `EnqueueRequest` | `{"job_id": "..."}` (202) |
| `GET`  | `/jobs?status=&limit=100` | -    | `JobRecordResponse[]` |
| `GET`  | `/jobs/{job_id}` | - | `JobRecordResponse` |
| `POST` | `/jobs/{job_id}/cancel` | - | `{"status": "cancellation_requested"}` (202) |
| `GET`  | `/healthz` | - | `{"status": "ok"}` (or 503 on backend failure) |

`EnqueueRequest` accepts the full enqueue surface:

```json
{
  "kind": "ingest",
  "payload": {"file": "s3://..."},
  "priority": 100,
  "max_attempts": null,
  "scheduled_for": "2030-01-01T12:00:00+00:00",
  "timeout_s": 600,
  "idempotency_key": "ingest-42",
  "tags": ["urgent"],
  "shadow": false,
  "retry_policy": "exponential:1:2",
  "webhooks": {"complete": "https://api.example.com/hooks"}
}
```

`max_attempts: null` (the default) defers to
`Engine.default_max_attempts`, so the worker's CLI / TOML default
applies.

Internal `__pjk_*` payload markers (shadow flag, tags, chain
follow-ups, trace context, retry policy spec, webhooks) are
**stripped** from `JobRecordResponse.payload` before serialisation -
they never leak through the REST surface.

`GET /jobs` requires the backend to implement
`all_jobs(status=, limit=)`. `MemoryBackend` does; the SQL backend
does not (the list endpoint will return 501 on a SQL backend until
we add it). Most production deployments query the SQL table
directly for listings.

### Authentication

The router has no built-in auth. Pass FastAPI dependencies that
should run before every endpoint:

```python
from fastapi import Depends, Header, HTTPException

def verify_token(authorization: str | None = Header(default=None)):
    if authorization != f"Bearer {settings.API_TOKEN}":
        raise HTTPException(401, "unauthorized")

app.include_router(
    make_router(engine, dependencies=[Depends(verify_token)]),
    prefix="/api/v1",
)
```

`dependencies` is an iterable of `Depends(...)` objects (same shape
as `APIRouter(dependencies=...)`). Apply it to every endpoint by
including it on the router; per-endpoint dependencies must wrap
your own mount-point.

### Missing dependency handling

If `fastapi` / `pydantic` are not installed, `make_router(engine)`
raises `FastAPIDependencyMissing` with an actionable hint. Catch it
when you decide whether to enable the REST surface at startup:

```python
try:
    from pyjobkit.integrations.fastapi import make_router, FastAPIDependencyMissing
    app.include_router(make_router(engine), prefix="/api/v1")
except FastAPIDependencyMissing:
    log.info("REST API disabled - install pyjobkit[fastapi] to enable")
```

## HTML dashboard

```python
from pyjobkit.integrations.fastapi import make_router
from pyjobkit.integrations.ui import mount_dashboard

router = make_router(engine, prefix="/api/v1")
app.include_router(router)
mount_dashboard(app, api_prefix="/api/v1", ui_path="/ui")
```

Open `http://host/ui` for a single-page status table with a
status filter, cancel-in-place buttons, and 5-second auto-refresh.
The page is plain vanilla JS / CSS; no build step.

The dashboard talks to the REST API via the same `api_prefix` you
pass to `make_router`. Add auth dependencies to both the router and
the `/ui` endpoint if needed:

```python
mount_dashboard(app, api_prefix="/api/v1", ui_path="/ui")

@app.middleware("http")
async def require_token(request, call_next):
    if request.url.path.startswith("/ui") and not authorised(request):
        return Response(status_code=401)
    return await call_next(request)
```

## TypeScript / JavaScript client

`ts/pyjobkit.d.ts` ships in the repo (and in the source
distribution) with hand-written declarations for `JobRecord`,
`EnqueueRequest`, `JobStatus`, and a `PyjobkitClient` interface.

A minimal vanilla-JS client lives at `ts/pyjobkit.js`:

```js
import { createClient } from "./pyjobkit";

const pyjobkit = createClient("/api/v1");

const { job_id } = await pyjobkit.enqueue({
  kind: "echo",
  payload: { greeting: "hi" },
});

const record = await pyjobkit.get(job_id);
await pyjobkit.cancel(job_id);
```

No runtime dependencies; works in browsers, modern Node, and Deno.
See [`ts/README.md`](https://github.com/4stm4/pyjobkit/blob/main/ts/README.md)
for the full surface.

## Common patterns

### Combine with a worker in the same process

```python
import asyncio

@app.on_event("startup")
async def start_worker():
    worker = Worker(engine, max_concurrency=4)
    asyncio.create_task(worker.run())
```

Fine for small-scale deployments. For production, run workers in
separate processes (`pyjobkit ...` containers) so HTTP load cannot
starve job execution and vice versa.

### Health check that probes both layers

```python
@app.get("/livez")
async def livez():
    health = await worker.check_health()
    if health["status"] != "healthy":
        return Response(status_code=503)
    return health
```
