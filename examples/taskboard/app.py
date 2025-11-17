"""One-page demo dashboard for Pyjobkit using FastAPI and the memory backend."""

from __future__ import annotations

import asyncio
import random
from contextlib import suppress
from datetime import datetime
from typing import Any

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from pyjobkit import Engine, Worker
from pyjobkit.backends.memory import MemoryBackend
from pyjobkit.contracts import ExecContext, Executor


class InspectableMemoryBackend(MemoryBackend):
    """Memory backend variant that exposes a helper to list jobs."""

    async def list_jobs(self) -> list[dict[str, Any]]:
        async with self._lock:  # type: ignore[attr-defined]
            return [self._job_to_dict(job) for job in self._jobs.values()]  # type: ignore[attr-defined]


class DemoSleepExecutor(Executor):
    kind = "demo.sleep"

    async def run(self, *, job_id, payload, ctx: ExecContext):  # type: ignore[override]
        label = payload.get("label", str(job_id))
        duration = float(payload.get("duration", random.uniform(1.0, 4.0)))
        steps = 5
        for step in range(steps):
            await ctx.log(f"{label}: working chunk {step + 1}/{steps}")
            await ctx.set_progress((step + 1) / steps)
            await asyncio.sleep(duration / steps)
        message = f"{label} finished in {duration:.2f}s"
        return {"message": message, "duration": duration}


backend = InspectableMemoryBackend()
engine = Engine(backend=backend, executors=[DemoSleepExecutor()])
worker = Worker(engine, max_concurrency=4, batch=4)
app = FastAPI(title="Pyjobkit taskboard demo")


@app.on_event("startup")
async def start_worker() -> None:
    app.state.worker_task = asyncio.create_task(worker.run())


@app.on_event("shutdown")
async def stop_worker() -> None:
    worker.request_stop()
    task: asyncio.Task[None] | None = getattr(app.state, "worker_task", None)
    if task:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return PAGE_HTML


@app.get("/health")
async def health() -> dict[str, Any]:
    return await worker.check_health()


@app.post("/api/jobs")
async def enqueue_job() -> dict[str, Any]:
    duration = round(random.uniform(1.0, 5.0), 2)
    label = f"Demo #{random.randint(1000, 9999)}"
    payload = {"duration": duration, "label": label}
    job_id = await engine.enqueue(kind=DemoSleepExecutor.kind, payload=payload)
    return {"id": str(job_id), "label": label, "duration": duration}


@app.get("/api/jobs")
async def list_jobs() -> list[dict[str, Any]]:
    jobs = await backend.list_jobs()
    jobs.sort(key=lambda job: job["created_at"])
    return [_serialize_job(job) for job in jobs]


def _serialize_job(job: dict[str, Any]) -> dict[str, Any]:
    payload = job.get("payload", {})
    result = job.get("result")
    if isinstance(result, dict):
        result_payload = result
    else:
        result_payload = result

    return {
        "id": str(job["id"]),
        "label": payload.get("label", str(job["id"])),
        "duration": payload.get("duration"),
        "status": job.get("status"),
        "attempts": job.get("attempts"),
        "result": result_payload,
        "created_at": _iso(job.get("created_at")),
        "started_at": _iso(job.get("started_at")),
        "finished_at": _iso(job.get("finished_at")),
    }


def _iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


PAGE_HTML = """
<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\">
    <title>Pyjobkit demo dashboard</title>
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
    <style>
      body { font-family: system-ui, sans-serif; margin: 0; padding: 2rem; background: #f9fafb; }
      h1 { margin-top: 0; }
      button { background: #2563eb; color: white; border: none; padding: 0.75rem 1.5rem; border-radius: 0.5rem; font-size: 1rem; cursor: pointer; }
      button:hover { background: #1d4ed8; }
      table { width: 100%; border-collapse: collapse; margin-top: 1.5rem; }
      th, td { padding: 0.75rem; text-align: left; border-bottom: 1px solid #e5e7eb; }
      th { background: #e0e7ff; }
      tr:nth-child(even) { background: #f8fafc; }
      .status { text-transform: capitalize; font-weight: 600; }
      .status.queued { color: #92400e; }
      .status.running { color: #1d4ed8; }
      .status.success { color: #15803d; }
      .status.failed { color: #b91c1c; }
      .status.timeout { color: #b91c1c; }
      .muted { color: #6b7280; font-size: 0.9rem; }
    </style>
  </head>
  <body>
    <h1>Pyjobkit demo dashboard</h1>
    <p>Press the button to enqueue demo jobs that sleep for a random duration.</p>
    <button id=\"enqueue\">Enqueue demo job</button>
    <table>
      <thead>
        <tr>
          <th>Label</th>
          <th>Status</th>
          <th>Duration (s)</th>
          <th>Result</th>
          <th>Created</th>
        </tr>
      </thead>
      <tbody id=\"jobs-body\">
        <tr><td colspan=\"5\" class=\"muted\">No jobs yet</td></tr>
      </tbody>
    </table>
    <script>
      const body = document.getElementById('jobs-body');
      const button = document.getElementById('enqueue');

      async function refresh() {
        const resp = await fetch('/api/jobs');
        const jobs = await resp.json();
        if (jobs.length === 0) {
          body.innerHTML = '<tr><td colspan="5" class="muted">No jobs yet</td></tr>';
          return;
        }
        body.innerHTML = jobs.map(job => {
          const result = job.result ? JSON.stringify(job.result) : '';
          return `<tr>
            <td>${job.label}</td>
            <td class="status ${job.status}">${job.status}</td>
            <td>${job.duration ?? ''}</td>
            <td><code>${result}</code></td>
            <td>${job.created_at?.replace('T', ' ') ?? ''}</td>
          </tr>`;
        }).join('');
      }

      async function enqueue() {
        button.disabled = true;
        try {
          await fetch('/api/jobs', { method: 'POST' });
          await refresh();
        } finally {
          button.disabled = false;
        }
      }

      button.addEventListener('click', enqueue);
      refresh();
      setInterval(refresh, 2000);
    </script>
  </body>
</html>
"""
