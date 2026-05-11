"""Minimal HTML dashboard served from the FastAPI router (#55).

The dashboard is a single static page that uses the REST endpoints
exposed by :func:`pyjobkit.integrations.fastapi.make_router` to list
recent jobs and cancel them in place. It is intentionally simple - it
exists so production deployments have a "this is what's going on right
now" surface without adopting an additional UI stack.

Usage::

    from pyjobkit.integrations.fastapi import make_router
    from pyjobkit.integrations.ui import mount_dashboard

    router = make_router(engine, prefix="/api/v1")
    app.include_router(router)
    mount_dashboard(app, api_prefix="/api/v1", ui_path="/ui")
"""

from __future__ import annotations

from typing import Any

__all__ = ["DASHBOARD_HTML", "mount_dashboard"]


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Pyjobkit dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root {
  --bg: #0f1117; --fg: #e6edf3; --accent: #4f6bff; --muted: #8b949e;
  --row: #161b22; --border: #30363d; --error: #f85149; --ok: #3fb950;
}
* { box-sizing: border-box; }
body { background: var(--bg); color: var(--fg); font-family: ui-sans-serif, system-ui, sans-serif;
       margin: 0; padding: 24px; }
h1 { margin: 0 0 16px; font-size: 20px; }
.controls { display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; }
button, select, input { background: var(--row); color: var(--fg);
                       border: 1px solid var(--border); padding: 6px 10px;
                       border-radius: 6px; font-size: 14px; }
button { cursor: pointer; }
button.primary { background: var(--accent); border-color: var(--accent); }
table { width: 100%; border-collapse: collapse; background: var(--row);
        border-radius: 8px; overflow: hidden; }
th, td { padding: 8px 12px; text-align: left; border-bottom: 1px solid var(--border);
         font-size: 13px; }
th { background: rgba(255,255,255,0.04); color: var(--muted); font-weight: 500; }
tr:last-child td { border-bottom: none; }
.status { display: inline-block; padding: 2px 8px; border-radius: 4px;
          font-size: 11px; text-transform: uppercase; }
.status.queued { background: rgba(255,255,255,0.06); color: var(--muted); }
.status.running { background: rgba(88,166,255,0.15); color: #58a6ff; }
.status.success { background: rgba(63,185,80,0.15); color: var(--ok); }
.status.failed, .status.timeout { background: rgba(248,81,73,0.15); color: var(--error); }
.status.cancelled { background: rgba(255,255,255,0.08); color: var(--muted); }
.empty { color: var(--muted); padding: 24px; text-align: center; }
small { color: var(--muted); }
</style>
</head>
<body>
<h1>Pyjobkit dashboard</h1>
<div class="controls">
  <select id="filter">
    <option value="">All statuses</option>
    <option value="queued">queued</option>
    <option value="running">running</option>
    <option value="success">success</option>
    <option value="failed">failed</option>
    <option value="timeout">timeout</option>
    <option value="cancelled">cancelled</option>
  </select>
  <input id="limit" type="number" min="1" max="500" value="50" style="width:80px">
  <button class="primary" onclick="refresh()">Refresh</button>
  <small id="status"></small>
</div>
<table id="jobs">
  <thead>
    <tr>
      <th>id</th><th>kind</th><th>status</th><th>attempts</th><th>result</th><th></th>
    </tr>
  </thead>
  <tbody>
    <tr><td colspan="6" class="empty">Loading...</td></tr>
  </tbody>
</table>
<script>
const API = "__API_PREFIX__";

async function refresh() {
  const status = document.getElementById("filter").value;
  const limit = document.getElementById("limit").value;
  const tbody = document.querySelector("#jobs tbody");
  document.getElementById("status").textContent = "loading...";
  const params = new URLSearchParams();
  if (status) params.set("status", status);
  if (limit) params.set("limit", limit);
  try {
    const r = await fetch(`${API}/jobs?${params}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const jobs = await r.json();
    if (!jobs.length) {
      tbody.innerHTML = '<tr><td colspan="6" class="empty">No jobs match.</td></tr>';
    } else {
      tbody.innerHTML = jobs.map(j => `
        <tr>
          <td><code>${j.id.slice(0, 8)}</code></td>
          <td>${j.kind ?? ""}</td>
          <td><span class="status ${j.status ?? ''}">${j.status ?? ""}</span></td>
          <td>${j.attempts ?? 0}/${j.max_attempts ?? "?"}</td>
          <td><small>${j.result ? JSON.stringify(j.result).slice(0, 80) : ""}</small></td>
          <td>${j.status === 'queued' || j.status === 'running'
              ? `<button onclick="cancel('${j.id}')">cancel</button>` : ''}</td>
        </tr>`).join("");
    }
    document.getElementById("status").textContent = `${jobs.length} job(s)`;
  } catch (exc) {
    tbody.innerHTML = `<tr><td colspan="6" class="empty">${exc.message}</td></tr>`;
    document.getElementById("status").textContent = "error";
  }
}

async function cancel(id) {
  const r = await fetch(`${API}/jobs/${id}/cancel`, {method: "POST"});
  if (r.ok) refresh();
}

document.getElementById("filter").addEventListener("change", refresh);
refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""


def mount_dashboard(app: Any, *, api_prefix: str = "", ui_path: str = "/ui") -> None:
    """Mount a vanilla-JS dashboard at ``ui_path`` that talks to ``api_prefix``.

    The dashboard is a single static HTML page; it consumes the REST
    endpoints from :func:`make_router` (so be sure to mount the router
    too). Requires ``fastapi`` to be available at runtime.
    """

    from fastapi.responses import HTMLResponse

    html = DASHBOARD_HTML.replace("__API_PREFIX__", api_prefix)

    @app.get(ui_path, response_class=HTMLResponse)
    async def _dashboard() -> str:
        return html
