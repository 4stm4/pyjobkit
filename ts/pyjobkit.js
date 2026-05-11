// Tiny JavaScript Pyjobkit REST client (#71).
//
// Pair with `pyjobkit.d.ts` for type hints. Uses the global `fetch`,
// so it works in browsers, modern Node, and Deno without extra deps.

export function createClient(baseUrl) {
  const base = baseUrl.replace(/\/$/, "");
  async function _json(method, path, body) {
    const init = { method, headers: { "Content-Type": "application/json" } };
    if (body !== undefined) init.body = JSON.stringify(body);
    const r = await fetch(`${base}${path}`, init);
    if (!r.ok) throw new Error(`${method} ${path} -> HTTP ${r.status}`);
    if (r.status === 204) return null;
    return r.json();
  }
  return {
    enqueue: (req) => _json("POST", "/jobs", req),
    get: (id) => _json("GET", `/jobs/${id}`),
    list: ({ status, limit } = {}) => {
      const params = new URLSearchParams();
      if (status) params.set("status", status);
      if (limit !== undefined) params.set("limit", String(limit));
      return _json("GET", `/jobs?${params}`);
    },
    cancel: (id) => _json("POST", `/jobs/${id}/cancel`),
    healthz: () => _json("GET", "/healthz"),
  };
}
