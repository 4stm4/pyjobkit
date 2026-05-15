# Prometheus metrics

Pyjobkit exposes a small set of Prometheus-compatible metrics via
the `prometheus_client` library. When `prometheus_client` is not
installed the counters / histograms degrade to no-op stubs and the
rest of the library keeps working.

## Install

```bash
pip install "pyjobkit[metrics]"
```

## Exposed series

| Metric | Type | Description |
| --- | --- | --- |
| `pyjobkit_extend_lease_conflicts_total` | counter | Optimistic-lock conflicts on lease extension. |
| `pyjobkit_extend_lease_latency_seconds` | histogram | Latency distribution for `extend_lease`. |
| `pyjobkit_lease_ttl_seconds` | histogram | Observed lease TTL values. |
| `pyjobkit_phase_duration_seconds` | histogram | Durations recorded by `ctx.profile_phase(...)`. |
| `pyjobkit_webhook_failures_total` | counter | Failed webhook deliveries (per attempt, before retry). |
| `pyjobkit_chain_broken_total` | counter | Chain tails that could not be enqueued after the head succeeded. |
| `pyjobkit_scheduler_enqueue_failures_total` | counter | Scheduler ticks that failed to enqueue an entry. |

The metric objects live in `pyjobkit.metrics`; importing them is
safe even without `prometheus_client`.

## Serving `/metrics`

### From the worker CLI

```bash
pyjobkit --metrics-port 9000 --metrics-host 0.0.0.0 ...
```

This starts the bundled HTTP exporter in a background thread inside
the worker process; scrape it like any other Prometheus exporter.

In `.pyjobkit.toml`:

```toml
[pyjobkit]
metrics_port = 9000
```

(There is no `metrics_port` TOML key today; pass the flag or env
`PYJOBKIT_METRICS_PORT` instead.)

### From a Python entry-point

```python
from pyjobkit.metrics import start_metrics_server

if not start_metrics_server(port=9000, host="0.0.0.0"):
    raise SystemExit("install pyjobkit[metrics] to expose /metrics")
```

`start_metrics_server` returns `True` on success, `False` (with a
WARNING log line) when `prometheus_client` is missing. The thread is
a daemon so it dies with the process; the function is idempotent if
you bind the same port twice (Prometheus' `start_http_server` will
raise `OSError: Address already in use`).

## Scraping in Kubernetes

The bundled Helm chart exposes a `metrics` Service and an optional
`ServiceMonitor` for `kube-prometheus-stack`:

```bash
helm install jobs deploy/helm/pyjobkit \
  --set dsn="postgresql+asyncpg://..." \
  --set serviceMonitor.enabled=true \
  --set serviceMonitor.interval=15s
```

Without the operator, scrape the headless Service directly:

```yaml
- job_name: pyjobkit
  static_configs:
    - targets: ['pyjobkit-metrics:9000']
```

## Grafana dashboard

A starter dashboard ships at
`deploy/grafana/pyjobkit-dashboard.json`. Import it in Grafana
(Dashboards -> Import -> upload JSON). The panels track lease
conflicts (5m rate), webhook failures (5m rate), lease extension
latency p50/p95/p99, executor phase duration p50/p95, and observed
lease TTL.

## Adding your own metrics

The library deliberately keeps its built-in surface small. To
expose application-level metrics inside an executor, instantiate
your own `Counter` / `Histogram` against the global registry:

```python
from prometheus_client import Counter
from pyjobkit.contracts import Executor

emails = Counter("myapp_emails_sent_total", "Emails sent successfully")

class SendEmail(Executor):
    kind = "email"

    async def run(self, *, job_id, payload, ctx):
        await send(payload)
        emails.inc()
        return {"ok": True}
```

They will appear on the same `/metrics` endpoint as the Pyjobkit
counters because both register against `prometheus_client.REGISTRY`.
