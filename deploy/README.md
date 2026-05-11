# Deployment assets

This directory holds the reusable manifests and dashboards shipped
alongside Pyjobkit.

## `helm/pyjobkit`

A minimal Helm chart for a Postgres-backed Pyjobkit worker
deployment.

```bash
helm install jobs deploy/helm/pyjobkit \
  --set image.tag=1.0.0 \
  --set dsn="postgresql+asyncpg://user:pass@host/db" \
  --set webhookSecret="$(openssl rand -hex 16)" \
  --set replicaCount=3
```

The chart renders:

- a `Deployment` running `pyjobkit` with all configured CLI flags
- a `Secret` for the DSN (and optionally the webhook secret)
- a one-shot `Job` that runs `pyjobkit-migrate up` before the
  workers start (`migrate.enabled: true` by default)
- a `Service` exposing the Prometheus `/metrics` endpoint
- an optional `ServiceMonitor` for the `kube-prometheus-stack`
  operator

Override anything via `--set ...` or a custom `values.yaml`. The
[values file](helm/pyjobkit/values.yaml) documents every knob.

## `grafana/pyjobkit-dashboard.json`

Import this file in Grafana (Dashboards -> Import -> upload JSON).
It plots:

- lease conflict rate (5m)
- webhook failure rate (5m)
- lease extension latency (p50 / p95 / p99)
- executor phase duration (p50 / p95)
- observed lease TTL distribution (p50 / p95)

The metrics are produced by `pyjobkit[metrics]` (Prometheus client).
Set the `--metrics-port` flag (already enabled in the chart) and
scrape it via the bundled `ServiceMonitor` or your own scrape config.
