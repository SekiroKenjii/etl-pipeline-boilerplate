# 4. OpenTelemetry for logs, traces and metrics

**Status:** accepted.

## Context

Teams run Elastic, Grafana (Loki, Tempo, Prometheus), Seq, or a cloud's
service. Shipping to each from the code multiplies the integrations.

## Decision

- **Logs.** The standard `logging`, with the context of the run, as JSON
  lines or by OTLP.
- **Traces and metrics.** By OTLP, behind two ports of the core that record
  nothing until the application installs OpenTelemetry.
- **Routing.** An OpenTelemetry Collector routes to the backends
  (`deploy/otel-collector/`).

## Consequences

- **What it buys.** Changing backend is changing the collector's
  configuration.
- **What it costs.** One more component, the collector, unless the backend
  takes OTLP itself.
