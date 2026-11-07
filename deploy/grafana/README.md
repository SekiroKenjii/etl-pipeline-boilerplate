# Grafana: a dashboard and alerts

- `etl-dashboard.json`: runs by status, the time since the last success,
  rows per second, reject rates, step durations (p95), CDC and Kafka lag,
  failed attempts. Import it in Grafana, on a Prometheus or Mimir data
  source.
- `alerts.yaml`: Prometheus alerting rules for:
  - a failed run;
  - no successful run for a day;
  - a reject rate over 5%;
  - lagging changes or messages;
  - a lost run lock.

The metrics come from the application by OTLP, through the collector
(`deploy/otel-collector/`). Prometheus names them after the OpenTelemetry
names: `etl.rows.loaded` becomes `etl_rows_loaded_total`, `etl.step.duration`
(seconds) becomes `etl_step_duration_seconds`, and the attribute
`etl.pipeline` becomes the label `etl_pipeline`.

`make otel-validate` checks the rules with `promtool`.
