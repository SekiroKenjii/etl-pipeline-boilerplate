# OpenTelemetry Collector configurations

The application exports its logs, traces and metrics by OTLP
(`logging.otlp.endpoint`, `logging.otlp.signals`). A collector routes them
to the backends of the team. Changing backend means changing this file, not
the code.

| File                 | Logs                  | Traces        | Metrics                       |
| -------------------- | --------------------- | ------------- | ----------------------------- |
| `elasticsearch.yaml` | Elasticsearch (ECS)   | Elasticsearch | Elasticsearch                 |
| `loki.yaml`          | Grafana Loki          | Grafana Tempo | Prometheus (remote write)     |
| `seq.yaml`           | Seq                   | Seq           | Prometheus (scrapes `:8889`)  |
| `file.yaml`          | `/out/*.jsonl` files, for tests and for a look ||                               |

`deploy/grafana/` holds a dashboard and alert rules on these metrics.

The three backend configurations accept exports over TLS only, with a
bearer token, on the pod's address (`MY_POD_IP`): an unauthenticated
collector lets anyone in the cluster write log lines and metrics that look
like the ETL's. They read:

- `/etc/otelcol/tls/tls.crt`, `/etc/otelcol/tls/tls.key`: the collector's
  certificate, e.g. a `kubernetes.io/tls` Secret from cert-manager;
- `/etc/otelcol/auth/token`: the token, a Secret; the ETL sends it as
  `Authorization: Bearer <token>` (`logging.otlp.headers` in
  `config/prod.toml`, the token from a mounted file);
- `MY_POD_IP`: the address to listen on, from the Downward API
  (`fieldRef: status.podIP`).

Mutual TLS: uncomment `client_ca_file` (and drop `auth` if the client
certificate is enough); the ETL presents its certificate with
`OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` and `OTEL_EXPORTER_OTLP_CLIENT_KEY`,
and trusts a private CA with `OTEL_EXPORTER_OTLP_CERTIFICATE`. Allow the
ETL's pods to reach port 4318 in the collector's own NetworkPolicy, and the
ETL's egress to it (`deploy/k8s/base/networkpolicy.yaml`).

A quick look on one machine:

```sh
docker run -p 4318:4318 -e MY_POD_IP=0.0.0.0 \
  -e SEQ_URL=http://seq:5341 -e SEQ_API_KEY=... \
  -v $PWD/deploy/otel-collector/seq.yaml:/etc/otelcol-contrib/config.yaml \
  -v $PWD/var/otel-tls:/etc/otelcol/tls -v $PWD/var/otel-auth:/etc/otelcol/auth \
  otel/opentelemetry-collector-contrib:0.164.0
```

`file.yaml` has neither TLS nor a token: it is for the tests and for a look
on a developer's machine.

Without a collector, point `logging.otlp.endpoint` at a backend taking OTLP
itself (Seq: `http://seq:5341/ingest/otlp`), or ship the JSON lines of stdout
(`logging.format = "json"`) with Fluent Bit, Vector, Filebeat or Alloy.
Backend addresses and keys come from environment variables.
