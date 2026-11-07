# ETL pipeline boilerplate

A Python 3.14 boilerplate for ETL and data migration pipelines: streaming
extract / transform / load jobs, an independent data quality gate,
spreadsheet-driven source-to-target mappings, deterministic surrogate keys, and
adapters for files, Google Cloud Storage, REST APIs, relational databases
(SQL Server, Oracle, Postgres, MySQL... through SQLAlchemy Core) and MongoDB.

## Getting started

Requires [uv](https://docs.astral.sh/uv/).

```sh
make install   # .venv with every extra and the dev tools, plus the git hooks
make check     # lint, type checks, tests on SQLite, compiled mappings
```

| Command          | What it does                                                    |
| ---------------- | --------------------------------------------------------------- |
| `make format`    | Rewrites the code in the project style (isort, pyink).          |
| `make lint`      | Checks the style without changing anything; pylint on the code, and less strictly on tests, scripts and benchmarks. |
| `make typecheck` | mypy (strict) and pyright (strict on `core`, standard elsewhere). |
| `make test`      | The tests, the database ones on SQLite only.                    |
| `make test-db`   | The same on every database server (`make db-up` first).         |
| `make cov`       | Coverage of `make test`. A report only: no floor.               |
| `make cov-db`    | Coverage of `make test-db`; fails below 90%.                    |
| `make mappings`  | Fails if `mappings/compiled/` is out of date.                   |
| `make security`  | Dependency audit, bandit, SBOM.                                 |
| `make docs`      | The documentation site; a warning fails it.                     |

CI runs more than `make check`: the tests on every database with the
coverage floor (as `make cov-db`), `make security`, `make docs`, the
collector configurations (`make otel-validate`), the image (scanned, then
run on Kubernetes and Airflow on kind), and a SonarQube analysis with that
coverage when the repository has a `SONAR_TOKEN`.

## Running

```sh
make demo                         # SQLite demo databases, then a CRM load
uv run etl list                   # the pipelines of the modules
uv run etl run crm.full_load      # run one; exit code for schedulers
uv run etl runs                   # the history of the runs
uv run etl check                  # can every resource be reached? (5: not ready)
uv run etl cutover report crm     # does the target hold what the source says?
```

`etl run` exits with 0 (success), 1 (failed), 2 (invalid settings or
arguments), 3 (partial success: some rows rejected; 0 with `--partial-ok`),
4 (some of its steps running elsewhere: nothing ran) or 130 (cancelled by
SIGTERM or Ctrl-C, running steps ending their batch). Every command shares
these codes, with 5 (`etl check`, `etl-db check`: not ready), 6
(`etl-mappings diff`: changes) and 7 (nothing found): see
`docs/reference/cli.md`. `--format json` makes `etl run` and `etl runs`
print JSON for scripts.

Scheduling is the scheduler's job (cron, Kubernetes CronJob, Airflow, Cloud
Scheduler): the kernel keeps one run of each step at a time (a lock per
step, with a lease, across hosts) and records every run and step in a
ledger.

### Cutover: reconciling before the switch

`etl cutover report <module>` proves the migration before the legacy system
is switched off, entity by entity. It reports:

- the source rows;
- the rows the mapping rejects, and why;
- the target rows expected;
- the rows of the target: matched, missing, extra or different, row by row.

The source rows go through the module's own mapping without being loaded.
Both sides are compared in a canonical form, so two different databases
reconcile: `Decimal("120.50")` and `120.5`, a `UUID` and its text, Oracle's
midnight `DATE` and a date, an aware timestamp and the naive UTC one all
compare equal. A number and its text never do: `Decimal("1.5")` loaded as
`"1.5"` is a difference. A first pass sums row digests per bucket of keys; a second pass
reads again only the buckets that differ, so memory never holds a whole
table. Missing rows that are orphans parked for their parent are explained,
not counted against the cutover.

```sh
uv run etl cutover report crm                            # Markdown
uv run etl cutover report crm --format xlsx -o crm.xlsx  # for the sign-off
uv run etl cutover report crm --format json              # for CI
uv run etl cutover report crm --counts-only              # COUNT(*) only: fast
```

Its verdict is its exit code, which makes it the gate of a cutover
runbook: `PASSED` 0, `PASSED-WITH-REJECTS` 3 (rejected rows left behind,
every one accepted by the BAs with `--accept-rejections`), `FAILED` 1;
`--counts-only` gives no verdict (`COUNTED`, 0). A module declares its entities in
`Module.reconciliations`. With `verify_loads = true`, the CRM module also
reads every batch back as it is loaded (`cutover.LoadedRowsCheck`, an
AFTER_LOAD rule). That catches a trigger, a truncation or a lost time zone at
load time rather than at cutover.

### Rejected rows: fix, replay, close

A rejected row is acknowledged like any other row: the watermark moves past
it, and later runs do not read it again (except the rows at the last
watermark, read once more by the next run: a row rejected there refreshes
its quarantine entry). It waits in the quarantine (`open`) until someone
deals with it, in one of three ways:

- fix the source, the mapping or a lookup, then replay the row;
- correct the row by hand, then replay it;
- close the row, if it will never be loaded.

```sh
uv run etl quarantine list                       # what, from where, why
uv run etl quarantine export -o rejected.csv     # for the BAs
uv run etl run crm.replay                        # read again by key, map, load
uv run etl quarantine close 12 13                # given up on
uv run etl housekeeping                          # purge after retention_days
```

When the source cannot be fixed, a BA corrects the row in a CSV file:
`mapping` and `source_key` (copied from the export), then the corrected
source columns. The file is versioned and reviewed like the mappings
(`modules.crm.settings.corrections`, `operators.Corrections`). Every pipeline
applies it before the mapping, and so does the cutover's expectation. A later
full load keeps the fix, and the cutover report agrees with the target.
Corrected rows are counted in the stats (`corrected`).

`etl parked list|release <queue>` shows or gives up on waiting orphans.
`etl state show|reset <pipeline> <key>` shows or forgets a saved watermark
or position, so that the next run starts over (`reset` asks first; `--yes`
for scripts). `etl keys lookup <entity>
<key>` gives the natural key behind a surrogate key. `etl run --param
name=value` passes parameters to one run. With `[lineage] endpoint = ...`,
every run and step sends OpenLineage events, with the tables they read and
write, to Marquez, DataHub or OpenMetadata.

### Large tables: parallel ranges, and their throughput

`operators.KeyPartitions` reads a table by ranges of its key, one step each,
in parallel. The ranges hold as many rows each; their bounds are quantiles
of the key, read once from the key only, so keys of any type split. They are
saved with the state at the first run and kept, so that no row moves from
one range to another and slips past a watermark. The CRM module reads one
range by default (`partitions = 1`): more help a reload more than a load
(docs/performance.md). Writes are already bulk: `COPY` on Postgres,
bulk copy on SQL Server (mssql-python, batches of 2,000 rows or more), array
binds on Oracle and MySQL,
then one set-based merge per batch.

`make bench` measures the rows per second of `crm.full_load`, source to
target, with the peak memory:
`make bench ROWS=1000000 PARTITIONS=1,4 SOURCE=<url> TARGET=<url>`. The
results, and what they do not tell, are in
[docs/performance.md](docs/performance.md).

### Deploying: Kubernetes, Airflow

The image (`Dockerfile`, `make image`) runs as non-root and works with a
read-only root filesystem. There are two ways to schedule it:

- `deploy/k8s/` runs each pipeline as a Kubernetes CronJob. The exit codes
  drive a pod failure policy: invalid settings and a held lock fail at once.
- `deploy/airflow/` holds an Airflow DAG with a pod per pipeline. A held lock
  fails the task, which is retried: a locked night is never skipped.

Both label each run with the scheduler's job (`--label`) and pass
`--resume`, so a retry runs only the steps its try did not finish. A load
too large for one host is shared out by the scheduler: several jobs (an
Indexed Job), each running some key ranges with `--step`.

`make kind-up k8s-demo airflow-demo` runs both on a local cluster, and CI
does it on every push. See [docs/deployment.md](docs/deployment.md).

`etl cutover report crm --save` keeps each cutover rehearsal, sealed with
its SHA-256 for the sign-off, and records the digest in the run ledger.
`etl cutover compare` shows what changed between two rehearsals, and `etl
cutover verify` checks that a report is the one the ledger recorded (exit
1 otherwise). Reports also come as HTML (`--format html`). See
[the cutover runbook](docs/cutover-runbook.md).

### How it is wired

`app.bootstrap` is the composition root: it loads the settings, sets up the
logging, imports the modules listed in the settings (or installed as entry
points `etl.modules`), and declares every resource (databases, stores, the
ETL's records) with its lifetime: the process, or one run. Resources are
created lazily, shared, health-checked, and closed in the reverse order of
their creation. Business modules never create adapters: they ask the
resources for ports by name (`runtime.names`).

### Adding a module

```sh
uv run etl new-module billing     # src/modules/billing, mappings/billing.xlsx
```

The module is about fifty lines: a settings class
(`config.MappingModuleSettings`), its custom transforms (a
`TransformRegistry`) and `create`, returning a `core.mapping_pipelines.MappingModule`.
From its workbook it gets the pipelines of the CRM module: `full_load`,
`sync`, `parked`, `replay` and `deletes`, with the quality gate, surrogate
keys, retries, lineage and the reconciliations of the cutover. Compile its
workbook (`etl-mappings --registry modules.billing:registry compile`), add
it to `config/base.toml` (`[modules.billing]`), and run it. `src/modules/crm`
is the same, with a custom transform; see
[Getting started](docs/getting-started.md).

## Code style

The code follows the
[Google Python Style Guide](https://google.github.io/styleguide/pyguide.html),
formatted with [pyink](https://github.com/google/pyink) (4 spaces, 80
columns). In particular:

- **Import modules, not names** (§2.2): `from core import contract`, then
  `contract.ETLContext`. Exceptions: `typing` and `collections.abc`. A package
  `__init__.py` re-exports its public names, so that callers write
  `pipeline.ETLJob` rather than `job.ETLJob`. No relative imports.
- **Docstrings** (§3.8): a one-line summary ending with a period, then
  `Args:`, `Returns:` / `Yields:`, `Raises:` and, for classes, `Attributes:`.
- **Exceptions** end in `Error`; catch-all `except Exception` only at
  isolation points (a step, a row hook), with a comment saying so.
- Unused arguments of interface methods are deleted: `del ctx  # Unused.`

pylint (configured in `pyproject.toml`, after Google's pylintrc) enforces what
the formatters cannot.

## Settings and logging

Settings are layered (`config/`): `base.toml`, then `<env>.toml`, then
`ETL__*` environment variables (`ETL__DATABASES__CRM__URL`), then secrets
through placeholders: `${env:NAME}`, `${file:/run/secrets/x}`, and secret
managers, `${vault:secret/crm/db#password}`, `${gcp:crm-db-url}`,
`${azure:acme-kv/crm-db-url}`. They are validated at start-up, every problem
reported with its path (exit code 2), and secrets never print. `uv run etl
--env prod config show` shows them, without reaching the secret managers:
secret values as `**********`, the values
of keys naming a secret (`password`, `secret`, `token`, `api_key`...) in
`options`, `connect_args` and module settings too, and other values given by
a placeholder as the placeholder. The settings hash in the run ledger is
taken from that same view.

- `databases.<name>.options` are the engine's (`EngineOptions`: `pool_size`,
  `max_overflow`, `pool_timeout`, `pool_pre_ping`, `connect_args`...) or,
  for a MongoDB URI, the client's (`MongoOptions`: `maxPoolSize`,
  `tls`...). An unknown option is refused at load, not at the first
  connection; options of one dialect go to `dialect_options` (MongoDB:
  `client_options`), unchecked until the engine is created.
- `databases.<name>.page_size` (1000) is the rows read per query; keep it
  at the `batch_size` of the pipelines reading the database.
- The pools must hold the steps run at once: with `partitions = 16`, the
  CRM module refuses to load unless the target's pool opens
  `2 x 16 + 2` connections (a transaction and SQL Server's bulk copy per
  range, plus the ETL records) and the source's 16. Raise `max_overflow`,
  or cap the steps with `runs.max_parallel_steps`.
- `privacy.anonymize` is refused in production: an environment named
  `prod`, `production` or `prd` (or holding one of these words, `prod-eu`),
  or any environment setting `production = true`.

## Security and personal data

[docs/security.md](docs/security.md) covers:

- the least privileges the ETL needs on each database and on Kafka;
- TLS;
- secret managers.

In production, secrets are files: `config/prod.toml` reads the connection
strings from `/run/secrets/legacy_database_url` and
`/run/secrets/crm_database_url`, mounted from the `etl-secrets` Secret,
not from environment variables.

What the ETL does about personal data:

- **Anonymizing.** Mapping workbooks mark the personal columns (**Personal
  data**: `name`, `email`, `phone`...). With `privacy.anonymize`
  (`config/uat.toml`), a UAT copy gets consistent fake values: the same
  real value always gives the same fake one. Production refuses it.
- **Retention.** `etl housekeeping` purges the quarantine, the quality
  issues and the parked records after their retention.
- **Right to erasure.** `etl keys forget <entity> --key-file -` erases the
  ETL's records of a natural key (read from stdin, not the command line):
  its keys, its quarantined and parked records, those referencing it, and
  their quality issues. The run ledger records the erasure and the
  operator (`$ETL_OPERATOR`), without the natural key. What it cannot
  erase (the target, backups, log backends...) is in `docs/security.md`.

`make security` audits the dependencies (pip-audit), checks the code
(bandit), and writes the SBOM (CycloneDX); `make image-scan` (trivy) and
`make image-sbom` (syft) do the same for the image. CI runs them on every
push; a release also signs the image and attests its provenance.

## Documentation and tests

`make docs` builds the documentation site (Sphinx), with:

- the architecture;
- a guide to mapping workbooks for business analysts;
- operations;
- the cutover runbook;
- deployment;
- the architecture decision records;
- the command line and API reference.

The tests are of four kinds:

- **Unit tests** (`tests/units`), run in parallel (pytest-xdist,
  `XDIST=-n 4` to cap the workers): property-based tests (Hypothesis) for
  the canonical values, keys, types and splits, among them; the layering
  rules (`test_architecture.py`).
- **Integration tests** (`tests/integrations`). The conformance suite of
  the relational adapters (`tests/integrations/databases`) runs every test
  on SQLite, and on Postgres, SQL Server, Oracle and MySQL when their
  `ETL_TEST_*_URL` is set (`make db-up` starts them, `make test-db` sets
  them); MongoDB has tests of its own there. The other integration tests
  (the CRM module, logging, the collector, Kafka) run on SQLite, or on the
  one service they need.
- **End-to-end tests** (`tests/e2e`, marked `slow`: `-m "not slow"` leaves
  them out). They run the installed `etl` as a scheduler does, and break
  runs on purpose: `kill -9` half-way, then a lost connection. They check
  that the next run finishes the work, nothing lost and nothing twice.
  Coverage counts their `etl` processes too.
- **The image tested on Kubernetes and Airflow** (`make k8s-demo
  airflow-demo`).

Coverage is judged with every database only: `make cov-db`, as CI, fails
below 90%; `make cov`, on SQLite, reports without a floor (a good part of
the adapters needs their database).

Logs go through the standard `logging` module, carrying the context of the
run (`etl.pipeline`, `etl.run_id`, `etl.step`...), secrets redacted, off the
pipelines' threads (a bounded queue). Two sinks: JSON lines on stdout (ECS
field names), for log agents (Fluent Bit, Vector, Filebeat, Grafana Alloy),
and OpenTelemetry (OTLP, `otel` extra). Through an OpenTelemetry Collector
(`deploy/otel-collector/`), they reach Elasticsearch and Kibana, Grafana
Loki, Seq, or any OTLP backend, without code changes.

Traces and metrics go out by OTLP too (`logging.otlp.signals`).

- **Spans.** There is a span for each run, for each attempt of a step and
  for each batch. Every log carries the `trace.id` and `span.id` of its span,
  so the logs of a run open from its trace.
- **Metrics.**
  - Rows read, loaded, rejected, created, updated, deleted and corrected,
    per step.
  - The durations of steps and runs.
  - Runs by status, and when a run last completed.
  - Failed attempts.
  - The lag of change logs (`etl.changes.lag`, in seconds) and of Kafka
    topics (`etl.messages.lag`).
  - Lost run locks.
- **Dashboard and alerts.** `deploy/grafana/` holds a dashboard and the
  alerting rules: failed runs, no success for a day, rejects over 5%,
  lagging changes, a lost lock.

The core knows only its own ports (`core.observability.tracing`,
`core.observability.metrics`), which record nothing until the application
installs OpenTelemetry.

The run ledger records what ran, for audits: the user, the environment, the
version and build of the code (`$ETL_BUILD`, else `git describe`), the hash
of the settings (secrets hidden), and the module's fingerprint, such as the
hash of the CRM's compiled mappings and corrections. `etl runs` shows them.

## Mappings

Business analysts author source-to-target mappings in `mappings/<domain>.xlsx`;
the workbook is compiled to JSON, which is what gets reviewed and what runs.

```sh
uv run etl-mappings template mappings/crm.xlsx   # start a workbook
uv run etl-mappings validate                     # report problems
uv run etl-mappings compile                      # write compiled JSON
uv run etl-mappings diff mappings/compiled/crm.json mappings/crm.xlsx
```

`diff` shows a review what a change of the workbook does:

- the fields and lookups changed;
- the mappings whose surrogate keys change;
- the mappings that reference a changed one.

A large or changing lookup can stay in a database table. Keep its
`lookup_<name>` sheet, with the header only, and name the table in the
module's settings (`modules.crm.settings.lookups`). The module reads it at
each run.

Custom transforms and row hooks are Python functions registered in a
`core.mapping.TransformRegistry`; pass it with
`--registry package.module:attribute`.

## Databases

The table ports (`contract.ITableReader`, `ITableWriter`, `ISchemaInspector`)
carry no SQL text, driver paramstyle or native type name. One adapter family,
`infra.database.relational`, implements them on any database SQLAlchemy Core
supports; what differs between databases (keyset conditions, staging tables,
MERGE, collations, LOBs, bulk copy) is in its `dialects` module. The same
conformance suite (`tests/integrations/databases`) runs on SQLite, Postgres,
SQL Server, Oracle and MySQL.

`operators.TableExtractor` reads a table incrementally: rows in the order
`(watermark, key)`, by keyset pagination, from the position of the last loaded
row (saved in a state store). `operators.TableLoader` writes each batch in one
transaction: into a staging table, then merged set-based, upserting by key by
default.

```python
db = relational.Database(os.environ["CRM_DATABASE_URL"])  # mssql+mssqlpython://...
extractor = operators.TableExtractor(
    relational.RelationalTableReader(db), "legacy.CUSTOMER",
    watermark="updated_at", key=["cust_no"],
    state=relational.RelationalStateStore(db),
)
loader = operators.TableLoader(
    relational.RelationalTableWriter(db),
    contract.TableTarget("crm.customer", key=("customer_key",)),
    row=lambda record: record.data,
)
```

`Database` takes connection strings as legacy applications write them: a
SQLAlchemy URL, or an ADO.NET (SqlClient, ODP.NET, Npgsql, Connector/NET),
ODBC, OLE DB, JDBC or libpq string. `uv run etl-db url "Server=...;..."`
shows the translation, and the settings with no equivalent.

Sources with a case-insensitive collation (SQL Server, MySQL) hold `c001`
and `C001` as one key: set the mapping's **Key case** to `upper` or `lower`,
so that they get one surrogate key.

The ETL keeps its own records (key crosswalk, state, parked records,
quarantine, quality issues) in `etl_*` tables of the database you choose.
`uv run etl-db upgrade` creates them, or upgrades them to the revision the
release needs (Alembic; `etl-db check` exits 5 while they are behind);
`uv run etl-db ddl --dialect mssql [--from <revision>]` prints the SQL for a
DBA instead. A run refuses to start on tables behind: see "Upgrading the
ETL's tables" in `docs/operations.md`.

MongoDB collections implement the same ports (`infra.database.mongodb`):
documents are rows, field paths (`address.city`) work in orders, filters
and mappings, and a mapping's Row split `explode(addresses)` makes one row
per element of an array, so that embedded documents become child tables. On
a replica set, each batch is written in one transaction. The ETL's records
can live in MongoDB too (`MongoKeyRegistry`, `MongoStateStore`...), and
`etl-db discover` takes a MongoDB URI.

### Change data capture and deletes

A watermark sees inserts and updates, never deletes. `operators.ChangeExtractor`
reads a database's change log through an `IChangeSource`: SQL Server Change
Tracking, Postgres logical replication (`wal_level = logical`), MongoDB change
streams. Its first run takes the position of the log, loads a snapshot, then
the changes made since; the next runs read the changes after the last loaded
one: a migration's full load, then its catch-up until the cutover.

Mappings turn a DELETE into the deletion of the target row; a mapping
splitting rows (`explode`) given a `scope` replaces the children of a row, so
that an address removed from an array leaves the target too.
`operators.ChangeLoader` applies the deletes `HARD`, `SOFT` (an `is_deleted`
flag) or `IGNORE`. For sources without a change log (files, or a database
without Debezium), `operators.MissingSourceRows` finds the deletes by
reconciling the source keys with the key registry.

#### Oracle (and others) through Debezium and Kafka

The ETL cannot read every log itself: Oracle's needs LogMiner or XStream.
Debezium can, and so can GoldenGate for Big Data. Either one writes the changes
of each table to a Kafka topic, `<topic.prefix>.<SCHEMA>.<TABLE>`, and
`messaging.DebeziumChangeSource` reads them as changes. This is a matter of
settings; the module does not change:

```toml
[kafka.main]
bootstrap_servers = "kafka-1:9092,kafka-2:9092"
group_id = "etl-crm"          # Its lag tells how far behind the ETL is.
options = { "security.protocol" = "SASL_SSL", "sasl.mechanisms" = "PLAIN" }
credentials = { "sasl.username" = "etl", "sasl.password" = "${env:KAFKA_PW}" }

[databases.legacy.changes]
kind = "debezium"
kafka = "main"
topic_prefix = "legacy"       # The connector's `topic.prefix`.

[modules.crm.settings]
snapshot = "log"              # Debezium's own snapshot: `snapshot.mode=initial`.
```

A run reads every partition up to its end at the start of the run, then stops.
Offsets are saved with the state once their batch is loaded (and its dead
letters sent), then committed to the consumer group; tombstones and dead
letters at the end of a topic are passed too. Tombstones are skipped. A truncate fails the run, because
the target then needs a new snapshot. With the JSON converter's schemas,
dates, timestamps and decimals arrive as Python types; without them, set the
connector's `decimal.handling.mode=string`. With
`changes.dead_letter_topic`, a message that is not a change event goes, as
it was, to that topic, with headers saying why and where from, and the run
goes on. `operators.MessageExtractor` reads any other topic the same way.

Orphans, child rows whose parent is not loaded, follow a policy per foreign
key: rejected into the quarantine (the default), parked and retried at the
next runs (`quality.ParkingQuarantine` and `operators.ParkedRecordExtractor`),
or given a placeholder parent (`mapping.InferringLoader`).

`uv run etl-db discover legacy --tables "CUST*"` suggests the keys and
foreign keys of the schema `legacy` (declared, or inferred from names and
values, with their orphans) for BAs and DBAs to confirm in the mapping
workbooks. It connects with `$ETL_DATABASE_URL` (or `--url`), not the
settings.
