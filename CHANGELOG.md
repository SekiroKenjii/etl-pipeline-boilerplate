# Changelog

Every release of the boilerplate, newest first, as
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) describes. The
version follows [Semantic Versioning](https://semver.org/). A tag
`v<version>` publishes the version of `pyproject.toml`: the release workflow
fails if they differ, and takes the notes of the release from this file.

## [Unreleased]

This release follows an independent review of 0.1.0 (findings A1 to F8).
Read **Upgrading** before deploying it over 0.1.0.

### Upgrading

- **ETL tables.** Run `etl-db upgrade` (or apply the SQL of
  `etl-db ddl --from <revision>`; `--legacy` for databases created by 0.1.0,
  which have no version table). `etl-db check` exits 5 until it is done.
  The upgrade adds `etl_run_steps`, the quarantine's `status`,
  `resolved_at` and `record_key`, two `quality_issues` indexes, and a unique
  index on open quarantine entries (none on MySQL).
- **Configuration.** Unknown database options, and `runs.poll_seconds`,
  now fail at load (exit 2). Oracle wallet and MySQL TLS options go under
  `options.connect_args`. Environments named `prod`, `production` or `prd`
  refuse `privacy.anonymize`.
- **Secrets and deployment.** The `etl-secrets` Secret is required, with
  the key `legacy-database-url`: prod reads
  `/run/secrets/legacy_database_url`, no longer `LEGACY_DATABASE_URL`.
  Collectors need a TLS certificate, key and bearer token, and
  `MY_POD_IP`. Kubernetes 1.27 or later. Adapt the NetworkPolicy's
  database CIDR and collector selector before `kubectl apply -k`.
- **Scripts and schedulers.** Exit codes now come from one table
  (`docs/reference/cli.md`): 5 not ready, 6 changed, 7 not found are new.
  `etl check`, `etl-mappings diff/check/validate` and lookups that find
  nothing no longer exit 1. `etl state reset` needs `--yes`. A run whose
  steps all had nothing to do exits 0 (SKIPPED), no longer 1. A cutover
  with accepted rejections exits 3.
- **Modules and adapters.** Import `core.mapping_pipelines` (was
  `modules.crm.pipelines`), `config.LookupTable` (was
  `modules.crm.settings.LookupTable`). Ports other than extractor,
  transformer and loader lost `setup`/`teardown`; message sources implement
  `covers` and give an opaque `position` instead of partition and offset.
  `cutover.reconcile/count/build` take no context.
- **Data.**
  - Debezium committed offsets move one past a trailing tombstone or dead
    letter.
  - Timezone-aware values come back in UTC.
  - MySQL sessions run in READ COMMITTED: a MySQL target that writes a
    binary log needs `binlog_format = ROW`.
  - Quarantined source keys are canonical text: entries written by 0.1.0
    as `1003.0` may not replay.
  - Corrections files from older exports must drop or prefix with `_` the
    columns `id`, `status`, `issues`, `record`, `entity` and `key`; a cell
    really starting with `'` followed by `=`, `+`, `-` or `@` loses that
    quote.
- **Cutover rehearsals.** Reports saved before still verify OK. A number
  and its text no longer compare equal, so a rehearsal may now count
  `different` rows an older one matched (`compare` shows `canonical_form`
  going to 2). Sign off on a rehearsal made with this version.

### Added

- **Runs.**
  - Resuming: each step is recorded as soon as it ends (`etl_run_steps`);
    `etl run --resume [RUN_ID]` skips the steps already done,
    `--step <pattern>` runs some steps only, and `etl runs --run <id>` shows
    each step.
  - `etl run --label <text>`: a retry of the same label resumes it and takes
    over the locks of its dead try. CronJobs run
    `--label $(JOB_NAME) --resume`; the Airflow DAG labels each task.
  - `etl run --rekey`, required when a mapping's key fingerprint changed.
  - `--format json` for `etl run` and `etl runs`.
  - Key ranges split once by their own step, `<mapping>:ranges`, from the
    database's key positions (`ITableReader.keys_at`).
- **Mapping-driven modules.** `core.mapping_pipelines` (`MappingModule`,
  `MappingPipeline`, `FullLoad`, `Sync`, `Parked`, `Replay`, `Deletes`,
  reconciliations) and `config.MappingModuleSettings`, which checks database
  pools. `etl new-module` writes such a module (about 50 lines) and its
  workbook.
- **Mappings.** A "Key namespace" column, so tables can be renamed without
  new keys; `etl-mappings diff` explains each rekey; `etl-mappings template
  --module`; warnings for draft mappings and 2-digit years.
- **Cutover.** `--accept-rejections CSV` (PASSED-WITH-REJECTS);
  `provenance` and `canonical_form` sealed into each report; `--save`
  records the digest in the run ledger and `verify` checks it there (OK,
  CHANGED, UNRECORDED); `--time-resolution`; `benchmarks/bench_diff.py`.
- **Migrations.** Alembic revisions for the ETL tables: `etl-db upgrade`,
  `current`, `check`, `ddl --from REV [--legacy]`.
- **Operations.** `etl keys forget --key-file PATH|-`; `ETL_OPERATOR`,
  recorded as the operator of erasures, state resets and saved cutover
  reports; `etl housekeeping --queue`, which also purges quality issues and
  parked records; `etl check` probes object stores, lineage and OTLP
  endpoints; the `EtlStepFailed` alert.
- **Settings.** `databases.<name>.page_size` (default 1000);
  `EngineOptions`/`MongoOptions`; the `production` flag; `VAULT_CACERT`,
  `VAULT_CAPATH` and `AZURE_KEYVAULT_DNS_SUFFIX`.
- **Supply chain and CI.**
  - Image scan (`make image-scan`, trivy) and image SBOM
    (`make image-sbom`, syft) in CI and in releases.
  - Releases are signed with cosign (keyless) and carry SLSA provenance
    and SBOM attestations.
  - Dependabot for actions and base images.
  - A Sonar job with coverage; `make cov-db`, the 90% floor with every
    database.
  - pyright and test linting in pre-commit; `ETL_TEST_ORACLE_ADMIN_URL`.
- **Kubernetes.** A NetworkPolicy for the ETL's pods.

### Changed

- **Locks.** The run lock is per step (`<pipeline>/<step>`), all or nothing,
  with no waiting: runs of disjoint `--step` selections run at the same
  time, overlapping ones exit 4. Scale-out is the scheduler's: several Jobs
  or an Indexed Job, each with its own `--step`.
- **Engine.**
  - The quality gate stores rejected records and issues when their batch is
    acknowledged, not before.
  - A table extractor reads the rows at the previous largest watermark once
    more.
  - The first run of a key range reads in key order; later runs read by
    watermark.
  - `crm.deletes` refuses to delete more than `max_delete_share` of a
    table, or from an empty source, unless the run passes
    `--param confirm_deletes=<entity>:<count>`.
  - A window holding only rejected rows is PARTIAL_SUCCESS.
  - The quarantine refreshes the open entry of a record rejected again
    instead of adding one.
- **Databases.**
  - Errors are classified as retryable or fatal by SQLSTATE and vendor
    codes, not by exception class.
  - Sessions run in UTC on Postgres, MySQL and Oracle.
  - Engines never print batch rows in errors (`hide_parameters`).
  - The NULL check on the read order runs once per table and run.
  - Resources are created in parallel, and the ETL tables once per process.
- **Mappings and privacy.**
  - Empty key hashing means HMAC when a key secret is set.
  - `uuid5` is refused for natural keys holding personal data.
  - Draft mappings are skipped with a warning.
  - `quarantine export` prefixes its own columns with `_`.
  - Date shifts are one per record.
  - Fakes are wider, so they no longer collide.
- **Cutover.** Rejected rows make an entity a mismatch unless accepted. The
  diff partitions keys by range (hash when the two databases order them
  differently), re-reads only the differing ranges, and spills to
  `$TMPDIR`, so memory stays bounded. A number and its text differ
  (canonical form 2).
- **Logs.** `logging.redact` adds to the built-in secret names. A retried
  attempt is logged in one line, the traceback only with the last. Kafka
  secrets stay out of `KafkaCluster.config`.
- **API.** Messages are broker-neutral:
  `Message(id, key, value, headers, timestamp, position)` and
  `IMessageSource.covers`. `close()` replaces port lifecycles where a port
  holds a client. Mapping locations are text. `PipelineListener` moved to
  `core.contract`. `QualityGate` options are keyword-only.
- **Image and CI.**
  - Base images and actions are pinned by digest or SHA; uv is 0.13.6.
  - pip is no longer in the image.
  - Releases push no `latest` tag; the Python SBOM asset is
    `python.cdx.json`.
  - Unit tests run in parallel; coverage includes the `etl` processes of
    the end-to-end tests.
  - `etl-mappings check` runs with the module's transform registry.
  - pylint's design checks, pyright strict on `core`, and pylint on tests
    are part of `make check`.
- **Kubernetes and collectors.** Secrets are mounted as files.
  `imagePullPolicy: Always`. Housekeeping has a deadline and a pod failure
  policy. The collector configurations require TLS and a bearer token and
  listen on the pod's IP.
- **Benchmarks.** `make bench` measures the product's `crm.full_load`
  (gate, key registry, quarantine, state, ledger; CRM defaults) instead of
  an operator passthrough, splits the time into extract, transform and
  gate, write, keys and other, and reports each run's own peak RSS
  (`PARTITIONS=`, `--no-reload`, `--keep`). `docs/performance.md` is
  re-measured on Postgres and SQL Server targets and with the cutover diff
  at 5M rows; the earlier 25k–70k rows/s figures and the "50M rows in 16
  minutes" estimate are withdrawn.

### Removed

- `etl run --join` (shared runs and claimed steps), `runs.poll_seconds`,
  and the `deploy/k8s/overlays/workers` overlay.
- `ETLContext`'s key-value store, `core.pipeline.IModuleOrchestrator`,
  `names.SETTINGS`, `shared.utils.ensure_type`, `contract.SourceLocation`,
  `Database`/`MongoDatabase.acquire` and `release` (use `close()`),
  `Message.topic/partition/offset`.
- `modules.crm.pipelines`.

### Fixed

- **Data loss and duplicates.**
  - A change log batch updating then deleting a row no longer brings it
    back.
  - A row committed late with the watermark of a run's end is no longer
    lost.
  - A retried step no longer quarantines, parks or reports its rejects
    twice.
  - A batch with NULL keys, or two rows the target collates to one key, is
    refused instead of inserted again on every run.
  - SQL Server writes changes made only of trailing spaces.
  - Debezium dead letters are sent once, after their batch is loaded and
    before the position is saved.
- **Databases.**
  - SQL Server: key ranges written at once (`partitions` > 1, or scheduler
    jobs running ranges in parallel) no longer deadlock in the merge. Each
    batch's update, insert and delete seek the target's keys with row locks
    instead of scanning the whole table.
  - MySQL: sessions run in READ COMMITTED, so key ranges written at once
    no longer deadlock on the gap locks of the merge (REPEATABLE READ).
    A target that writes a binary log needs `binlog_format = ROW`.
  - The key registry inserts a batch's keys in multi-row statements, not
    one statement per row (Postgres with psycopg, SQLite): parallel ranges
    no longer stall on it.
- **Runs.**
  - The lock heartbeat survives renewal errors.
  - Listener errors no longer skip recording a step.
  - Configuration errors exit 2 without a ledger entry.
  - A failed step fails the run.
- **Privacy and security.**
  - Tracebacks and exception messages are redacted for every sink.
  - Redaction recognises many more secret shapes, and the row values that
    Postgres, SQL Server, MySQL and SQLAlchemy errors quote.
  - Redaction takes linear time on long log lines; before, one long
    message could stall a run for minutes.
  - Issues on personal columns no longer carry the raw value.
  - `etl config show` and the settings hash no longer show passwords.
  - Quarantine exports and xlsx reports never write formulas.
  - Lineage does not follow redirects with its token.
  - Erasure also covers parked records, references held by other entities,
    and issues.
- **Mappings.**
  - Renamed tables are reported as rekeys.
  - `ReferenceRule` looks orphans up per batch, not per row.
  - Duplicate workbook headers and ambiguous date formats are refused.
- **Cutover.**
  - Parked orphans explain missing rows by key.
  - A report edited and sealed again no longer verifies.
- **CLI.** `etl parked list <queue>` exits 7 for a queue no mapping parks
  to.
- **Earlier fixes in this release.**
  - Creating the ETL tables from processes starting together no longer
    fails.
  - The run ledger returns UTC times on SQLite and MySQL.
  - SQLite files no longer pool connections
    (`ResourceWarning: unclosed database`).

## [0.1.0]

Never tagged: the first state of the boilerplate, kept as the baseline the
migrations call revision 0001.

### Added

- **Engine.**
  - Extract, transform and load jobs, batch by batch, at least once.
  - Pipelines of steps, run in parallel, with retries and failure policies.
  - A quality gate, with a quarantine and a parking lot for orphans.
- **Databases.**
  - SQL Server, Oracle, PostgreSQL, MySQL and SQLite through SQLAlchemy
    Core, and MongoDB.
  - Keyset reads.
  - Set-based upserts, through bulk staging.
  - Connection strings of .NET, JDBC and ODBC applications.
- **Change data capture.**
  - SQL Server Change Tracking, PostgreSQL logical replication, MongoDB
    change streams, and Debezium topics on Kafka (Oracle).
  - Deletes found by reconciliation where no change log exists.
- **Mappings.**
  - Excel workbooks for business analysts, compiled to JSON.
  - Deterministic surrogate keys, foreign keys, lookups, row filters and
    splits, nested documents.
  - Columns marked as personal data, anonymized in UAT.
- **Operations.**
  - The `etl` command, with exit codes for schedulers.
  - A run ledger, run locks with leases, and graceful SIGTERM.
  - Quarantine replay, manual corrections, retention, and the right to
    erasure.
- **Cutover.**
  - A data diff across databases.
  - Reports in Markdown, JSON, HTML and xlsx.
  - Rehearsals sealed with SHA-256, and compared.
- **Performance.** Parallel key ranges and benchmarks; results in
  `docs/performance.md`.
- **Observability.**
  - Structured logs, traces and metrics by OpenTelemetry.
  - Collector configurations for Elastic, Grafana (Loki, Tempo,
    Prometheus) and Seq.
  - A Grafana dashboard and alert rules.
- **Security.**
  - Secrets from Vault, GCP Secret Manager and Azure Key Vault.
  - pip-audit, bandit, and a CycloneDX SBOM in CI.
  - Least privileges, documented in `docs/security.md`.
- **Deployment.**
  - A non-root image.
  - Kubernetes CronJobs, with a pod failure policy.
  - An Airflow DAG with a task per pipeline.
  - The image is tested on kind in CI.
