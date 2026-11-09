# Changelog

Every release of the boilerplate, newest first, as
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) describes. The
version follows [Semantic Versioning](https://semver.org/). A tag
`v<version>` publishes the version of `pyproject.toml`: the release workflow
fails if they differ, and takes the notes of the release from this file.

## [Unreleased]

This release follows three independent reviews of 0.1.0 and of its fixes
(findings A1 to F8, then R2-01 to R2-34, then R3-01 to R3-19). Read
**Upgrading** before deploying it over 0.1.0.

### Upgrading

- **Extras.** `sql`, `app`, `http`, `gcs`, `gcp-secrets`, `azure-secrets`,
  `mongodb`, `kafka`, `otel`, `postgres` and `mappings` are gone: every
  library the code imports is now a dependency of the package, imported at
  the top of its module (no import in a function, no `TYPE_CHECKING`; the
  architecture test refuses them). Only the drivers that SQLAlchemy loads
  by URL remain extras: `mssql`, `oracle`, `mysql`. Replace
  `[app,postgres,mappings]` with nothing, `[app,mssql]` with `[mssql]`.
- **ETL tables.** Run `etl-db upgrade` (or apply the SQL of
  `etl-db ddl --from <revision>`; `--legacy` for databases created by 0.1.0,
  which have no version table). `etl-db check` exits 5 until it is done.
  The upgrade adds `etl_run_steps`, the quarantine's `status`,
  `resolved_at` and `record_key`, two `quality_issues` indexes, and a unique
  index on open quarantine entries (none on MySQL). On MongoDB, the upgrade
  (or a run with `bookkeeping.create`) gives duplicate open quarantine
  entries the key `legacy-<id>` and replaces the index
  `pipeline_1_record_key_1` with a unique one: stop 0.1.0's processes first.
- **Configuration.** Unknown database options, and `runs.poll_seconds`,
  now fail at load (exit 2). Oracle wallet and MySQL TLS options go under
  `options.connect_args`. Environments named `prod`, `production` or `prd`
  refuse `privacy.anonymize`. `max_delete_share` defaults to 0.1 (was
  0.5): raise it explicitly where more deletes a night are normal.
  `databases.<name>.page_size` is at most 100,000. On Postgres, set
  `lock_timeout` below `statement_timeout`: a statement timeout is fatal.
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
  with accepted rejections exits 3. Deletes to confirm exit 2, not 1, so
  schedulers do not retry them. Labels are at most 161 characters. A retry
  of a label waits up to `runs.lease_seconds` for a live try of the same
  label before it loads or exits 4: allow for it in Job and task timeouts.
  `etl cutover` exits 130 on SIGTERM and Ctrl-C.
- **Modules and adapters.** Import `core.mapping_pipelines` (was
  `modules.crm.pipelines`), `config.LookupTable` (was
  `modules.crm.settings.LookupTable`). Ports other than extractor,
  transformer and loader lost `setup`/`teardown`; message sources implement
  `covers` and give an opaque `position` instead of partition and offset.
  `cutover.reconcile/count/build` take no context. Custom run ledgers
  implement `IRunLedger.latest`; custom dialects overriding `run` take
  `table=` and `on=`; custom table readers inherit a `column_family` that
  guesses from `max_value` (override it when the reader has a catalog);
  custom `GcsClient`s match the new blob and bucket protocols. Values of
  `mongodb.Document`, `ApiRecord`, `connect_args`, `arguments()`,
  `QuarantineEntry.record` and `CutoverReport.as_dict()` are typed `object`:
  narrow them before use. A heterogeneous `pipeline.chain(a, b)` is now
  `a.then(b)`. A `database` key in MongoDB `client_options` is no longer the
  database name.
- **Data.**
  - Debezium committed offsets move one past a trailing tombstone or dead
    letter.
  - Timezone-aware values come back in UTC.
  - MySQL transactions run in READ COMMITTED: a MySQL target that writes a
    binary log needs `binlog_format = ROW`.
  - A first run with `--since` no longer becomes the saved position, and a
    backfill moves it only when it starts at or before it.
  - The first run after the upgrade logs new `<mapping>:natural_key` key
    entries; it needs no `--rekey`, but do not change a mapping's natural
    key columns in the same deploy.
  - Quarantined source keys are canonical text: entries written by 0.1.0
    as `1003.0` may not replay.
  - Corrections files from older exports must drop or prefix with `_` the
    columns `id`, `status`, `issues`, `record`, `entity` and `key`; a cell
    really starting with `'` followed by `=`, `+`, `-` or `@` loses that
    quote.
- **Cutover rehearsals.** Reports saved by 0.1.0 verify UNRECORDED (their
  digest is in no ledger) until `etl cutover record <report>` adopts them;
  they then verify ADOPTED, with who recorded them. A number and its text no
  longer compare equal, so a rehearsal may now count `different` rows an
  older one matched (`compare` shows `canonical_form` going to 2). An entity
  empty on both sides fails the report unless accepted (`--accept-empty`).
  Sign off on a rehearsal made with this version.
- **SQL Server `datetime` watermarks.** The next normal run reads the rows
  that were stuck at the last `.003`/`.007` stamp (the saved position is below
  them): nothing to reset. A `--since`/`--until` bound on a `datetime` or
  `smalldatetime` column now rounds like the column stores (`--until
  10:00:00.002` includes rows stored at `.003`).
- **`etl run --format json`.** `message` is a string on a failed or cancelled
  run (`<step>: failed after <n> attempt(s): <error>`) instead of `null`; the
  keys are unchanged. The text summary gains a second, indented line with that
  reason.
- **Custom dialects and registries.** A `Dialect` subclass inherits the
  staging path of `insert_missing`; override `insert_skipping` where the
  database has a duplicate-skipping INSERT. `dialects.coerce` moved to
  `bulk_loading.coerce`. `for_engine` and `for_name` return a `SqliteDialect`
  named `sqlite` for SQLite (was the base `Dialect`, `generic`). A custom
  `IKeyRegistry` behind `CachedKeyRegistry` now receives `register` calls with
  the uncached entries only, possibly none.
- **Key cache.** `bookkeeping.key_cache_entries` is removed: the cache is
  sized by `bookkeeping.key_cache_memory` (bytes, with units; 256 MiB by
  default, about 280 MB per million entries held) and spills to
  `bookkeeping.key_cache_disk` (2 GiB by default) under `$TMPDIR`, which must
  be writable (the image's writable volume the cutover diff already needs);
  a settings file naming `key_cache_entries` is refused at start-up.
  `keys.CachedKeyRegistry` is removed: use `keys.TieredKeyCache(inner,
  memory_budget=..., disk_budget=0)` for an in-memory cache; its counters are
  `memory_hits`, `disk_hits` and `misses`. A 1,000,000-row load with the
  defaults: 12,686 rows/s and 497 MB peak (13,257 and 540 MB with the
  1,000,000-entry cache); with `key_cache_memory = "64MiB"`: 11,765 rows/s,
  318 MB, reload 17,918 rows/s (800,000 keys read from disk).
- **Custom locks and ledgers.** A custom `IRunLock` must implement
  `holder(name)`, which returns the owner or None when the lock is free or
  expired (abstract: instantiation fails with `TypeError` otherwise). A custom
  `IRunLedger` must implement `last_run(pipeline_name, label=None)`.
- **Postgres messages.** If `etl check` warns of `lc_messages`, a superuser
  runs `ALTER ROLE etl SET lc_messages = 'C'`. Code that used the bookkeeping
  module's private names (`_Bookkeeper`, `_dumps`, `_CHUNK`) finds them in
  `bookkeeping._base` without the underscore.
- **Tuning keys moved.** `bookkeeping.key_cache_memory` and `key_cache_disk`
  are `tuning.key_cache_memory` and `tuning.key_cache_disk`;
  `runs.max_parallel_steps` is `tuning.max_parallel_steps`; `logging.queue_size`
  is `tuning.queue_size` (`ETL__TUNING__*`). Defaults are derived from the
  container's limits: set them to keep the old numbers (`"256MiB"`, `"2GiB"`,
  `bulk_copy_rows = 2000`, `in_chunk = 500`). `etl check` prints a first line
  `tuning ...`: scripts reading its output line by line skip it.
- **Moved names.** `core.observability.types.Attribute`/`Attributes` are
  `contract.Attribute`/`contract.Attributes`; `core.cutover.types.Kind`,
  `Status`, `Verdict` are `contract.DifferenceKind`, `contract.EntityStatus`,
  `contract.Verdict` (`core.cutover.Verdict` still works); the cast functions
  of `core.mapping.types` (`parse_type`, `TargetType`, `CastError`, `TypeKind`,
  `text_parser`, `to_strftime`, `format_problems`, `two_digit_year`,
  `SUPPORTED_TYPE_NAMES`, `FORMAT_TOKENS`) are in `core.mapping.casts`; the
  `core.mapping` re-exports are unchanged.

### Added

- **Runs.**
  - Resuming: each step is recorded as soon as it ends (`etl_run_steps`);
    `etl run --resume [RUN_ID]` skips the steps already done,
    `--step <pattern>` runs some steps only, and `etl runs --run <id>` shows
    each step.
  - `etl run --label <text>`: a retry of the same label resumes it, waits
    for a live try of the label to end, takes over the locks of a dead try
    once they expire, and carries over the steps that try finished
    meanwhile. CronJobs run `--label $(JOB_NAME) --resume`; the Airflow DAG
    labels each task.
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
  CHANGED, UNRECORDED); `--time-resolution`; `benchmarks/bench_diff.py`;
  `etl cutover record <report>...` for reports saved by 0.1.0;
  `--accept-empty ENTITY|*`.
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
- **API.** `IRunLedger.latest`, `IRunLock.holder`, `ETLContext.wait`,
  `Kernel(redact=)`, `runtime.steps.hide_secrets`,
  `reconciliation.DeletesNotConfirmed`, `ITableReader.column_family`,
  `contract.family_of`, `runtime.Declaration`, `operators.SourceRecord`,
  `observability.describe_error`, `Dialect.isolation_level`,
  `Dialect.prepare_join`, `dialects.for_name`, `shared.utils.one_line`,
  `JsonValue` and `JsonObject`.
- **Typing.** A ratchet test caps the uses of `typing.Any` per package
  (`tests/units/test_typing_ratchet.py`): none in `core`, `shared`, `app`,
  `cli`; four left where pydantic, pymongo, SQLAlchemy and confluent-kafka
  require one.
- **Relational dialect hooks** `Dialect.insert_missing(engine, table, rows,
  key)` and `Dialect.insert_skipping(table, key)`, `Dialect.bulk_stage(...,
  minimum=)`; a `SqliteDialect` (dialect name `sqlite`); the modules
  `relational.base_dialect` (the base `Dialect`, still importable as
  `dialects.Dialect` and `relational.Dialect`) and `relational.bulk_loading`
  (`coerce`, `copy_rows`, `bulk_copied`).
- **`core.keys.TieredKeyCache`**: the key cache in tiers, memory bounded by
  bytes (the cost of an entry is measured on the first 1,000 entries and
  re-sampled every 100,000), then a SQLite file under `$TMPDIR` for the keys
  memory evicts (owner-only directory, removed on close, garbage collection
  and exit; past `key_cache_disk` one warning and the registry answers), then
  the registry. Settings `bookkeeping.key_cache_memory` (`"256MiB"`) and
  `bookkeeping.key_cache_disk` (`"2GiB"`; `0` for no disk tier);
  `shared.utils.parse_byte_size` reads sizes with units. `TargetType.caster(source_format)`,
  `core.mapping.casts.text_parser`, `core.keys.generation.encoded_natural_key`,
  `CompiledField.run` and `.issue`.
- **`IRunLedger.last_run(pipeline_name, label=None)`**: the newest non-dry run
  of a pipeline, or of one of its labels, searching every run.
  `core.runtime.LockWait` and `Kernel(lock_wait=)`: how a labelled run waits
  for its label's locks, injectable for tests.
- **`etl check` warns** (a WARNING log line, the database still `ok`) of a
  Postgres session whose `lc_messages` is not English, since the statement
  timeout is told apart by its message. `docs/operations.md` "Known
  limitations" gathers the behaviours accepted and documented.
- **`shared.utils.ExternalSorter` and `sorted_spilling`**: a generic external
  sort, memory bounded by `spill_rows`, stable, merging its runs by tiers, in
  an owner-only temporary directory removed on close, garbage collection,
  exceptions and interpreter exit (pickles by default, a `SpillFormat` of the
  caller's otherwise). `docs/algorithms.md`: the heavy paths with their
  algorithm, time and memory bounds and the test or benchmark proving each.
- **Tests that lock what is stored and shared:** the values of every stored
  enum and boundary `Literal` are recorded (`tests/units/core/test_stored_enums.py`);
  the config `Literal`s match the core enums and protocols; the exit-code
  tables of `cli/exit_codes.py` and `docs/reference/cli.md` list every code
  once; an architecture rule gives public enums and type definitions their
  homes (`tests/units/test_type_homes.py`, with reasoned allow-lists).
  `docs/reference/types.md`: every contract and boundary type on one page.
- **`[tuning]` settings**: every resource-bound threshold (memory and CPU
  budget, key cache memory and disk, spill rows, parallel steps, log queue,
  bulk-copy threshold, IN-list chunk) is derived from the container's cgroup
  limits (else the host's) unless set; `etl config tuning` prints each value
  with its source; `etl check` prints and logs them. **`etl calibrate
  <database>`** measures a database on a scratch table (parameters against
  bulk copy or COPY, IN lists of 250, 500 and 1,000 values, the key registry
  statement) and prints a `[tuning]` snippet (`--format json` too).
  `docs/tuning.md` lists every threshold with its class, derivation, override
  and trade-off, and `tests/units/test_tuning_inventory.py` keeps it complete.
- **`etl check` prints `WARN <resource>: <what>`** after the `ok` of a
  resource that works but not as the ETL expects (a Postgres session whose
  messages are not in English); the exit code stays 0, a `FAIL` still exits
  5. An architecture test: a package's `documents.py`, `clients.py` and
  `types.py` are used only by its package, `app`, the benchmarks and the
  tests (`CROSS_PACKAGE_TYPES` lists the exceptions, with reasons).

### Changed

- **Locks.** The run lock is per step (`<pipeline>/<step>`), all or nothing:
  runs of disjoint `--step` selections run at the same time, overlapping
  ones exit 4. Locks are owned per process (`label:<label>@<run id>`,
  `<host>:<run id>`); a process renews and releases only its own. A run
  waits only for a live try of its own label, up to the lease. Scale-out is
  the scheduler's: several Jobs or an Indexed Job, each with its own
  `--step`.
- **Engine.**
  - The quality gate stores rejected records and issues when their batch is
    acknowledged, not before.
  - A table extractor reads the rows at the previous largest watermark once
    more.
  - The first run of a key range reads in key order; later runs read by
    watermark.
  - `crm.deletes` refuses to delete more than `max_delete_share` (default
    0.1) of a table, or from an empty source, unless the run passes
    `--param confirm_deletes=<entity>:<count>`; the refusal exits 2.
  - A step reporting failed rows (`stats.failed`) fails, whatever it
    loaded.
  - The key check compares with the last run that loaded of any pipeline of
    the module, over the whole ledger, and includes the natural key
    columns.
  - A window holding only rejected rows is PARTIAL_SUCCESS.
  - The quarantine refreshes the open entry of a record rejected again
    instead of adding one.
- **Databases.**
  - Errors are classified as retryable or fatal by SQLSTATE and vendor
    codes, not by exception class.
  - Sessions run in UTC on Postgres, MySQL and Oracle; MySQL transactions
    in READ COMMITTED, set on the engine (`options.isolation_level` wins).
  - SQL Server merges a target without an index led by its key without the
    seek hints, and warns once (`has no index on`).
  - A Postgres statement timeout is fatal; ORA-01555, ORA-04031 and SQL
    Server 8645 are retryable.
  - Engines never print batch rows in errors (`hide_parameters`).
  - The NULL check on the read order runs once per table and run.
  - Resources are created in parallel, and the ETL tables once per process.
- **Mappings and privacy.**
  - Empty key hashing means HMAC when a key secret is set.
  - `uuid5` is refused for natural keys holding personal data.
  - Draft mappings are skipped with a warning.
  - `quarantine export` prefixes its own columns with `_`.
  - Date shifts are one per record; a record without a key takes a digest
    of its row.
  - Workbook parameters with text after a closing quote are refused;
    transform errors in issues are one line.
  - Fakes are wider, so they no longer collide.
- **Cutover.** Rejected rows make an entity a mismatch unless accepted. The
  diff partitions keys by range (hash when the two databases order them
  differently), re-reads only the differing ranges, and spills to
  `$TMPDIR`, so memory stays bounded. A number and its text differ
  (canonical form 2). An entity with nothing expected, rejected or loaded
  is `empty` and fails unless accepted. `--counts-only` gives no verdict:
  it exits 0 whatever the counts.
- **Logs.** `logging.redact` adds to the built-in secret names. A retried
  attempt is logged in one line, the traceback only with the last. Kafka
  secrets stay out of `KafkaCluster.config`.
- **API.** Messages are broker-neutral:
  `Message(id, key, value, headers, timestamp, position)` and
  `IMessageSource.covers`. `close()` replaces port lifecycles where a port
  holds a client. Mapping locations are text. `PipelineListener` moved to
  `core.contract`. `QualityGate` options are keyword-only and its checks
  `CheckSet[Never]`; `RejectedRecord` is covariant, `CheckSet`
  contravariant. `QuarantineEntry.record` is a `StateValue`.
  `ITableReader.max_value` returns `object`. `mongodb.Document` is
  `dict[str, object]`. `typing.Any` is gone from every public signature.
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
  - SonarQube analyses the Dockerfile, the manifests of `deploy/` and the
    Airflow DAG too, with every Docker and Kubernetes rule (`Sonar way
    comprehensive`).
  - The architecture test enforces an allow-list per part of `core`, and no
    star imports. It also refuses any import cycle in `src/`: between two
    modules, or between two parts of a package through different modules,
    at every level. Imports in `TYPE_CHECKING` blocks and in function
    bodies count too. `ETLResult` moves from `core.contract.etl` to
    `core.contract.results`, which ends the cycle between `etl` and
    `quality`. `contract.ETLResult` is unchanged.
- **Kubernetes and collectors.** Every ETL container requests and limits
  its ephemeral storage (load: 1Gi, limit 4Gi; housekeeping: 256Mi,
  limit 1Gi): past the limit the pod is evicted, so raise it where a
  cutover diff spills to `/tmp`. The demo's Airflow pod requests CPU and
  memory too. Secrets are mounted as files.
  `imagePullPolicy: Always`. Housekeeping has a deadline and a pod failure
  policy. The deployed collector configurations (Elasticsearch, Loki, Seq)
  require TLS and a bearer token and listen on the pod's IP; `file.yaml`,
  for tests only, has neither. The demo overlay has no housekeeping
  CronJob.
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
- **Relational dialects.** `Dialect.bind(column, value)` hook: a dialect may
  cast a bound value to its column's type; `_conditions` takes the dialect.
  `etl run` text prints the run's message for any status that has one (was
  FAILED only).
- **Key registry.** A batch of keys is registered in one statement that
  inserts only the missing ones, with no read first: `INSERT ... SELECT FROM
  unnest(arrays) ON CONFLICT DO NOTHING` on Postgres, `INSERT ... ON CONFLICT
  DO NOTHING` on SQLite, `INSERT ... ON DUPLICATE KEY UPDATE k = k` on MySQL, a
  staging table and `INSERT ... SELECT ... WHERE NOT EXISTS` on SQL Server (bulk
  copy from 300 rows) and Oracle. Registering 1,000 new keys alone: Postgres 35
  to 20 ms, MySQL 33 to 19 ms, SQL Server 217 to 49 ms; a 200,000-row load into
  Postgres spends 4.6 s in the registry instead of 15.2 s. `CachedKeyRegistry`
  sends the inner registry only the entries it does not hold: a batch of known
  keys costs 0.2 ms instead of a round trip. On SQLite the registry's INSERT
  carries `RETURNING`, so that one multi-row statement goes out per page of
  1,000 rows (16 ms a batch of 1,000 keys; 178 ms a row at a time).
- **Mapping engine.** Each field is compiled once into a runner that already
  knows its sources, transform, default, type cast and anonymizer; a key field
  derives its natural key once per row, for the value, the record's key and the
  foreign keys. The transform and the gate take 53% less thread time (SQLite
  100,000 rows: 4.93 to 2.29 s; Postgres 200,000 rows: 10.2 to 4.45 s). Dates
  written as in Excel (`dd/MM/yyyy`, `yyyy-MM-dd HH:mm:ss`...) are read by a
  parser compiled once per format that accepts and refuses exactly what
  `strptime` did (property-tested against it); Python `%` formats keep
  `strptime`. The natural key's JSON text is built directly (same text, same
  surrogate keys: property test and recorded keys). A corrections transformer
  with no corrections for its mapping passes records through without
  computing their lineage key.
- **Resuming a label.** A `--label --resume` retry resumes its label's last
  run whatever its outcome; after the first try's success it runs nothing
  (`resumed_from` set), the same as a retry that waited for that success.
  Without `--label`, `--resume` after a success still runs every step.
- **`infra.database.relational.bookkeeping` is a package** (`definitions`,
  `stores`, `quarantine`, `runs`); every public name imports as before, the
  private helpers live in `bookkeeping._base`.
- **Bounded memory for the reconciliations.** The cutover diff's second pass
  sorts through `shared.utils.ExternalSorter` (chunked pickles instead of JSON
  lines in the same `etl-cutover-*` directory; runs merged by tiers, rewritten
  once per tier instead of at every 64 runs). `MissingSourceRows` compares the
  source's keys and the registry's as sorted streams spilled to disk
  (`spill_rows`, default 50,000) instead of holding every source key in
  memory: 10,000,000 keys peak at 66 MB instead of 1.5 GB, for about 2.7
  times the CPU; same deletes, same order, same guard (R3-20).
- **Where types live.** Type definitions have one home each, enforced by
  the architecture tests: `core.contract` for cross-layer contracts; per
  package, `documents.py` (stored shapes), `clients.py` (library objects) and
  `types.py`; private aliases otherwise. Mongo documents moved to
  `infra.database.mongodb.documents`, the secret-manager SDK protocols to
  `app.clients`, the GCS protocols to `infra.storage.clients` (`GcsBlobLike`,
  `GcsBucketLike`, `GcsListedBlobLike`, `GcsClientFactory`), the compiled
  catalog's JSON shapes to `core.mapping.documents`;
  `core.mapping_pipelines.settings` is now `core.mapping_pipelines.types`. The
  package exports are unchanged. The kernel's lock code moved to
  `core.runtime.locks` (`runtime.LockWait` still exported).
  `core.cutover.checksum.restored`, without a caller since the diff's spill
  format changed, is removed.
- **Thresholds by policy.** On SQL Server the writer and the key registry
  choose online, per table, between staging a batch as parameters and bulk
  copy: the first three batches alternate, the faster is kept, re-checked
  every 200 batches with 20% hysteresis, one INFO line. Postgres → SQL Server,
  200,000 rows: 2,149 to 4,109 rows/s with 1 range, 4,044 to 7,180 with 4;
  reloads 3,505 to 8,754 and 4,857 to 12,863. The values per IN list come
  from the database's parameter limit (`Dialect.in_chunk`, at most 1,000; was
  500 for the ETL tables). `max_parallel_steps` defaults to the CPU budget.
- **Health and types.** `Resources.health()` and `Kernel.check()` return
  `runtime.Health` (status and warnings) per resource instead of a string; a
  `ResourceSpec.health` may return warnings. On SQL Server the derived
  IN-list chunk is 500 (1,000 keys read back 17% faster than with 1,000);
  the other databases keep 1,000; a set `[tuning] in_chunk` still wins.

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
  - Late rows at the last watermark are no longer lost when a run dies, is
    cancelled or loses its lease while reading them again, nor when a
    backfill runs in between.
  - Two live processes of one label no longer load the same steps at once,
    and a process cut off from the lock no longer frees its retry's locks.
  - A batch whose acknowledgement failed is no longer written to the issue
    sink twice on retry.
  - Two MongoDB writers rejecting the same record no longer leave two open
    quarantine entries.
  - `delete()` refuses NULL in a column it deletes by: it deleted nothing on
    SQL databases and too much on MongoDB.
- **Databases.**
  - SQL Server: key ranges written at once (`partitions` > 1, or scheduler
    jobs running ranges in parallel) no longer deadlock in the merge. Each
    batch's update, insert and delete seek the target's keys with row locks
    instead of scanning the whole table.
  - MySQL: transactions run in READ COMMITTED, so key ranges written at
    once no longer deadlock on the gap locks of the merge. It is set on the
    engine: set per session, SQLAlchemy put REPEATABLE READ back after the
    first autocommit read.
  - SQL Server no longer forces a nested loop on a target without an index
    on its key, which scanned the table once per batch row.
  - The key registry inserts a batch's keys in multi-row statements, not
    one statement per row (Postgres with psycopg, SQLite): parallel ranges
    no longer stall on it.
- **Runs.**
  - The lock heartbeat survives renewal errors.
  - Listener errors no longer skip recording a step.
  - Configuration errors exit 2 without a ledger entry.
  - A failed step fails the run.
  - A key change got past the key check through a sibling pipeline of the
    module, or after 200 runs; a change of natural key columns was not
    seen.
  - `etl` printed a traceback, and `run --format json` nothing, when the
    bookkeeping database was unreachable: it prints `FAILED <message>` or
    the JSON summary and exits 1.
  - A cancelled run no longer waits out its retry backoff and reads again.
  - Replay crashed on Postgres uuid or boolean keys and on an empty integer
    table, and found nothing for SQLite boolean keys.
- **Privacy and security.**
  - Tracebacks and exception messages are redacted, with the
    `logging.redact` names, in logs, trace spans (event and status), lineage
    events, Kafka dead-letter headers (redacted before they are cut), and
    the run and step messages of the ledger, `etl run` and `etl check`. The
    CLI's `FAILED` line uses the built-in names only, since the settings may
    be what failed.
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
  - The cutover diff's spill directory, which holds source rows, is removed
    on SIGTERM and Ctrl-C.
- **Mappings.**
  - Renamed tables are reported as rekeys.
  - `ReferenceRule` looks orphans up per batch, not per row.
  - Duplicate workbook headers are refused, and so are unknown date tokens
    (`DD`, `YYYY`, `M`, `MMM`, `SSS`) and `mm` used as a month; 2-digit
    years are warned.
- **Cutover.**
  - Parked orphans explain missing rows by key.
  - A report edited and sealed again no longer verifies.
  - An empty source and an empty target no longer pass.
  - Line breaks in a rejection reason no longer break the Markdown report;
    control characters no longer crash the xlsx one.
- **CLI.** `etl parked list <queue>` exits 7 for a queue no mapping parks
  to.
- **Messaging and storage.** A malformed Debezium message whose source or
  schema parts are not objects is a dead letter (or "not a Debezium change
  event"), not an AttributeError; a GCS blob listed without an update time,
  and corrupt Change Tracking or change stream positions, fail with a clear
  error.
- **Earlier fixes in this release.**
  - Creating the ETL tables from processes starting together no longer
    fails.
  - The run ledger returns UTC times on SQLite and MySQL.
  - SQLite files no longer pool connections
    (`ResourceWarning: unclosed database`).
- **SQL Server `datetime` and `smalldatetime` watermarks** (R3-01, found by
  the round-3 review, present since 0.1.0). mssql-python binds every Python
  `datetime` as `datetime2(7)`, so `col <= :p` and `col = :p` missed the rows
  stored at `.003`/`.007` and `col > :p` matched them again: the newest rows of
  a quiet table were never extracted, `recheck` never engaged, and a keyset
  read over more rows than `page_size` at one stamp looped forever yielding
  duplicates. Every bound value (keyset position, `--since`/`--until`, filters)
  is now cast to the column's type on the parameter side; the seek on the
  watermark index is kept.
- **MongoDB error messages** no longer print the failed operation, the
  duplicate key's value or the document that failed validation (R3-02): they
  tell the code, the error name, the index and the key pattern, in under 250
  characters, in the exception, the ledger and the logs; the chained pymongo
  error prints the same. MySQL 1062 and SQL Server 2627/2601 duplicate-key
  messages no longer quote the duplicate value.
- **The reason of a failed or cancelled run** reaches `etl run`'s text summary
  and JSON `message`, the ledger's run and step messages and `etl runs --run`
  as `<step>: failed after <n> attempt(s): <error>`, redacted like the ledger
  (R3-03); a refusal (`PipelineConfigError`, exit 2) is logged without a
  traceback, other errors keep theirs.
- `--resume` found the label's run only among the last 200 runs of the
  pipeline, dry runs included; it now searches every run (R3-14).
- Stateless sources: a batch whose acknowledgement failed was stored twice
  by a retry that read from the start again, once more per extra failed
  attempt (R3-12).
- A lock owner written by 0.1.0 (`label:<label>`, the label containing `@`)
  is read as its whole label: a retry of that label waits instead of exiting
  4, and other labels exit 4 at once (R3-15).
- Replaying quarantined rows whose key is a `DECIMAL(20, 0)` above 2^63 failed
  on SQL Server (the driver refused a BIGINT) and Postgres ("integer out of
  range"): an integer compared with an exact-number column is now bound as a
  `Decimal` (R3-06). A value the driver refuses to bind (mssql-python's
  `ValueError`, SQLite's `OverflowError`) fails the statement as a fatal
  error, without the value, instead of escaping untranslated.
- MySQL `BIT(n)` key columns are integers: replay finds their rows (R3-07).
- SQL Server: a target named with and without its schema (`dbo.t`, `t`) has
  its indexes read once and is warned of once (R3-09).
- Cutover Markdown report: a `|` in an entity, a pipeline, a rejection reason
  or a difference no longer splits a table cell (written `\|`); the values of
  a difference are HTML-escaped like the reasons (R3-11).
- A `--label --resume` retry that waited for its label's locks ran, without
  their locks, the steps it had carried over before the wait when the
  label's last run changed meanwhile (a try that failed early): it now reads
  the run to resume again after each wait and takes the locks of every step
  it runs, by the same rules (waits for its own label, exits 4 for another),
  twice at most; past that it runs the steps as last read, every lock held
  (R3-22).
- Checking the ETL tables at start-up (`etl run`, `etl check`) looks again a
  few times when reading the catalog fails transiently, as upgrading already
  did: on SQL Server the catalog reads of a process starting deadlock with the
  DDL of another one upgrading at the same time (1205).

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
