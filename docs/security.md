# Security and compliance

What the ETL needs from the systems it touches, how it keeps secrets and
personal data, and how its own supply chain is checked.

## Secrets

Settings files hold placeholders, never secrets. Placeholders are resolved
once, at start-up. In production, prefer files and secret managers to
environment variables: every process the ETL starts inherits its
environment, and `/proc` shows it. `config/prod.toml` reads its connection
strings from files (`${file:/run/secrets/legacy_database_url}`), which the
Kubernetes CronJobs mount from the `etl-secrets` Secret (keys
`legacy-database-url`, `crm-database-url`); `config/uat.toml` reads
environment variables.

| Placeholder | Where the secret lives | Credentials of the ETL |
| --- | --- | --- |
| `${env:NAME}` | An environment variable | — |
| `${file:/run/secrets/x}` | A mounted file (Docker, Kubernetes secrets) | — |
| `${vault:secret/crm/db#password}` | HashiCorp Vault, KV version 2 | `VAULT_ADDR`, `VAULT_TOKEN` or `VAULT_TOKEN_FILE`, `VAULT_NAMESPACE`, `VAULT_CACERT` or `VAULT_CAPATH` |
| `${gcp:crm-db-url}` | Google Cloud Secret Manager (`gcp-secrets` extra) | Application Default Credentials, `GOOGLE_CLOUD_PROJECT` |
| `${azure:acme-kv/crm-db-url}` | Azure Key Vault (`azure-secrets` extra) | `DefaultAzureCredential` (managed identity first), `AZURE_KEYVAULT_DNS_SUFFIX` |

How secrets are handled:

- Secret settings are `SecretStr`. `etl config show` masks them, and the run
  ledger hashes the settings with the secrets masked, so rotating a secret
  changes no hash.
- Logs are redacted before they leave the process: see [Logs](#logs).
- A secret is read once per process, whatever the number of its
  placeholders.
- **Vault.** TLS is always verified: against the system's CAs, or a private
  CA given by `VAULT_CACERT` (a PEM file) or `VAULT_CAPATH` (a directory).
  `VAULT_SKIP_VERIFY` is not supported. Redirects are not followed, so the
  token never goes anywhere but `VAULT_ADDR`: point it at the active node
  or the load balancer, not a standby. An answer that is not Vault's JSON
  (a proxy's login page) fails with a clear error.
- **Azure Key Vault in another cloud.** Vaults are
  `<vault>.vault.azure.net`. In a sovereign cloud set
  `AZURE_KEYVAULT_DNS_SUFFIX` (`vault.azure.cn`,
  `vault.usgovcloudapi.net`) and `AZURE_AUTHORITY_HOST`, or give the
  vault's full host name: `${azure:acme-kv.vault.azure.cn/crm-db-url}`.
- **Kafka.** Put SASL and TLS key passwords in `kafka.<name>.credentials`,
  not `options`. They are kept apart from the cluster's properties
  (`KafkaCluster.config`) and given to each consumer, producer and admin
  client only when it is built. Properties whose name holds a secret name
  (`sasl.password`, `ssl.key.password`) are moved there even if given in
  `options`.
- **Lineage.** The OpenLineage API key is sent as a bearer token to the
  configured URL only: redirects are not followed. Configure the final
  `https://` URL.
- In Kubernetes, prefer short-lived tokens: Vault's Kubernetes auth writing
  `VAULT_TOKEN_FILE`, GCP Workload Identity, Azure managed identities.
- Other managers plug in with `config.register_provider`.

## Logs

Every record goes through one redaction filter (`RedactingFilter`), on the
pipeline's thread, before any sink sees it: the console and JSON lines,
the OTLP exporter, and any handler added to the root logger after
`configure_logging`.

What is redacted, in every part of a record (message, arguments, extra
and context fields, the exception's message, its traceback and chained
exceptions, `stack_info`):

- **Passwords of URLs:** `postgresql://etl:***@db/crm`.
- **Values of secret keys**, in `key=value`, `key: value`, JSON,
  query strings and headers, case-insensitively. A key is secret when it
  holds one of `password`, `passwd`, `pwd`, `secret`, `token`, `api_key`
  (`apikey`, `api-key`), `credential`, `private_key`, `authorization`,
  `account_key`, `signature`, or a name of `logging.redact`; `sig` is
  secret as a whole key. So `PWD=`, `client_secret=`, `key_secret:`,
  `x-api-key:`, `sasl.password=`, `ssl_key_password=`, `AccountKey=`,
  `SharedAccessSignature=` and `&sig=` are all hidden. Quoted (`'...'`)
  and ODBC-braced (`{...}`) values are hidden whole.
- **Bearer tokens** (`Authorization: Bearer ***`, and `Bearer ***`
  anywhere) and **PEM private keys**.
- **Row values quoted by database errors:** SQLAlchemy's
  `[parameters: ...]`, Postgres' `Key (...)=(...)` and
  `Failing row contains (...)`, SQL Server's `The duplicate key value is
  (...)`, `Truncated value: '...'` and bulk copy's `Cannot convert
  string '...'`, MySQL's `Duplicate entry '...'` and `Incorrect ... value:
  '...'`, Postgres' `invalid input syntax for type ...: "..."` and the
  `COPY ..., line N: "..."` context line. The engines also run with
  `hide_parameters`, so SQLAlchemy does not print the parameters at all.
- **Extra fields and dict keys** whose name is secret: their whole value.

The names above are always redacted; `logging.redact` adds to them (an
`ssn` field, say).

Exceptions are rendered once, redacted, by the filter: the formatters and
the OTLP exporter (`exception.message`, `exception.stacktrace`) use that
text, and the record's `exc_info` is cleared so no handler can print the
raw traceback. A failed attempt that will be retried is logged in one
line; the traceback comes with the last attempt only. Lineage events
carry the redacted error message too.

What is **not** redacted:

- Values in free text that has no recognisable shape: an exception raised
  as `ValueError(f"bad email {email}")` shows the email. Do not put row
  values in exception messages; reject records with the field name and
  the rule instead.
- Source lines quoted in tracebacks: keep secrets and data out of the code.
- Logs written before `configure_logging` runs, or to handlers attached to
  a non-root logger.
- Values a driver quotes in another shape than those above. They reach
  the logs and the run ledger's message of a failed step.
- The value and key of a Kafka dead letter: the source message as it was
  (its `etl.error` header is redacted); see
  [Kafka dead letters](#kafka-dead-letters).

Redaction is a safety net. Treat log stores as holding personal data
anyway: restrict who reads them and set their retention, as for the ETL's
own tables. Airflow's `get_logs=True` copies the pod's logs into Airflow's
log store, which needs the same care.

## Least privileges, by system

Give the ETL its own accounts: one to read each source, and one to write
each target and its own tables. These privileges are the least each feature
needs.

### Every target database

- **Target tables.** SELECT, INSERT, UPDATE, DELETE. DELETE only for `hard`
  deletes. Soft deletes are UPDATEs.
- **Staging tables**, which live only as long as each batch:
  - Postgres: `TEMP` on the database. `PUBLIC` has it by default.
  - SQL Server: `#` and `##` tables in `tempdb`, which any login may create.
    Bulk copy needs nothing beyond INSERT.
  - Oracle 18c and later: CREATE TABLE, for private temporary tables.
  - MySQL: CREATE TEMPORARY TABLES.
- **The ETL's own tables** (`etl_*`):
  - At runtime: SELECT, INSERT, UPDATE, DELETE on their schema
    (`bookkeeping.schema_name`).
  - To create or upgrade them, a schema owner runs `etl-db upgrade` once
    per release, or a DBA the SQL of `etl-db ddl` (`--from <revision>` for
    an upgrade: {doc}`operations`). Keep `bookkeeping.create = false`
    outside development: the runtime login then needs no DDL rights, and a
    run refuses to start while the tables are behind.

### Sources

- **Full and incremental loads.** SELECT on the source tables. An index on
  `(watermark, key)` keeps the reads cheap.
- **SQL Server Change Tracking.**
  - A DBA turns it on once: `ALTER DATABASE … SET CHANGE_TRACKING = ON`,
    then `ALTER TABLE … ENABLE CHANGE_TRACKING`.
  - The ETL needs SELECT and `VIEW CHANGE TRACKING` on each table.
- **Postgres logical replication.**
  - `wal_level = logical`.
  - The ETL's role needs `REPLICATION` (on RDS, `rds_replication`) to use
    its slots.
  - Creating the publications needs ownership of the tables.
  - Have the slots and publications created once by the owner
    (`relational.create_replication`).
  - Slots hold WAL until they are read: watch their lag, and drop the slot
    of a table no longer synchronized.
- **MongoDB change streams.** A replica set or sharded cluster. The `read`
  role on the database is enough. Size the oplog for the longest pause
  between two runs.
- **Oracle, through Debezium.**
  - The connector's user needs the LogMiner privileges that the Debezium
    Oracle connector documents. Only the connector holds them; the ETL
    never connects to Oracle's log.
  - On Kafka, the ETL needs, for its topics: Read and Describe on the
    topics, and Read on its consumer group (`kafka.<name>.group_id`).

## Network

- **Databases.** Use TLS everywhere. The settings take the drivers' own
  options, in the URL or in `databases.<name>.options`:
  - Postgres: `?sslmode=verify-full&sslrootcert=/etc/ssl/ca.pem`.
  - SQL Server: `Encrypt=yes;TrustServerCertificate=no`. The test URLs
    turn verification off; production must not.
  - Oracle: a `tcps://` DSN, or a wallet: `config_dir`,
    `wallet_location` and `wallet_password` in
    `options.connect_args`.
  - MySQL: `ssl_ca`, `ssl_verify_cert` in `options.connect_args`.
  - Driver arguments given beside `connect_args`, at the top of `options`,
    are refused at start-up (exit 2).
  - MongoDB: `?tls=true&tlsCAFile=/etc/ssl/ca.pem`.
- **Kafka.** Set `security.protocol = "SASL_SSL"` and the SASL mechanism in
  `kafka.<name>.options`, and the passwords in `credentials` (hidden, and
  kept out of `KafkaCluster.config`).
- **OTLP.** Use an `https://` endpoint, with the collector's token in
  `logging.otlp.headers` (`Authorization = "Bearer ${file:...}"`, hidden),
  as `config/prod.toml` shows. The collector configurations of
  `deploy/otel-collector/` accept OTLP over TLS only, with that bearer
  token, on the pod's address: an open collector lets anyone in the
  cluster write logs and metrics that look like the ETL's. A private CA:
  `OTEL_EXPORTER_OTLP_CERTIFICATE`; mutual TLS: its README.
- **Kubernetes.** `deploy/k8s/base/networkpolicy.yaml` limits the ETL's
  pods to DNS, the databases and the collector; adapt its addresses (a
  database CIDR, the collector's labels) first, and add a rule for a
  secret manager or Kafka if used. Kubernetes enforces it only with a
  network plugin that supports policies.
- **OpenLineage.** Use an `https://` endpoint. Redirects are not followed:
  a redirect fails the event (logged), it never carries the bearer token
  elsewhere.

## Kafka dead letters

With `changes.<name>.dead_letter_topic`, a message that is not a change
event goes to that topic instead of failing the run. It goes **as it
was**: its value, key and headers, plus `etl.error` (the reason: the
exception's text, redacted as logs are and cut to 1,000 characters),
`etl.message` (the id of the message, `<topic>[<partition>]@<offset>`),
`etl.topic`, `etl.partition` and `etl.offset`.

A dead letter is sent once the changes read around it are loaded, just
before the run saves its position past it. A batch read again after a
failed load sends it once; a dry run sends nothing; a dead letter that
cannot be sent (topic missing, no Write ACL) fails the run before its
position moves, so none is lost. Dead letters and tombstones at the end of
a topic are passed too: the next run neither reads nor sends them again.
A run that stops between sending a dead letter and saving its position
sends it again: `etl.topic`, `etl.partition` and `etl.offset` identify
the message, so a copy is easy to drop.

The dead letter topic holds personal data like the source topics:

- **Content.** The redaction of `etl.error` is a safety net (see
  [Logs](#logs)); the value and key are the source message itself.
- **Retention.** Set `retention.ms` on the topic to what the source data is
  allowed (for instance 7 to 30 days), and replay or purge it before then.
  Erasure requests reach it only by its retention: keep it short.
- **ACLs.** The ETL needs Write and Describe on the topic; only the people
  and tools that replay it need Read. Do not give it the source topics'
  wide Read ACLs.
- **Monitoring.** `etl.messages.dead_lettered` counts what goes there:
  alert on it, a dead letter is a message the target did not get.

## Personal data

- **Surrogate keys.** How each mapping derives them is its **Key
  hashing**:
  - `hmac`: an HMAC of the natural key, keyed by `key_secret`. Without the
    secret, a key reveals nothing of the legacy id. A mapping that leaves
    Key hashing empty gets it wherever `key_secret` is set.
  - `uuid5`: a public UUIDv5. Anyone can compute it from a natural key, so
    a small key space (national ids, phone numbers) can be enumerated back.
    The mappings refuse it for a natural key that a personal column also
    reads; use it only for keys that are not personal data, like the CRM
    example's customer and order numbers.
- **Rotating `key_secret` changes every HMAC key**: it is a full re-key,
  not a routine secret rotation.
  - The run ledger records a hash of the secret (never the secret) with the
    key namespaces of the mappings (`keys=` in `etl runs`), and `etl run`
    refuses to run once it changed, unless `--rekey`.
  - To rotate it: stop the schedules; empty the target tables of the HMAC
    mappings and their crosswalk, or point the ETL at a fresh target; set
    the new secret; reset the pipelines' state (`etl state reset`) so that
    every row is read again; run `etl run crm.full_load --rekey`; then
    `etl cutover report crm`. Foreign keys follow, as children derive
    them from the same secret.
  - Keep the old secret until the new keys are cut over: the target's keys
    can only be traced back to the legacy ids with it.
- **Anonymizing personal columns.**
  - The mapping workbooks mark them in **Personal data**: `name`, `email`,
    `phone`, `address`, `date`, `mask` or `hash`.
  - With `privacy.anonymize = true` (`config/uat.toml`), they get fake
    values, derived from the real ones by HMAC with `privacy.secret`. The
    same value always gets the same fake, so joins hold and the cutover
    report still reconciles. `email`, `phone` and `hash` fakes keep unique
    columns unique (80, about 50 and 64 bits of the HMAC: no collision in
    10^6 values, tested); `name`, `address` and `mask` are not unique.
  - Each kind needs a target type holding its fakes, checked when the
    mapping compiles: text of at least 15 characters for `name`, 41 for
    `email`, 20 for `phone`, 29 for `address`, 16 for `hash`; text for
    `mask`; `DATE` or `TIMESTAMP` for `date`.
  - `date` shifts every date of a record by the same 1 to 180 days, either
    way, derived from the record's natural key: a start stays before its
    end, and the days between them stay the same. Ages and fiscal periods
    move with the shift.
  - The issues of a personal column never hold its value, in production
    as in UAT: the quarantine, the issue sinks, `etl quarantine export` and
    the cutover reports say what is wrong (`the value does not match the
    format dd/MM/yyyy`), not the value. The source row is found by its
    `source_key`.
  - The `prod` environment refuses it, and the run ledger records
    `anonymized=yes` for every run that did it.
- **Quarantine, parking lot and issues.** They hold source records and the
  values at fault, so personal data too: a quarantined or parked record
  keeps its natural key and the natural keys it references
  (`natural_key`, `foreign_keys`), an issue its `value` and `record_id`
  (the source key).
  - `etl housekeeping` purges the quarantined records, the quality issues
    and the parked records older than `bookkeeping.retention_days` (90 by
    default). Run it on a schedule.
  - `etl quarantine export` writes personal data to a file: keep those
    files where the source data is allowed.
- **Cutover diffs.** `etl cutover report` spills the source rows of the
  buckets that differ to disk past 50,000 rows: JSON lines, personal data
  included, in a directory of `$TMPDIR` only its user can read, removed when
  the report is done (not after `kill -9`). Point `TMPDIR` at a volume
  allowed to hold the source data, sized for the source table; the image's
  read-only root filesystem needs a writable volume there anyway. The
  report's samples show the values that differ: treat the reports as the
  source data too.
- **Right to erasure.**

  ```sh
  printf '1003\n' | ETL_OPERATOR="jane.doe DPO-17" \
      etl keys forget crm.customer --key-file -
  ```

  - Give the natural key on stdin or in a file (`--key-file`, one
    component per line). On the command line (`etl keys forget
    crm.customer 1003`) it ends up in shell histories, process lists, Job
    specs and scheduler logs.
  - This erases what the ETL keeps of that natural key:
    - its keys in the crosswalk;
    - its quarantined records, and its records parked;
    - the quarantined and parked records of the other entities that
      reference it: an order parked, waiting for that customer, holds its
      natural key in `foreign_keys`;
    - the quality issues of all those records (by `record_id`, their
      source key), in the issue sink: table, collection or files.
  - The key case is read from the entity's mappings in the compiled
    catalog (`upper` for the CRM customer); `--case` overrides it. The
    parking queues searched are those of the mappings of the entity and of
    the mappings referencing it; `--queue` adds others.
  - Matching relies on the mapped records' lineage: a row rejected before
    it was mapped (a check at `after_extract`) holds no natural key the
    command can recognize, and an issue of a record kept (a warning)
    is erased only if that record is the entity's own.
  - The run ledger records each erasure, as a run of `etl.erasure`, with
    the operator and the surrogate keys only, never the natural key. The
    operator is `$ETL_OPERATOR` when set (the person or ticket behind a
    Job or a pipeline run), else the user of the process, `etl` in the
    container image. It is a statement, not an authentication: whoever can
    run the command can set it, and the credentials that write the ledger
    can also change it.
  - Not erased, by this command or by anything in the ETL:
    - the target system: it erases its own rows;
    - backups, of the target and of the ETL's database: they age out with
      their retention, or are restored without the erased records;
    - log and trace backends (Loki, Elasticsearch, Seq, the OTLP
      collector and what it exports to): redaction hides secrets and
      known patterns, not every id or key a message names; their own
      retention removes them;
    - Kafka dead letters: the topic's retention removes them;
    - exported files: `etl quarantine export` CSVs, cutover reports, saved
      rehearsals;
    - lineage backends (Marquez, DataHub...): they hold dataset and job
      metadata, and the run facets.

## Supply chain

`make security` runs three checks, and CI runs it on every push:

- **Known vulnerabilities.** `pip-audit` audits every locked dependency,
  development tools included.
  - It ignores one advisory, GHSA-cg8m-r9f2-5wm2. It is in Black's GitHub
    Action, which nothing here uses; Black comes with pyink, which pins
    it.
  - Every ignored advisory is listed with its reason in the `Makefile`.
- **Insecure code.** `bandit` checks the code. The few findings that are
  not issues are marked `# nosec <test>`, with the reason in a comment:
  - SQL built from quoted identifiers;
  - randomness used for sampling and jitter;
  - `git describe` at start-up.
- **Software bill of materials.** A CycloneDX SBOM of what ships, the extras
  without the development tools. It is written to `var/sbom.cdx.json` and
  kept as a CI artifact.

`uv.lock` pins every dependency, with hashes, and CI installs with
`--frozen`. The rest is pinned too: base images and the tools' images by
digest, GitHub Actions by commit, the scanners by version (`Makefile`);
Dependabot proposes their updates.

The image is checked as well, in CI and before a release:

- **Vulnerabilities.** `make image-scan` (trivy) fails on a high or
  critical vulnerability that has a fix, in OS packages and Python
  libraries alike; `.trivyignore` lists the exceptions with their reasons.
- **SBOM of the image.** `make image-sbom` (syft) writes
  `var/image.cdx.json`, OS packages included.
- **Signature and provenance.** A release signs the image keyless with
  cosign, and attaches SLSA build provenance and the SBOM as attestations,
  to the digest it pushed: deploy that digest ({doc}`deployment`).
