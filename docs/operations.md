# Operations

Every command reads the settings of `--env` (else `$ETL_ENV`, else `dev`).

## Runs

```sh
etl run crm.full_load [--partial-ok] [--dry-run] [--since ...] [--until ...]
etl runs [crm.full_load]          # the ledger: status, who, label, mappings
etl runs --run <run id>           # how each step of a run ended, and where
etl plan crm.full_load            # the steps, in their order
etl check                         # can every resource be reached? exit 5 if not
```

`etl check` connects to every database, Kafka cluster, store (a `local`
store's directory, a page of a `gcs` bucket) and event endpoint
(`lineage.endpoint`, `logging.otlp.endpoint`: a TCP connection, no event
sent). Use it as a readiness probe: it exits 5 (not ready) when one fails,
not 1, which schedulers retry. `etl config show` prints the settings,
secrets hidden, without reaching Vault, GCP or Azure: their placeholders are
shown as they are written.

The exit codes of every command are in {doc}`reference/cli`; what
schedulers do with them is in {doc}`deployment`. The ledger records the user, the build and the hashes of
the settings and the mappings of every run, and each step as soon as it
ends. A run still `in_progress` long after it started died with its
process. The locks of its steps expire after `runs.lease_seconds`, and the
next run goes on from its last loaded batch.

- **A partial success** (`partial_success`, exit 3) means rows were
  rejected into the quarantine, nothing else. A step that failed fails the
  run (exit 1), even when its policy let the other steps go on.
- **A pipeline that cannot be built** (missing compiled mappings, a setting
  the module refuses) exits 2 and records no run: retrying would not help.
- **The locks.** A run locks each step it runs, `<pipeline>/<step>`, with a
  lease renewed every third of `runs.lease_seconds`. A step locked by
  another run makes it exit 4 at once, holding nothing. If the lock's
  database cannot be reached, it tries again until the lease is about to
  expire, then cancels the run (`EtlLockLost`).

## Retries

- **Inside a run.** A step failing on a transient error runs again, up to
  `step_attempts` times (3 by default), after `retry_backoff_seconds` (5),
  doubled at each attempt. It goes on from its last loaded batch.
  `etl runs --run <id>` shows the attempts of each step.
- **Transient** is a lost connection, a timeout, a deadlock or
  serialization failure, too many connections, a server starting or
  stopping (told by the database's error codes and SQLSTATEs), a MongoDB
  transient transaction error. **Fatal** is everything else, and fails the
  step at once: a login refused, bad SQL, a missing table or column, bad
  data, a constraint. Retrying those would only repeat them.
- **Across runs.** A failed run exits 1. The scheduler retries it with the
  same `--label` and `--resume` (below): the steps it finished are not run
  again.

## Time zones

Every database session the ETL opens runs in UTC (Postgres, Oracle,
MySQL; SQL Server and SQLite have no session time zone). A naive datetime
is UTC, and aware values come back in UTC, whatever the zone of the server
or the client. Behind PgBouncer in transaction pooling the session's time
zone is lost between transactions: give the ETL session pooling, or set
its role's zone (`ALTER ROLE etl SET timezone = 'UTC'`).

MySQL sessions also run in READ COMMITTED, the default of the other
databases: under InnoDB's REPEATABLE READ, key ranges written at once
deadlock in the merge. READ COMMITTED needs row-based binary logging
(`binlog_format = ROW`, MySQL 8's default) on a target that logs.

## Resuming, and running a step again

```sh
etl run crm.full_load --resume            # the last run, unless it succeeded
etl run crm.full_load --resume <run id>   # that run
etl run crm.full_load --step 'customer*'  # one entity: its steps only
etl run crm.full_load --label "$JOB" --resume   # a scheduler's job, retried
```

- **`--resume`.** The steps the run finished are not run again; the others
  run. Finished means `success` or `partial_success`: the rejected rows of
  a partial success are replayed from the quarantine, not by running the
  step again.
  - It follows the chain: resuming a run that resumed another carries the
    steps of both.
  - Each step still goes on from its last loaded batch, so a step that
    failed half-way does not start over.
  - Without a run id, it resumes the last run of the pipeline, unless that
    one succeeded: then every step runs. With `--label`, the last run of
    that label only.
  - Resume retries, not the next scheduled run: after a failed night, a
    resuming run would not run the steps that night finished, and their
    new rows would wait for the night after. Hence the labels.
  - The ledger records `resumed_from`; `etl runs --run` shows what ran.
- **`--label`.** The scheduler's name of the job: the Kubernetes Job
  (`$(JOB_NAME)`), the Airflow DAG run and task. With `--resume`, a retry of
  the job resumes its run, and the next night's job, another label, runs
  every step. A retry also takes over the locks of a try that died (OOM,
  `kill -9`) at once, instead of waiting for their lease: never give two
  runs at once the same label.
- **`--step`.** Runs these steps only, by name or pattern; `--step` again
  for more. The others are taken as done: they do not hold back the steps
  selected, and do not run. Use it to load an entity again after fixing
  its mapping, or a range of a partitioned table (`customer:2`).
- Pass the same `--since`, `--until` and `--param` as the run resumed: the
  ledger does not keep them.
- **`--rekey`.** A run refuses to start (exit 2) when the module's keys
  changed since the last successful run: a rotated `key_secret`, a renamed
  key namespace ({doc}`security`). `--rekey` runs it anyway, once the
  target is ready for new keys; the ledger records `rekeyed_from`.

## Several processes for one pipeline

A run is one process. To load a pipeline on several hosts, the scheduler
runs several jobs, each with its own steps:

```sh
etl run crm.full_load --label split --step '*:ranges'      # first, once
etl run crm.full_load --label part-0 --step 'customer:0'   # then, at once
etl run crm.full_load --label part-1 --step 'customer:1'
...
etl run crm.full_load --label orders --step 'order:*'      # the children last
```

- **The ranges first.** A partitioned table (`partitions` of the CRM module)
  is read by one step per key range. `<mapping>:ranges` splits it once and
  saves the ranges; a range run alone before that fails, rather than split
  the table again on its own (two jobs could split it differently, and
  miss rows).
- **The first read of a range is by key.** A range step reads its range
  in key order the first time, up to the largest watermark at its start
  (the primary key's index is enough); a failed first read goes on after
  the last key loaded. The later runs read by watermark, and want an index
  on `(watermark, key)`. A backfill (`--since`, `--until`) run during an
  unfinished first read saves no position.
- **Parents before children.** The steps a job does not select are taken
  as done: run the ranges of a parent table before those of its children,
  or the orphans are parked.
- **Locks.** Jobs of other steps run at the same time; a job whose steps
  overlap one running exits 4.
- **The bookkeeping** must be in a database every job reaches
  (`bookkeeping.database`): the files of a local bookkeeping serve one host.
- **Retries** are the scheduler's, per job: a label each, and `--resume`.
  {doc}`deployment` has the Kubernetes and Airflow shapes.

## Rejected, parked, saved

```sh
etl quarantine list|export|close|purge   # rejected rows, why
etl run crm.replay                       # once their cause is fixed
etl parked list|release <queue>          # orphans waiting for their parent
etl state show|reset <pipeline> <key>    # watermarks, positions
etl keys lookup <entity> <key>           # the legacy key of a key
etl keys forget <entity> --key-file -    # the right to erasure
etl housekeeping                         # purge after retention_days
```

- **The quarantine.** A row rejected again (the rows at the last watermark
  are read again by the next run, a run resumed) refreshes its open entry:
  its issues, run and time, keeping its id. One open entry per record and
  checkpoint: a mapped record is identified by its mapping, source key and
  part, any other record by its data.
- **`etl quarantine export`.** A CSV for the BAs, opened in Excel: its own
  columns, prefixed with `_` (`_id`, `_pipeline`, `_run_id`,
  `_rejected_at`, `_status`, `_entity`, `_key`, `_issues`, `_record`), and
  `mapping` and `source_key`. A corrections file made from it keeps
  `mapping` and `source_key`, adds the corrected source columns, and the
  `_` columns are skipped. A cell starting like a formula (`=`, `+`, `-`,
  `@`, a tab, a carriage return) or with `'` is written with a leading `'`,
  so that no spreadsheet runs it; the corrections read it back as it was.
- **`etl state reset`** asks before forgetting a value (`--yes` for
  scripts), exits 7 when nothing is saved, and is recorded in the run
  ledger as a run of `etl.state-reset`, with the operator.
- **The operator.** `etl state reset` and `etl keys forget` record who ran
  them: `$ETL_OPERATOR` when set (a person, a ticket), else the user of the
  process. Set it in the Job or the CI step that runs them.
- **`etl housekeeping`** purges what is older than
  `bookkeeping.retention_days` (`--days` overrides it): the quarantined
  records, whatever their status; the quality issues; the parked records
  waiting since before then (given up on, as `etl parked release`), in the
  queues of the modules' mappings (`--queue` adds others). Schedule it daily.
- **`etl keys forget`**: {doc}`security`.

## Upgrading the ETL's tables

The `etl_*` tables have revisions, applied by Alembic, and the one they are
at is recorded in `<prefix>alembic_version`, in their schema
(`bookkeeping.schema_name`). Each release says the revision it needs: run
its upgrade before its pipelines.

| Revision | What it changes |
| --- | --- |
| 0001 | The tables of 0.1.0 |
| 0002 | `etl_run_steps`; the quarantine's `status`, `resolved_at` |
| 0003 | The quarantine's `record_key`; indexes of `etl_quality_issues` on `record_id`, `found_at` |
| 0004 | One open quarantine entry per record: a unique index (partial on Postgres, SQL Server, SQLite; on expressions on Oracle; none on MySQL) |

- **Where they stand.** `etl-db current` prints the revision, and what to
  run. `etl-db check` exits 0 at the latest revision, 5 when
  `etl-db upgrade` would bring them there, 1 when it cannot (a revision
  this release does not know: a later one upgraded them; or tables that
  match no revision).
- **With the owner's rights.** `etl-db upgrade --url ...` creates the
  tables, or upgrades them. Run it once per release, as a step of the
  deployment (a Job, a CI step, an init container) before the pipelines.
  Processes upgrading at once wait for each other (an advisory lock on
  Postgres, SQL Server and MySQL; tries again on Oracle and SQLite).
- **Through a DBA.** `etl-db ddl --dialect mssql` prints the SQL creating
  the tables at the latest revision; `etl-db ddl --dialect mssql --from
  0002` the SQL upgrading tables at 0002 (`GO` separates the batches of
  SQL Server, `/` the statements of Oracle). Add `--schema` and `--prefix`
  as in the settings.
- **Tables created before the version table** (0.1.0, or `create_all` of
  the code up to this release) have no revision recorded. `etl-db upgrade`
  tells it from the columns and indexes it finds, records it, then upgrades
  them; `etl-db current` says which it found. For a DBA:
  `etl-db ddl --dialect mssql --from 0001 --legacy` records it first. When
  the tables match no revision (some of a revision there, a table missing),
  nothing is changed: `check` and `upgrade` say what differs; fix it by
  hand, comparing with `etl-db ddl`.
- **The records are kept.** The quarantined records of 0.1.0 are open.
  `etl-db upgrade` computes the key of each record (`record_key`), so that a
  record rejected again refreshes its entry; of the open entries of a record
  (rejected again before 0003), the latest keeps the key, the others stay
  open with a key of their own. The SQL of `etl-db ddl --from` cannot
  compute the keys: every entry gets its own, and a record rejected again
  gets one more entry, once.
- **At startup.** With `bookkeeping.create = true` (development), the
  first process to need the tables creates or upgrades them. Otherwise a
  run refuses to start while they are behind, with exit 2 and what to run;
  `etl check` reports it (`FAIL etl.tables`, exit 5).
- **No downgrade.** Back the tables up before upgrading; to go back,
  restore the backup and run the previous release.

## Backing up the ETL's records

The `etl_*` tables (or collections, or the bookkeeping directory) are the
ETL's memory: back them up with the target database, on the same schedule,
and restore them together.

| Lost | What happens | Recovery |
| --- | --- | --- |
| `etl_key_registry` (crosswalk) | Children of parents loaded before are seen as orphans: rejected or parked, or inferred again. Lookups of legacy ids fail. Keys are derived (HMAC, UUIDv5), so reloading does not duplicate them, but a mapping with generated keys would. | Restore it; or rebuild it by reloading every parent (`--step` each parent entity, state reset) before the children. |
| `etl_state` (watermarks, positions, ranges) | The next run reads every row again (full re-read): upserts make it safe, but long. A change log position lost means changes missed or replayed from the slot's retention. | Restore it; or accept the full re-read; for CDC, reset the slot and run a full load. |
| `etl_runs`, `etl_run_steps` (ledger) | `--resume` has nothing to resume: steps run again. The audit of erasures, state resets and cutover reports (`etl cutover verify`) is gone. The key fingerprint check starts over. | Restore it; export it to an archive for audits. |
| `etl_quarantine`, `etl_parked`, `etl_quality_issues` | Rejected and orphan rows are forgotten until read again: a full load rejects or parks them again. | Restore it, or run a full load. |
| `etl_locks` | Nothing: leases. | Nothing to restore. |

- **Restore order.** Stop the schedules. Restore the target database and
  the `etl_*` tables from the same point in time, or the `etl_*` tables
  from a point before the target's: a crosswalk older than the target makes
  the rows loaded since look new (their children orphans until reloaded); a
  crosswalk newer than the target holds keys of rows the target lost, so
  their children load against missing parents. Then `etl state reset`
  the pipelines whose state is newer than the target, run them
  (`etl run ... --resume` does not apply: the ledger is from another time),
  and `etl cutover report` before reopening.
- **An erasure after a backup** is undone by restoring it: keep the list of
  erasures since (the `etl.erasure` runs of the ledger record their
  surrogate keys and when) and run them again (see {doc}`security`).

## Watching

- **Grafana.** `deploy/grafana/etl-dashboard.json` shows runs, rows per
  second, reject rates, durations and lags.
- **Alerts.** `deploy/grafana/alerts.yaml` alerts on:
  - a failed run, and which step failed;
  - no success for a day;
  - more than 5% rejects;
  - lagging changes;
  - a lost lock.
- **Lineage.** With `[lineage] endpoint = ...`, every run and step sends
  OpenLineage events, with the tables read and written, to Marquez, DataHub
  or OpenMetadata.

## When something goes wrong

| Symptom | Look at | Then |
| --- | --- | --- |
| Exit 1, a step failed | `etl runs --run <id>`, the logs of the run (`etl.run_id`) | Fix, `etl run --resume`: the steps done are not run again |
| Exit 3, rows rejected | `etl quarantine list` | Fix the cause or correct the rows, `etl run crm.replay` |
| Exit 2 | The message: settings, missing compiled mappings, keys changed | Fix it; nothing ran, nothing was recorded |
| Exit 2, "The ETL tables are at revision ..." | `etl-db current` | `etl-db upgrade`, or the SQL of `etl-db ddl --from <revision>` (see above) |
| Exit 4, already running | `etl runs`: a run `in_progress` with some of the same steps | Wait; a dead run's locks expire after their lease, or its retry (same `--label`) takes them over |
| Exit 130, cancelled | The logs: SIGTERM (a pod evicted, a timeout) or Ctrl-C | Run it again with `--resume`: running steps ended their batch |
| `etl check` exit 5 | The `FAIL` lines: which resource, why | Fix the network, the credentials or the setting; check again |
| Orphans waiting | `etl parked list <queue>` | Load their parents; `crm.parked` replays them |
| Change log lag growing | The `EtlChangesLagging` alert | Is the sync scheduled? Is the slot/stream kept? |
| Debezium message rejected | The dead letter topic (`changes.dead_letter_topic`) | The `etl.error` header says why |
