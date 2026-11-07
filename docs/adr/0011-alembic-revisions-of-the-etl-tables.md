# 11. Alembic revisions for the ETL's tables

**Status:** accepted.

## Context

- **No upgrade path.** The `etl_*` tables were made by `create_all`, which
  adds missing tables, never columns or indexes, and `etl-db ddl` printed
  CREATE statements only. Since 0.1.0 the tables gained `etl_run_steps`,
  the quarantine's `status`, `resolved_at` and `record_key`, and indexes.
  A 0.1.0 deployment upgraded failed at runtime ("column status does not
  exist" in `etl quarantine list`, `etl_run_steps` missing at the end of
  the first step), and nothing told the operator what to run.
- **No version.** Nothing recorded which tables a database had, so a run
  could not tell that they were behind.
- **DBAs run the DDL.** In production the runtime login has no DDL rights:
  the SQL must be printable for a DBA, per database, as well as applied by
  a command.
- **Several sets of tables.** Tables of one deployment share a database
  with others (`bookkeeping.prefix`, `bookkeeping.schema_name`).

## Decision

- **Alembic**, the migration tool of SQLAlchemy, which the adapters
  already use: one revision script per change, in
  `infra/database/relational/migrations/versions`, shipped in the wheel.
  The revisions are written by hand (no autogenerate) and never changed
  once released. `bookkeeping.etl_tables` declares the latest revision; a
  test checks, on every database, that the revisions make the same tables.
- **The baseline is 0.1.0.** 0.1.0 has no tag: its tables are those of the
  code before the review, without what its changelog lists as unreleased
  (`etl_run_steps`) and without the quarantine's status (Phase 8, review
  D4). One revision per later state of the code that `create_all` could
  have left in a database: 0002 (steps, status), 0003 (record keys, indexes
  of the issues), 0004 (one open quarantine entry per record).
- **A version per set of tables:** `<prefix>alembic_version`, in their
  schema. No `alembic.ini`: `migrations` gives Alembic the connection, or
  the dialect for the SQL, the schema and the prefix (`env.py` reads them).
- **Commands.** `etl-db upgrade` (creates or upgrades), `etl-db current`,
  `etl-db check` (exit 5 NOT_READY when behind: the tables are not ready
  for this release, and `upgrade` fixes it; 1 when it cannot), and
  `etl-db ddl --from <revision>` printing Alembic's offline SQL for a DBA,
  `ddl` alone the CREATE statements and the version recorded.
- **Tables without version** are told by the columns and indexes each
  revision adds, recorded, then upgraded. Whole tables are not markers:
  `create_all` added them to older tables too, and the revision that adds
  one skips it if there. Tables matching no revision are refused,
  unchanged.
- **At startup.** `bookkeeping.create = true` upgrades to the latest
  revision; `false` checks it, and a run refuses to start (exit 2) with what
  to run. Concurrent upgrades take the database's advisory lock where it
  has one (Postgres, SQL Server, MySQL), and try again elsewhere.
- **No downgrade.** Restore the backup taken before the upgrade.

## Consequences

- **What it buys.** Upgrades from any earlier state, on the five
  databases, applied or printed; a run that refuses old tables instead of
  failing in the middle; the open quarantine entries made unique where the
  database can index them, so idempotence holds between concurrent
  writers.
- **What it costs.**
  - A change of `etl_tables` needs a revision script too, and a marker in
    `migrations` (what it adds to existing tables), else tables created by
    an older release cannot be told.
  - The SQL for DBAs cannot run Python: the quarantine keys of 0003 are
    computed by `etl-db upgrade` only; the SQL gives each entry its own.
  - MySQL has no partial index: its quarantine relies on reading the open
    entries first, as before.
  - The version table is Alembic's: a team running its own Alembic on the
    same schema keeps it apart by the prefix.
