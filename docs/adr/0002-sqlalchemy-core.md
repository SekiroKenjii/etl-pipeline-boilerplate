# 2. SQLAlchemy Core for relational databases; MongoDB on its own

**Status:** accepted.

## Context

Relational sources and targets differ in SQL dialect, in how they bulk
load, and in how they log changes. Writing a driver layer per database costs
much; an ORM hides what an ETL must control (batches, transactions,
staging).

## Decision

- **Relational databases.** They go through SQLAlchemy Core: reflected
  tables, keyset reads, and staging tables merged with set-based SQL.
- **Dialects.** Where databases differ, a `Dialect` object covers it (the
  base class in `infra.database.relational.base_dialect`, one subclass per
  database in `dialects`): bulk copy, `MERGE`, `COPY`, temporary tables,
  inserting the rows a table lacks (`insert_missing`), the error codes that
  are transient, the isolation level, the hints a merge takes.
- **MongoDB.** It has adapters of its own, behind the same ports.

## Consequences

- **What it buys.** One adapter serves SQL Server, Oracle, PostgreSQL,
  MySQL and SQLite. The same conformance suite runs on all of them.
- **What it costs.** A feature one database lacks has to be written in its
  dialect, or left out.
