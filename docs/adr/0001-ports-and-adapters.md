# 1. Ports and adapters; the core uses the standard library only

**Status:** accepted.

## Context

Migrations meet whatever the legacy system runs on: SQL Server, Oracle,
MongoDB, files, APIs. They also meet whatever the target is. Tying the ETL
logic to one database, one driver or one framework repeats the work in
every project.

## Decision

- **The core.** `core` defines ports (`core.contract`) and holds the logic:
  jobs, quality, keys, mappings, change data capture, cutover. It imports
  the standard library only. `core.contract` imports no other part of the
  core, so an adapter implements a port without knowing the engine.
- **Adapters.** They live in `infra` and implement the ports. Their
  libraries are dependencies of the package, imported at the top of the
  modules that use them; only the SQL drivers that SQLAlchemy loads by URL
  (`mssql`, `oracle`, `mysql`) are extras.
- **The composition root.** `app` picks the adapters from the settings.
- **Modules.** They ask for ports by name, and never import `infra`.
- **Entry points.** `cli` goes through `app` for anything built from the
  settings. The offline tools drive one adapter themselves:
  `infra.database` (`etl-db` DDL and discovery from `--url`, connection
  strings), `infra.mapping` (`etl-mappings` workbooks) and `infra.reports`
  (xlsx cutover reports). They need no settings, so `app` would only pass
  the calls through; any other part of `infra` stays behind `app`.
- **The rule is enforced.** `tests/units/test_architecture.py` checks every
  import, `from core import mapping` as `core.mapping`. `core` and `shared`
  are held to an allow-list (the standard library, `core`, `shared`), not a
  list of forbidden libraries, and `config` to it plus pydantic.

## Consequences

- **What it buys.** A new source database is a new adapter and its
  conformance tests, not a change of pipelines. The core is tested in
  memory, quickly.
- **What it costs.** Ports must stay small enough for every adapter, so
  database-specific power goes behind a port, or into the adapter.
