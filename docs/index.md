# ETL pipeline boilerplate

A boilerplate for data migrations and ETL: legacy databases (SQL Server,
Oracle, MySQL, PostgreSQL, MongoDB) moved into new systems.

The parts of a migration are already wired together, ready to run:

- mappings that business analysts write in Excel;
- quality gates;
- change data capture;
- a reconciliation for the cutover;
- operations commands;
- observability.

```{toctree}
:maxdepth: 2
:caption: Using it

getting-started
architecture
mappings
operations
cutover-runbook
```

```{toctree}
:maxdepth: 2
:caption: Running it

deployment
performance
security
```

```{toctree}
:maxdepth: 1
:caption: Reference

reference/cli
reference/api
adr/index
```
