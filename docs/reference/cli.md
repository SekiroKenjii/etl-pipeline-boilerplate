# Command line

## Exit codes

Every command line, `etl`, `etl-mappings` and `etl-db`, exits with the codes
of one table (`cli.exit_codes`), so that a scheduler, a CI job or a script
reads them the same way.

| Code | Name | When | Retry? |
| --- | --- | --- | --- |
| 0 | `SUCCESS` | Done. `etl run --partial-ok` also exits 0 for a partial success. | |
| 1 | `FAILED` | A run failed: a source down, a lock lost, a step's error. Also a cutover report whose verdict is `FAILED`, and an `etl cutover verify` or `compare` that finds a report changed, which a retry does not change. | Yes, for a run (`--resume`) |
| 2 | `INVALID` | Bad arguments, settings, workbook or file; a pipeline that cannot be built; keys changed without `--rekey`; a question answered no (`etl state reset`). Nothing ran. | No: fix it |
| 3 | `PARTIAL_SUCCESS` | Done, some rows rejected (`etl run`); `PASSED-WITH-REJECTS` (`etl cutover report`). | No |
| 4 | `LOCKED` | Another run holds some of the steps: nothing ran. | Later |
| 5 | `NOT_READY` | `etl check`: a resource cannot be reached. `etl-db check`: the ETL tables need `etl-db upgrade`. | Later; after the upgrade |
| 6 | `CHANGED` | `etl-mappings diff` finds changes; `etl-mappings check` finds a compiled file missing, stale or without workbook. | No |
| 7 | `NOT_FOUND` | Nothing matched: `etl keys lookup`, `etl keys forget`, `etl parked release` (some identities), `etl state reset` (nothing saved), `etl runs --run`. | No |
| 130 | `CANCELLED` | Stopped by SIGINT or SIGTERM; a run goes on from its last batch. | Yes |

An invalid workbook in `etl-mappings validate`, `compile` or `check` is 2;
a database `etl-db` cannot reach is 1. `argparse` itself exits 2 on a
usage error.

The rest of this page is generated from the commands' `--help` when the
documentation is built, so it cannot drift from the code.

## etl

Every `etl` command takes `--env` and `--config` before the command:
`etl --env prod config show`, not `etl config show --env prod`.
`--format json` (`run`, `runs`) prints one JSON document for scripts and
tests; with `run` it is the last line of the output, after the logs.

```{program-output} etl --help
:cwd: ../..
```

### etl run

```{program-output} etl run --help
:cwd: ../..
```

### etl list

```{program-output} etl list --help
:cwd: ../..
```

### etl plan

```{program-output} etl plan --help
:cwd: ../..
```

### etl check

```{program-output} etl check --help
:cwd: ../..
```

### etl runs

```{program-output} etl runs --help
:cwd: ../..
```

### etl cutover report

```{program-output} etl cutover report --help
:cwd: ../..
```

### etl cutover compare

```{program-output} etl cutover compare --help
:cwd: ../..
```

### etl cutover verify

```{program-output} etl cutover verify --help
:cwd: ../..
```

### etl quarantine list

```{program-output} etl quarantine list --help
:cwd: ../..
```

### etl quarantine export

```{program-output} etl quarantine export --help
:cwd: ../..
```

### etl quarantine close

```{program-output} etl quarantine close --help
:cwd: ../..
```

### etl quarantine purge

```{program-output} etl quarantine purge --help
:cwd: ../..
```

### etl parked list

```{program-output} etl parked list --help
:cwd: ../..
```

### etl parked release

```{program-output} etl parked release --help
:cwd: ../..
```

### etl state show

```{program-output} etl state show --help
:cwd: ../..
```

### etl state reset

```{program-output} etl state reset --help
:cwd: ../..
```

### etl keys lookup

```{program-output} etl keys lookup --help
:cwd: ../..
```

### etl keys forget

```{program-output} etl keys forget --help
:cwd: ../..
```

### etl housekeeping

```{program-output} etl housekeeping --help
:cwd: ../..
```

### etl new-module

```{program-output} etl new-module --help
:cwd: ../..
```

### etl config show

```{program-output} etl config show --help
:cwd: ../..
```

## etl-mappings

`--registry` comes before the command:
`etl-mappings --registry modules.crm.transforms:registry check`.

```{program-output} etl-mappings --help
:cwd: ../..
```

### etl-mappings template

```{program-output} etl-mappings template --help
:cwd: ../..
```

### etl-mappings validate

```{program-output} etl-mappings validate --help
:cwd: ../..
```

### etl-mappings compile

```{program-output} etl-mappings compile --help
:cwd: ../..
```

### etl-mappings check

```{program-output} etl-mappings check --help
:cwd: ../..
```

### etl-mappings diff

```{program-output} etl-mappings diff --help
:cwd: ../..
```

## etl-db

The commands that connect take `--url`, else `$ETL_DATABASE_URL`: keep
passwords out of command lines. `init` is the same as `upgrade`.

```{program-output} etl-db --help
:cwd: ../..
```

### etl-db upgrade

```{program-output} etl-db upgrade --help
:cwd: ../..
```

### etl-db current

```{program-output} etl-db current --help
:cwd: ../..
```

### etl-db check

```{program-output} etl-db check --help
:cwd: ../..
```

### etl-db ddl

```{program-output} etl-db ddl --help
:cwd: ../..
```

### etl-db url

```{program-output} etl-db url --help
:cwd: ../..
```

### etl-db discover

```{program-output} etl-db discover --help
:cwd: ../..
```
