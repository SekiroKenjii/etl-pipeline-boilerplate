# Command line

## Exit codes

Every command line, `etl`, `etl-mappings` and `etl-db`, exits with the codes
of one table (`cli.exit_codes`), so that a scheduler, a CI job or a script
reads them the same way.

| Code | Name | When | Retry? |
| --- | --- | --- | --- |
| 0 | `SUCCESS` | Done. `etl run --partial-ok` also exits 0 for a partial success. | |
| 1 | `FAILED` | A run failed: a source down, a lock lost, a step's error, records failed. Any command whose bookkeeping database or resource is down (one line, `FAILED <reason>`, or the JSON summary of `etl run --format json`). Also a cutover report whose verdict is `FAILED`, an `etl cutover verify` or `compare` that finds a report changed or unrecorded, and an `etl cutover record` that refuses one (then it records none), which a retry does not change. | Yes, for a run (`--resume`) |
| 2 | `INVALID` | Bad arguments, settings, workbook or file; a pipeline that cannot be built; keys changed without `--rekey`; a question answered no (`etl state reset`). Nothing ran; or a step refused until the run confirms it (`--param confirm_deletes=...`). | No: fix it |
| 3 | `PARTIAL_SUCCESS` | Done, some rows rejected (`etl run`); `PASSED-WITH-REJECTS` (`etl cutover report`). | No |
| 4 | `LOCKED` | Another run holds some of the steps: nothing ran. A run of a `--label` held by another run of that label exits 4 after waiting up to `runs.lease_seconds`. | Later |
| 5 | `NOT_READY` | `etl check`: a resource cannot be reached. `etl-db check`: the ETL tables need `etl-db upgrade`. | Later; after the upgrade |
| 6 | `CHANGED` | `etl-mappings diff` finds changes; `etl-mappings check` finds a compiled file missing, stale or without workbook. | No |
| 7 | `NOT_FOUND` | Nothing matched: `etl keys lookup`, `etl keys forget`, `etl parked release` (some identities), `etl state reset` (nothing saved), `etl runs --run`. | No |
| 130 | `CANCELLED` | Stopped by SIGINT or SIGTERM; a run goes on from its last batch. `etl cutover` exits 130 too, once it has removed the source rows it spilled to disk. | Yes |

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

A run that failed or was cancelled tells why: its `message` (the JSON
summary, `etl runs`, and a second line of the text summary) is the first
step that did, `<step>: failed after <n> attempt(s): <error>`, the error's
own message redacted like the ledger's. A refusal (exit 2) thus carries its
hint, e.g. `--param confirm_deletes=...`, and is logged without a
traceback.

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

A line per resource: `ok <name>`, or `FAIL <name>: <why>` (exit 5). A
resource that works but not as the ETL expects is still `ok`, followed by
`WARN <name>: <what>` lines (a Postgres session whose messages are not in
English: `WARN database.<name>: lc_messages is ...`); warnings alone leave
the exit code 0.

```text
tuning ... (* derived; etl config tuning)
ok   database.crm
WARN database.crm: lc_messages is 'de_DE.utf8': ...
FAIL database.legacy: RetryableError: checking the database: ...
```

### etl calibrate

Measures a database on a scratch table and prints a `[tuning]` snippet
({doc}`../tuning`); writes nothing else.

```{program-output} etl calibrate --help
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

### etl cutover record

```{program-output} etl cutover record --help
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

### etl config tuning

```{program-output} etl config tuning --help
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
