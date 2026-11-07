# Cutover runbook

A migration rehearses its cutover several times before the real one. Each
rehearsal ends with a sealed report. The real cutover is the last
rehearsal, with the legacy system frozen.

## What the report decides

`etl cutover report <module>` maps every source row through the module's
mapping, without loading anything, and compares the rows it should give
with the rows of the target, one by one and across databases. Each entity
gets a status, and the report gets a verdict:

| Verdict | When | Exit code |
| --- | --- | --- |
| `PASSED` | Every expected row is in the target, equal; nothing extra; every missing row is a parked orphan with that key | 0 |
| `PASSED-WITH-REJECTS` | The same, but some source rows are rejected by the mapping, and every one of them is accepted (`--accept-rejections`) | 3 |
| `FAILED` | Something is missing, extra, different, duplicated or rejected that nothing explains | 1 |
| `COUNTED` | `--counts-only`: rows counted, none compared. Not a verdict: never sign it off | 0 |

Two rules make the verdict one an auditor can rely on:

- **Parked orphans explain missing rows by key, not by count.** A missing
  row is explained only when the parking lot holds an orphan with the same
  target key. Ten stale entries in the parking lot explain nothing about
  another missing row. The report shows both numbers: the entries waiting
  (`parked`) and the missing rows they explain (`parked_missing`).
- **A rejected row is a difference until someone accepts it.** It is never
  loaded, so the target lacks it. The BAs decide which rejected rows are
  left behind, in a CSV they review and keep with the sign-off. Its columns
  are those of the report's *Rejected* sheet:

  ```text
  entity,reason,rows
  customer,created_at: '31/02/2020' does not match the format dd/MM/yyyy,1
  customer,country: no translation for 'Narnia',1
  order,*,2
  ```

  Each line accepts up to `rows` rows rejected for that reason; `*` accepts
  up to `rows` rows for any reason. A row rejected for several reasons is
  accepted only when each reason is, or by a `*`. One more rejected row
  than accepted, or a new reason, and the report fails again. The report
  quotes the file's name, its SHA-256 and what it accepts.

The report also says what produced it (`provenance`): the environment, the
version and build of the code, the hash of the settings, the module's
fingerprint (the hashes of its mappings, its corrections and how its
surrogate keys are made), the source and target databases (dialect, host,
port and database; never a user or a password), the options, the operator
(`$ETL_OPERATOR`, else the user), the rejected rows accepted, and the
canonical form. `etl cutover compare` lists what of it changed between two
rehearsals.

### What counts as equal

Both sides are compared in a canonical form, so that two databases
reconcile: `Decimal("120.50")` and `120.5`, a `UUID` and its text, Oracle's
midnight `DATE` and a date, an aware timestamp and the naive UTC one are
equal. A number and its text never are: `Decimal("1.5")` and `"1.5"`, `NaN`
and `"NaN"`, are `different`, since a column loaded as text where a number
was expected is a defect.

The form has a number (`canonical_form`, 2 since this release), kept in the
provenance and shown by `verify` and `compare`. A new form can count as
`different` rows that an older one matched: after an upgrade, run the
sign-off rehearsal again and save it, rather than compare with a report of
the old form. Reports saved before still verify.

### Timestamps on a coarser side

Timestamps compare to the microsecond. A side that stores them coarser
differs on every fraction it rounds: SQL Server `datetime` keeps 1/300 s,
MySQL `DATETIME` and Oracle `DATE` whole seconds. When one side is such a
column, name its resolution and both sides round to its nearest tick:

```sh
etl cutover report crm --time-resolution 300   # SQL Server datetime
etl cutover report crm --time-resolution 1     # whole seconds
```

The report records the resolution. Only ever the coarser side's: a coarser
one hides real differences.

### Large tables

The diff reads each side once and sums the digests of its rows per range of
the target's key, about 100 target rows each. Only the ranges that differ
are compared row by row: the target reads them again, in the database, one
range at a time; the source is read and mapped again in full, as the target
keys of its rows are only known once mapped, and keeps the rows of those
ranges, sorted on disk past 50,000 rows (in `$TMPDIR`: give it room, and
keep it private, as the rows hold the data compared). Memory stays bounded
whatever the table and the drift.

Where the target's order of its keys is not the order of their text (a SQL
Server `uniqueidentifier`), the diff splits the keys by hash instead, and
reads the target again in full when something differs. `report.partitioning`
says which. `--counts-only` is the fallback when even that is too long:
it counts in the databases, and decides nothing.

## Each rehearsal

1. **Load and catch up.**

   ```sh
   etl run crm.full_load --partial-ok      # or crm.sync, by the change log
   etl run crm.parked --partial-ok         # orphans whose parents arrived
   etl run crm.deletes                     # rows gone from the source
   ```

   `crm.deletes` refuses to delete when the source has no row at all, or
   when more than `max_delete_share` of an entity's loaded rows are missing
   (half, by default): the step fails and deletes nothing, as a source
   pointed at the wrong schema would look. Its message gives the number;
   once it is checked, confirm exactly that number for one run:

   ```sh
   etl run crm.deletes --param confirm_deletes=crm.customer:1200
   ```

2. **Deal with the rejected rows.** Fix the cause or correct the rows, then
   `etl run crm.replay`. The rows left are those the BAs decide not to
   migrate: they write them in the acceptance CSV. See {doc}`mappings`.
3. **Reconcile, and keep the report.**

   ```sh
   etl cutover report crm --save --accept-rejections var/cutover/crm-accepted.csv \
       --format html -o var/cutover/crm.html
   ```

   `--save` writes the report, sealed with its SHA-256, under
   `var/cutover/crm/` (`--save DIR` elsewhere), and records that digest in
   the run ledger, as a run of `etl.cutover` (`etl runs etl.cutover`), with
   the operator. It needs a run ledger. Set `ETL_OPERATOR` to the person or
   the change ticket when the user of the process says nothing (a Job, a
   CI step).
4. **Compare with the last rehearsal.**

   ```sh
   etl cutover compare var/cutover/crm/<before>.json var/cutover/crm/<after>.json
   ```

   Per entity, what got better or worse; and what produced the two reports
   otherwise (other mappings, another database, other accepted rejections).
5. **Sign off.** The BAs sign the report and quote its SHA-256.
   `etl cutover verify <report>` checks later that it is the report saved:
   `OK` when its content has its digest and the run ledger recorded that
   digest when it was saved, with who saved it and what produced it
   (environment, build, mappings, keys, canonical form); `CHANGED` when the
   content was edited;
   `UNRECORDED` when it was edited and sealed again, which anyone can do,
   since the ledger never saw that digest. `verify` reads the run ledger:
   run it with the settings of the environment that saved the report.

## The cutover

1. Freeze writes to the legacy system.
2. Catch up: `etl run crm.sync` (or `crm.full_load`, then `crm.deletes`),
   until a run reads nothing new.
3. `etl cutover report crm --save --accept-rejections <signed CSV>`: it must
   pass, `PASSED` or `PASSED-WITH-REJECTS`. Compare it with the last signed
   rehearsal, and sign it.
4. Switch the applications to the new system.
5. Keep the reports, the acceptance CSV, the ledger (`etl runs`) and the
   quarantine as the record of the migration. Then purge the personal data
   past its retention (`etl housekeeping`).

## Go and no-go

| Check | Go when |
| --- | --- |
| `etl cutover report` | Exit 0 (`PASSED`) or 3 (`PASSED-WITH-REJECTS`), never from `--counts-only` |
| Rejected rows | Each one fixed, corrected, or accepted in the signed CSV |
| Parked orphans | None left, or each one accepted |
| Provenance | The environment, databases and mappings are those of the cutover |
| Last runs | `etl runs`: no failed run since the freeze |
| Rehearsals | `etl cutover compare`: no entity worse than the signed one |
| Sign-off | `etl cutover verify`: `OK` for the report signed |
