# Performance

**Pipeline measured = `crm.full_load`**, the product's: `benchmarks/bench_load.py`
(`make bench`) runs it through the kernel, as `etl run crm.full_load` does.
That includes the mappings of `mappings/compiled/crm.json`, the quality gate
(mapping issues, references), the key registry, the quarantine, the state
and the run ledger. The ETL's own tables are in the target database.

## Setup

- **Settings:** the CRM defaults: batches of 1,000 rows, pages of 1,000 rows,
  rejects into the quarantine, `max_reject_rate` 5%. `partitions` is 1 or 4;
  4 passes the pool check at start-up with the default pools (15
  connections; 4 ranges need 4 on the source and 8 on the target).
- **Data:** the demo's legacy tables, half `CUST_MASTER` (8 narrow text
  columns) and half `ORD_MASTER` (5). 1% of the rows are dirty: a customer
  with a date that does not exist is rejected, and its order with it, as an
  orphan. `UPDATED_AT` is not correlated with the key.
- **Indexes:** the primary key, and `(UPDATED_AT, key)` on each source table.
  The latter is the order one range reads in. The first run of several ranges
  reads each one by key.
- **Target:** the demo's CRM tables (UUID keys), empty for a load. A reload
  runs again after the state is reset: everything is read and compared, and
  nothing is written.
- **Machine:** a single laptop, AMD Ryzen 5 5600H (6 cores, 12 threads),
  14.5 GB of RAM, also running an IDE. Python 3.14.4 with the GIL.
- **Databases:** Postgres 18.6 and SQL Server 2022 (16.0.4305) in podman
  containers on the same laptop, over loopback, with no tuning. The source and
  the target are two databases of one instance.
- **Measures:** each run is a process of its own, so its peak RSS is its own.
  Where the time goes is in seconds of the steps' threads. These come from the
  engine's step and batch spans, plus timers on the source reader, the target
  loader and the key registry. With 4 ranges, they add up to more than the
  wall clock.

## Results

1,000,000 rows, in rows per second:

| Source → target | Ranges | Load | Reload | Peak RSS |
| --- | --- | --- | --- | --- |
| Postgres → Postgres | 1 | 7,076 | 11,066 | 190 MB |
| Postgres → Postgres | 4 | 8,688 | 12,925 | 230 MB |
| Postgres → SQL Server | 1 | 1,534 | 2,657 | 198 MB |
| Postgres → SQL Server | 4 | 2,793 | 4,132 | 229 MB |
| SQL Server → Postgres ¹ | 1 | 8,085 | 10,587 | 198 MB |
| SQL Server → Postgres ¹ | 4 | 7,576 | 11,993 | 237 MB |

5,000,000 rows, Postgres → Postgres, load only ¹:

| Ranges | Seconds | Rows/s | Peak RSS |
| --- | --- | --- | --- |
| 1 | 761 | 6,573 | 188 MB |
| 4 | 575 | 8,703 | 244 MB |

¹ Measured before the two fixes below (key registry, SQL Server merge),
which help 4 ranges into Postgres: there, 7,516 rows/s before against 8,688
after, on 1,000,000 rows.

The laptop was in use, so runs vary. The same Postgres → Postgres run of
1,000,000 rows, with 1 range, measured 4,188 rows/s once and 7,217 rows/s the
next time. The slow run spent twice the time in the transform, which is CPU
only.

### Where the time goes

1,000,000 rows, load, in seconds of the steps' threads:

| Source → target | Ranges | Wall | Extract | Transform + gate | Write | Key registry | Other |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Postgres → Postgres | 1 | 141 | 6 | 53 | 17 | 60 | 5 |
| Postgres → Postgres | 4 | 115 | 22 | 91 | 73 | 215 | 53 |
| Postgres → SQL Server | 1 | 652 | 6 | 64 | 283 | 288 | 12 |
| Postgres → SQL Server | 4 | 358 | 51 | 258 | 534 | 475 | 103 |

- **The transform and the gate** cost about 54 µs a row, in Python: the
  mapping of each field, the keys, the checks. That caps one process at about
  18,000 rows/s, whatever the databases.
- **The key registry** costs as much as the transform. With 4 ranges, each
  thread waits for the GIL between its round trips. It used to insert the
  keys one statement a row: registering 1,000 keys took 59 ms alone, and
  4.4 s with one thread busy in Python next to it. It now inserts them in
  multi-row statements: 35 ms alone, 119 ms next to a busy thread.
- **SQL Server as the target** is the slowest: about 0.28 ms a row both to
  write and to register keys, with 1 range. A reload with nothing to write
  takes as long in the writer, so this is the staging of the batch, not the
  writing: rows go in as parameters. Bulk copy took 10 ms for 1,000 rows,
  measured apart, but the writer uses it only from 2,000 rows a batch, and
  the CRM's batches hold 1,000.
- **4 ranges into SQL Server deadlocked before this release.** SQL Server
  scanned the whole target for each batch's `UPDATE` and `INSERT ... WHERE
  NOT EXISTS`, a plan it made while the table was small. The scans locked the
  rows and pages of the other ranges' batches, and they deadlocked: 3 of the
  4 ranges failed after their 3 attempts. The merge now seeks each key (a
  nested loop from the staging table, with row locks), which also holds for
  ranges run by separate processes.
- **MySQL was exposed the same way, through gap locks.** Under InnoDB's
  REPEATABLE READ, the `NOT EXISTS` read of the target locked the gaps
  where the other ranges insert. MySQL sessions now run in READ COMMITTED.
  Not benchmarked.

## What the numbers support

- On one laptop, with narrow tables, the full product path loads 6,500 to
  8,700 rows/s into Postgres, and reloads (compares) 10,500 to 13,000.
- It loads 1,500 rows/s into SQL Server with 1 range, and 2,800 with 4.
- The memory is bounded. The peak was 188 MB with 1 range for 1 and for
  5 million rows. It grows with the ranges (40 to 60 MB more for 4), not with
  the table.
- More ranges help a reload more than a load: the threads share one GIL.
  To go faster, run more processes: scheduler jobs, each running some ranges
  (`--step`).

## What they do not support

- The target of 20,000 rows/s. None of these runs reached it.
- Any cutover window. On this laptop, 50 million rows at 8,700 rows/s would
  take about 1.6 hours into Postgres, and about 5 hours into SQL Server with
  4 ranges. That arithmetic says nothing of a customer's network, remote
  databases, their load or their tuning.
- Wide tables. A row of 60 columns costs more to transform, stage and
  compare, and more memory per batch.
- Earlier figures of this page (25,000 to 70,000 rows/s). Those measured a
  passthrough of the operators, without the gate, the key registry or the
  quarantine, in batches of 10,000, from a source with no index for its read
  order.

## Cutover diff

`benchmarks/bench_diff.py` times `cutover.reconcile` on 5,000,000 rows, SQLite
files on a disk (same laptop). The target has a UUID key, as text. With a
drift of 1%, a third of the drifted rows are changed, a third deleted and a
third added: 50,000 differences, spread over every partition of the keys.

| Partitions | Drift | Seconds | Rows read, source / target | Peak RSS |
| --- | --- | --- | --- | --- |
| range | 0 | 184 | 5.0M / 5.0M | 86 MB |
| range | 1% | 408 | 10.0M / 8.2M | 127 MB |
| hash (`--hash`) | 0 | 189 | 5.0M / 5.0M | 81 MB |
| hash (`--hash`) | 1% | 459 | 10.0M / 10.0M | 118 MB |

- **Memory is bounded.** The process held 81 MB before the diff, and at most
  127 MB during it. In the hash run with a 1% drift, the second pass spilled
  about 1.3 GB, in 128 files, to `$TMPDIR`.
- **A drift spread everywhere costs a second pass.** Every partition then
  differs, so the source is read twice. Ranges read less of the target again
  (8.2M rows instead of 10M) and were about 10% faster.
- **No drift** costs one read of each side: about 27,000 rows/s from SQLite.

## Running it

Give the benchmark databases of its own. It drops and creates the CRM tables
and the ETL's tables (`etl_*`) of the target, and the legacy tables of the
source:

```sh
make bench                                   # SQLite, 200,000 rows
make bench ROWS=1000000 PARTITIONS=1,4 \
    SOURCE=postgresql+psycopg://etl:etl@localhost:55432/bench_legacy \
    TARGET=postgresql+psycopg://etl:etl@localhost:55432/bench_crm
uv run python benchmarks/bench_load.py --rows 5000000 --partitions 4 \
    --no-reload --keep --source ... --target ... --output results.json
TMPDIR=var/tmp uv run python benchmarks/bench_diff.py --rows 5000000
```

`--keep` keeps the seeded source for the next run. `--output` writes every
measure as JSON, with the setup: CPU, memory, Python and server versions. Put
`TMPDIR` on a disk, not a tmpfs: the diff spills its rows there.

Rerun the benchmark on the project's own hardware, network and tables before
planning a cutover window.
