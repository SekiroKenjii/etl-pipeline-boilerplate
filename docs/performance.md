# Performance

The numbers of this page are one laptop's, against databases in containers
on that laptop. The thresholds they were first tuned with are now derived
from the budgets of the process, or chosen online, as {doc}`tuning`
explains; `etl calibrate <database>` measures the project's own databases
and prints the values to set.

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

Measured on 2026-11-09 on the laptop above, idle, with the Postgres, SQL
Server, MySQL and MongoDB test containers up. "Before" is the same tree
earlier that day, before the key registry, the key cache and the mapping
engine changed; "after" is this release.

1,000,000 rows, Postgres → Postgres, in rows per second:

| Ranges | Load, before → after | Reload, before → after | Peak RSS, before → after |
| --- | --- | --- | --- |
| 1 | 5,211 → **13,236** | 10,367 → **21,558** | 245 → 542 MB ² |
| 4 | 8,720 → **20,323** | 11,665 → **25,479** | 276 → 554 MB ² |

200,000 rows, after:

| Source → target | Ranges | Load | Reload | Peak RSS |
| --- | --- | --- | --- | --- |
| Postgres → SQL Server | 1 | 2,433 → **4,109** ³ | 3,565 → **8,754** | 259 → 267 MB |
| Postgres → SQL Server | 4 | 4,563 → **7,180** | 5,392 → **12,863** | 278 → 277 MB |
| SQLite → SQLite (`make bench`) | 1 | 15,327 | 23,376 | 237 MB |
| SQLite → SQLite (`make bench`) | 4 | 17,043 | 20,878 | 256 MB |

Measured before this release and not again since ¹: Postgres → SQL Server,
1,000,000 rows, 1,534 rows/s with 1 range and 2,793 with 4 (loads), 2,657
and 4,132 (reloads); SQL Server → Postgres, 1,000,000 rows, 8,085 and 7,576
(loads), 10,587 and 11,993 (reloads); Postgres → Postgres, 5,000,000 rows,
load only, 6,573 rows/s with 1 range (761 s, 188 MB) and 8,703 with 4 (575 s,
244 MB).

¹ Before the key registry, the key cache and the mapping engine changed. The
runs out of Postgres were measured again on 2026-11-09 (above).

² With the `[tuning]` defaults derived on this laptop (no cgroup limit:
`memory_budget` 4,186 MiB from `MemAvailable`, so `key_cache_memory`
1,046 MiB holds every key of the run in memory). With
`tuning.key_cache_memory = "256MiB"`, 1 range gave 12,686 rows/s (load),
21,170 (reload) and a peak of 497 MB; with `"64MiB"`, 11,765 / 17,918 and
318 MB, 800,000 keys served from the disk tier. In a pod the budget follows
the container's limit: a 1 GiB pod derives 614 MiB and a 154 MiB key cache.

³ Before → after the online choice of the staging on SQL Server
(`tuning.bulk_copy_rows = "auto"`, {doc}`tuning`), measured the same day
with the same command; the "before" run of that pair measured 2,149 / 4,044
(loads) and 3,505 / 4,857 (reloads), the laptop busier than for the first
figures.

The laptop was in use for the earlier figures, so they vary: the same
Postgres → Postgres run of 1,000,000 rows, 1 range, once measured 4,188
rows/s and 7,217 the next time, the slow run spending twice the time in the
transform. The runs of 2026-11-09 were taken with nothing else running.

### Where the time goes

Load, in seconds of the steps' threads (with 4 ranges they add up to more
than the wall clock):

| Source → target | Rows | Ranges | Wall | Extract | Transform + gate | Write | Key registry | Other |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Postgres → Postgres | 1,000,000 | 1 | 75 | 5 | 23 | 17 | 25 | 5 |
| Postgres → Postgres | 1,000,000 | 4 | 50 | 18 | 38 | 62 | 39 | 39 |
| Postgres → SQL Server | 200,000 | 1 | 82 → 49 | 1 | 8 → 9 | 57 → 20 | 14 → 16 | 2 → 3 |
| Postgres → SQL Server | 200,000 | 4 | 44 → 28 | 6 → 3 | 15 → 13 | 101 → 48 | 30 → 33 | 17 → 11 |

- **The transform and the gate** cost about 23 µs a row, in Python (54
  before this release): each field is compiled once into a runner that
  knows its sources, transform, default and type cast; dates written as in
  Excel are read by a parser compiled once per format; the natural key's
  text is built directly. One process is capped at about 40,000 rows/s on
  this side, whatever the databases.
- **The key registry** registers a batch in one statement that inserts only
  the missing keys (`INSERT ... SELECT FROM unnest(arrays) ON CONFLICT DO
  NOTHING` on Postgres): 20 ms for 1,000 new keys alone (35 before), 25 s
  for the 1,000,000 of a load (105 before). The Postgres server's own cost
  of such an insert is about 13 ms on this laptop (index maintenance, WAL,
  fsync), so the statement is not where more would come from. A reload
  spends 1.3 s in the registry: the keys it meets again are in the
  process's key cache, which costs 0.2 ms a batch instead of a round trip.
- **The key cache is sized by memory, and spills to disk.**
  `tuning.key_cache_memory` (a quarter of the memory budget by default,
  {doc}`tuning`; 1,046 MiB as derived on this laptop, 154 MiB in a 1 GiB
  pod) bounds what each process keeps in memory, at the measured cost of an entry (about 280 MB
  per million keys here: `sys.getsizeof` of the key's strings and tuples,
  plus the map's slot); the keys past it go to a SQLite file under
  `$TMPDIR`, up to `tuning.key_cache_disk` (`"2GiB"`). A key in memory
  costs 0.2 µs, a key on disk about 3.7 µs (an `IN` lookup per batch), a
  key the registry answers for 20 µs (one round trip per 1,000). The load
  above with the defaults: 12,686 rows/s and 497 MB, 210,000 of the keys
  on disk (13,257 rows/s and 540 MB with the 1,000,000-entry cache it
  replaces); the reload 21,170 rows/s, 2.1 s in the registry step (1.4 s).
  With `key_cache_memory = "64MiB"`: the load 11,765 rows/s and 318 MB
  (a 100,000-entry cache gave 11,495 and 264 MB, its orphan checks going to
  Postgres), the reload 17,918 rows/s with 4.4 s in the registry step:
  800,000 keys read from disk. Lower the memory on small pods: the run gets
  slower, never wrong; the disk file goes with the process.
- **SQL Server as the target** was the slowest: about 0.29 ms a row to
  write with 1 range, a reload with nothing to write taking as long, so
  this was the staging of the batch, not the writing: rows went in as
  parameters, since the writer bulk copied only from 2,000 rows a batch and
  the CRM's batches hold 1,000. The writer now measures both ways on the
  first three batches of each table and keeps the faster
  (`tuning.bulk_copy_rows = "auto"`, {doc}`tuning`): on this laptop bulk
  copy stages 1,000 rows in about 10 ms against 120 ms as parameters, and
  the write of 200,000 rows fell from 57 to 20 thread-seconds (82 to 49 s
  of wall clock, 2,149 to 4,109 rows/s measured the same afternoon). The
  key registry goes through the same choice. Registering 1,000 keys takes
  about 80 ms on SQL Server during a load. `etl calibrate crm` prints the
  two timings for the project's server.
- **4 ranges into SQL Server deadlocked before this release.** SQL Server
  scanned the whole target for each batch's `UPDATE` and `INSERT ... WHERE
  NOT EXISTS`, a plan it made while the table was small. The scans locked the
  rows and pages of the other ranges' batches, and they deadlocked: 3 of the
  4 ranges failed after their 3 attempts. The merge now seeks each key (a
  nested loop from the staging table, with row locks), which also holds for
  ranges run by separate processes.
- **The seek needs an index led by the key.** Forced on a target without
  one (a heap, a staging table), the nested loop would scan the whole table
  for each row of the batch: 1,000 scans a batch instead of one. The writer
  reads the target's indexes once per run, and merges a target without such
  an index without the hints, one scan a batch, with a warning (`has no
  index on`); writes at once into it may then deadlock again. With the
  primary key, the plan of the `UPDATE` reads the target by `Clustered
  Index Seek` only (`tests/integrations/databases/test_join_hints.py` reads
  the cached plans).
- **The indexes are read once per table and key, per process.** A table
  named with its schema and without (`dbo.customer`, `customer`) is one
  read and one warning. An index created while a run writes is seen by the
  next run (the next process): restart a long-running job after adding one.
- **An index on part of the key does not count, unless unique.** Forced on
  a non-unique index on a prefix of the key, the nested loop seeks by the
  prefix, then filters: on SQL Server 16 (20,000 rows, a batch of 200 keys
  `(tenant, customer_key)`), an index on `customer_key` gave an `Index Seek`
  and 890 logical reads; one on `tenant` (two values) a scan per row, 65,351
  reads; without the hints, either one scan, 337 to 341. The catalog does
  not tell how selective a prefix is, so the writer leaves such a table
  without the hints (`has no index on`).
- **MySQL was exposed the same way, through gap locks.** Under InnoDB's
  REPEATABLE READ, the `NOT EXISTS` read of the target locked the gaps
  where the other ranges insert. MySQL transactions now run in READ
  COMMITTED, set on the engine. An earlier fix set it per session, and
  SQLAlchemy reset it to REPEATABLE READ when a connection that had served
  an autocommit read (state, key registry) went back to the pool: the
  merges deadlocked again from the second batch. `test_concurrent_writes`
  now reads the key registry before each batch, and passes 3 times in a row
  on MySQL 8.4 (it deadlocked every time before). Not benchmarked.
- **Finding deletes without a change log** (`MissingSourceRows`) compares
  the source's keys and the registry's as sorted streams spilled to disk
  instead of a set of every key: 10,000,000 keys peak at 66 MB (1.5 GB
  before), in 53 s instead of 19 (`docs/algorithms.md`).

## What the numbers support

- On one laptop, with narrow tables, one process loads 13,000 rows/s into
  Postgres with 1 range and 20,000 with 4 ranges (threads), and reloads
  (compares) 21,500 and 25,000. The target of 10,000 rows/s a process and
  20,000 in total is met here with one process; it is to be met on the
  project's hardware with 3 processes (`--step`) when one does not.
- It loads 2,400 rows/s into SQL Server with 1 range and 4,600 with 4
  (200,000 rows).
- The memory is bounded by the batch, the ranges and the key cache, not by
  the table: 262 MB for 1,000,000 rows with a cache of 100,000 keys, 540 MB
  with the default cache of 1,000,000 (about 280 MB per million entries),
  11 to 50 MB more with 4 ranges.
- More ranges help a load less than threads suggest: they share one GIL. To
  go faster, run more processes: scheduler jobs, each running some ranges
  (`--step`).

## What they do not support

- 20,000 rows/s a process into SQL Server, or any figure for Oracle, MySQL
  and MongoDB as targets: not benchmarked.
- Any cutover window. On this laptop, 50 million rows at 13,000 rows/s would
  take about an hour into Postgres with one process, and about 3 hours into
  SQL Server with 4 ranges. That arithmetic says nothing of a customer's
  network, remote databases, their load or their tuning.
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
  about 1.3 GB, in 128 files, to `$TMPDIR`. The second pass now sorts through
  `shared.utils.ExternalSorter` (runs merged by tiers, chunked pickles):
  measured again on 200,000 rows on 2026-11-09, 7.0 / 11.9 / 14.9 s for a
  drift of 0 / 0.1% / 1% against 7.1 / 11.7 / 15.5 before, same peak.
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
planning a cutover window, and `etl calibrate <database>` for the
thresholds of {doc}`tuning`.
