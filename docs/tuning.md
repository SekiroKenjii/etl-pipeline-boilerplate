# Tuning

The thresholds of the ETL (how many rows a batch holds, when a sort spills
to disk, how many steps run at once, when SQL Server gets a bulk copy) are
chosen by policy, not by the laptop they were first measured on. Every
number is of one of three classes:

- **(a) Capability bounds.** What a database, a driver or a format allows:
  parameters per statement, identifier lengths, format versions. Derived
  from the dialect; not configurable; listed below so that nobody tunes
  them.
- **(b) Resource-bound thresholds.** Cache budgets, spill rows, parallel
  steps, pools, queues: their defaults are **derived from a budget read at
  start-up**, and each one can be set under `[tuning]`.
- **(c) Database- and table-dependent trade-offs.** Bulk copy against
  parameters on SQL Server, the values per IN list: **chosen online** in a
  run, per target table, from what the run measures; and offline by `etl
  calibrate`, which prints the values to set.

`etl config tuning` prints every `[tuning]` value as a process would run
with it, each with its source; `etl check` prints them on its first line.

## The budgets

At start-up the process reads what bounds it:

| Budget | Read from, in order | Then |
| --- | --- | --- |
| Memory | cgroup v2 `/sys/fs/cgroup/memory.max` (`max` means no limit); cgroup v1 `memory.limit_in_bytes` (its 2⁶³ value means none); the host's `MemAvailable` in `/proc/meminfo` | `memory_budget` = 60% of it, at least 256 MiB |
| CPUs | cgroup v2 `cpu.max` (`quota period`); cgroup v1 `cpu.cfs_quota_us` / `cpu.cfs_period_us`; the CPUs the process may run on (`sched_getaffinity`) | `cpu_budget` = the quota rounded down, at least 1 |

In a pod, the limit is the container's (`resources.limits.memory`,
`resources.limits.cpu`): a run planned for a 1 GiB pod gets 614 MiB to
plan with. On a host without a limit, `MemAvailable` moves from one run to
the next, and so do the derived values: set `memory_budget` where the
numbers must not move. When nothing is found, the floors apply (256 MiB, 1
CPU). The detection never raises: a missing or unreadable file means "not
found" (`shared.utils.limits`).

## The `[tuning]` section

```toml
[tuning]
memory_budget = "auto"       # 60% of the memory limit found, at least 256 MiB
cpu_budget = "auto"          # the CPU quota found, at least 1
key_cache_memory = "auto"    # 25% of memory_budget
key_cache_disk = "auto"      # the lesser of 2 GiB and half the free space of $TMPDIR; 0 for none
spill_rows = "auto"          # 20% of memory_budget at 200 bytes a row, 10,000 to 1,000,000
max_parallel_steps = "auto"  # cpu_budget
queue_size = 10000           # log records buffered
bulk_copy_rows = "auto"      # SQL Server: chosen online per table; or a row count
in_chunk = "auto"            # the database's parameter limit, at most 1,000 (SQL Server 500); or a count
```

A value set wins over the derived one and is shown as `set`; byte counts
take a unit (`"256MiB"`, `"2GiB"`; `shared.utils.parse_byte_size`). A
module may set `spill_rows` and `in_chunk` for itself, under
`[modules.<name>.settings]`. The environment overrides as usual
(`ETL__TUNING__KEY_CACHE_MEMORY=64MiB`).

```console
$ etl config tuning
[tuning]
memory_budget = "614MiB"        # 60% of memory.max 1.0GiB
cpu_budget = 2                  # cpu.max 2
key_cache_memory = "153MiB"     # 25% of memory_budget
key_cache_disk = "2GiB"         # the lesser of 2.0GiB and 50% of the 37GiB free under /tmp
spill_rows = 643_825            # 20% of memory_budget at 200 bytes a row
max_parallel_steps = 2          # cpu_budget
queue_size = 10000              # default
bulk_copy_rows = "auto"         # chosen online per table; etl calibrate measures it
in_chunk = "auto"               # the database's parameter limit, at most 1,000 (SQL Server 500)
```

## The thresholds

Every numeric module constant and numeric setting default of `src`
(`tests/units/test_tuning_inventory.py` keeps the list, and fails when one
is added without a row here). "Override" names the `[tuning]` key, the
setting, or says that there is none.

| Threshold | What it bounds | Default | Class | How it is chosen | How to override | Pros / cons of the choice |
| --- | --- | --- | --- | --- | --- | --- |
| `memory_budget` | What every derived threshold below shares | 60% of the memory limit, at least 256 MiB (`MEMORY_BUDGET_SHARE`, `MIN_MEMORY_BUDGET`) | b | The container's limit (cgroup v2, v1), else the host's `MemAvailable` | `[tuning] memory_budget` | 40% is left to the interpreter, the drivers, the batch in flight and the pages read; a laptop's `MemAvailable` moves between runs |
| `cpu_budget` | The parallel steps | The CPU quota, rounded down, at least 1 | b | cgroup `cpu.max` or the CFS quota, else the CPUs the process may run on | `[tuning] cpu_budget` | Half a CPU is one step: a step is one thread, and the GIL serializes the Python side anyway |
| `key_cache_memory` (`TieredKeyCache.memory_budget`) | The keys the process keeps in memory in front of the key registry, about 280 MB per million | 25% of `memory_budget` (`KEY_CACHE_MEMORY_SHARE`) | b | A share of the budget: the cache is the one structure that grows with the keys a job touches | `[tuning] key_cache_memory` | More memory, fewer registry round trips (0.2 µs a key against 20 µs); the keys past it go to the disk tier, never lost |
| `key_cache_disk` (`TieredKeyCache.disk_budget`) | The SQLite file under `$TMPDIR` for the keys memory evicted | The lesser of 2 GiB and half the free space of `$TMPDIR` (`MAX_KEY_CACHE_DISK`, `KEY_CACHE_DISK_SHARE`) | b | Half the free space, so that the spills of a sort still fit; capped, as a cache larger than the registry's hot set gains nothing | `[tuning] key_cache_disk` (0 for none) | A key on disk costs 3.7 µs instead of 20; the file holds personal data: an owner-only directory, removed with the process ({doc}`security`) |
| `spill_rows` (`core.cutover.diff.SPILL_ROWS`, `core.operators.reconciliation.SPILL_ROWS`, `MappingModuleSettings.spill_rows`) | The rows a sort holds in memory before it writes a run to disk: the cutover diff's second pass, `MissingSourceRows` | 20% of `memory_budget` at 200 bytes a row, from 10,000 to 1,000,000 (`SPILL_SHARE`, `SPILL_ROW_BYTES`, `MIN_SPILL_ROWS`, `MAX_SPILL_ROWS`); 50,000 when called without settings | b | A share of the budget at the measured cost of a canonical row; the cap keeps a run's file under about 200 MB, the floor keeps small pods sorting in memory | `[tuning] spill_rows`; per module `spill_rows` | More rows, fewer runs and merges (`docs/algorithms.md`); past the share, the second pass competes with the key cache |
| `max_parallel_steps` | The steps a run runs at the same time, whatever its module asks | `cpu_budget` | b | One step per CPU: a step is a thread, and more threads than CPUs only share the GIL and the pools | `[tuning] max_parallel_steps` | The pools must hold two connections a step on the target, one on the source (`check_pools`); fewer steps than `partitions` leave ranges waiting |
| `queue_size` | The log records buffered before the slowest sink drops some | 10,000 | b | A fixed default: a record is small, and a full queue means a sink is down, not a budget | `[tuning] queue_size` | Larger, more memory under an outage; smaller, records lost sooner |
| `batch_size` (`MappingModuleSettings.batch_size`) | The rows mapped, checked, written and registered at once | 1,000 | b | The CRM's: a batch of 1,000 narrow rows is about 1 MB in Python, a transaction of 1,000 rows and one registry statement | `[modules.<name>.settings] batch_size` | Larger batches amortize the round trips, cost memory per step, and lose more on a failed batch |
| `page_size` (`DatabaseSettings.page_size`) | The rows one keyset query reads | 1,000 | b | The batch size of the pipelines reading the database, so that a page is a batch | `[databases.<name>] page_size` | Larger pages hold more rows per step at once; smaller ones take more queries |
| `partitions` (`MappingModuleSettings.partitions`) | The key ranges `full_load` reads a table by, in parallel | 1 | b | One: the project decides how many processes and ranges it runs; capped by `max_parallel_steps` | `[modules.<name>.settings] partitions` | More ranges, more connections and threads; the ranges are kept once chosen |
| Pools (`EngineOptions.DEFAULT_POOL_SIZE` 5, `DEFAULT_MAX_OVERFLOW` 10, `MongoOptions.DEFAULT_POOL_SIZE` 100, `BOOKKEEPING_CONNECTIONS` 2) | The connections a process opens to a database | SQLAlchemy's and pymongo's defaults; two for the lock renewal and the ledger | b | The libraries' defaults; `check_pools` refuses a module whose steps need more | `[databases.<name>.options] pool_size`, `max_overflow`, `maxPoolSize` | A pool too small blocks steps on `pool_timeout`; too large, idle connections on the server |
| `lease_seconds` (`RunsSettings.lease_seconds`) | How long a run's locks outlive its process | 300 | b | Long enough for a GC pause or a slow renewal, short enough that a crashed run's retry starts within minutes | `[runs] lease_seconds` | Longer, a retry waits longer after a crash; shorter, a live run risks losing its lock under load |
| `retention_days` (`BookkeepingSettings.retention_days`) | How long quarantined records, issues and parked rows are kept | 90 | b | A quarter: the records may hold personal data ({doc}`security`) | `[bookkeeping] retention_days`; `etl housekeeping --days` | Longer, more to audit and to erase on request |
| Retries (`MappingModuleSettings.step_attempts` 3, `retry_backoff_seconds` 5.0, `_CONCURRENT_KEY_WRITE_ATTEMPTS` 8, `_RACING_UPGRADE_ATTEMPTS` 5, `_UPGRADE_LOCK_WAIT_SECONDS` 600, `_RECOMPUTE_ROUNDS` 2) | How many times a step, a concurrent key write, a racing upgrade or a lock wait is tried | As listed | b | Transient errors (deadlocks, lost connections) clear within a few attempts; a concurrent writer is caught by the primary key at the next | `step_attempts`, `retry_backoff_seconds` per module; the others are fixed | More attempts hide a database that is down for longer; fewer fail runs on a deadlock |
| Quality shares (`MappingModuleSettings.max_reject_rate` 0.05, `max_delete_share` 0.1, `DEFAULT_MAX_SAMPLES_PER_RULE` 20) | When a batch fails for its rejects, when deletes need confirming, how many issues a report keeps per rule | As listed | b | A twentieth of a batch rejected is a broken mapping, not bad rows; a tenth of an entity deleted is a misconfigured source; 20 samples are read by a person | Per module `max_reject_rate`, `max_delete_share`; `max_samples_per_rule` of a `QualityReport` | Lower shares stop runs sooner; higher ones let a bad mapping through |
| Timeouts (`OtlpSettings.timeout_seconds` 10.0, `export_seconds` 15.0, `KafkaSettings.timeout_seconds` 30.0, `LineageSettings.timeout_seconds` 5.0) | How long an export, a broker or an event endpoint may take | As listed | b | The clients' usual defaults: an endpoint that takes longer is down | `[logging.otlp]`, `[kafka.<name>]`, `[lineage]` `timeout_seconds` | Longer waits delay the detection of an outage; shorter ones drop events under load |
| Pages of the ETL's own records (`stores._PAGE_SIZE` 1,000, `stores._KEY_PAGE_SIZE` 10,000, `mongodb.bookkeeping._PAGE_SIZE` 1,000, `changes._PAGE_SIZE` 10,000, `v0003_quarantine_keys._PAGE` 500) | The rows one query of the parked records, the key registry's entries, a change log or an upgrade reads | As listed | b | Narrow rows read in bulk take 10,000 a page; wider ones (payloads, records) 1,000 | None | Larger pages hold more rows in memory per step |
| Diff partitions (`ROWS_PER_PARTITION` 100, `MAX_PARTITIONS` 500,000) | How many target rows a partition of the cutover diff holds, and how many partitions there are | As listed | b | 100 rows a partition: a differing partition reads 100 rows again; 500,000 partitions cost 16 MB | `etl cutover report --buckets` | Smaller partitions read less again in the second pass, cost more memory in the first |
| `FAN_IN` (`shared.utils.external_sort`) | The runs an external sort merges at once | 64 | b | 64 open files and chunks fit any pod; a tier holds 64 runs of `spill_rows` items before it is merged into one | `fan_in` of `ExternalSorter` | Higher, fewer tiers and rewrites, more open files and chunks in memory |
| Key cache sampling (`SAMPLE_SIZE` 1,000, `RESAMPLE_EVERY` 100,000, `_SLOT_BYTES` 96) | How the key cache measures the cost of an entry | As listed | b | The first 1,000 entries give the mean, re-sampled every 100,000 as keys change shape; 96 bytes is the map slot measured on CPython 3.14 | None | A larger sample is more exact and costs `sys.getsizeof` calls |
| Async bridge (`_PUT_RETRY_SECONDS` 0.005, `_GET_POLL_SECONDS` 0.1) | How often the bridge between the async and the sync sides polls | As listed | b | Short enough not to be seen, long enough not to spin | None | Shorter polls burn CPU; longer ones add latency per batch |
| `_MAX_FIELD_PATH_DEPTH` (`mongodb.inspector`) | How deep the MongoDB inspector walks nested documents | 3 | b | Three levels name every field a mapping reaches; deeper trees are arrays of documents | None | Deeper, more fields discovered, slower discovery |
| `etl calibrate` (`REPEATS` 3, `DEFAULT_BATCH_SIZE` 1,000) | How many times each measurement runs, and the batch when no module names one | As listed | b | The median of three ignores one slow run; 1,000 is the modules' default batch | `--batch-size` | More repeats take longer on the database being measured |
| `bulk_copy_rows` (`MssqlDialect`; `BULK_INSERT_MISSING` 300 for the key registry when it is set) | On SQL Server, whether a batch is bulk copied into its staging table or sent as parameters | `"auto"`: chosen online per table | c | The first `TRIAL_BATCHES` (3) batches of a table alternate the two ways, timed per row; the faster one is kept; every `RECHECK_EVERY` (200) batches the other is tried once and wins only if `HYSTERESIS` (20%) faster; batches under `SMALL_BATCH` (100) rows go by parameters, unmeasured. One INFO line with both timings when chosen (`infra.database.relational.staging`) | `[tuning] bulk_copy_rows = 2000` (then the registry uses 300); `etl calibrate` prints the measured value | Online, the run adapts to each server and table and costs three measured batches; set, it is predictable and may be wrong for the next server |
| `in_chunk` (`Dialect.in_chunk`, `IN_CHUNK_CAP` 1,000, SQL Server's `MSSQL_IN_CHUNK` 500, `_RESERVED_PARAMETERS` 32; `rules.DEFAULT_CHUNK` 1,000 and `MappingModuleSettings.in_chunk` for the load verification; `mongodb.bookkeeping.DEFAULT_IN_CHUNK` 1,000; `_IN_CHUNK` 900 for the key cache's own SQLite tier) | The values of one IN list: the key registry's `known`, the quarantine's lookups, the rows read back by `verify_loads`, the MongoDB `$in` | `"auto"`: the database's parameter limit less 32, at most 1,000; 500 on SQL Server | c | From the parameter limit (class a, below) with room for the statement's other parameters; 1,000 is where longer lists stop paying (the plan degrades, the statement text grows); on SQL Server 1,000 keys read back in lists of 500 took 13.3 ms, in one list of 1,000 15.6 ms (`etl calibrate`, below); `etl calibrate` measures 250, 500 and 1,000; a set `in_chunk` wins on every database | `[tuning] in_chunk`; per module `in_chunk` | Longer lists, fewer round trips; SQL Server compiles a new plan per list length |
| `insertmanyvalues_page_size` (`EngineOptions`) | The rows of one multi-row INSERT SQLAlchemy sends | SQLAlchemy's per dialect (1,000; 32,700 parameters at most) | c | The library's, from the parameter limit | `[databases.<name>.options] insertmanyvalues_page_size` | Larger pages, fewer round trips, up to the parameter limit |
| Parameter limits (`Dialect.parameter_limit`: SQLite 999 before 3.32 and 32,766 since, SQL Server 2,100, Postgres 32,767 through psycopg, MySQL 65,535, Oracle 65,535; `Dialect.in_list_limit`: Oracle 1,000 values per IN list) | What one statement may bind | By dialect | a | The databases' and drivers' documented limits | None | — |
| Lengths (`_MAX_LABEL_LENGTH` and `_MAX_HOST_IN_OWNER` of the locks, `MAX_NAME_LENGTH` 20 of a module name, `_LONGEST_MESSAGE` 120 and `_LONGEST` 250 of a MongoDB error kept, `_MAX_REASON` 1,000 of a dead letter's reason, the `Unicode(n)` widths of the ETL tables) | What fits an identifier, a column, a header | As listed | a | The columns of the ETL tables and the databases' identifier limits (SQL Server's 900-byte index keys) | None | — |
| Formats and codes (`FORMAT_VERSION` 1 of the compiled mappings, `checksum.FORM` 2 and `MICROSECONDS` 1,000,000 of the canonical form, `diff._MASK` 2⁶⁴ − 1, `_INDEX_NOT_FOUND` 27, `_SQL_ATTR_LOGIN_TIMEOUT` 103, `_V1_UNLIMITED` 2⁶⁰, `MIB`, `GIB`) | How values are encoded, which error or attribute is which | Fixed | a | Protocols and formats, not thresholds | None | — |
| Anonymization (`_MAX_SHIFT_DAYS` 180, `_EMAIL_HEX_DIGITS` 20, `_PHONE_DIGITS` 15, `_HASH_HEX_DIGITS` 16) | How far a date shifts, how long a fake value is | As listed | a | Enough bits that fakes do not collide, values that still fit the columns ({doc}`security`) | None | — |

## `etl calibrate`

```console
$ etl calibrate crm
# etl calibrate crm: mssql 16.0.4305.5 (mssqlpython), batch_size 1000, 2026-11-09T13:34:39Z
# staging 1000 rows: parameters 139.0 ms (0.139 ms a row), bulk copy 26.0 ms (0.026 ms a row)
# reading 1000 keys back by IN lists: 250 a list 12.9 ms, 500 a list 13.3 ms, 1000 a list 15.6 ms
# key registry, 1000 new keys: 60.6 ms by bulk copy, 53.1 ms by parameters
# key registry, 1000 known keys: 43.4 ms by bulk copy, 37.2 ms by parameters
[tuning]
bulk_copy_rows = 1000           # bulk copy 5.3x faster than parameters at 1000 rows
in_chunk = 500                  # the fastest IN list measured
```

The two key registry lines show why the choice is per table: the CRM rows
(5 columns, 1,000 a batch) bulk copy 5 times faster, the registry's narrow
rows a little slower; left to `"auto"`, the writer picks bulk copy for the
CRM tables and parameters for `etl_key_registry` on this server, where a
set `bulk_copy_rows = 1000` would bulk copy both.

On a table of its own in the named database (created, filled, dropped;
nothing else is written), at the `batch_size` of the modules using that
database (`--batch-size` for another), it times: a batch staged as
parameters against the driver's bulk loader (bulk copy on SQL Server, COPY
on Postgres); IN lists of 250, 500 and 1,000 values reading the batch's
keys back; the key registry's statement for new and for known keys. Each
timing is the median of three. It recommends `bulk_copy_rows` as the batch
size when bulk copy was more than 20% faster, `"auto"` otherwise, and the
fastest IN list. `--format json` prints the measurements for scripts. It
refuses (exit 2) a database that is not in the settings, or MongoDB.

Run it on the project's own databases, from where the ETL runs: the
numbers of {doc}`performance` are one laptop's.
