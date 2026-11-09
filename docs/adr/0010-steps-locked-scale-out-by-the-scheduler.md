# 10. Steps locked one by one; the scheduler shares a pipeline out

**Status:** accepted. Supersedes {doc}`0009-steps-in-the-ledger` for
sharing runs; the ledger of steps and resuming stay.

## Context

- **`--join` broke its locks.** Workers of a shared run held the run's lock
  with one owner: the first to stop (a failed step, a SIGTERM, an eviction)
  released it for all, and another run could start while the others still
  loaded. A pod retried by an Indexed Job exited with the code of the whole
  run, so one failed step failed every pod, and the Job, with no retry.
- **The lease died quietly.** A renewal that raised (the bookkeeping
  database failing over) ended the heartbeat thread; the run went on without
  a lease, and the next scheduled run could take the lock and load the same
  steps at the same time.
- **Retries did not resume.** A scheduler retrying a run started a new one:
  every step ran again, or, if the run that died still held the lock, the
  retry was locked out and Airflow marked the night skipped, its DAG run a
  success.
- **A protocol of its own.** Claims, polling and adopting the steps of
  other workers made a small distributed scheduler, inside the ETL, that
  the schedulers it runs under already are.

## Decision

- **No shared runs.** `--join`, the claims of steps, the polling of other
  workers and `runs.poll_seconds` are gone. A run is one process.
- **A lock per step.** A run takes `<pipeline>/<step>` for every step it
  runs (every step without `--step`, the steps it carries over from the run
  it resumes aside), all of them or none: on the first one held elsewhere
  it gives back what it took, so two runs never deadlock, and exits 4 (a
  retry of the same label first waits: below). It renews them all while it runs, and releases them all at the
  end. Two runs of other steps of one pipeline run at the same time;
  overlapping ones do not.
- **The scheduler shares a pipeline out.** To load a large table on several
  hosts, run several jobs, each with its own steps: the ranges of the table
  (`--step 'customer:0'`...), after one job ran its `<mapping>:ranges`
  steps. A range run without its ranges step, before any ranges are saved,
  fails instead of splitting the table itself: two jobs splitting it at
  different moments would read different ranges and miss rows.
- **Labels for retries.** `etl run --label <job>` records the scheduler's
  name of the job with the run. `--resume` without a run id then resumes
  the last run of that label only. The Kubernetes CronJob runs `--label
  $(JOB_NAME) --resume`, the Airflow DAG `--label <DAG run>/<task>
  --resume`: a retry resumes its job's run, the next night is a new label
  and runs every step.
- **A process owns its locks; a retry waits for them.** The locks of a
  run are held by its process (`label:<label>@<run id>` for a labelled run,
  `<host>:<run id>` otherwise): only it renews them, only it frees them. A
  retry of a label finding a lock held by another run of that label (a try
  that died, or one still running) waits for it, up to `lease_seconds`,
  polling every tenth of it: a dead try's leases expire within that time
  and the retry takes them over; a try still alive keeps renewing them and
  the retry exits 4, having run nothing. SIGTERM cancels the wait (exit
  130). A lock held by a run of another label, or any lock for an
  unlabelled run, exits 4 at once. The schedulers' own guarantees are not
  relied on: a node cut off from the API server but not from the database,
  a zombie Airflow task, an operator reusing a label all leave a try alive
  while its retry starts.
  - Revised after review 2 (R2-02): the locks were owned by the label, and
    a retry took them over at once, from a try still running too; both
    loaded the same steps, and the first to end freed the other's locks.
- **The heartbeat outlives errors.** A renewal that raises is logged and
  tried again every tenth of the lease; if the lease cannot be renewed
  before it expires, the run is cancelled and `etl.lock.lost` counted, as
  when the lock is lost.
- **A locked run fails.** Exit 4 is a failure for the schedulers: the
  CronJob fails the Job, the DAG fails and retries the task, alerts fire.

## Consequences

- **What it buys.**
  - A lock is held by one process; releasing it never frees another run's
    steps, whatever the label.
  - Retries resume, on Kubernetes and Airflow, without a run id to share.
  - Several jobs load one pipeline at once, each its steps, with the
    scheduler's own retries per job (an Indexed Job with
    `backoffLimitPerIndex`, Airflow tasks).
  - No polling: a run reads the steps it carries over once, when it starts.
- **What it costs.**
  - The scheduler, not the ETL, orders the jobs of a shared load: the
    ranges steps first, a parent table before its children.
  - A retry of a try that died starts loading up to `lease_seconds`
    after it starts (5 minutes by default), not at once. Once it has the
    locks, a retry that waited (`--resume`) reads the ledger again: the
    steps the try finished meanwhile are carried over, and their locks
    freed; the steps that try failed are run, their locks taken first by
    the same rules (a retry never holds a lock while it waits, so it gives
    every lock back and takes the full set again); a try that succeeded
    leaves it nothing to run (success, exit 0, recorded as resuming that
    try). The ledger is read again after a wait twice at most: past that
    the retry runs what it read last, every lock held, so a label whose
    tries keep failing cannot make it wait forever (about 3.3 leases at
    worst before exit 4).
  - A label is at most 161 characters: with the run id, the owner of a
    lock fits its column, 200 characters.
  - A run of the whole pipeline takes one lock per step: a few more rows in
    `etl_locks`, renewed together.
