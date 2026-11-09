# 9. Steps recorded in the ledger: resuming runs, sharing them between hosts

**Status:** superseded by {doc}`0010-steps-locked-scale-out-by-the-scheduler`
for sharing runs between hosts: `--join`, claims, `runs.poll_seconds` and
`deploy/k8s/overlays/workers` are gone. Recording the steps and resuming
runs stand. The table is now created by a revision applied by `etl-db
upgrade` ({doc}`0011-alembic-revisions-of-the-etl-tables`).

## Context

- **Resuming.** The ledger recorded whole runs only. After a failure, the
  next run ran every step again: each went on from its last batch, but the
  steps already finished still read their source to find nothing new, and
  an operator could not say which step to fix.
- **Several hosts.** The run lock kept a pipeline on one process. A cutover
  window too short for one host had no way out but bigger hosts.

## Decision

- **Each step in the ledger.** A step is recorded (`etl_run_steps`) as soon
  as it ends, not with the run: a run killed half-way still tells what it
  finished.
- **Resuming carries the steps finished.** `etl run --resume` does not run
  again the steps the run it resumes finished, nor those of the runs that
  one resumed. `--step` runs some steps only, taking the others as done.
- **Workers share a run by its id.** `etl run --join <id>`: every process
  given the id takes the run's lock with the same owner, then claims each
  step it runs with a lease in the same lock table (`<pipeline>/<step>`).
  The others take its outcome from the ledger, and wait for it when their
  next steps depend on it. No coordinator process, no queue: the
  bookkeeping database is the only thing shared.
- **Steps stay the unit of work.** A large table is split in steps, one per
  key range (`partitions`), and a `<mapping>:ranges` step computes the
  ranges once, so that every worker reads the same ones.

## Consequences

- **What it buys.**
  - A failed run is resumed where it stopped, after `kill -9` too
    (`tests/e2e/test_runs_resumed_locked_or_unbuilt.py`).
  - Any number of pods, an Indexed Job of Kubernetes for instance, share a
    run: `deploy/k8s/overlays/workers`.
  - A worker that dies loses its claims when their lease expires; another
    worker runs its steps again. Retrying the workers of a failed run runs
    its failed steps again.
- **What it costs.**
  - One more table, in the DDL of `etl-db ddl`.
  - A shared run needs the bookkeeping in a database every worker reaches,
    and waits up to `runs.poll_seconds` to see a step another worker ended.
  - A step is never split between workers: a table loads on as many
    workers as it has ranges.
  - The workers release the run's lock one after the other: between two
    releases, another run could take it, and the workers still running
    would then cancel. They end together, when every step has ended, so the
    window is short.
