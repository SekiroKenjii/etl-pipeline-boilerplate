# 8. At-least-once delivery, made idempotent by upserts

**Status:** accepted.

## Context

Exactly-once delivery across a source, a process and a target needs
distributed transactions, which legacy databases rarely offer.

## Decision

- **When a batch is done.** A batch is acknowledged, and its position
  saved, only once it is loaded. What the quality gate set aside for it
  (quarantined rows, issues, parked orphans) is stored first, and dropped if
  the batch fails; Debezium dead letters are sent then too.
- **What a crash costs.** A crash replays at most the batches in flight.
- **Why that is harmless.** Loads are upserts by a deterministic key, and
  updates skip unchanged rows, so loading a batch twice changes nothing.
  Changes are applied in their order: deletes go before upserts only where
  they cannot touch the same row (`ChangeLoader`), so a batch applied again
  ends in the same state. A record rejected again refreshes its open
  quarantine entry rather than adding one.
- **The boundary is read twice.** A table read by watermark reads the rows
  at the last saved watermark again once, at the next run, for rows
  committed late with a smaller key. A row rejected there is rejected
  again: the run is a partial success once more.

## Consequences

- **What it buys.** A run killed at any moment is finished by the next.
  `tests/e2e/test_chaos.py` checks it with `kill -9` and with a lost
  connection.
- **What it costs.** Loads must be idempotent: plain inserts into tables
  without a key are not.
