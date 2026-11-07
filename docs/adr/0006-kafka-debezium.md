# 6. Debezium on Kafka for the change logs the ETL cannot read

**Status:** accepted.

## Context

- **What the ETL reads itself.** SQL Server Change Tracking, PostgreSQL
  logical replication and MongoDB change streams.
- **What it cannot.** Oracle's log needs LogMiner or XStream, and the
  skills to run them.

## Decision

- **Debezium.** It writes Oracle's changes (and those of other databases)
  to Kafka.
- **Reading them.** The ETL reads them with confluent-kafka, run after run,
  up to the end of the topic at the start of the run, and commits offsets
  once loaded.
- **Broker-neutral ports.** A `Message` carries an id, a key, a value,
  headers, a timestamp and an opaque position; the source tells whether a
  position covers another (`IMessageSource.covers`) and confirms it.
  Partitions and offsets stay in `infra.messaging.kafka`. A source holding
  a consumer is closed by whoever asked for it (`close`).
- **Dead letters.** Messages that are not change events go to a dead letter
  topic, as they were, with headers saying why (`etl.error`, redacted) and
  where from (`etl.message`, Kafka Connect's own headers). They are sent
  once the changes around them are loaded, before the position is saved: a
  batch read again sends them once, a dry run never.

## Consequences

- **What it buys.** One path for every log the ETL does not read itself.
  It is a matter of settings: the modules do not change.
- **What it costs.** Kafka, Kafka Connect and Debezium have to run, and
  their retention must outlast the longest pause between runs.
