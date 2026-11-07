# 5. A container of resources written by hand

**Status:** accepted.

## Context

Pipelines need databases, stores, clusters and the ETL's own records. Each
has a lifetime (the process, one run), a health check, and a way to close.
Dependency injection frameworks add magic, and more dependencies.

## Decision

`core.runtime.Resources` declares each resource by name. Each resource has:

- a factory;
- a scope (the process, or one run);
- a health check;
- a way to close.

Resources are created lazily and closed in reverse order. `app.factories`
declares them from the settings.

The resource owns what an adapter holds (a connection pool, a client), and
closes it (`Database.close`, `MongoDatabase.close`). Ports have no setup or
teardown: an adapter is ready once built. What a port hands out that holds
a consumer (`IChangeLog.source`, `IMessageLog.topic`) is closed by whoever
asked for it (`close`). Table readers are run resources, created again for
each run.

## Consequences

- **What it buys.** The whole graph is in one place, typed, and testable.
- **What it costs.** No autowiring: a new resource is a few explicit lines.
