# 3. Layered settings with pydantic-settings

**Status:** accepted.

## Context

The same code runs in dev, UAT and production. Settings come from files and
from the environment, and secrets come from secret stores.

## Decision

- **Layers.** `config/base.toml`, then `config/<env>.toml`, then `ETL__*`
  variables.
- **Secrets.** They come in as placeholders: `${env:}`, `${file:}`,
  `${vault:}`, `${gcp:}`, `${azure:}`.
- **Validation.** pydantic validates the settings at start-up, reporting
  every problem with its path. Modules validate their own section.

## Consequences

- **What it buys.** Settings errors fail at start (exit 2), not mid-run.
  Secrets are never in files, nor in logs.
- **What it costs.** `config` and `app` depend on pydantic; `core` does not.
