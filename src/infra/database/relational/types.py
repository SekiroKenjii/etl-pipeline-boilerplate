"""The types the modules of the relational adapters share."""

from __future__ import annotations

from typing import Literal, TypedDict

import sqlalchemy as sa

__all__ = ["Joining", "MigrationOptions", "Staging"]

type Joining = sa.Insert | sa.Update | sa.Delete
# How a batch gets into its staging table (`staging.StagingChooser`).
type Staging = Literal["parameters", "bulk copy"]


class MigrationOptions(TypedDict):
    """What `alembic.context.configure` is given in every mode."""

    version_table: str
    version_table_schema: str | None
    transaction_per_migration: bool
