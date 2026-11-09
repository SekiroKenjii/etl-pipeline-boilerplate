"""The types the modules of the composition root share."""

from __future__ import annotations

from typing import TypedDict

__all__ = ["TablesOptions"]


class TablesOptions(TypedDict):
    """Where the bookkeeping adapters of a SQL database find their tables."""

    schema: str | None
    prefix: str
