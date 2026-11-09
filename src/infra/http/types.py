"""The types the HTTP adapters share, and the ones their callers pass."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

import httpx

from infra.http import pagination
from shared import utils

__all__ = ["ApiRecord", "Paginator"]

type ApiRecord = utils.JsonObject


class Paginator(Protocol):
    """Tells which page comes after which."""

    def first_page(
        self, url: str, params: Mapping[str, object]
    ) -> pagination.Page:
        """Returns the first page of the endpoint at `url`."""
        ...

    def next_page(
        self,
        page: pagination.Page,
        response: httpx.Response,
        records: Sequence[object],
    ) -> pagination.Page | None:
        """Returns the page after `page`, or None when there is none."""
        ...
