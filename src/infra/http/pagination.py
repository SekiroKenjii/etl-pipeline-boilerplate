"""How REST APIs page their results."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import dataclasses
from typing import Any, Protocol

import httpx

from shared import utils


@dataclasses.dataclass(frozen=True, slots=True)
class Page:
    """Where to fetch a page.

    Attributes:
        url: Absolute, or relative to the base URL of the API.
        params: The query parameters.
    """

    url: str
    params: Mapping[str, Any] = dataclasses.field(default_factory=dict)


class Paginator(Protocol):
    """Tells which page comes after which."""

    def first_page(self, url: str, params: Mapping[str, Any]) -> Page:
        """Returns the first page of the endpoint at `url`."""
        ...

    def next_page(
        self, page: Page, response: httpx.Response, records: Sequence[object]
    ) -> Page | None:
        """Returns the page after `page`, or None when there is none."""
        ...


class NoPagination:
    """A single request."""

    def first_page(self, url: str, params: Mapping[str, Any]) -> Page:
        return Page(url, dict(params))

    def next_page(
        self, page: Page, response: httpx.Response, records: Sequence[object]
    ) -> Page | None:
        del page, response, records  # Unused.
        return None


@dataclasses.dataclass(frozen=True, slots=True)
class PageNumberPaginator:
    """Pages by number: `?page=1`, `?page=2`...

    Stops at an empty page, or at a short one when `size` is set:
    `?page=3&per_page=100` returning 40 records is the last page.

    Attributes:
        page_param: The query parameter of the page number.
        start: The number of the first page.
        size_param: The query parameter of the page size, if any.
        size: The page size requested.
    """

    page_param: str = "page"
    start: int = 1
    size_param: str | None = None
    size: int | None = None

    def first_page(self, url: str, params: Mapping[str, Any]) -> Page:
        first = {**params, self.page_param: self.start}
        if self.size_param is not None and self.size is not None:
            first[self.size_param] = self.size
        return Page(url, first)

    def next_page(
        self, page: Page, response: httpx.Response, records: Sequence[object]
    ) -> Page | None:
        del response  # Unused.
        if not records or (self.size is not None and len(records) < self.size):
            return None

        current = int(page.params[self.page_param])
        return Page(page.url, {**page.params, self.page_param: current + 1})


@dataclasses.dataclass(frozen=True, slots=True)
class CursorPaginator:
    """Pages by cursor: the body says where to continue.

    `{"next_cursor": "abc"}` is sent back as `?cursor=abc`. Stops when the
    cursor is missing or empty.

    Attributes:
        cursor_param: The query parameter of the cursor.
        cursor_path: Where the cursor is in the body, as a dotted path.
    """

    cursor_param: str = "cursor"
    cursor_path: str = "next_cursor"

    def first_page(self, url: str, params: Mapping[str, Any]) -> Page:
        return Page(url, dict(params))

    def next_page(
        self, page: Page, response: httpx.Response, records: Sequence[object]
    ) -> Page | None:
        del records  # Unused.
        cursor = utils.get_path(response.json(), self.cursor_path)
        if cursor in (None, ""):
            return None

        return Page(page.url, {**page.params, self.cursor_param: cursor})


class LinkHeaderPaginator:
    """Pages by RFC 8288 `Link: <https://...?page=2>; rel="next"` headers."""

    def first_page(self, url: str, params: Mapping[str, Any]) -> Page:
        return Page(url, dict(params))

    def next_page(
        self, page: Page, response: httpx.Response, records: Sequence[object]
    ) -> Page | None:
        del page, records  # Unused.
        next_url = response.links.get("next", {}).get("url")
        # The next URL carries its own query parameters.
        return Page(next_url) if next_url else None
