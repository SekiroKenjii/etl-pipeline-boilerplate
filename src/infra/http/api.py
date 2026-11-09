"""REST APIs and their endpoints, as asynchronous record sources."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping

import httpx

from core import contract
from infra.http import client as http_client
from infra.http import pagination
from infra.http import types as http_types
from shared import utils

type _Primitive = str | int | float | bool | None


class RestApi:
    """The settings shared by every endpoint of one REST API.

    Thread-safe and independent of event loops: share one instance, so that the
    rate limit applies across all its endpoints.

        api = RestApi("https://api.example.com", rate_limit=TokenBucket(10))
        orders = api.endpoint(
            "/v1/orders",
            paginator=pagination.CursorPaginator(),
            records_path="data",
        )

    Attributes:
        base_url: The URL endpoint paths are relative to.
        headers: Sent with every request.
        rate_limit: Throttles every request of every endpoint.
        retry: The request-level retries.
        timeout_seconds: The timeout of each request.
        transport: Replaces the network, e.g. `httpx.MockTransport` in tests.
        sleep: Waits between retries; replaceable in tests.
    """

    def __init__(
        self,
        base_url: str,
        *,
        headers: Mapping[str, str] | None = None,
        rate_limit: utils.TokenBucket | None = None,
        retry: http_client.HttpRetry | None = None,
        timeout_seconds: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.base_url = base_url
        self.headers = dict(headers or {})
        self.rate_limit = rate_limit
        self.retry = retry or http_client.HttpRetry()
        self.timeout_seconds = timeout_seconds
        self.transport = transport
        self.sleep = sleep

    def endpoint(
        self,
        path: str,
        *,
        paginator: http_types.Paginator | None = None,
        records_path: str | None = None,
        max_pages: int | None = None,
    ) -> RestEndpoint:
        """Returns the endpoint at `path`; see `RestEndpoint`."""
        return RestEndpoint(
            self,
            path,
            paginator=paginator,
            records_path=records_path,
            max_pages=max_pages,
        )

    def requester(self) -> http_client.HttpRequester:
        """Returns a requester with a new client, for one event loop.

        The caller closes its client, on that loop.
        """
        client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=self.headers,
            timeout=self.timeout_seconds,
            transport=self.transport,
        )
        return http_client.HttpRequester(
            client, self.retry, self.rate_limit, self.sleep
        )


class RestEndpoint(contract.IAsyncRecordSource[http_types.ApiRecord]):
    """The JSON records of one paginated GET endpoint.

    Its HTTP client is created in `asetup` and closed in `ateardown`, on the
    caller's event loop.

    Attributes:
        api: The API the endpoint belongs to.
        path: The path of the endpoint, relative to the base URL.
        paginator: How the endpoint pages its results.
        records_path: Where the list of records is in the body, as a dotted
            path; the body itself when None.
        max_pages: Stops after that many pages, against an API that never
            stops paginating.
    """

    def __init__(
        self,
        api: RestApi,
        path: str,
        *,
        paginator: http_types.Paginator | None = None,
        records_path: str | None = None,
        max_pages: int | None = None,
    ) -> None:
        """Initializes the endpoint; prefer `RestApi.endpoint`.

        Raises:
            ValueError: If `max_pages` is below 1.
        """
        if max_pages is not None and max_pages < 1:
            raise ValueError("max_pages must be >= 1")

        self.api = api
        self.path = path
        self.paginator: http_types.Paginator = (
            paginator or pagination.NoPagination()
        )
        self.records_path = records_path
        self.max_pages = max_pages
        self._requester: http_client.HttpRequester | None = None

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.path})"

    async def asetup(self, ctx: contract.ETLContext) -> None:
        self._requester = self.api.requester()

    async def ateardown(self, ctx: contract.ETLContext) -> None:
        if self._requester is not None:
            await self._requester.client.aclose()
            self._requester = None

    # pylint takes this async generator for a coroutine.
    async def records(  # pylint: disable=invalid-overridden-method
        self, params: Mapping[str, object]
    ) -> AsyncIterator[http_types.ApiRecord]:
        """Yields the records of every page.

        Raises:
            RuntimeError: If called before `asetup`.
            RetryableError: On a transient failure that outlasted the retries.
            FatalError: On a client error, or a body without records.
        """
        requester = self._requester
        if requester is None:
            raise RuntimeError(f"{self.name}.records() called before asetup()")

        page: pagination.Page | None = self.paginator.first_page(
            self.path, params
        )
        fetched = 0

        while page is not None:
            # httpx replaces the URL's query even with empty `params`.
            request = requester.client.build_request(
                "GET", page.url, params=_query(page.params) or None
            )
            response = await requester.send(request)
            records = self._records(response)
            fetched += 1

            for record in records:
                yield record

            if self.max_pages is not None and fetched >= self.max_pages:
                return
            page = self.paginator.next_page(page, response, records)

    def _records(self, response: httpx.Response) -> list[http_types.ApiRecord]:
        """Returns the records of a page.

        Raises:
            FatalError: If the body is not JSON, or holds no list of objects
                at `records_path`.
        """
        try:
            body: utils.JsonValue = response.json()
        except ValueError as exc:
            raise contract.FatalError(
                f"{response.request.url}: response is not JSON"
            ) from exc

        records = (
            utils.get_path(body, self.records_path)
            if self.records_path
            else body
        )
        if not isinstance(records, list):
            raise contract.FatalError(
                f"{response.request.url}: expected a list of records at"
                f" {self.records_path or 'the body root'!r}"
            )
        objects: list[http_types.ApiRecord] = [
            record for record in records if isinstance(record, dict)
        ]
        if len(objects) != len(records):
            raise contract.FatalError(
                f"{response.request.url}: every record must be an object"
            )
        return objects


def _query(
    params: Mapping[str, object],
) -> dict[str, _Primitive | list[_Primitive]]:
    """The query parameters, as httpx takes them: primitives, or lists."""
    return {
        name: (
            [_primitive(item) for item in value]
            if isinstance(value, (list, tuple))
            else _primitive(value)
        )
        for name, value in params.items()
    }


def _primitive(value: object) -> _Primitive:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)  # As httpx sends any other value.
