from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import Any

import httpx
import pytest

from core import contract
from infra import http
from shared import utils
from tests.factories import fakes

BASE = "https://api.test"
type Handler = Callable[[httpx.Request], httpx.Response]


class MockApi:
    """Answers with `handler` and keeps every request it received."""

    def __init__(self, handler: Handler) -> None:
        self.handler = handler
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self.handler(request)

    @property
    def params(self) -> list[dict[str, str]]:
        return [dict(request.url.params) for request in self.requests]


class Sleeps:

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


def _api(mock: MockApi, **kwargs: Any) -> http.RestApi:
    kwargs.setdefault("sleep", Sleeps())
    return http.RestApi(BASE, transport=httpx.MockTransport(mock), **kwargs)


def _fetch(
    endpoint: http.RestEndpoint, params: Mapping[str, Any] | None = None
) -> list[http.ApiRecord]:
    async def run() -> list[http.ApiRecord]:
        async with endpoint.alifecycle(fakes.make_context()):
            return [record async for record in endpoint.records(params or {})]

    return asyncio.run(run())


def _pages(*pages: list[dict[str, Any]], param: str = "page") -> Handler:
    def handler(request: httpx.Request) -> httpx.Response:
        index = int(request.url.params.get(param, "1")) - 1
        return httpx.Response(
            200, json=pages[index] if index < len(pages) else []
        )

    return handler


class TestPagination:

    def test_single_request_with_records_path_and_params(self) -> None:
        mock = MockApi(
            lambda r: httpx.Response(200, json={"data": [{"id": 1}]})
        )
        endpoint = _api(mock).endpoint("/items", records_path="data")

        assert _fetch(endpoint, {"status": "open"}) == [{"id": 1}]
        assert mock.params == [{"status": "open"}]

    def test_page_numbers_until_an_empty_page(self) -> None:
        mock = MockApi(_pages([{"id": 1}], [{"id": 2}]))
        endpoint = _api(mock).endpoint(
            "/items", paginator=http.PageNumberPaginator()
        )

        assert _fetch(endpoint) == [{"id": 1}, {"id": 2}]
        assert mock.params == [{"page": "1"}, {"page": "2"}, {"page": "3"}]

    def test_page_numbers_stop_on_a_short_page(self) -> None:
        mock = MockApi(_pages([{"id": 1}, {"id": 2}], [{"id": 3}]))
        paginator = http.PageNumberPaginator(size_param="per_page", size=2)

        records = _fetch(_api(mock).endpoint("/items", paginator=paginator))

        assert [r["id"] for r in records] == [1, 2, 3]
        assert mock.params[-1] == {
            "page": "2",
            "per_page": "2",
        }  # no 3rd request

    def test_cursor_from_the_body(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.params.get("cursor") == "c2":
                return httpx.Response(
                    200, json={"items": [{"id": 2}], "meta": {}}
                )
            return httpx.Response(
                200, json={"items": [{"id": 1}], "meta": {"next": "c2"}}
            )

        mock = MockApi(handler)
        endpoint = _api(mock).endpoint(
            "/items",
            paginator=http.CursorPaginator(cursor_path="meta.next"),
            records_path="items",
        )

        assert [r["id"] for r in _fetch(endpoint)] == [1, 2]
        assert mock.params == [{}, {"cursor": "c2"}]

    def test_link_header_keeps_the_next_url_query(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.params.get("page") == "2":
                return httpx.Response(200, json=[{"id": 2}])
            return httpx.Response(
                200,
                json=[{"id": 1}],
                headers={"Link": f'<{BASE}/items?page=2>; rel="next"'},
            )

        endpoint = _api(MockApi(handler)).endpoint(
            "/items", paginator=http.LinkHeaderPaginator(), max_pages=5
        )

        assert [r["id"] for r in _fetch(endpoint)] == [1, 2]

    def test_max_pages_stops_an_endless_api(self) -> None:
        mock = MockApi(lambda r: httpx.Response(200, json=[{"id": 1}]))
        endpoint = _api(mock).endpoint(
            "/items", paginator=http.PageNumberPaginator(), max_pages=3
        )

        assert len(_fetch(endpoint)) == 3
        assert len(mock.requests) == 3


class TestRetries:

    def test_transient_statuses_are_retried_with_backoff(self) -> None:
        answers = iter([
            httpx.Response(503),
            httpx.Response(429),
            httpx.Response(200, json=[{"id": 1}]),
        ])
        sleeps = Sleeps()
        endpoint = _api(
            MockApi(lambda r: next(answers)), sleep=sleeps
        ).endpoint("/items")

        assert _fetch(endpoint) == [{"id": 1}]
        assert sleeps.delays == [0.5, 1.0]

    def test_retry_after_header_wins(self) -> None:
        answers = iter([
            httpx.Response(429, headers={"Retry-After": "7"}),
            httpx.Response(200, json=[]),
        ])
        sleeps = Sleeps()

        _fetch(
            _api(MockApi(lambda r: next(answers)), sleep=sleeps).endpoint(
                "/items"
            )
        )

        assert sleeps.delays == [7.0]

    def test_network_errors_are_retried(self) -> None:
        calls = iter([True, False])

        def handler(request: httpx.Request) -> httpx.Response:
            if next(calls):
                raise httpx.ConnectError("connection reset", request=request)
            return httpx.Response(200, json=[{"id": 1}])

        assert _fetch(_api(MockApi(handler)).endpoint("/items")) == [{"id": 1}]

    def test_exhausted_retries_raise_a_retryable_error(self) -> None:
        mock = MockApi(lambda r: httpx.Response(503))
        endpoint = _api(mock, retry=http.HttpRetry(max_attempts=3)).endpoint(
            "/items"
        )

        with pytest.raises(contract.RetryableError, match="after 3 attempt"):
            _fetch(endpoint)
        assert len(mock.requests) == 3

    def test_client_errors_are_fatal_and_not_retried(self) -> None:
        mock = MockApi(lambda r: httpx.Response(404))
        endpoint = _api(mock).endpoint("/items")

        with pytest.raises(contract.FatalError, match="HTTP 404"):
            _fetch(endpoint)
        assert len(mock.requests) == 1


class TestResponses:

    def test_records_must_be_a_list(self) -> None:
        mock = MockApi(lambda r: httpx.Response(200, json={"data": {"id": 1}}))
        endpoint = _api(mock).endpoint("/items", records_path="data")

        with pytest.raises(contract.FatalError, match="expected a list"):
            _fetch(endpoint)

    def test_body_must_be_json(self) -> None:
        mock = MockApi(lambda r: httpx.Response(200, text="<html>"))
        endpoint = _api(mock).endpoint("/items")

        with pytest.raises(contract.FatalError, match="not JSON"):
            _fetch(endpoint)


def test_endpoints_of_one_api_share_its_rate_limit() -> None:
    class CountingBucket(utils.TokenBucket):

        def __init__(self) -> None:
            super().__init__(rate=1_000_000)
            self.calls = 0

        def reserve(self, tokens: float = 1) -> float:
            self.calls += 1
            return super().reserve(tokens)

    bucket = CountingBucket()
    api = _api(
        MockApi(lambda r: httpx.Response(200, json=[])), rate_limit=bucket
    )

    _fetch(api.endpoint("/orders"))
    _fetch(api.endpoint("/customers"))

    assert bucket.calls == 2


def test_records_before_asetup_is_refused() -> None:
    endpoint = _api(MockApi(lambda r: httpx.Response(200, json=[]))).endpoint(
        "/items"
    )

    async def run() -> None:
        async for _ in endpoint.records({}):
            pass

    reading = run()

    with pytest.raises(RuntimeError, match="before asetup"):
        asyncio.run(reading)


def test_rejects_invalid_max_pages() -> None:
    api = http.RestApi(BASE)

    with pytest.raises(ValueError):
        api.endpoint("/items", max_pages=0)
