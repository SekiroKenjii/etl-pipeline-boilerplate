"""HTTP and REST adapters. Needs the `http` extra (httpx).

`api.RestEndpoint` implements `contract.IAsyncRecordSource`.
"""

from infra.http.api import ApiRecord
from infra.http.api import RestApi
from infra.http.api import RestEndpoint
from infra.http.client import HttpRequester
from infra.http.client import HttpRetry
from infra.http.pagination import CursorPaginator
from infra.http.pagination import LinkHeaderPaginator
from infra.http.pagination import NoPagination
from infra.http.pagination import Page
from infra.http.pagination import PageNumberPaginator
from infra.http.pagination import Paginator

__all__ = [
    "ApiRecord",
    "CursorPaginator",
    "HttpRequester",
    "HttpRetry",
    "LinkHeaderPaginator",
    "NoPagination",
    "Page",
    "PageNumberPaginator",
    "Paginator",
    "RestApi",
    "RestEndpoint",
]
