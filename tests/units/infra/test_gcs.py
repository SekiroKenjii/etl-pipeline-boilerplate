from __future__ import annotations

from collections.abc import Callable, Iterator
import dataclasses
import datetime
import io
import threading
from typing import Any

from google.api_core import exceptions as google_exceptions
import pytest

from core import contract
from core import operators
from core import pipeline
from core import state as core_state
from infra import formats
from infra.storage import gcs as gcs_store
from tests.factories import fakes

# api_core exceptions are not typed: call them through a typed alias.
NotFound: Callable[[str], Exception] = google_exceptions.NotFound
UPDATED = datetime.datetime(2026, 10, 31, tzinfo=datetime.timezone.utc)


@dataclasses.dataclass
class Listed:
    name: str
    size: int
    generation: int
    updated: datetime.datetime = UPDATED


class FakeGcs:
    """Behaves like GCS where it matters: name order, generations, NotFound."""

    def __init__(self, bucket: str = "landing") -> None:
        self.bucket_name = bucket
        self.objects: dict[str, tuple[bytes, int]] = {}
        self.closed = False

    def put(self, name: str, text: str) -> None:
        _, generation = self.objects.get(name, (b"", 1000))
        self.objects[name] = (text.encode(), generation + 1)

    def list_blobs(
        self, bucket_or_name: str, *, prefix: str | None = None
    ) -> Iterator[Listed]:
        assert bucket_or_name == self.bucket_name
        for name in sorted(self.objects):
            if prefix is None or name.startswith(prefix):
                data, generation = self.objects[name]
                yield Listed(name, len(data), generation)

    def bucket(self, bucket_name: str) -> Any:
        gcs = self

        class Bucket:

            def blob(self, name: str, generation: int | None = None) -> Any:
                class Blob:

                    def reload(self) -> None:
                        if (
                            name not in gcs.objects
                            or gcs.objects[name][1] != generation
                        ):
                            raise NotFound(f"{name}#{generation}")

                    def open(self, mode: str) -> io.BytesIO:
                        assert mode == "rb"
                        return io.BytesIO(gcs.objects[name][0])

                return Blob()

        return Bucket()

    def close(self) -> None:
        self.closed = True


def _store(gcs: FakeGcs) -> gcs_store.GcsObjectStore:
    return gcs_store.GcsObjectStore(gcs.bucket_name, client_factory=lambda: gcs)


def test_lists_blobs_as_objects_without_folder_placeholders() -> None:
    gcs = FakeGcs()
    gcs.put("2026/10/orders.csv", "id\n1\n")
    gcs.objects["2026/10/"] = (
        b"",
        1,
    )  # created by "Create folder" in the console

    (obj,) = _store(gcs).list("2026/")

    assert (obj.key, obj.size, obj.version, obj.updated_at) == (
        "2026/10/orders.csv",
        5,
        "1001",
        UPDATED,
    )


def test_reads_the_listed_generation() -> None:
    gcs = FakeGcs()
    gcs.put("a.csv", "id\n1\n")
    store = _store(gcs)

    with store.open(next(store.list())) as stream:
        assert stream.read() == b"id\n1\n"


def test_an_object_overwritten_after_listing_is_a_retryable_error() -> None:
    gcs = FakeGcs()
    gcs.put("a.csv", "id\n1\n")
    store = _store(gcs)
    listed = next(store.list())

    gcs.put("a.csv", "id\n2\n")  # new generation

    with pytest.raises(
        contract.RetryableError, match="generation 1001 is gone"
    ):
        store.open(listed)


def test_one_client_per_thread_all_closed_on_teardown() -> None:
    made: list[FakeGcs] = []

    def factory() -> FakeGcs:
        made.append(FakeGcs())
        return made[-1]

    store = gcs_store.GcsObjectStore("landing", client_factory=factory)
    threads = [
        threading.Thread(target=lambda: list(store.list())) for _ in range(3)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    list(store.list())
    list(store.list())  # same thread: same client

    store.close()

    assert len(made) == 4
    assert all(client.closed for client in made)


def test_incremental_extraction_from_a_bucket() -> None:
    gcs = FakeGcs()
    gcs.put("orders/2026-10-30.csv", "id\n1\n2\n")
    gcs.put("orders/2026-10-31.csv", "id\n3\n")
    state = core_state.InMemoryStateStore()

    def run() -> fakes.CollectingLoader[operators.ObjectRecord]:
        loader = fakes.CollectingLoader[operators.ObjectRecord]()
        extractor = operators.ObjectStoreExtractor(
            _store(gcs),
            formats.CsvFormat(),
            prefix="orders/",
            state=state,
            concurrency=2,
        )
        result = pipeline.ETLJob(
            extractor, fakes.Passthrough[operators.ObjectRecord](), loader
        )(fakes.make_context("orders"), {})
        assert result.status is contract.ETLStatus.SUCCESS
        return loader

    assert [r.data["id"] for r in run().rows] == ["1", "2", "3"]
    assert run().rows == []

    gcs.put(
        "orders/2026-10-31.csv", "id\n3\n4\n"
    )  # re-uploaded: new generation
    assert [r.data["id"] for r in run().rows] == ["3", "4"]


def test_the_default_factory_builds_a_real_client() -> None:
    storage = pytest.importorskip("google.cloud.storage")
    store = gcs_store.GcsObjectStore(
        "landing", client_factory=storage.Client.create_anonymous_client
    )

    client = store._client()  # no request is sent

    assert isinstance(client, storage.Client)
    store.close()
