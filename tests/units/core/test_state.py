from __future__ import annotations

import json
import pathlib

import pytest

from core import contract
from core import state
from shared import utils


@pytest.fixture(params=["memory", "json"])
def store(
    request: pytest.FixtureRequest, tmp_path: pathlib.Path
) -> contract.IStateStore:
    if request.param == "memory":
        return state.InMemoryStateStore()
    return state.JsonFileStateStore(tmp_path)


class TestStateStores:

    def test_get_set_delete(self, store: contract.IStateStore) -> None:
        assert store.get("users", "watermark") is None

        store.set("users", "watermark", "2026-10-31T00:00:00+00:00")
        assert store.get("users", "watermark") == "2026-10-31T00:00:00+00:00"

        store.delete("users", "watermark")
        assert store.get("users", "watermark") is None

    def test_namespaces_are_isolated(self, store: contract.IStateStore) -> None:
        store.set("users", "cursor", 1)
        store.set("orders", "cursor", 2)

        assert (
            store.get("users", "cursor"),
            store.get("orders", "cursor"),
        ) == (1, 2)

    def test_returned_values_are_copies(
        self, store: contract.IStateStore
    ) -> None:
        store.set("files", "processed", ["a.csv"])

        value = store.get("files", "processed")
        assert isinstance(value, list)
        value.append("b.csv")

        assert store.get("files", "processed") == ["a.csv"]

    def test_concurrent_writes_are_not_lost(
        self, store: contract.IStateStore
    ) -> None:
        def write(index: int) -> None:
            store.set("parallel", f"key-{index}", index)

        list(utils.bounded_map(write, range(50), max_workers=8))

        assert [store.get("parallel", f"key-{i}") for i in range(50)] == list(
            range(50)
        )


def test_json_store_survives_a_restart(tmp_path: pathlib.Path) -> None:
    state.JsonFileStateStore(tmp_path).set("users", "watermark", {"id": 42})

    reopened = state.JsonFileStateStore(tmp_path)

    assert reopened.get("users", "watermark") == {"id": 42}
    content = json.loads(reopened.path_for("users").read_text(encoding="utf-8"))
    assert content == {"watermark": {"id": 42}}


def test_json_store_refuses_a_corrupted_file(tmp_path: pathlib.Path) -> None:
    store = state.JsonFileStateStore(tmp_path)
    store.path_for("users").write_text("[1, 2]", encoding="utf-8")

    with pytest.raises(ValueError, match="JSON object"):
        store.get("users", "watermark")
