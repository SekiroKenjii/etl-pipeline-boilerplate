from __future__ import annotations

import pathlib

import pytest

from infra import storage


def _write(root: pathlib.Path, key: str, text: str) -> pathlib.Path:
    path = root / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_lists_files_recursively_by_key(tmp_path: pathlib.Path) -> None:
    _write(tmp_path, "b.csv", "x")
    _write(tmp_path, "2026/10/a.csv", "x")
    _write(tmp_path, "2026/09/z.csv", "x")

    store = storage.LocalObjectStore(tmp_path)

    assert [o.key for o in store.list()] == [
        "2026/09/z.csv",
        "2026/10/a.csv",
        "b.csv",
    ]
    assert [o.key for o in store.list("2026/10/")] == ["2026/10/a.csv"]


def test_version_changes_with_the_content(tmp_path: pathlib.Path) -> None:
    path = _write(tmp_path, "a.csv", "id\n1\n")
    store = storage.LocalObjectStore(tmp_path)
    before = next(store.list()).version

    path.write_text("id\n1\n2\n", encoding="utf-8")

    assert next(store.list()).version != before


def test_open_reads_the_content(tmp_path: pathlib.Path) -> None:
    _write(tmp_path, "a.csv", "id\n1\n")
    store = storage.LocalObjectStore(tmp_path)

    with store.open(next(store.list())) as stream:
        assert stream.read() == b"id\n1\n"


def test_missing_root_is_an_error(tmp_path: pathlib.Path) -> None:
    objects = storage.LocalObjectStore(tmp_path / "missing").list()

    with pytest.raises(FileNotFoundError):
        list(objects)
