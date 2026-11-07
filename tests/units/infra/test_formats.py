from __future__ import annotations

import io

import pytest

from core import contract
from infra import formats


def test_csv_with_bom_and_custom_delimiter() -> None:
    stream = io.BytesIO("﻿id;name\n1;Ana\n2;Bo\n".encode())

    rows = list(formats.CsvFormat(delimiter=";").read(stream))

    assert rows == [{"id": "1", "name": "Ana"}, {"id": "2", "name": "Bo"}]
    assert not stream.closed  # the caller owns the stream


def test_jsonl_skips_blank_lines() -> None:
    stream = io.BytesIO(b'{"id": 1}\n\n{"id": 2}\n')

    assert list(formats.JsonLinesFormat().read(stream)) == [
        {"id": 1},
        {"id": 2},
    ]


def test_jsonl_rejects_non_objects() -> None:
    stream = io.BytesIO(b'{"id": 1}\n[1, 2]\n')
    rows = formats.JsonLinesFormat().read(stream)

    with pytest.raises(contract.FatalError, match="line 2"):
        list(rows)
