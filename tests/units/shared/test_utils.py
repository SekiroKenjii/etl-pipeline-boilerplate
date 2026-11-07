from __future__ import annotations

import collections
import dataclasses
import datetime
import decimal
import enum
import json
import os
import pathlib
import random
import stat
import uuid

import pytest

from shared import utils


def test_utc_now_is_timezone_aware() -> None:
    assert utils.utc_now().tzinfo is datetime.timezone.utc


class TestReservoirSampling:

    def test_keeps_everything_below_capacity(self) -> None:
        sample: list[int] = []
        for seen, item in enumerate([10, 20], start=1):
            utils.reservoir_offer(
                sample, item, seen, capacity=5, rng=random.Random(0)
            )

        assert sample == [10, 20]

    def test_zero_capacity_keeps_nothing(self) -> None:
        sample: list[int] = []
        utils.reservoir_offer(
            sample, 1, seen=1, capacity=0, rng=random.Random(0)
        )

        assert sample == []

    def test_every_item_has_the_same_chance(self) -> None:
        rng = random.Random(42)
        hits = collections.Counter[int]()
        trials = 20_000

        for _ in range(trials):
            sample: list[int] = []
            for seen, item in enumerate(range(10), start=1):
                utils.reservoir_offer(sample, item, seen, capacity=3, rng=rng)
            hits.update(sample)

        assert all(0.28 < hits[item] / trials < 0.32 for item in range(10))

    def test_merge_of_two_streams_stays_uniform(self) -> None:
        rng = random.Random(7)
        hits = collections.Counter[int]()
        trials = 20_000

        for _ in range(trials):
            first = _reservoir(range(0, 6), capacity=3, rng=rng)
            second = _reservoir(range(6, 10), capacity=3, rng=rng)
            hits.update(
                utils.merge_reservoirs(first, 6, second, 4, capacity=3, rng=rng)
            )

        assert all(0.28 < hits[item] / trials < 0.32 for item in range(10))

    def test_merge_of_small_samples_keeps_them_all(self) -> None:
        merged = utils.merge_reservoirs(
            [1], 1, [2, 3], 2, capacity=5, rng=random.Random(0)
        )

        assert sorted(merged) == [1, 2, 3]


def _reservoir(items: range, capacity: int, rng: random.Random) -> list[int]:
    sample: list[int] = []
    for seen, item in enumerate(items, start=1):
        utils.reservoir_offer(sample, item, seen, capacity, rng)
    return sample


@dataclasses.dataclass
class _Point:
    x: int
    at: datetime.datetime


class _Color(enum.Enum):
    RED = "red"


def test_to_jsonable_handles_common_types() -> None:
    at = datetime.datetime(2026, 1, 2, tzinfo=datetime.timezone.utc)
    payload = {
        "point": _Point(1, at),
        "color": _Color.RED,
        "path": pathlib.Path("/tmp/x"),
        "tags": {"a"},
        "other": object,
    }

    decoded = json.loads(json.dumps(payload, default=utils.to_jsonable))

    assert decoded["point"] == {"x": 1, "at": "2026-01-02T00:00:00+00:00"}
    assert decoded["color"] == "red"
    assert decoded["path"] == "/tmp/x"
    assert decoded["tags"] == ["a"]
    assert decoded["other"] == "<class 'object'>"


class TestJsonLinesWriter:

    def test_appends_one_object_per_line(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "nested" / "out.jsonl"
        writer = utils.JsonLinesWriter()

        writer.open(path)
        writer.write([{"a": 1}, {"a": 2}])
        writer.close()
        writer.close()  # idempotent

        lines = path.read_text(encoding="utf-8").splitlines()
        assert [json.loads(line) for line in lines] == [{"a": 1}, {"a": 2}]

    def test_write_requires_open(self) -> None:
        writer = utils.JsonLinesWriter()

        with pytest.raises(RuntimeError, match="before open"):
            writer.write([{"a": 1}])


class TestTagged:

    def test_values_keep_their_type_through_json(self) -> None:
        value = {
            "at": datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.UTC),
            "day": datetime.date(2026, 1, 2),
            "time": datetime.time(3, 4),
            "amount": decimal.Decimal("10.50"),
            "id": uuid.UUID(int=7),
            "nested": [1, 2.5, True, None, "x", (3, 4)],
            "odd": {"$type": "not a tag"},
            "rowversion": b"\x00\x00\x00\x00\x00\x00\x07\xd1",
        }

        decoded = utils.from_tagged(
            json.loads(json.dumps(utils.to_tagged(value)))
        )

        assert decoded == {**value, "nested": [1, 2.5, True, None, "x", [3, 4]]}

    def test_values_without_a_lossless_form_are_refused(self) -> None:
        with pytest.raises(TypeError, match="complex"):
            utils.to_tagged(1j)

    def test_adapters_register_their_types(self) -> None:
        class Ident:

            def __init__(self, text: str) -> None:
                self.text = text

            def __eq__(self, other: object) -> bool:
                return isinstance(other, Ident) and other.text == self.text

            __hash__ = None  # type: ignore[assignment]

        utils.register_tag("test-ident", Ident, lambda i: i.text, Ident)

        encoded = json.loads(json.dumps(utils.to_tagged([Ident("x1")])))

        assert encoded == [{"$type": "test-ident", "value": "x1"}]
        assert utils.from_tagged(encoded) == [Ident("x1")]
        with pytest.raises(ValueError, match="built-in"):
            utils.register_tag("uuid", Ident, str, Ident)

    def test_unknown_tags_are_refused(self) -> None:
        with pytest.raises(ValueError, match="Unknown tag"):
            utils.from_tagged({"$type": "money", "value": "1"})


class TestRetry:

    def test_tries_again_on_the_errors_allowed(self) -> None:
        calls: list[int] = []

        def flaky() -> str:
            calls.append(1)
            if len(calls) < 3:
                raise KeyError("busy")
            return "done"

        assert (
            utils.retry(flaky, when=lambda e: isinstance(e, KeyError), delay=0)
            == "done"
        )
        assert len(calls) == 3

    def test_other_errors_and_the_last_one_propagate(self) -> None:
        def broken() -> None:
            raise ValueError("bad")

        with pytest.raises(ValueError, match="bad"):
            utils.retry(broken, when=lambda e: False, delay=0)
        with pytest.raises(ValueError, match="bad"):
            utils.retry(broken, when=lambda e: True, attempts=2, delay=0)
        with pytest.raises(ValueError, match="attempts"):
            utils.retry(broken, when=lambda e: True, attempts=0)


def test_atomic_writes_keep_the_usual_modes(tmp_path: pathlib.Path) -> None:
    """Not the owner-only mode of a temporary file: others may read it."""
    fresh = tmp_path / "fresh.json"
    kept = tmp_path / "kept.json"
    kept.write_text("old", encoding="utf-8")
    kept.chmod(0o640)

    utils.atomic_write_text(fresh, "new")
    utils.atomic_write_text(kept, "new")

    umask = os.umask(0)
    os.umask(umask)
    assert stat.S_IMODE(fresh.stat().st_mode) == 0o666 & ~umask
    assert stat.S_IMODE(kept.stat().st_mode) == 0o640
