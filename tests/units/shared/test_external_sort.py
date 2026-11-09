"""The external sort: sorted and stable, memory bounded, runs removed."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
import gc
import json
import pathlib
import stat
from typing import BinaryIO

from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
import pytest

from shared import utils
from shared.utils import external_sort

type Item = tuple[int, int]


def _first(item: Item) -> int:
    return item[0]


def _only_child(directory: pathlib.Path) -> pathlib.Path:
    (child,) = directory.iterdir()
    return child


class TestExternalSorter:

    def test_a_few_items_are_sorted_in_memory(
        self, tmp_path: pathlib.Path
    ) -> None:
        with utils.ExternalSorter(
            _first, spill_rows=10, directory=tmp_path
        ) as sorter:
            sorter.extend([(3, 0), (1, 1), (2, 2)])

            assert list(sorter) == [(1, 1), (2, 2), (3, 0)]
            assert len(sorter) == 3
            assert sorter.spill_directory is None
            assert sorter.runs_written == 0
        assert list(tmp_path.iterdir()) == []

    def test_items_past_the_memory_are_sorted_through_runs_on_disk(
        self, tmp_path: pathlib.Path
    ) -> None:
        items = [(i % 7, i) for i in range(20)]
        with utils.ExternalSorter(
            _first, spill_rows=3, directory=tmp_path
        ) as sorter:
            sorter.extend(items)
            directory = sorter.spill_directory

            assert directory is not None
            assert directory.parent == tmp_path
            assert stat.S_IMODE(directory.stat().st_mode) == 0o700
            assert sorter.runs_written == 6
            assert len(list(directory.iterdir())) == 6

            assert list(sorter) == sorted(items, key=_first)
            # The 2 items left in memory made a run; each run went as read.
            assert sorter.runs_written == 7
            assert list(directory.iterdir()) == []
        assert not directory.exists()

    def test_equal_keys_keep_the_order_they_came_in(
        self, tmp_path: pathlib.Path
    ) -> None:
        items = [(i % 3, i) for i in range(30)]

        found = list(
            utils.sorted_spilling(
                items, _first, spill_rows=4, directory=tmp_path
            )
        )

        assert found == sorted(items, key=_first)
        assert [i for key, i in found if key == 1] == list(range(1, 30, 3))

    def test_runs_past_the_fan_in_are_merged_by_tiers(
        self, tmp_path: pathlib.Path
    ) -> None:
        items = [(i % 5, i) for i in range(10)]
        with utils.ExternalSorter(
            _first, spill_rows=1, fan_in=2, directory=tmp_path
        ) as sorter:
            sorter.extend(items)
            directory = sorter.spill_directory

            # 10 runs of 1: tiers of 2, 4, 8 made; 8 + 2 are left.
            assert directory is not None
            assert len(list(directory.iterdir())) == 2
            assert sorter.runs_written == 10 + 5 + 2 + 1
            assert list(sorter) == sorted(items, key=_first)

    def test_a_merge_of_tiers_keeps_equal_keys_in_order(
        self, tmp_path: pathlib.Path
    ) -> None:
        items = [(i % 3, i) for i in range(100)]

        found = list(
            utils.sorted_spilling(
                items, _first, spill_rows=1, fan_in=2, directory=tmp_path
            )
        )

        assert found == sorted(items, key=_first)

    def test_an_exception_mid_iteration_removes_the_runs(
        self, tmp_path: pathlib.Path
    ) -> None:
        sorter = utils.ExternalSorter(_first, spill_rows=2, directory=tmp_path)
        sorter.extend((i, i) for i in range(9))
        directory = sorter.spill_directory
        assert directory is not None

        with pytest.raises(RuntimeError, match="stop"):
            with sorter:
                for item in sorter:
                    if item == (3, 3):
                        raise RuntimeError("stop")

        assert not directory.exists()
        assert list(tmp_path.iterdir()) == []

    def test_a_sorter_garbage_collected_removes_its_runs(
        self, tmp_path: pathlib.Path
    ) -> None:
        sorter = utils.ExternalSorter(_first, spill_rows=2, directory=tmp_path)
        sorter.extend((i, i) for i in range(5))
        directory = sorter.spill_directory
        assert directory is not None
        assert directory.exists()

        del sorter
        gc.collect()

        assert not directory.exists()

    def test_closing_forgets_the_items(self, tmp_path: pathlib.Path) -> None:
        sorter = utils.ExternalSorter(_first, spill_rows=2, directory=tmp_path)
        sorter.extend((i, i) for i in range(5))

        sorter.close()
        sorter.close()

        assert list(sorter) == []
        assert sorter.spill_directory is None
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.parametrize("items", [[], [(1, 1)]])
    def test_nothing_or_one_item_below_the_spill_touches_no_disk(
        self, tmp_path: pathlib.Path, items: list[Item]
    ) -> None:
        with utils.ExternalSorter(
            _first, spill_rows=2, directory=tmp_path
        ) as sorter:
            sorter.extend(items)

            assert list(sorter) == items
            assert sorter.spill_directory is None
        assert list(tmp_path.iterdir()) == []

    def test_one_item_at_a_spill_of_one_goes_through_one_run(
        self, tmp_path: pathlib.Path
    ) -> None:
        with utils.ExternalSorter(
            _first, spill_rows=1, directory=tmp_path
        ) as sorter:
            sorter.extend([(1, 1)])

            assert sorter.runs_written == 1
            assert list(sorter) == [(1, 1)]
        assert list(tmp_path.iterdir()) == []

    def test_text_keys_with_a_lone_surrogate_round_trip(
        self, tmp_path: pathlib.Path
    ) -> None:
        items = ["b", "\ud800", "a", "é"]

        found = list(
            utils.sorted_spilling(items, str, spill_rows=1, directory=tmp_path)
        )

        assert found == sorted(items)

    def test_the_runs_are_written_in_the_format_given(
        self, tmp_path: pathlib.Path
    ) -> None:
        with utils.ExternalSorter(
            _first,
            spill_rows=2,
            directory=tmp_path,
            spill_format=_JsonLines(),
        ) as sorter:
            sorter.extend([(2, 0), (1, 1), (3, 2)])
            directory = sorter.spill_directory
            assert directory is not None
            lines = _only_child(directory).read_text().splitlines()

            assert [json.loads(line) for line in lines] == [[1, 1], [2, 0]]
            assert list(sorter) == [(1, 1), (2, 0), (3, 2)]

    @pytest.mark.parametrize(
        ("spill_rows", "fan_in"), [(0, 64), (-1, 64), (1, 1), (1, 0)]
    )
    def test_the_bounds_are_checked(self, spill_rows: int, fan_in: int) -> None:
        with pytest.raises(ValueError, match="spill_rows"):
            utils.ExternalSorter(_first, spill_rows=spill_rows, fan_in=fan_in)

    def test_a_run_holds_chunks_of_the_memory_shared_by_the_fan_in(
        self, tmp_path: pathlib.Path
    ) -> None:
        with utils.ExternalSorter(
            _first, spill_rows=6, fan_in=3, directory=tmp_path
        ) as sorter:
            sorter.extend((i % 4, i) for i in range(9))
            directory = sorter.spill_directory
            assert directory is not None
            pickles = _only_child(directory).read_bytes().count(b"\x80\x05")

            # 6 items a run, 2 (6 // 3) a pickle.
            assert pickles == 3
            assert list(sorter) == sorted(
                ((i % 4, i) for i in range(9)), key=_first
            )

    def test_a_chunk_below_one_is_refused(self) -> None:
        with pytest.raises(ValueError, match="chunk"):
            external_sort.PickleFormat[int](0)


class TestSortedSpilling:

    def test_yields_sorted_and_leaves_nothing_on_disk(
        self, tmp_path: pathlib.Path
    ) -> None:
        items = [(i % 4, i) for i in range(11)]

        found = list(
            utils.sorted_spilling(
                items, _first, spill_rows=2, directory=tmp_path
            )
        )

        assert found == sorted(items, key=_first)
        assert list(tmp_path.iterdir()) == []

    def test_an_iterator_dropped_half_way_removes_the_runs(
        self, tmp_path: pathlib.Path
    ) -> None:
        items = [(i, i) for i in range(6)]
        found = utils.sorted_spilling(
            items, _first, spill_rows=2, directory=tmp_path
        )

        assert next(found) == (0, 0)
        directory = _only_child(tmp_path)
        assert directory.exists()
        found.close()

        assert not directory.exists()


class _JsonLines:
    """Runs of JSON lines, one item each."""

    def dump(self, items: Iterable[Item], file: BinaryIO) -> None:
        for item in items:
            file.write(json.dumps(item).encode())
            file.write(b"\n")

    def load(self, file: BinaryIO) -> Iterator[Item]:
        for line in file:
            first, second = json.loads(line)
            yield int(first), int(second)


@pytest.fixture(scope="module")
def scratch(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    return tmp_path_factory.mktemp("external-sort")


@settings(deadline=None)
@given(
    items=st.lists(st.tuples(st.integers(-3, 3), st.integers())),
    spill_rows=st.integers(1, 6),
    fan_in=st.integers(2, 4),
)
def test_equals_sorted_whatever_the_spill(
    scratch: pathlib.Path, items: list[Item], spill_rows: int, fan_in: int
) -> None:
    with utils.ExternalSorter(
        _first, spill_rows=spill_rows, fan_in=fan_in, directory=scratch
    ) as sorter:
        sorter.extend(items)

        assert list(sorter) == sorted(items, key=_first)
    assert list(scratch.iterdir()) == []


@settings(deadline=None)
@given(
    items=st.lists(st.tuples(st.text(max_size=4), st.integers())),
    spill_rows=st.integers(1, 5),
)
def test_text_keys_sort_as_sorted_does(
    scratch: pathlib.Path, items: list[tuple[str, int]], spill_rows: int
) -> None:
    found = list(
        utils.sorted_spilling(
            items,
            lambda item: item[0],
            spill_rows=spill_rows,
            directory=scratch,
        )
    )

    assert found == sorted(items, key=lambda item: item[0])
