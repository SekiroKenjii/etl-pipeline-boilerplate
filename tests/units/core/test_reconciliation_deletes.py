"""`MissingSourceRows`: the same deletes as a set of every key would find,
in memory bounded whatever the size of the table."""

from __future__ import annotations

from collections.abc import Collection, Generator, Iterator, Sequence
import pathlib
import tempfile
import tracemalloc

from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
import pytest

from core import contract
from core import keys as core_keys
from core import operators
from tests.factories import fake_ports

ENTITY = "crm.customer"
TABLE = "legacy.cust"
MEGABYTE = 1024 * 1024
# Coprime with the counts below: `i * STRIDE % count` walks every key once,
# in an order that is not the keys' own.
STRIDE = 7919


class _Keys(contract.ITableReader):
    """A source of `count` keys `C0`.. `C<count-1>`, streamed, never held,
    but for those `missing`."""

    def __init__(self, count: int, missing: Collection[int] = ()) -> None:
        self.total = count
        self.missing = frozenset(missing)

    def read(self, query: contract.TableQuery) -> Iterator[contract.Row]:
        for i in range(self.total):
            if i not in self.missing:
                yield {"cust_no": f"C{i}"}

    def max_value(
        self,
        table: str,
        column: str,
        filters: Sequence[contract.Condition] = (),
    ) -> object:
        return None

    def count(
        self, table: str, filters: Sequence[contract.Condition] = ()
    ) -> int:
        return self.total - len(self.missing)


class _Registry(contract.IKeyRegistry):
    """A crosswalk of `count` keys, streamed in an order of its own, as a
    database orders them by surrogate key: not the order of the natural
    keys."""

    def __init__(self, count: int) -> None:
        self.total = count

    def order(self) -> Iterator[int]:
        """The keys, in the order the entries come."""
        for position in range(self.total):
            yield position * STRIDE % self.total

    def register(self, entries: Sequence[contract.KeyEntry]) -> None:
        raise NotImplementedError

    def known(self, entity: str, surrogate_keys: Collection[str]) -> set[str]:
        raise NotImplementedError

    def entries(self, entity: str) -> Iterator[contract.KeyEntry]:
        for i in self.order():
            yield contract.KeyEntry(entity, f"k{i}", (f"C{i}",))

    def natural_key(
        self, entity: str, surrogate_key: str
    ) -> tuple[str, ...] | None:
        raise NotImplementedError

    def forget(self, entity: str, natural_key: Sequence[str]) -> list[str]:
        raise NotImplementedError


def _oracle(
    reader: contract.ITableReader,
    key: Sequence[str],
    registry: contract.IKeyRegistry,
    case: contract.KeyCase,
) -> list[tuple[str, ...]]:
    """The deletes as the first implementation found them: every source key
    in a set, then the registry's entries against it, in their order."""
    query = contract.TableQuery(TABLE, order_by=tuple(key), columns=tuple(key))
    present = {
        core_keys.canonical([row[c] for c in key], case)
        for row in reader.read(query)
    }
    return [
        entry.natural_key
        for entry in registry.entries(ENTITY)
        if entry.natural_key not in present
    ]


def _extractor(
    reader: contract.ITableReader,
    registry: contract.IKeyRegistry,
    key: Sequence[str] = ("cust_no",),
    **options: int | contract.KeyCase | None,
) -> operators.MissingSourceRows:
    return operators.MissingSourceRows(
        reader,
        TABLE,
        key=key,
        registry=registry,
        entity=ENTITY,
        max_share=None,
        **options,  # type: ignore[arg-type]
    )


def _context() -> contract.ETLContext:
    return contract.ETLContext(contract.ETLRunInfo("crm"))


def _registry_of(
    natural_keys: Sequence[tuple[str, ...]],
) -> core_keys.InMemoryKeyRegistry:
    """A registry streaming its entries in the order given; two entries may
    share a natural key, as two key versions would."""
    registry = core_keys.InMemoryKeyRegistry()
    registry.register([
        contract.KeyEntry(ENTITY, f"k{i}", natural)
        for i, natural in enumerate(natural_keys)
    ])
    return registry


class TestBoundedMemory:

    def test_many_keys_are_compared_in_bounded_memory(self) -> None:
        # A set of every key peaked at 67 MB for these 300,000 (223 MB for a
        # million); the sorted streams peak at 16 MB whatever the count.
        missing = frozenset(range(0, 300_000, 1_000))
        registry = _Registry(300_000)
        extractor = _extractor(_Keys(300_000, missing), registry)

        tracemalloc.start()
        try:
            events = list(extractor.extract(_context()))
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        assert [e.row["cust_no"] for e in events] == [
            f"C{i}" for i in registry.order() if i in missing
        ]
        assert peak < 32 * MEGABYTE, f"peak {peak / MEGABYTE:.0f} MB"

    def test_the_spilled_keys_go_with_the_extractor(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
        extractor = _extractor(
            _Keys(50, range(0, 50, 5)), _Registry(50), spill_rows=4
        )

        events = extractor.extract(_context())
        assert isinstance(events, Generator)
        first = next(events)
        assert first.row == {"cust_no": "C0"}
        assert list(tmp_path.iterdir())
        events.close()

        assert list(tmp_path.iterdir()) == []

    def test_a_source_failing_half_way_leaves_no_key_on_disk(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))

        class _Failing(_Keys):

            def read(
                self, query: contract.TableQuery
            ) -> Iterator[contract.Row]:
                yield from super().read(query)
                raise contract.RetryableError("connection lost")

        extractor = _extractor(_Failing(50), _Registry(50), spill_rows=4)
        events = extractor.extract(_context())

        with pytest.raises(contract.RetryableError, match="connection lost"):
            list(events)

        assert list(tmp_path.iterdir()) == []

    def test_spill_rows_must_be_positive(self) -> None:
        reader = _Keys(1)
        registry = _Registry(1)

        with pytest.raises(ValueError, match="spill_rows"):
            _extractor(reader, registry, spill_rows=0)


class TestSameDeletesAsASetWouldFind:

    def test_one_delete_per_entry_whatever_the_duplicates(self) -> None:
        registry = _registry_of([("C2",), ("C1",), ("C2",), ("C3",)])
        reader = fake_ports.InMemoryTableReader(
            {TABLE: [{"cust_no": "c3"}, {"cust_no": " C3 "}, {"cust_no": "c0"}]}
        )
        case = contract.KeyCase.UPPER
        extractor = _extractor(reader, registry, case=case)

        events = list(extractor.extract(_context()))

        assert [e.key for e in events] == [("C2",), ("C1",), ("C2",)]
        assert [e.key for e in events] == _oracle(
            reader, ["cust_no"], registry, case
        )
        assert events[0].row == {"cust_no": "C2"}
        assert all(
            e.operation is contract.ChangeOperation.DELETE for e in events
        )
        assert all(e.table == TABLE for e in events)

    def test_a_registry_without_entries_deletes_nothing(self) -> None:
        reader = fake_ports.InMemoryTableReader({TABLE: [{"cust_no": "C1"}]})

        events = list(_extractor(reader, _registry_of([])).extract(_context()))

        assert events == []

    def test_the_source_is_read_by_key_with_the_filters(self) -> None:
        reader = fake_ports.InMemoryTableReader(
            {TABLE: [{"cust_no": "C1", "region": "N"}]}
        )
        filters = (contract.Condition("region", contract.Operator.EQ, "N"),)
        extractor = operators.MissingSourceRows(
            reader,
            TABLE,
            key=["cust_no"],
            registry=_registry_of([("C1",), ("C2",)]),
            entity=ENTITY,
            filters=filters,
            max_share=None,
        )

        events = list(extractor.extract(_context()))

        assert [e.key for e in events] == [("C2",)]
        assert reader.queries == [
            contract.TableQuery(
                TABLE,
                order_by=("cust_no",),
                columns=("cust_no",),
                filters=filters,
            )
        ]


_TEXT = st.text(alphabet="abAB -", min_size=1, max_size=3).filter(str.strip)
_CANONICAL = st.text(alphabet="AB-", min_size=1, max_size=3)


@settings(deadline=None)
@given(
    registry_keys=st.lists(_CANONICAL, max_size=10),
    source=st.one_of(
        st.lists(_TEXT, max_size=10), st.lists(st.integers(0, 12), max_size=10)
    ),
    spill_rows=st.integers(1, 4),
)
def test_one_column_keys_whatever_their_case_and_spaces(
    registry_keys: list[str], source: list[str] | list[int], spill_rows: int
) -> None:
    registry = _registry_of([(natural,) for natural in registry_keys])
    reader = fake_ports.InMemoryTableReader(
        {TABLE: [{"cust_no": value} for value in source]}
    )
    case = contract.KeyCase.UPPER
    extractor = _extractor(reader, registry, case=case, spill_rows=spill_rows)

    if registry_keys and not source:
        refused = extractor.extract(_context())
        with pytest.raises(contract.PipelineConfigError, match="no row"):
            list(refused)
        return
    events = list(extractor.extract(_context()))

    assert [e.key for e in events] == _oracle(
        reader, ["cust_no"], registry, case
    )


@settings(deadline=None)
@given(
    registry_keys=st.lists(st.tuples(_CANONICAL, _CANONICAL), max_size=10),
    source=st.lists(st.tuples(_TEXT, _TEXT), max_size=10),
    spill_rows=st.integers(1, 4),
)
def test_composite_keys(
    registry_keys: list[tuple[str, str]],
    source: list[tuple[str, str]],
    spill_rows: int,
) -> None:
    registry = _registry_of(registry_keys)
    reader = fake_ports.InMemoryTableReader({
        TABLE: [{"cust_no": first, "site": second} for first, second in source]
    })
    key = ("cust_no", "site")
    case = contract.KeyCase.UPPER
    extractor = _extractor(
        reader, registry, key=key, case=case, spill_rows=spill_rows
    )

    if registry_keys and not source:
        refused = extractor.extract(_context())
        with pytest.raises(contract.PipelineConfigError, match="no row"):
            list(refused)
        return
    events = list(extractor.extract(_context()))

    assert [e.key for e in events] == _oracle(reader, key, registry, case)
    assert all(e.row == dict(zip(key, e.key)) for e in events)
