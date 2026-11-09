"""Finding the rows deleted at a source that has no change log."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
import contextlib
import operator

from core import contract
from core import keys
from core import pipeline
from shared import utils

SPILL_ROWS = 50_000

# A canonical natural key, and the position of its entry in the registry.
type _Loaded = tuple[tuple[str, ...], int]

_natural_key = operator.itemgetter(0)
_position = operator.itemgetter(1)


class DeletesNotConfirmed(contract.PipelineConfigError):
    """Deletes that look like a misconfigured source, not confirmed by the
    run: an operator must check, and confirm them."""


class MissingSourceRows(pipeline.IDataExtractor[contract.ChangeEvent]):
    """Yields a DELETE event for every loaded row gone from the source.

    For sources without change data capture: compares the natural keys of the
    source with those of the key registry, and yields the missing ones, to load
    with a `ChangeLoader` through the same mapping.

        MissingSourceRows(
            reader, "legacy.CUSTOMER", key=["cust_no"],
            registry=registry, entity="crm.customer",
        )

    The keys of both sides are compared as two sorted streams, spilled to
    disk past `spill_rows` (`utils.ExternalSorter`): memory holds that many
    keys or so, whatever the size of the table. The deletes come in the order
    of the registry. The filters must select the same rows as the load, or rows
    outside them pass for deleted. Placeholders of inferred parents never
    loaded from the source count as deleted too.

    Nothing is deleted, and the step fails, when the source is empty while rows
    were loaded, or when more than `max_share` of the loaded rows are missing
    (a tenth by default), unless the run confirms the count: `--param
    confirm_deletes=<entity>:<count>`. The refusal is a `DeletesNotConfirmed`,
    a `PipelineConfigError`: the run exits 2, which schedulers do not retry,
    as retrying would refuse again.

    Attributes:
        reader: Reads the source.
        table: The source table.
        key: The columns of the natural key, as mapped to the entity's key.
        registry: The crosswalk of the loaded keys.
        entity: The entity loaded from the table.
        max_share: The share of the loaded rows deleted without confirmation.
        confirm_param: The run parameter confirming more deletes.
        spill_rows: The keys held in memory before they are sorted on disk.
    """

    def __init__(
        self,
        reader: contract.ITableReader,
        table: str,
        *,
        key: Sequence[str],
        registry: contract.IKeyRegistry,
        entity: str,
        filters: Sequence[contract.Condition] = (),
        case: contract.KeyCase = contract.KeyCase.PRESERVE,
        max_share: float | None = 0.1,
        confirm_param: str = "confirm_deletes",
        spill_rows: int = SPILL_ROWS,
    ) -> None:
        """Initializes the extractor.

        Args:
            reader: Reads the source.
            table: The source table.
            key: The columns of the natural key, in the order of the
                mapping's primary key sources.
            registry: The crosswalk of the loaded keys.
            entity: The entity loaded from the table.
            filters: Only the source rows matching these, as for the load.
            case: The key case of the mapping, as natural keys are compared
                in canonical form.
            max_share: The share of the loaded rows, from 0.0 to 1.0, that may
                be deleted without confirmation; None for no limit. A source
                without any row is refused all the same.
            confirm_param: The run parameter confirming more deletes, as
                `<entity>:<count>[,<entity>:<count>...]`.
            spill_rows: The keys held in memory before they are sorted on
                disk.

        Raises:
            ValueError: If `key` is empty, `max_share` is not a share, or
                `spill_rows` is below 1.
        """
        if not key:
            raise ValueError("key must name at least one column")
        if max_share is not None and not 0.0 <= max_share <= 1.0:
            raise ValueError("max_share must be between 0.0 and 1.0")
        if spill_rows < 1:
            raise ValueError("spill_rows must be >= 1")
        self.reader = reader
        self.table = table
        self.key = tuple(key)
        self.registry = registry
        self.entity = entity
        self.max_share = max_share
        self.confirm_param = confirm_param
        self.spill_rows = spill_rows
        self._filters = tuple(filters)
        self._case = case

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.table})"

    def extract(
        self, ctx: contract.ETLContext
    ) -> Iterator[contract.ChangeEvent]:
        """Yields the deletes, once their number is checked.

        Raises:
            DeletesNotConfirmed: If the deletes look like a misconfigured
                source, and the run does not confirm them.
            FatalError: If the confirmation is not `<entity>:<count>`.
        """
        query = contract.TableQuery(
            self.table,
            order_by=self.key,
            columns=self.key,
            filters=self._filters,
        )
        with contextlib.ExitStack() as stack:
            present = self._sorted(
                stack,
                _same,
                (
                    keys.canonical([row[c] for c in self.key], self._case)
                    for row in self.reader.read(query)
                ),
            )
            loaded = self._sorted(
                stack,
                _natural_key,
                (
                    (entry.natural_key, position)
                    for position, entry in enumerate(
                        self.registry.entries(self.entity)
                    )
                ),
            )
            # Sorted back by position: the deletes in the registry's order.
            missing = self._sorted(
                stack, _position, _absent(iter(present), loaded)
            )
            self._check(len(missing), len(loaded), len(present) == 0, ctx)

            for natural_key, _ in missing:
                yield contract.ChangeEvent(
                    table=self.table,
                    operation=contract.ChangeOperation.DELETE,
                    row=dict(zip(self.key, natural_key)),
                    key=natural_key,
                )

    def _sorted[T](
        self,
        stack: contextlib.ExitStack,
        key: Callable[[T], utils.Comparable],
        items: Iterable[T],
    ) -> utils.ExternalSorter[T]:
        """The items, sorted by `key` on disk past `spill_rows`; the stack
        removes the files."""
        sorter = stack.enter_context(
            utils.ExternalSorter[T](
                key, spill_rows=self.spill_rows, prefix="etl-deletes-"
            )
        )
        sorter.extend(items)
        return sorter

    def _check(
        self, deletes: int, loaded: int, empty: bool, ctx: contract.ETLContext
    ) -> None:
        """Refuses deletes that look like a misconfigured source.

        Raises:
            DeletesNotConfirmed: If the source is empty, or too many rows are
                missing, and the run does not confirm that number.
        """
        if not deletes:
            return
        share = deletes / loaded
        if empty:
            why = "the source has no row"
        elif self.max_share is not None and share > self.max_share:
            why = f"{share:.1%} of them, above {self.max_share:.1%}"
        else:
            return
        confirmed = _confirmed(ctx.run.params.get(self.confirm_param, ""))
        if confirmed.get(self.entity) == deletes:
            return
        raise DeletesNotConfirmed(
            f"{self.name}: refusing to delete {deletes} of the {loaded} rows"
            f" of {self.entity} loaded: {why}. Check the source; if they are"
            " deleted indeed, run again with"
            f" `--param {self.confirm_param}={self.entity}:{deletes}`"
        )


def _same(natural_key: tuple[str, ...]) -> tuple[str, ...]:
    return natural_key


def _absent(
    present: Iterator[tuple[str, ...]], loaded: Iterable[_Loaded]
) -> Iterator[_Loaded]:
    """The loaded keys the source lacks, from two streams sorted by key;
    a key loaded twice is absent twice, one present several times once."""
    current = next(present, None)
    for natural_key, position in loaded:
        while current is not None and current < natural_key:
            current = next(present, None)
        if current != natural_key:
            yield natural_key, position


def _confirmed(value: str) -> Mapping[str, int]:
    """The confirmed deletes by entity, from `<entity>:<count>,...`.

    Raises:
        FatalError: If a confirmation is not `<entity>:<count>`.
    """
    confirmed: dict[str, int] = {}
    for item in filter(None, (part.strip() for part in value.split(","))):
        entity, colon, count = item.rpartition(":")
        if not colon or not entity or not count.isdigit():
            raise contract.FatalError(
                f"Deletes confirmation {item!r} is not <entity>:<count>"
            )
        confirmed[entity] = int(count)
    return confirmed
