"""Finding the rows deleted at a source that has no change log."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence

from core import contract
from core import keys
from core import pipeline


class MissingSourceRows(pipeline.IDataExtractor[contract.ChangeEvent]):
    """Yields a DELETE event for every loaded row gone from the source.

    For sources without change data capture: compares the natural keys of the
    source with those of the key registry, and yields the missing ones, to load
    with a `ChangeLoader` through the same mapping.

        MissingSourceRows(
            reader, "legacy.CUSTOMER", key=["cust_no"],
            registry=registry, entity="crm.customer",
        )

    Every key of the source is held in memory: reconcile a partition at a time
    with `filters` for very large tables. The filters must select the same rows
    as the load, or rows outside them pass for deleted. Placeholders of inferred
    parents never loaded from the source count as deleted too.

    Nothing is deleted, and the step fails, when the source is empty while rows
    were loaded, or when more than `max_share` of the loaded rows are missing,
    unless the run confirms the count: `--param
    confirm_deletes=<entity>:<count>`.

    Attributes:
        reader: Reads the source.
        table: The source table.
        key: The columns of the natural key, as mapped to the entity's key.
        registry: The crosswalk of the loaded keys.
        entity: The entity loaded from the table.
        max_share: The share of the loaded rows deleted without confirmation.
        confirm_param: The run parameter confirming more deletes.
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
        max_share: float | None = 0.5,
        confirm_param: str = "confirm_deletes",
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

        Raises:
            ValueError: If `key` is empty, or `max_share` is not a share.
        """
        if not key:
            raise ValueError("key must name at least one column")
        if max_share is not None and not 0.0 <= max_share <= 1.0:
            raise ValueError("max_share must be between 0.0 and 1.0")
        self.reader = reader
        self.table = table
        self.key = tuple(key)
        self.registry = registry
        self.entity = entity
        self.max_share = max_share
        self.confirm_param = confirm_param
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
            FatalError: If the deletes look like a misconfigured source, and
                the run does not confirm them.
        """
        query = contract.TableQuery(
            self.table,
            order_by=self.key,
            columns=self.key,
            filters=self._filters,
        )
        present = {
            keys.canonical([row[c] for c in self.key], self._case)
            for row in self.reader.read(query)
        }
        loaded = 0
        missing: list[tuple[str, ...]] = []
        for entry in self.registry.entries(self.entity):
            loaded += 1
            if entry.natural_key not in present:
                missing.append(entry.natural_key)
        self._check(len(missing), loaded, not present, ctx)

        for natural_key in missing:
            yield contract.ChangeEvent(
                table=self.table,
                operation=contract.ChangeOperation.DELETE,
                row=dict(zip(self.key, natural_key)),
                key=natural_key,
            )

    def _check(
        self, deletes: int, loaded: int, empty: bool, ctx: contract.ETLContext
    ) -> None:
        """Refuses deletes that look like a misconfigured source.

        Raises:
            FatalError: If the source is empty, or too many rows are missing,
                and the run does not confirm that number.
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
        raise contract.FatalError(
            f"{self.name}: refusing to delete {deletes} of the {loaded} rows"
            f" of {self.entity} loaded: {why}. Check the source; if they are"
            " deleted indeed, run again with"
            f" `--param {self.confirm_param}={self.entity}:{deletes}`"
        )


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
