"""Orphans: child rows whose parent is not loaded (yet).

One policy per foreign key:

- reject (the default): `reference_rules` reject orphans into the quarantine;
- park: the same rules, with a `quality.ParkingQuarantine` keeping orphans
  in a parking lot, replayed by a `operators.ParkedRecordExtractor` step
  until their parent is loaded. `encode_record`, `decode_record` and
  `record_identity` serialize mapped records for the lot;
- infer: an `InferringLoader` creates a placeholder parent (an inferred
  member) before loading the child; the parent's real row replaces it when
  it arrives. Exclude those foreign keys from `reference_rules`.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
import dataclasses
import logging
from typing import cast

from core import contract
from core import pipeline
from core.mapping import compiled
from core.mapping import results
from shared import utils

_LOG = logging.getLogger(__name__)


def record_identity(record: results.MappedRecord) -> str:
    """Identifies a mapped record in a parking queue: its key, else its source.

    Raises:
        ValueError: If the record has neither a key nor a source key.
    """
    if record.key is not None:
        return record.key
    if record.source_key is not None:
        return f"{record.source_key}#{record.part}"
    raise ValueError(
        f"A {record.mapping} record without key nor source key cannot be"
        " parked: give the mapping a primary key, or the transformer a `key`"
    )


def encode_record(record: results.MappedRecord) -> contract.StateValue:
    """Encodes a mapped record into JSON-compatible data, losslessly.

    Raises:
        TypeError: If a value of the record has no lossless JSON form.
    """
    encoded = {
        "data": utils.to_tagged(record.data),
        "issues": [
            {
                "field": issue.field,
                "message": issue.message,
                "value": _issue_value(issue.value),
            }
            for issue in record.issues
        ],
        "mapping": record.mapping,
        "version": record.version,
        "source_key": record.source_key,
        "part": record.part,
        "entity": record.entity,
        "key": record.key,
        "natural_key": (
            list(record.natural_key) if record.natural_key is not None else None
        ),
        "foreign_keys": {
            field: list(natural)
            for field, natural in record.foreign_keys.items()
        },
        "deleted": record.deleted,
    }
    return cast(contract.StateValue, encoded)


def decode_record(payload: contract.StateValue) -> results.MappedRecord:
    """Decodes what `encode_record` encoded.

    Raises:
        ValueError: If `payload` is not an encoded mapped record.
    """
    if not isinstance(payload, dict):
        raise ValueError("Not an encoded mapped record")
    data = utils.from_tagged(payload["data"])
    issues = payload["issues"]
    natural = payload["natural_key"]
    foreign_keys = payload["foreign_keys"]
    if not (
        isinstance(data, dict)
        and isinstance(issues, list)
        and isinstance(foreign_keys, dict)
    ):
        raise ValueError("Not an encoded mapped record")

    return results.MappedRecord(
        data=cast(dict[str, object], data),
        issues=tuple(
            results.MappingIssue(
                str(issue["field"]),
                str(issue["message"]),
                utils.from_tagged(issue["value"]),
            )
            for issue in issues
            if isinstance(issue, dict)
        ),
        mapping=str(payload["mapping"]),
        version=int(cast(int, payload["version"])),
        source_key=_optional_text(payload["source_key"]),
        part=int(cast(int, payload["part"])),
        entity=str(payload["entity"]),
        key=_optional_text(payload["key"]),
        natural_key=(
            tuple(str(c) for c in natural)
            if isinstance(natural, list)
            else None
        ),
        foreign_keys={
            str(field): tuple(str(c) for c in cast(list[str], natural_key))
            for field, natural_key in foreign_keys.items()
        },
        deleted=bool(payload.get("deleted", False)),
    )


def _issue_value(value: object) -> object:
    """Encodes the value of an issue: losslessly when possible, else as text.

    Issues are for humans: an odd value must not prevent parking a record.
    """
    try:
        return utils.to_tagged(value)
    except TypeError:
        return repr(value)


def _optional_text(value: contract.StateValue) -> str | None:
    return None if value is None else str(value)


@dataclasses.dataclass(frozen=True, slots=True)
class InferredParent:
    """How to create the missing parents of one foreign key.

    Attributes:
        field: The foreign key column of the child mapping.
        loader: Writes the placeholders into the parent table. It must never
            overwrite a real row: e.g. a `TableLoader` in INSERT_MISSING mode.
        placeholder: Builds the placeholder row of a parent from its surrogate
            and natural keys. Flag it (`is_inferred = true`), and let the
            parent mapping set the flag back to false, so that the real row
            replaces the placeholder when it arrives.
    """

    field: str
    loader: pipeline.IDataLoader[contract.Row]
    placeholder: Callable[[str, tuple[str, ...]], contract.Row]


class InferringLoader(pipeline.IDataLoader[results.MappedRecord]):
    """Creates the missing parents of a batch, then loads the batch.

    For each configured foreign key, the parents the key registry does not
    know are written as placeholders (inferred members), and registered, before
    the children are loaded: children are never rejected for a late parent,
    and a parent loaded later overwrites its placeholder (upsert).

    Placeholders are committed before the children: if the children fail, the
    placeholders stay, which is harmless (the real rows replace them).

    Attributes:
        inner: Loads the children.
        mapping: The child mapping.
        registry: The crosswalk of loaded keys.
        parents: How to create the parents, by foreign key.
        inferred: The placeholders created so far.
    """

    def __init__(
        self,
        inner: pipeline.IDataLoader[results.MappedRecord],
        mapping: compiled.CompiledMapping,
        registry: contract.IKeyRegistry,
        parents: Sequence[InferredParent],
    ) -> None:
        """Initializes the loader.

        Args:
            inner: Loads the children.
            mapping: The child mapping.
            registry: The crosswalk of loaded keys.
            parents: How to create the parents, by foreign key.

        Raises:
            PipelineConfigError: If a field is not a foreign key of `mapping`.
        """
        unknown = sorted(
            parent.field
            for parent in parents
            if parent.field not in mapping.references
        )
        if unknown:
            raise contract.PipelineConfigError(
                f"{unknown} are not foreign keys of mapping {mapping.name!r}"
            )
        self.inner = inner
        self.mapping = mapping
        self.registry = registry
        self.parents = tuple(parents)
        self.inferred = 0

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.inner.name})"

    def setup(self, ctx: contract.ETLContext) -> None:
        loaders: list[contract.Lifecycle] = [
            *(parent.loader for parent in self.parents),
            self.inner,
        ]
        ready: list[contract.Lifecycle] = []
        try:
            for loader in loaders:
                loader.setup(ctx)
                ready.append(loader)
        except BaseException:
            for loader in reversed(ready):
                loader.teardown(ctx)
            raise

    def teardown(self, ctx: contract.ETLContext) -> None:
        try:
            self.inner.teardown(ctx)
        finally:
            for parent in reversed(self.parents):
                parent.loader.teardown(ctx)

    def load(
        self, records: Sequence[results.MappedRecord], ctx: contract.ETLContext
    ) -> contract.ETLStats:
        for parent in self.parents:
            self._infer(parent, records, ctx)
        return self.inner.load(records, ctx)

    def _infer(
        self,
        parent: InferredParent,
        records: Sequence[results.MappedRecord],
        ctx: contract.ETLContext,
    ) -> None:
        """Creates and registers the missing parents of one foreign key."""
        entity = self.mapping.references[parent.field].entity
        wanted: dict[str, tuple[str, ...]] = {}
        for record in records:
            value = record.data.get(parent.field)
            natural = record.foreign_keys.get(parent.field)
            if value is not None and natural is not None:
                wanted[str(value)] = natural

        missing = sorted(set(wanted) - self.registry.known(entity, wanted))
        if not missing:
            return

        parent.loader.load(
            [parent.placeholder(key, wanted[key]) for key in missing], ctx
        )
        self.registry.register(
            [contract.KeyEntry(entity, key, wanted[key]) for key in missing]
        )
        self.inferred += len(missing)
        _LOG.info(
            "%s: inferred %d %s placeholder(s) for %s",
            ctx.run.pipeline_name,
            len(missing),
            entity,
            parent.field,
        )
