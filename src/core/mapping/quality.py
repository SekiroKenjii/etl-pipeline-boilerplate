"""Quality rules for mapped records: mapping issues and foreign keys."""

from __future__ import annotations

from collections.abc import Collection, Iterator, Sequence
import dataclasses
from typing import NamedTuple

from core import contract
from core.mapping import compiled
from core.mapping import results


@dataclasses.dataclass(frozen=True, slots=True)
class MappingIssuesRule:
    """Turns mapping issues into quality violations.

    The gate then decides what bad legacy data becomes, REJECT into the
    quarantine by default. Use it at AFTER_TRANSFORM:

        CheckSet([MappingIssuesRule()], max_reject_rate=0.01)

    Attributes:
        name: The name of the rule.
        on_violation: What the gate does with a record that has issues.
    """

    name: str = "mapping_issues"
    on_violation: contract.QualityAction = contract.QualityAction.REJECT

    def check_record(
        self, record: results.MappedRecord, ctx: contract.ETLContext
    ) -> Iterator[contract.Violation]:
        """See `contract.RecordRule.check_record`."""
        del ctx  # Unused.
        for issue in record.issues:
            yield contract.Violation(
                message=issue.message,
                field=issue.field,
                value=issue.value,
                record_id=record.source_key,
            )


class _BatchKeys(NamedTuple):
    loaded: frozenset[str]
    orphans: frozenset[str]


@dataclasses.dataclass(slots=True)
class ReferenceRule:
    """Referential integrity of one foreign key.

    The referenced entity must be in the key registry, i.e. loaded. Orphans
    are rejected into the quarantine by default; override per foreign key with
    the gate's `overrides`, by rule name.

    The quality gate calls `prepare_batch` before checking the records of a
    batch: the registry is asked about every foreign key of the batch at once,
    `chunk_size` keys per call, not once per record. Its answers, orphans
    included, hold for that batch only: a parent may be loaded between two
    batches (a self-reference, another step).

    Attributes:
        name: The name of the rule.
        field: The target column holding the foreign key.
        entity: The entity it references.
        registry: The crosswalk of loaded keys.
        on_violation: What the gate does with an orphan.
        chunk_size: The most keys asked about per registry call.
    """

    name: str
    field: str
    entity: str
    registry: contract.IKeyRegistry
    on_violation: contract.QualityAction = contract.QualityAction.REJECT
    chunk_size: int = 500
    _batch: _BatchKeys = dataclasses.field(
        default=_BatchKeys(frozenset(), frozenset()), init=False, repr=False
    )

    def prepare_batch(
        self, records: Sequence[results.MappedRecord], ctx: contract.ETLContext
    ) -> None:
        """Asks the registry which foreign keys of a batch are loaded."""
        del ctx  # Unused.
        values = sorted({
            str(value)
            for record in records
            if (value := record.data.get(self.field)) is not None
        })
        size = max(1, self.chunk_size)
        loaded: set[str] = set()
        for start in range(0, len(values), size):
            loaded |= self.registry.known(
                self.entity, values[start : start + size]
            )
        # Replaced at once: a reader never sees half a batch.
        self._batch = _BatchKeys(frozenset(loaded), frozenset(values) - loaded)

    def check_record(
        self, record: results.MappedRecord, ctx: contract.ETLContext
    ) -> Iterator[contract.Violation]:
        """See `contract.RecordRule.check_record`."""
        del ctx  # Unused.
        value = record.data.get(self.field)
        if value is None:
            return
        key = str(value)
        loaded, orphans = self._batch
        if key in loaded:
            return
        if key not in orphans and self.registry.known(self.entity, (key,)):
            return
        yield contract.Violation(
            message=f"references a {self.entity} that is not loaded",
            field=self.field,
            value=value,
            record_id=record.source_key,
        )


def reference_rule_name(mapping: str, field: str) -> str:
    """Returns the name of the integrity rule of a foreign key."""
    return f"references:{mapping}.{field}"


def reference_rules(
    mapping: compiled.CompiledMapping,
    registry: contract.IKeyRegistry,
    *,
    on_violation: contract.QualityAction = contract.QualityAction.REJECT,
    self_references: contract.QualityAction = contract.QualityAction.WARN,
    exclude: Collection[str] = (),
) -> list[ReferenceRule]:
    """Returns one referential integrity rule per foreign key of `mapping`.

    Rules are named `references:<mapping>.<field>` (`reference_rule_name`).
    Orphans are rejected by default: into the quarantine, or into the parking
    lot with a `quality.ParkingQuarantine`.

    Args:
        mapping: The mapping whose foreign keys to check.
        registry: The crosswalk of loaded keys.
        on_violation: What the gate does with an orphan.
        self_references: What the gate does with an orphan of a reference to
            the mapping itself (`employee.manager`): WARN by default, as a
            parent in the same run may come in a later batch than its
            children. Check those after the load, or load in two passes.
        exclude: The foreign keys not to check: those whose missing parents
            an `orphans.InferringLoader` creates.

    Returns:
        The rules, to put in a `CheckSet` at AFTER_TRANSFORM.
    """
    return [
        ReferenceRule(
            name=reference_rule_name(mapping.name, reference.field),
            field=reference.field,
            entity=reference.entity,
            registry=registry,
            on_violation=(
                self_references if reference.self_reference else on_violation
            ),
        )
        for reference in mapping.references.values()
        if reference.field not in exclude
    ]
