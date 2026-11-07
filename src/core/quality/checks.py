"""Quality rules built from predicates, for the common checks."""

from __future__ import annotations

from collections.abc import Callable, Hashable, Iterator, Sequence
import dataclasses

from core import contract


@dataclasses.dataclass(frozen=True, slots=True)
class RecordCheck[T]:
    """A record rule built from a predicate: "field must satisfy X".

        RecordCheck(
            "has_email",
            lambda u: bool(u.email),
            on_violation=contract.QualityAction.REJECT,
        )

    Attributes:
        name: The unique name of the rule.
        predicate: Whether a record is valid.
        message: What is wrong with an invalid record.
        on_violation: What the gate does with an invalid record.
        field: The field checked, read from records to report its value.
    """

    name: str
    predicate: Callable[[T], bool]
    message: str | None = None
    on_violation: contract.QualityAction = contract.QualityAction.FAIL
    field: str | None = None

    def check_record(
        self, record: T, ctx: contract.ETLContext
    ) -> Iterator[contract.Violation]:
        """See `contract.RecordRule.check_record`."""
        del ctx  # Unused.
        if self.predicate(record):
            return

        yield contract.Violation(
            message=self.message or f"check '{self.name}' failed",
            field=self.field,
            value=getattr(record, self.field, None) if self.field else None,
        )


@dataclasses.dataclass(frozen=True, slots=True)
class BatchCheck[T]:
    """A batch rule built from a predicate over the whole batch.

        BatchCheck("unique_id", unique_by(lambda u: u.id))

    Attributes:
        name: The unique name of the rule.
        predicate: Whether a batch is valid.
        message: What is wrong with an invalid batch.
        on_violation: What the gate does with an invalid batch.
    """

    name: str
    predicate: Callable[[Sequence[T]], bool]
    message: str | None = None
    on_violation: contract.QualityAction = contract.QualityAction.FAIL

    def check_batch(
        self, records: Sequence[T], ctx: contract.ETLContext
    ) -> Iterator[contract.Violation]:
        """See `contract.BatchRule.check_batch`."""
        del ctx  # Unused.
        if self.predicate(records):
            return

        yield contract.Violation(
            message=self.message or f"check '{self.name}' failed"
        )


def unique_by[T](key: Callable[[T], Hashable]) -> Callable[[Sequence[T]], bool]:
    """Returns a batch predicate: no two records share the same key."""

    def predicate(records: Sequence[T]) -> bool:
        keys = [key(record) for record in records]
        return len(keys) == len(set(keys))

    return predicate
