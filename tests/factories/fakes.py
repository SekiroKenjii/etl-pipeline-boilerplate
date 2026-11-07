from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
import dataclasses

from core import contract
from core import pipeline


@dataclasses.dataclass(frozen=True, slots=True)
class RawUser:
    id: str
    email: str
    age: int


@dataclasses.dataclass(frozen=True, slots=True)
class User:
    id: str
    email: str
    is_adult: bool


def make_context(
    name: str = "test-pipeline", dry_run: bool = False
) -> contract.ETLContext:
    return contract.ETLContext(
        contract.ETLRunInfo(pipeline_name=name, dry_run=dry_run)
    )


class LifecycleSpy:

    def __init__(self) -> None:
        self.events: list[str] = []

    def setup(self, ctx: contract.ETLContext) -> None:
        self.events.append("setup")

    def teardown(self, ctx: contract.ETLContext) -> None:
        self.events.append("teardown")


class InMemoryExtractor(LifecycleSpy, pipeline.IDataExtractor[RawUser]):

    def __init__(self, records: Sequence[RawUser]) -> None:
        super().__init__()
        self.records = list(records)
        self.acked: list[list[str]] = []

    def extract(self, ctx: contract.ETLContext) -> Iterator[RawUser]:
        yield from self.records

    def acknowledge(
        self, records: Sequence[RawUser], ctx: contract.ETLContext
    ) -> None:
        self.acked.append([record.id for record in records])


class UserTransformer(pipeline.IDataTransformer[RawUser, User]):

    def transform(
        self, records: Iterable[RawUser], ctx: contract.ETLContext
    ) -> Iterator[User]:
        for raw in records:
            yield User(
                id=raw.id, email=raw.email.lower(), is_adult=raw.age >= 18
            )


class InMemoryLoader(LifecycleSpy, pipeline.IDataLoader[User]):

    def __init__(self) -> None:
        super().__init__()
        self.rows: dict[str, User] = {}
        self.batches: list[int] = []

    def load(
        self, records: Sequence[User], ctx: contract.ETLContext
    ) -> contract.ETLStats:
        stats = contract.ETLStats()
        self.batches.append(len(records))
        for user in records:
            if user.id in self.rows:
                stats.updated += 1
            else:
                stats.created += 1
            self.rows[user.id] = user
            stats.successful += 1
        return stats


class Passthrough[T](pipeline.IDataTransformer[T, T]):

    def transform(
        self, records: Iterable[T], ctx: contract.ETLContext
    ) -> Iterator[T]:
        yield from records


class CollectingLoader[T](pipeline.IDataLoader[T]):
    """Keeps what it loads; raises RetryableError on batch `fail_on_batch`."""

    def __init__(self, fail_on_batch: int | None = None) -> None:
        self.rows: list[T] = []
        self.batches = 0
        self.fail_on_batch = fail_on_batch

    def load(
        self, records: Sequence[T], ctx: contract.ETLContext
    ) -> contract.ETLStats:
        self.batches += 1
        if self.batches == self.fail_on_batch:
            raise contract.RetryableError("warehouse unavailable")
        self.rows.extend(records)
        return contract.ETLStats(successful=len(records))
