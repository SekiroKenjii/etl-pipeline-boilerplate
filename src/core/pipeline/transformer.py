"""The transformer interface, and the composition of transformers."""

from __future__ import annotations

import abc
from collections.abc import Iterable
import functools
import typing

from core import contract

# Declared, not PEP 695: mypy infers both parameters invariant, as `then`
# returns this class with other arguments; and a transformer of
# `Row | ChangeEvent` must take a batch of rows.
TIn_contra = typing.TypeVar("TIn_contra", contravariant=True)
TOut_co = typing.TypeVar("TOut_co", covariant=True)


class IDataTransformer(
    contract.Lifecycle, abc.ABC, typing.Generic[TIn_contra, TOut_co]
):
    """Turns records into other records, one batch at a time.

    It may emit more or fewer records than it receives; records dropped on
    purpose must be counted through `collect_stats`. Compose with
    `first.then(second)` or `chain(a, b, c)`.
    """

    @abc.abstractmethod
    def transform(
        self, records: Iterable[TIn_contra], ctx: contract.ETLContext
    ) -> Iterable[TOut_co]:
        """Yields the transformed records of one batch."""

    def collect_stats(self) -> contract.ETLStats:
        """Returns the counters accumulated since the previous call, and resets.

        `ETLJob` calls it after each batch and adds the result to the step's
        stats, e.g. the `filtered` records.
        """
        return contract.ETLStats()

    def then[TNext](
        self, following: IDataTransformer[TOut_co, TNext]
    ) -> IDataTransformer[TIn_contra, TNext]:
        """Returns a transformer running this one, then `following`."""
        return ChainedTransformer(self, following)


class ChainedTransformer[TIn, TMid, TOut](IDataTransformer[TIn, TOut]):
    """Two transformers run one after the other, lazily, as one.

    Attributes:
        first: The transformer that runs first.
        second: The transformer that receives what `first` yields.
    """

    def __init__(
        self,
        first: IDataTransformer[TIn, TMid],
        second: IDataTransformer[TMid, TOut],
    ) -> None:
        self.first = first
        self.second = second

    @property
    def name(self) -> str:
        return f"{self.first.name} -> {self.second.name}"

    def setup(self, ctx: contract.ETLContext) -> None:
        self.first.setup(ctx)
        try:
            self.second.setup(ctx)
        except BaseException:
            self.first.teardown(ctx)
            raise

    def teardown(self, ctx: contract.ETLContext) -> None:
        try:
            self.second.teardown(ctx)
        finally:
            self.first.teardown(ctx)

    def transform(
        self, records: Iterable[TIn], ctx: contract.ETLContext
    ) -> Iterable[TOut]:
        return self.second.transform(self.first.transform(records, ctx), ctx)

    def collect_stats(self) -> contract.ETLStats:
        return self.first.collect_stats() + self.second.collect_stats()


def chain[T](
    first: IDataTransformer[T, T], *rest: IDataTransformer[T, T]
) -> IDataTransformer[T, T]:
    """Chains transformers of one record type (filters, deduplication...):
    `chain(a, b, c)` is `a.then(b).then(c)`.

    Transformers that change the type chain with `then`.
    """
    return functools.reduce(
        lambda chained, following: chained.then(following), rest, first
    )
