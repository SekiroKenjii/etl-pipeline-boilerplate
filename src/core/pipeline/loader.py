"""The loader interface."""

from __future__ import annotations

import abc
from collections.abc import Sequence

from core import contract


class IDataLoader[TIn](contract.Lifecycle, abc.ABC):
    """Writes records to a target."""

    @abc.abstractmethod
    def load(
        self, records: Sequence[TIn], ctx: contract.ETLContext
    ) -> contract.ETLStats:
        """Writes one batch of records.

        It must be idempotent (e.g. upsert by a business key) so that a retried
        step does not create duplicates.

        Args:
            records: The batch.
            ctx: The run context.

        Returns:
            What was done: successful, failed, created, updated, deleted. The
            `total` is counted by the job, not by the loader.
        """
