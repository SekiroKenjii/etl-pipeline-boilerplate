"""The contract of a business module: its pipelines, and what they need."""

from __future__ import annotations

import abc
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from core import contract
from core import pipeline
from core.runtime import resources as runtime_resources

# A change of this fingerprint entry blocks runs, unless `etl run --rekey`.
KEYS = "keys"

type PipelineFactory = Callable[
    [runtime_resources.Resources], pipeline.BaseOrchestrator
]


class Module(abc.ABC):
    """A business domain (CRM, billing...): a few pipelines and their needs.

    A module declares the resources only it uses, and builds its pipelines from
    the resources it is given, by name and port (`runtime.names`).

        class Crm(runtime.Module):
            name = "crm"

            def pipelines(self):
                return {"full_load": lambda resources: FullLoad(resources)}
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """The name of the module: the prefix of its pipelines."""

    def resources(self) -> Sequence[runtime_resources.ResourceSpec[Any]]:
        """Returns the resources only this module uses; none by default."""
        return ()

    @abc.abstractmethod
    def pipelines(self) -> Mapping[str, PipelineFactory]:
        """Returns the pipelines of the module, by name (`full_load`...)."""

    def fingerprint(self) -> Mapping[str, str]:
        """What identifies the module's logic, for the run ledger (none here).

        A `KEYS` entry identifies how it makes its surrogate keys, never with
        the secret itself: `name:value` pairs separated by commas
        (`customer:3f2a...,order:9b1c...`), each value a hash of namespace,
        hashing and secret. A pipeline whose keys changed since its last
        successful run does not run unless told to re-key; a key added or gone
        is no change.
        """
        return {}

    def reconciliations(
        self, resources: runtime_resources.Resources
    ) -> Sequence[contract.Reconciliation]:
        """Returns how each entity reconciles with its source, for the
        cutover report (`etl cutover report <module>`); none by default."""
        del resources  # Unused.
        return ()

    def catalogs(self) -> Sequence[contract.MappingCatalog]:
        """Returns the mapping catalogs of the module, as compiled, for the
        commands that need them: a right to erasure (`etl keys forget`)
        reads the key case of an entity and the foreign keys referencing
        it, housekeeping the parking queues. None by default."""
        return ()
