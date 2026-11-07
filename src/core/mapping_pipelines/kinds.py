"""The pipelines of a mapping-driven module: full load, sync, parked, replay
and deletes."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
import datetime
import decimal
import typing
from typing import Any

from core import contract
from core import mapping
from core import operators
from core import runtime
from core.mapping_pipelines import base
from core.mapping_pipelines import mappings as pipelines_mappings
from core.runtime import names


class FullLoad(base.MappingPipeline):
    """Loads the rows changed since the last run, by watermark.

    With `partitions` above 1, each table is read by as many ranges of its
    key, in parallel (`operators.KeyPartitions`): for the first load of
    large tables. A step `<mapping>:ranges` computes the ranges first.
    """

    def __init__(
        self,
        resources: runtime.Resources,
        mappings: pipelines_mappings.Mappings,
    ) -> None:
        super().__init__(
            resources, mappings, max_workers=mappings.settings.partitions
        )
        self._partitions: dict[str, operators.KeyPartitions] = {}

    def prepare(self, name: str) -> contract.StepHandler | None:
        partitions = self.partitions(name)
        return None if partitions is None else partitions.prepare

    def partitions(self, name: str) -> operators.KeyPartitions | None:
        """The key ranges of a mapping's table; None for one range."""
        if self.settings.partitions == 1:
            return None
        if name not in self._partitions:
            self._partitions[name] = operators.KeyPartitions(
                self.port(
                    names.reader(self.settings.source), contract.ITableReader
                ),
                self.mappings.source_table(name),
                key=self.mappings.natural_key(name),
                count=self.settings.partitions,
                state=self.port(names.STATE, contract.IStateStore),
                state_key=f"full_load:{name}",
            )
        return self._partitions[name]

    def jobs(self, name: str) -> list[contract.StepHandler]:
        partitions = self.partitions(name)
        if partitions is None:
            return [self.job(name)]
        return [
            self.etl_job(extractor, self.transformer(name), name)
            for extractor in partitions.extractors(
                watermark=self.settings.watermark
            )
        ]

    def job(self, name: str) -> contract.StepHandler:
        reader = self.port(
            names.reader(self.settings.source), contract.ITableReader
        )
        extractor = operators.TableExtractor(
            reader,
            self.mappings.source_table(name),
            watermark=self.settings.watermark,
            key=self.mappings.natural_key(name),
            state=self.port(names.STATE, contract.IStateStore),
            state_key=f"full_load:{name}",
        )
        return self.etl_job(extractor, self.transformer(name), name)


class Sync(base.MappingPipeline):
    """A snapshot, then the changes of the source's change log, deletes
    included."""

    def job(self, name: str) -> contract.StepHandler:
        source = self.settings.source
        table = self.mappings.source_table(name)
        log = self.port(names.changes(source), contract.IChangeLog)
        snapshot = None
        if self.settings.snapshot == "table":
            snapshot = operators.Snapshot(
                self.port(names.reader(source), contract.ITableReader),
                contract.TableQuery(
                    table, order_by=self.mappings.natural_key(name)
                ),
            )
        extractor = operators.ChangeExtractor(
            log.source(table),
            state=self.port(names.STATE, contract.IStateStore),
            state_key=f"sync:{name}",
            snapshot=snapshot,
            from_beginning=snapshot is None,
        )
        return self.etl_job(
            extractor, self.transformer(name), name, changes=True
        )


class Parked(base.MappingPipeline):
    """Loads the orphans parked until their parent was loaded.

    Orphans parked again are waiting, not bad: no circuit breaker, and a
    step loading none is a partial success.
    """

    def job(self, name: str) -> contract.StepHandler:
        extractor = operators.ParkedRecordExtractor(
            self.port(names.PARKING, contract.IParkingLot),
            name,
            mapping.decode_record,
            max_attempts=20,
        )
        return self.etl_job(
            extractor,
            operators.Passthrough[mapping.MappedRecord](),
            name,
            breaker=False,
        )


class Replay(base.MappingPipeline):
    """Takes the quarantined rows of the module through its mappings again.

    Once their cause is fixed: at the source, in the mapping or a lookup, or
    by a correction (`settings.corrections`). The rows are read again from
    the source, by the keys of the quarantined records.
    """

    def __init__(
        self,
        resources: runtime.Resources,
        mappings: pipelines_mappings.Mappings,
        *,
        module: str,
    ) -> None:
        """Initializes the pipeline.

        Args:
            resources: The resources of the run.
            mappings: The compiled mappings of the module.
            module: The name of the module: its pipelines' entries only are
                replayed.
        """
        super().__init__(resources, mappings)
        self._module = module

    def job(self, name: str) -> contract.StepHandler:
        table = self.mappings.source_table(name)
        columns = self.mappings.natural_key(name)
        reader = self.port(
            names.reader(self.settings.source), contract.ITableReader
        )
        lineage = self.mappings.lineage(name)

        def fetch(
            entries: Sequence[contract.QuarantineEntry],
            ctx: contract.ETLContext,
        ) -> Iterator[contract.Row]:
            del ctx  # Unused.
            wanted = {str(entry.record["source_key"]) for entry in entries}
            first = sorted({key.split("|")[0] for key in wanted})
            query = contract.TableQuery(
                table,
                order_by=columns,
                filters=(
                    contract.Condition(
                        columns[0],
                        contract.Operator.IN,
                        _typed(reader, table, columns[0], first),
                    ),
                ),
            )
            for row in reader.read(query):
                if lineage(row) in wanted:
                    yield row

        extractor = operators.QuarantineReplay[contract.Row](
            self.port(names.QUARANTINE, contract.IQuarantine),
            fetch=fetch,
            entry_identity=lambda entry: self._source_key(entry, name),
            identity=lineage,
        )
        # Rows rejected again are expected here: no circuit breaker.
        return self.etl_job(
            extractor, self.transformer(name), name, breaker=False
        )

    def _source_key(
        self, entry: contract.QuarantineEntry, name: str
    ) -> str | None:
        """The source key of an entry of the module rejected by mapping
        `name`."""
        record = entry.record
        if not entry.pipeline.startswith(f"{self._module}.") or not isinstance(
            record, dict
        ):
            return None
        fields = typing.cast(dict[str, object], record)
        if fields.get("mapping") != name or fields.get("source_key") is None:
            return None
        return str(fields["source_key"])


def _typed(
    reader: contract.ITableReader, table: str, column: str, texts: Sequence[str]
) -> list[Any]:
    """Key values as text, in the type of the column: the source key is text."""
    sample = reader.max_value(table, column)
    convert: Callable[[str], Any]
    match sample:
        case bool() | None | str():
            return list(texts)
        case int():
            convert = int
        case decimal.Decimal():
            convert = decimal.Decimal
        case float():
            convert = float
        case datetime.datetime():
            convert = datetime.datetime.fromisoformat
        case datetime.date():
            convert = datetime.date.fromisoformat
        case _:
            return list(texts)
    typed: list[Any] = []
    for text in texts:
        try:
            typed.append(convert(text))
        except (ValueError, ArithmeticError):
            continue  # Not a value of the column: no row has it.
    return typed


class Deletes(base.MappingPipeline):
    """Finds the rows deleted at the source, from the key registry."""

    def __init__(
        self,
        resources: runtime.Resources,
        mappings: pipelines_mappings.Mappings,
    ) -> None:
        super().__init__(resources, mappings, reverse=True)

    def job(self, name: str) -> contract.StepHandler:
        compiled = self.mappings.compiled[name]
        extractor = operators.MissingSourceRows(
            self.port(
                names.reader(self.settings.source), contract.ITableReader
            ),
            self.mappings.source_table(name),
            key=self.mappings.natural_key(name),
            registry=self.registry(),
            entity=compiled.entity,
            case=compiled.spec.key_case,
            max_share=self.settings.max_delete_share,
        )
        return self.etl_job(
            extractor, self.transformer(name), name, changes=True
        )


def factories(
    module: str,
    mappings: Callable[[runtime.Resources], pipelines_mappings.Mappings],
) -> dict[str, runtime.PipelineFactory]:
    """The factories of the pipelines of a module, by name.

    Args:
        module: The name of the module.
        mappings: Its mappings, from the resources of a run.
    """
    return {
        "full_load": lambda r: FullLoad(r, mappings(r)),
        "sync": lambda r: Sync(r, mappings(r)),
        "parked": lambda r: Parked(r, mappings(r)),
        "deletes": lambda r: Deletes(r, mappings(r)),
        "replay": lambda r: Replay(r, mappings(r), module=module),
    }
