"""The base of the mapping-driven pipelines: one step per mapping."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Never

from core import contract
from core import cutover
from core import keys
from core import mapping
from core import operators
from core import pipeline
from core import quality
from core import runtime
from core.mapping_pipelines import mappings as pipelines_mappings
from core.runtime import names


class MappingPipeline(pipeline.BaseOrchestrator):
    """One step per mapping, in the order their references need (parents
    first; children first with `reverse`, for deletes).

    A subclass builds the job of each mapping (`job`), from the parts every
    mapping-driven pipeline shares: `etl_job` composes the `transformer`
    (corrections, then the mapping), the `loader` (upserts or changes,
    registering the surrogate keys) and the `gate` (mapping issues and
    references; orphans rejected or parked; the circuit breaker). Each step
    is retried on transient failures (`step_attempts`) and declares the
    datasets it reads and writes, for lineage.

        class Archive(mapping_pipelines.MappingPipeline):
            def job(self, name):
                extractor = operators.TableExtractor(...)
                return self.etl_job(extractor, self.transformer(name), name)

    Attributes:
        resources: The resources of the run.
        mappings: The compiled mappings of the module.
        settings: The settings of the module.
    """

    def __init__(
        self,
        resources: runtime.Resources,
        mappings: pipelines_mappings.Mappings,
        *,
        reverse: bool = False,
        max_workers: int = 1,
    ) -> None:
        super().__init__(max_workers=max_workers)
        self.resources = resources
        self.mappings = mappings
        self.settings = mappings.settings
        self._reverse = reverse

    def steps(self) -> Sequence[contract.PipelineStep]:
        dependencies: dict[str, set[str]] = {
            name: set(parents)
            for name, parents in self.mappings.dependencies.items()
        }
        if self._reverse:
            # Children first: deleting a parent before them breaks their keys.
            children: dict[str, set[str]] = {
                name: set() for name in dependencies
            }
            for name, parents in dependencies.items():
                for parent in parents:
                    children[parent].add(name)
            dependencies = children
        jobs = {name: self.jobs(name) for name in self.mappings.compiled}
        retry = contract.RetryPolicy(
            max_attempts=self.settings.step_attempts,
            backoff_seconds=self.settings.retry_backoff_seconds,
        )

        def step_names(name: str) -> list[str]:
            count = len(jobs[name])
            return (
                [name] if count == 1 else [f"{name}:{i}" for i in range(count)]
            )

        def source(name: str) -> tuple[contract.Dataset, ...]:
            return (
                contract.Dataset(
                    self.settings.source, self.mappings.source_table(name)
                ),
            )

        prepared = {
            name: contract.PipelineStep(
                f"{name}:ranges", handler, retry=retry, inputs=source(name)
            )
            for name in self.mappings.compiled
            if (handler := self.prepare(name)) is not None
        }
        # A part waits for every part of the mappings it depends on.
        return list(prepared.values()) + [
            contract.PipelineStep(
                step,
                job,
                retry=retry,
                inputs=source(name),
                outputs=(
                    contract.Dataset(
                        self.settings.target, self.mappings.target_table(name)
                    ),
                ),
                dependencies=tuple(
                    sorted(
                        part
                        for parent in dependencies[name]
                        for part in step_names(parent)
                    )
                )
                + ((prepared[name].name,) if name in prepared else ()),
            )
            for name in self.mappings.compiled
            for step, job in zip(step_names(name), jobs[name], strict=True)
        ]

    def jobs(self, name: str) -> list[contract.StepHandler]:
        """The steps of a mapping: one, unless the pipeline splits it."""
        return [self.job(name)]

    def prepare(self, name: str) -> contract.StepHandler | None:
        """A step to run before those of a mapping, `<name>:ranges`; none
        by default."""
        del name  # Unused.
        return None

    def job(self, name: str) -> contract.StepHandler:
        """The step of mapping `name`: an `etl_job`, usually."""
        raise NotImplementedError

    def port[T](self, name: str, kind: type[T]) -> T:
        """The resource `name` (`runtime.names`), as port `kind`."""
        return self.resources.get(name, kind)

    def registry(self) -> contract.IKeyRegistry:
        return self.port(names.KEYS, contract.IKeyRegistry)

    def gate(self, name: str, *, breaker: bool = True) -> quality.QualityGate:
        """Bad rows to the quarantine; orphans too, or to the parking lot.

        Args:
            name: The mapping.
            breaker: Whether too many rejects fail the batch
                (`max_reject_rate`).
        """
        compiled = self.mappings.compiled[name]
        quarantine = self.port(names.QUARANTINE, contract.IQuarantine)
        if self.settings.orphans == "park":
            quarantine = quality.ParkingQuarantine(
                self.port(names.PARKING, contract.IParkingLot),
                name,
                rules={
                    mapping.reference_rule_name(name, field)
                    for field in compiled.references
                },
                encode=mapping.encode_record,
                identity=mapping.record_identity,
                fallback=quarantine,
            )
        checks: dict[contract.Checkpoint, quality.CheckSet[Never]] = {
            contract.Checkpoint.AFTER_TRANSFORM: quality.CheckSet(
                [
                    mapping.MappingIssuesRule(),
                    *mapping.reference_rules(compiled, self.registry()),
                ],
                max_reject_rate=(
                    self.settings.max_reject_rate if breaker else None
                ),
            )
        }
        if self.settings.verify_loads:
            checks[contract.Checkpoint.AFTER_LOAD] = quality.CheckSet(
                batch=[self.verification(name)]
            )
        return quality.QualityGate(
            checks,
            quarantine=quarantine,
            issue_sink=self.port(names.ISSUES, contract.IIssueSink),
        )

    def verification(
        self, name: str
    ) -> cutover.LoadedRowsCheck[mapping.MappedRecord]:
        """Reads every batch back from the target, to compare."""
        key = self.mappings.key_field(name)
        return cutover.LoadedRowsCheck[mapping.MappedRecord](
            self.port(
                names.reader(self.settings.target), contract.ITableReader
            ),
            self.mappings.target_table(name),
            key=(key,),
            columns=tuple(
                f.target for f in self.mappings.compiled[name].spec.fields
            ),
            row=lambda record: None if record.deleted else record.data,
            chunk=self.settings.in_chunk,
        )

    def loader(
        self, name: str, *, changes: bool = False
    ) -> pipeline.IDataLoader[mapping.MappedRecord]:
        """Upserts into the target table, registering the keys loaded.

        Args:
            name: The mapping.
            changes: Whether the records are changes, deletes included
                (`deletes` says what becomes of a deleted row).
        """
        writer = self.port(
            names.writer(self.settings.target), contract.ITableWriter
        )
        target = contract.TableTarget(
            self.mappings.target_table(name),
            key=(self.mappings.key_field(name),),
        )
        inner: pipeline.IDataLoader[mapping.MappedRecord]
        if changes:
            inner = operators.ChangeLoader[mapping.MappedRecord](
                writer,
                target,
                row=lambda record: record.data,
                deleted=lambda record: record.deleted,
                deletes=operators.Deletes(self.settings.deletes),
            )
        else:
            inner = operators.TableLoader[mapping.MappedRecord](
                writer, target, row=lambda record: record.data
            )
        return keys.RegisteringLoader(
            inner, self.registry(), key=mapping.MappedRecord.key_entry
        )

    def transformer(
        self, name: str
    ) -> pipeline.IDataTransformer[
        operators.SourceRecord, mapping.MappedRecord
    ]:
        """The corrections of the rows, then their mapping."""
        lineage = self.mappings.lineage(name)
        return self.mappings.corrections.transformer(name, lineage).then(
            mapping.MappingTransformer[operators.SourceRecord](
                self.mappings.compiled[name], key=lineage
            )
        )

    def etl_job[T](
        self,
        extractor: pipeline.IDataExtractor[T],
        transformer: pipeline.IDataTransformer[T, mapping.MappedRecord],
        name: str,
        *,
        changes: bool = False,
        breaker: bool = True,
    ) -> pipeline.ETLJob[T, mapping.MappedRecord]:
        """The job of mapping `name`: extracted, transformed, gated, loaded.

        Args:
            extractor: Where the rows come from.
            transformer: What they become: `transformer(name)`, usually.
            name: The mapping.
            changes: Whether the records are changes (`loader`).
            breaker: Whether too many rejects fail the batch (`gate`).
        """
        return pipeline.ETLJob(
            extractor,
            transformer,
            self.loader(name, changes=changes),
            self.gate(name, breaker=breaker),
            batch_size=self.settings.batch_size,
        )
