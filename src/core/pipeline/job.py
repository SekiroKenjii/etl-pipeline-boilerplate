"""A pipeline step made of an extractor, a transformer and a loader."""

from __future__ import annotations

from collections.abc import Mapping
import contextlib
import dataclasses
import itertools

from core import contract
from core.observability import tracing
from core.pipeline import extractor as pipeline_extractor
from core.pipeline import loader as pipeline_loader
from core.pipeline import transformer as pipeline_transformer


class ETLJob[TRaw, TOut]:
    """Runs an extractor, a transformer and a loader as one pipeline step.

    The source is streamed in batches. An optional quality gate audits each
    batch at the checkpoints, and the job goes on with what it lets through:

        BEFORE_EXTRACT -> extract -> AFTER_EXTRACT -> transform
                       -> AFTER_TRANSFORM -> load -> AFTER_LOAD

    A failed checkpoint ends the step FAILED, keeping the reports. A dry run
    loads nothing and counts the records as skipped.

    Once a batch is loaded or rejected, the gate stores what it set aside
    (`gate.flush`), then `extractor.acknowledge(batch)` is called, in that
    order. A failed batch and a dry run are never acknowledged, so the source
    delivers them again (at-least-once); what the gate set aside for a failed
    batch is dropped. Wrap an asyncio extractor in `AsyncExtractorAdapter`.

        PipelineStep("users", ETLJob(extractor, transformer, loader))

    Attributes:
        extractor: Reads the source.
        transformer: Turns source records into target records.
        loader: Writes the target.
        gate: Audits the data at each checkpoint, if any.
        batch_size: The records handled at once.
    """

    def __init__(
        self,
        extractor: pipeline_extractor.IDataExtractor[TRaw],
        transformer: pipeline_transformer.IDataTransformer[TRaw, TOut],
        loader: pipeline_loader.IDataLoader[TOut],
        gate: contract.IQualityGate | None = None,
        batch_size: int = 1000,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")

        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.gate = gate
        self.batch_size = batch_size

    def __call__(
        self,
        ctx: contract.ETLContext,
        upstream: Mapping[str, contract.ETLResult],
    ) -> contract.ETLResult:
        """Runs the job; the signature of a `contract.StepHandler`.

        Args:
            ctx: The run context.
            upstream: The results of the steps this one depends on (unused).

        Returns:
            The result of the step. A failed quality checkpoint gives a FAILED
            result; other errors propagate, for the orchestrator to retry or
            record.
        """
        result = contract.ETLResult()
        components: list[contract.Lifecycle] = [
            self.extractor,
            self.transformer,
            self.loader,
        ]
        with contextlib.ExitStack() as stack:
            for component in components:
                stack.enter_context(component.lifecycle(ctx))

            try:
                self._run(ctx, result)
            except contract.QualityGateError as exc:
                self._flush(ctx)
                result.add_error(
                    contract.ETLError.from_exception(exc, phase=exc.phase)
                )
                result.complete(contract.ETLStatus.FAILED, str(exc))
                return result
            except BaseException:
                if self.gate is not None:
                    self.gate.discard()
                raise

        result.complete(
            contract.ETLStatus.CANCELLED if ctx.is_cancelled else None
        )
        return result

    def _run(
        self, ctx: contract.ETLContext, result: contract.ETLResult
    ) -> None:
        """Streams the source batch by batch, acknowledging each handled one."""
        self._inspect(contract.Checkpoint.BEFORE_EXTRACT, [], ctx, result)

        for raw_batch in itertools.batched(
            self.extractor.extract(ctx), self.batch_size
        ):
            if ctx.is_cancelled:
                break

            records = list(raw_batch)
            with tracing.span("etl.batch", {"etl.rows": len(records)}):
                self._process(records, ctx, result)

            # Only reached once the batch is loaded or rejected.
            self._flush(ctx)
            if not ctx.run.dry_run:
                self.extractor.acknowledge(records, ctx)

        # What BEFORE_EXTRACT found, when no batch was handled.
        self._flush(ctx)

    def _flush(self, ctx: contract.ETLContext) -> None:
        """Has the gate store what it set aside for the handled batch."""
        if self.gate is not None:
            self.gate.flush(ctx)

    def _process(
        self,
        records: list[TRaw],
        ctx: contract.ETLContext,
        result: contract.ETLResult,
    ) -> None:
        """Takes a batch through the checkpoints, transformer and loader."""
        result.stats.total += len(records)

        raw = self._inspect(
            contract.Checkpoint.AFTER_EXTRACT, records, ctx, result
        )
        transformed = list(self.transformer.transform(raw, ctx))
        result.stats += self.transformer.collect_stats()
        transformed = self._inspect(
            contract.Checkpoint.AFTER_TRANSFORM, transformed, ctx, result
        )

        if not transformed:
            return

        if ctx.run.dry_run:
            result.stats.skipped += len(transformed)
            return

        # `total` is counted from the source above, not by the loader.
        result.stats += dataclasses.replace(
            self.loader.load(transformed, ctx), total=0
        )
        self._inspect(contract.Checkpoint.AFTER_LOAD, transformed, ctx, result)

    def _inspect[T](
        self,
        checkpoint: contract.Checkpoint,
        records: list[T],
        ctx: contract.ETLContext,
        result: contract.ETLResult,
    ) -> list[T]:
        """Returns the records the gate lets through at `checkpoint`.

        Raises:
            QualityGateError: If the gate fails the batch.
        """
        if self.gate is None:
            return records

        outcome = self.gate.inspect(checkpoint, records, ctx)
        report = outcome.report

        if report is not None:
            result.add_quality_report(report)
            if report.failed:
                raise contract.QualityGateError(report)

        return outcome.records
