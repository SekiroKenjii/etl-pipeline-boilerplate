"""The steps of a run as the run ledger knows them: resuming a run.

Every step is recorded as soon as it ends; a run resuming another carries
over the steps that one finished (`done_steps`).
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
import dataclasses
import logging

from core import contract
from core import pipeline
from core.observability import redaction

_LOG = logging.getLogger("etl.kernel")

# Not run again on resume; their rejected rows replay from the quarantine.
DONE = frozenset({
    contract.ETLStatus.SUCCESS,
    contract.ETLStatus.PARTIAL_SUCCESS,
})
_STATS = frozenset(
    field.name for field in dataclasses.fields(contract.ETLStats)
)


@dataclasses.dataclass(frozen=True, slots=True)
class DoneStep:
    """A step finished by an earlier run.

    Attributes:
        run_id: The run that finished it.
        record: How it ended.
    """

    run_id: str
    record: contract.StepRecord


def done_steps(ledger: contract.IRunLedger, run_id: str) -> dict[str, DoneStep]:
    """The steps finished by a run, or by the runs it resumed.

    A step run again by a later run of the chain takes the outcome of that
    run: done if it ended so there, not done otherwise.
    """
    chain: list[contract.RunRecord] = []
    seen: set[str] = set()
    current: str | None = run_id
    while current is not None and current not in seen:
        seen.add(current)
        record = ledger.get(current)
        if record is None:
            break
        chain.append(record)
        current = record.meta.get("resumed_from")
    done: dict[str, DoneStep] = {}
    for record in reversed(chain):  # The oldest first.
        for name, step in record.steps.items():
            if step.status in DONE:
                done[name] = DoneStep(record.run_id, step)
            else:
                done.pop(name, None)
    return done


class LedgerSteps(pipeline.StepCoordinator):
    """Records the steps of a run, and carries over those done before.

    Attributes:
        run_id: The run.
    """

    def __init__(
        self,
        ledger: contract.IRunLedger,
        run_id: str,
        host: str,
        *,
        carried: Mapping[str, DoneStep] | None = None,
        redact: Collection[str] = (),
    ) -> None:
        """Initializes the coordinator.

        Args:
            ledger: The run ledger.
            run_id: The run.
            host: Where the run runs.
            carried: The steps an earlier run finished, not run again.
            redact: More secret names hidden in the messages recorded, on
                top of `redaction.SECRET_NAMES`: the logs' `redact`.
        """
        self.run_id = run_id
        self._ledger = ledger
        self._host = host
        self._carried = dict(carried or {})
        self._redact = tuple(redact)

    def settled(
        self, ctx: contract.ETLContext
    ) -> Mapping[str, contract.ETLResult]:
        del ctx  # Unused.
        return {
            name: _result(done.record, f"done in run {done.run_id}")
            for name, done in self._carried.items()
        }

    def finished(
        self, step: str, result: contract.ETLResult, ctx: contract.ETLContext
    ) -> None:
        del ctx  # Unused.
        hide_secrets(result, self._redact)
        try:
            self._ledger.record_step(self.run_id, step, result, self._host)
        # The run goes on: resuming it runs this step again.
        except Exception:  # pylint: disable=broad-exception-caught
            _LOG.exception("could not record the step %s", step)


def hide_secrets(
    result: contract.ETLResult | contract.PipelineResult,
    names: Collection[str] = (),
) -> None:
    """Redacts, in place, the message of a result and of its steps: the
    ledger keeps it, `etl run` and `etl runs` show it, and it may quote an
    exception (`redaction.redact_text`)."""
    if result.message:
        result.message = redaction.redact_text(result.message, names)
    if isinstance(result, contract.PipelineResult):
        for step in result.step_results.values():
            hide_secrets(step, names)


def _result(
    record: contract.StepRecord, message: str | None
) -> contract.ETLResult:
    """The result of a step that ended in an earlier run, as its record
    tells."""
    result = contract.ETLResult(
        stats=contract.ETLStats(**{
            name: value
            for name, value in record.stats.items()
            if name in _STATS
        }),
        data={"host": record.host, "attempts": record.attempts},
    )
    result.complete(record.status, message)
    result.completed_at = record.completed_at
    return result
