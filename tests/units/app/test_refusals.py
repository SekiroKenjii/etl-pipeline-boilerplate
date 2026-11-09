"""A run that refuses to go on until an operator acts exits 2: retrying
would refuse again."""

from __future__ import annotations

from collections.abc import Sequence

from core import contract
from core import pipeline
from core import runtime
from core.runtime import names


class _Refusing(runtime.Module):
    """A step refusing to go on until the operator confirms it."""

    def __init__(self) -> None:
        self.calls = 0

    @property
    def name(self) -> str:
        return "refusing"

    def pipelines(self) -> dict[str, runtime.PipelineFactory]:
        def refuse(
            ctx: contract.ETLContext, upstream: object
        ) -> contract.ETLResult:
            self.calls += 1
            if ctx.run.params.get("confirm") != "yes":
                raise contract.PipelineConfigError("confirm it: --param")
            return contract.ETLResult()

        class Steps(pipeline.BaseOrchestrator):

            def steps(self) -> Sequence[contract.PipelineStep]:
                return [
                    contract.PipelineStep("deletes", refuse),
                    contract.PipelineStep("other", refuse),
                ]

        return {"deletes": lambda resources: Steps()}


def test_a_refusal_waiting_for_an_operator_exits_2() -> None:
    module = _Refusing()
    ledger = runtime.InMemoryRunLedger()
    kernel = runtime.Kernel(
        runtime.Resources(
            [runtime.ResourceSpec(names.LEDGER, lambda r: ledger)]
        ),
        [module],
    )

    refused = kernel.run("refusing.deletes")
    confirmed = kernel.run("refusing.deletes", params={"confirm": "yes"})

    # Retrying would refuse again: INVALID, which schedulers do not retry.
    assert refused.exit_code is runtime.ExitCode.INVALID
    assert refused.result is not None
    assert refused.result.status is contract.ETLStatus.FAILED
    assert confirmed.exit_code is runtime.ExitCode.SUCCESS
    # The refused run is recorded: it ran, and refused.
    assert len(ledger.recent("refusing.deletes")) == 2
