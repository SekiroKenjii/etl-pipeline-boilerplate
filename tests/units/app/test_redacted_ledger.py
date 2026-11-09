"""What the kernel keeps of an error (ledger, run outcome, `etl check`) is
redacted, with the names of `logging.redact` too."""

from __future__ import annotations

from collections.abc import Callable, Sequence
import pathlib

import pytest

from app import bootstrap
from core import contract
from core import pipeline
from core import runtime
from core.runtime import names

# A URL's password, a value a database error quotes, a configured name.
_LEAKY = (
    "postgresql://etl:S3cr3t@db/crm: Key (email)=(jane@example.com)"
    " already exists; badge=B-4711"
)
_SECRETS = ("S3cr3t", "jane@example.com", "B-4711")
_REDACT = ("badge",)

type Handler = Callable[[contract.ETLContext, object], contract.ETLResult]


class _Steps(pipeline.BaseOrchestrator):

    def __init__(
        self,
        handler: Handler,
        retry: contract.RetryPolicy | None = None,
        setup_error: Exception | None = None,
    ) -> None:
        super().__init__(sleep=lambda seconds: None)
        self._handler = handler
        self._retry = retry or contract.RetryPolicy()
        self._setup_error = setup_error

    def setup(self, ctx: contract.ETLContext) -> None:
        if self._setup_error is not None:
            raise self._setup_error

    def steps(self) -> Sequence[contract.PipelineStep]:
        return [contract.PipelineStep("load", self._handler, retry=self._retry)]


class _Module(runtime.Module):

    def __init__(
        self,
        build: runtime.PipelineFactory,
        fingerprint_error: Exception | None = None,
    ) -> None:
        self._build = build
        self._fingerprint_error = fingerprint_error

    @property
    def name(self) -> str:
        return "leaky"

    def pipelines(self) -> dict[str, runtime.PipelineFactory]:
        return {"load": self._build}

    def fingerprint(self) -> dict[str, str]:
        if self._fingerprint_error is not None:
            raise self._fingerprint_error
        return {}


def _kernel(
    module: runtime.Module,
    ledger: contract.IRunLedger,
    health: Callable[[object], Sequence[str] | None] | None = None,
) -> runtime.Kernel:
    specs: list[runtime.ResourceSpec[object]] = [
        runtime.ResourceSpec(names.LEDGER, lambda r: ledger)
    ]
    if health is not None:
        specs.append(
            runtime.ResourceSpec("source", lambda r: object(), health=health)
        )
    return runtime.Kernel(runtime.Resources(specs), [module], redact=_REDACT)


def _cancelled_then_raises(
    ctx: contract.ETLContext, upstream: object
) -> contract.ETLResult:
    del upstream  # Unused.
    ctx.cancel()
    raise contract.RetryableError(_LEAKY)


def _shown(ledger: contract.IRunLedger, outcome: runtime.RunOutcome) -> str:
    """Everything the ledger and the outcome tell of the run."""
    assert outcome.result is not None
    record = ledger.get(outcome.result.run_id)
    assert record is not None
    shown = [record.message, *record.meta.values(), outcome.result.message]
    shown += [step.message for step in record.steps.values()]
    shown += [step.message for step in outcome.result.step_results.values()]
    return "\n".join(text for text in shown if text)


def _assert_hidden(text: str) -> None:
    for secret in _SECRETS:
        assert secret not in text, text


def test_a_step_cancelled_on_an_error_is_recorded_redacted() -> None:
    ledger = runtime.InMemoryRunLedger()
    kernel = _kernel(
        _Module(lambda resources: _Steps(_cancelled_then_raises)), ledger
    )

    outcome = kernel.run("leaky.load")

    assert outcome.exit_code is runtime.ExitCode.CANCELLED
    shown = _shown(ledger, outcome)
    _assert_hidden(shown)
    assert "cancelled after 1 attempt(s)" in shown
    assert "postgresql://etl:***@db/crm" in shown
    assert outcome.result is not None
    assert outcome.result.message is not None
    assert outcome.result.message.startswith(
        "load: cancelled after 1 attempt(s): postgresql://etl:***@db/crm"
    )


def test_the_reason_of_a_failure_is_recorded_redacted() -> None:
    def fails(ctx: contract.ETLContext, upstream: object) -> contract.ETLResult:
        del ctx, upstream  # Unused.
        raise contract.FatalError(_LEAKY)

    ledger = runtime.InMemoryRunLedger()
    kernel = _kernel(_Module(lambda resources: _Steps(fails)), ledger)

    outcome = kernel.run("leaky.load")

    assert outcome.exit_code is runtime.ExitCode.FAILED
    _assert_hidden(_shown(ledger, outcome))
    assert outcome.result is not None
    reason = (
        "failed after 1 attempt(s): postgresql://etl:***@db/crm: Key"
        " (email)=(***) already exists; badge=***"
    )
    assert outcome.result.message == f"load: {reason}"
    record = ledger.get(outcome.result.run_id)
    assert record is not None
    assert record.message == f"load: {reason}"
    assert record.steps["load"].message == reason


def test_the_reason_recorded_is_the_last_attempt_s() -> None:
    attempts: list[int] = []

    def fails_twice(
        ctx: contract.ETLContext, upstream: object
    ) -> contract.ETLResult:
        del ctx, upstream  # Unused.
        attempts.append(1)
        raise contract.RetryableError(f"{_LEAKY} (try {len(attempts)})")

    ledger = runtime.InMemoryRunLedger()
    retry = contract.RetryPolicy(max_attempts=2)
    kernel = _kernel(
        _Module(lambda resources: _Steps(fails_twice, retry)), ledger
    )

    outcome = kernel.run("leaky.load")

    assert len(attempts) == 2
    shown = _shown(ledger, outcome)
    _assert_hidden(shown)
    assert outcome.result is not None
    record = ledger.get(outcome.result.run_id)
    assert record is not None
    assert record.steps["load"].message == (
        "failed after 2 attempt(s): postgresql://etl:***@db/crm: Key"
        " (email)=(***) already exists; badge=*** (try 2)"
    )


def test_a_step_cancelled_during_its_backoff_is_recorded_redacted() -> None:
    attempts: list[int] = []

    def fails_then_cancelled(
        ctx: contract.ETLContext, upstream: object
    ) -> contract.ETLResult:
        del upstream  # Unused.
        attempts.append(1)
        if len(attempts) == 2:
            ctx.cancel()
        raise contract.RetryableError(_LEAKY)

    ledger = runtime.InMemoryRunLedger()
    retry = contract.RetryPolicy(max_attempts=3)
    kernel = _kernel(
        _Module(lambda resources: _Steps(fails_then_cancelled, retry)), ledger
    )

    outcome = kernel.run("leaky.load")

    assert len(attempts) == 2
    _assert_hidden(_shown(ledger, outcome))


def test_a_message_a_step_makes_of_an_error_is_recorded_redacted() -> None:
    def quotes_its_error(
        ctx: contract.ETLContext, upstream: object
    ) -> contract.ETLResult:
        del ctx, upstream  # Unused.
        result = contract.ETLResult()
        result.complete(contract.ETLStatus.FAILED, _LEAKY)
        return result

    ledger = runtime.InMemoryRunLedger()
    kernel = _kernel(
        _Module(lambda resources: _Steps(quotes_its_error)), ledger
    )

    outcome = kernel.run("leaky.load")

    assert outcome.exit_code is runtime.ExitCode.FAILED
    _assert_hidden(_shown(ledger, outcome))


def test_a_pipeline_that_cannot_start_is_recorded_redacted() -> None:
    ledger = runtime.InMemoryRunLedger()
    module = _Module(
        lambda resources: _Steps(
            _cancelled_then_raises, setup_error=contract.FatalError(_LEAKY)
        )
    )

    outcome = _kernel(module, ledger).run("leaky.load")

    assert outcome.exit_code is runtime.ExitCode.FAILED
    _assert_hidden(_shown(ledger, outcome))


def test_a_pipeline_that_cannot_be_built_is_recorded_redacted() -> None:
    def build(resources: runtime.Resources) -> pipeline.BaseOrchestrator:
        raise ConnectionRefusedError(_LEAKY)

    ledger = runtime.InMemoryRunLedger()

    outcome = _kernel(_Module(build), ledger).run("leaky.load")

    assert outcome.exit_code is runtime.ExitCode.FAILED
    _assert_hidden(_shown(ledger, outcome))


def test_a_fingerprint_error_is_recorded_redacted() -> None:
    ledger = runtime.InMemoryRunLedger()
    module = _Module(
        lambda resources: _Steps(_cancelled_then_raises),
        fingerprint_error=OSError(_LEAKY),
    )

    outcome = _kernel(module, ledger).run("leaky.load")

    shown = _shown(ledger, outcome)
    _assert_hidden(shown)
    assert "Key (email)=(***)" in shown


def test_a_file_ledger_keeps_nothing_of_the_secrets(
    tmp_path: pathlib.Path,
) -> None:
    ledger = runtime.JsonLinesRunLedger(tmp_path)
    kernel = _kernel(
        _Module(lambda resources: _Steps(_cancelled_then_raises)), ledger
    )

    kernel.run("leaky.load")

    _assert_hidden(ledger.path.read_text(encoding="utf-8"))


def test_a_health_check_s_error_is_shown_redacted() -> None:
    def down(source: object) -> None:
        del source  # Unused.
        raise ConnectionRefusedError(_LEAKY)

    kernel = _kernel(
        _Module(lambda resources: _Steps(_cancelled_then_raises)),
        runtime.InMemoryRunLedger(),
        health=down,
    )

    report = kernel.check()

    _assert_hidden(report["source"].status)
    assert report["source"].status.startswith("ConnectionRefusedError: ")


def test_a_health_check_s_warning_is_shown_redacted() -> None:
    def warns(source: object) -> list[str]:
        del source  # Unused.
        return [_LEAKY]

    kernel = _kernel(
        _Module(lambda resources: _Steps(_cancelled_then_raises)),
        runtime.InMemoryRunLedger(),
        health=warns,
    )

    report = kernel.check()

    assert report["source"].ok
    (warning,) = report["source"].warnings
    _assert_hidden(warning)


@pytest.mark.parametrize("redact", [(), ("badge",)])
def test_the_built_in_names_need_no_configuration(
    redact: tuple[str, ...],
) -> None:
    ledger = runtime.InMemoryRunLedger()
    kernel = runtime.Kernel(
        runtime.Resources(
            [runtime.ResourceSpec(names.LEDGER, lambda r: ledger)]
        ),
        [_Module(lambda resources: _Steps(_cancelled_then_raises))],
        redact=redact,
    )

    shown = _shown(ledger, kernel.run("leaky.load"))

    assert "S3cr3t" not in shown
    assert "jane@example.com" not in shown
    assert ("B-4711" in shown) is not bool(redact)


def test_the_application_s_kernel_hides_the_names_of_its_logs(
    tmp_path: pathlib.Path,
) -> None:
    (tmp_path / "base.toml").write_text(
        f'[bookkeeping]\ndirectory = "{tmp_path / "var"}"\n'
        '[logging]\nredact = ["badge"]\n',
        encoding="utf-8",
    )

    with bootstrap.bootstrap(
        "test", directory=tmp_path, logs=False
    ) as application:
        assert application.kernel.redact == ("badge",)
