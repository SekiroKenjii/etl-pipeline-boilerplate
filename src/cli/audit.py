"""What operators did to the ETL's records, in the run ledger.

An erasure (`etl keys forget`) or a reset of saved state (`etl state reset`)
is recorded as a run of its own pipeline (`etl.erasure`, `etl.state-reset`),
with who did it: `$ETL_OPERATOR` when set (the person, or the ticket, behind
a Job or a CI run: inside a container the user of the process is just
`etl`), else the user of the process.
"""

from __future__ import annotations

from collections.abc import Mapping
import getpass
import os
import socket

from core import contract
from core import runtime
from core.runtime import names

OPERATOR_VARIABLE = "ETL_OPERATOR"


def operator() -> str:
    """Who runs the command: `$ETL_OPERATOR`, else the process's user."""
    named = os.environ.get(OPERATOR_VARIABLE, "").strip()
    if named:
        return named
    try:
        return getpass.getuser()
    except (KeyError, OSError):
        return "unknown"


def record(
    resources: runtime.Resources,
    pipeline_name: str,
    meta: Mapping[str, str],
    summary: str,
) -> bool:
    """Records an operation in the run ledger, as a run of `pipeline_name`.

    Args:
        resources: The resources of the command.
        pipeline_name: What the operation is, e.g. `etl.erasure`.
        meta: What it was done to, never personal data; the operator is
            added as `user`.
        summary: What was done.

    Returns:
        Whether it was recorded: False when the settings give no ledger.
    """
    try:
        ledger = resources.get(names.LEDGER, contract.IRunLedger)
    except contract.PipelineConfigError:
        return False
    run = contract.ETLRunInfo(pipeline_name=pipeline_name)
    result = contract.PipelineResult(
        pipeline_name=run.pipeline_name, run_id=run.run_id
    )
    result.complete(contract.ETLStatus.SUCCESS, summary)
    ledger.start(run, socket.gethostname(), {**meta, "user": operator()})
    ledger.finish(result)
    return True
