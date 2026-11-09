"""The runs of a label: a module recording the steps it runs, its kernels,
and a clock that passes only while a run waits for its locks.

Shared by `test_resume_steps_and_locks.py` and `test_locks_after_the_wait.py`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
import threading
import time

from core import contract
from core import pipeline
from core import runtime
from core.runtime import names


class Steps(pipeline.BaseOrchestrator):
    """`extract` → `load` → `report`, plus `parts` independent steps."""

    def __init__(self, module: RecordingModule) -> None:
        super().__init__(max_workers=module.max_workers)
        self.module = module

    def steps(self) -> Sequence[contract.PipelineStep]:
        handler = self.module.handler
        return [
            contract.PipelineStep("extract", handler("extract")),
            contract.PipelineStep(
                "load", handler("load"), dependencies=("extract",)
            ),
            contract.PipelineStep(
                "report", handler("report"), dependencies=("load",)
            ),
            *(
                contract.PipelineStep(f"part:{i}", handler(f"part:{i}"))
                for i in range(self.module.parts)
            ),
        ]


class RecordingModule(runtime.Module):
    """Records the steps it runs; fails those in `failing`."""

    def __init__(self, parts: int = 0, max_workers: int = 1) -> None:
        self.parts = parts
        self.max_workers = max_workers
        self.failing: set[str] = set()
        self.calls: list[str] = []
        self.delay = 0.0
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        return "crm"

    def pipelines(self) -> dict[str, runtime.PipelineFactory]:
        return {"load": lambda resources: Steps(self)}

    def handler(self, step: str) -> contract.StepHandler:
        def run(
            ctx: contract.ETLContext,
            upstream: Mapping[str, contract.ETLResult],
        ) -> contract.ETLResult:
            del upstream  # Unused.
            with self._lock:
                self.calls.append(step)
            time.sleep(self.delay)
            if step in self.failing:
                raise contract.FatalError(f"{step} broke")
            return contract.ETLResult(stats=contract.ETLStats(total=1))

        return run


def kernel_of(
    module: RecordingModule,
    *,
    lock: contract.IRunLock | None = None,
    ledger: contract.IRunLedger | None = None,
    host: str = "host-1",
    lease_seconds: float = 60.0,
) -> runtime.Kernel:
    specs: list[runtime.ResourceSpec[object]] = [
        runtime.ResourceSpec(
            names.LEDGER, lambda r: ledger or runtime.InMemoryRunLedger()
        )
    ]
    if lock is not None:
        specs.append(runtime.ResourceSpec(names.LOCK, lambda r: lock))
    return runtime.Kernel(
        runtime.Resources(specs),
        [module],
        host=host,
        lease_seconds=lease_seconds,
    )


class FakeTime(runtime.LockWait):
    """Time that passes only while a run waits for its locks: `then` is
    called at each pause, before the time moves on."""

    def __init__(self, then: Callable[[], None] = lambda: None) -> None:
        self.time = 1000.0
        self.pauses: list[float] = []
        self.then = then

    def now(self) -> float:
        return self.time

    def pause(self, ctx: contract.ETLContext, seconds: float) -> bool:
        self.pauses.append(seconds)
        self.then()
        self.time += seconds
        return ctx.is_cancelled


# The lease of a run in these tests: it polls the locks every 6 seconds,
# and gives up after 66.
LEASE = 60.0
WAITED_THE_LEASE = [LEASE / 10] * 11


def retry_kernel(
    module: RecordingModule,
    lock: contract.IRunLock,
    ledger: contract.IRunLedger,
    time_: FakeTime,
) -> runtime.Kernel:
    """The kernel of a retry, in another pod, on fake time."""
    specs: list[runtime.ResourceSpec[object]] = [
        runtime.ResourceSpec(names.LEDGER, lambda r: ledger),
        runtime.ResourceSpec(names.LOCK, lambda r: lock),
    ]
    return runtime.Kernel(
        runtime.Resources(specs),
        [module],
        host="pod-2",
        lease_seconds=LEASE,
        lock_wait=time_,
    )
