"""The locks of a run: a lease per step it runs (`<pipeline>/<step>`), and
the wait of a retry of a label for the locks another try of it holds."""

from __future__ import annotations

from collections.abc import Callable, Collection, Sequence
import dataclasses
import enum
import logging
import re
import time

from core import contract
from core.runtime import steps as runtime_steps

_LOG = logging.getLogger("etl.kernel")

# A labelled run's locks are owned by `label:<label>@<run id>`: it must fit
# the owner column of the locks, 200 characters, with a run id of 32.
_LABELLED = "label:"
_MAX_LABEL_LENGTH = 200 - len(_LABELLED) - len("@") - 32
_MAX_HOST_IN_OWNER = 200 - len(":") - 32
_RUN_ID = re.compile("[0-9a-f]{32}")

# How many times a retry that waited for its label's locks reads the run it
# resumes again, when the steps no longer carried need a wait too. Each
# wait lasts up to the lease: the bound keeps a retry from waiting forever
# behind tries of its label ending one after the other. Past it, the retry
# runs the steps as last read, holding every lock it needs; a step a try
# finished during the last wait runs again (at least once).
_RECOMPUTE_ROUNDS = 2


class Refusal(enum.StrEnum):
    """Why a run took none of its locks, as its outcome tells it."""

    LOCKED = "already running"
    CANCELLED = "cancelled while waiting for the locks"


class LockWait:
    """How a run of a label waits for the locks another try of the label
    holds: on the monotonic clock, polling them.

    Tests pass a fake one, whose time passes only while a run waits.
    """

    def now(self) -> float:
        """The monotonic time, in seconds."""
        return time.monotonic()

    def pause(self, ctx: contract.ETLContext, seconds: float) -> bool:
        """Waits `seconds` between two polls; returns whether the run was
        cancelled meanwhile."""
        return ctx.wait(seconds)


@dataclasses.dataclass(frozen=True, slots=True)
class Leases:
    """The leases of a run on the locks of its steps.

    Attributes:
        lock: The run lock, if any.
        pipeline: The pipeline.
        names: The locks, `<pipeline>/<step>`.
        owner: Who holds them.
    """

    lock: contract.IRunLock | None
    pipeline: str
    names: tuple[str, ...]
    owner: str

    def acquire(self, lease_seconds: float) -> str | None:
        """Takes every lock, or none: never waits, so never deadlocks.

        Returns:
            None once every lock is taken, else the one held by another.
        """
        if self.lock is None:
            return None
        taken: list[str] = []
        try:
            for name in sorted(self.names):
                if not self.lock.acquire(name, self.owner, lease_seconds):
                    self._release(taken)
                    return name
                taken.append(name)
        except BaseException:
            self._release(taken)
            raise
        return None

    def without(self, freed: Collection[str]) -> Leases:
        """Frees the locks in `freed`, held: the leases of the others."""
        self._release([name for name in self.names if name in freed])
        return Leases(
            lock=self.lock,
            pipeline=self.pipeline,
            names=tuple(n for n in self.names if n not in freed),
            owner=self.owner,
        )

    def holder(self, name: str) -> str | None:
        """Who holds lock `name`; None if it is free."""
        return None if self.lock is None else self.lock.holder(name)

    def renew(self, lease_seconds: float) -> bool:
        """Extends every lease; False if one was lost.

        Raises:
            Exception: What the lock raised, if no lease was found lost.
        """
        if self.lock is None:
            return True
        lost: list[str] = []
        error: Exception | None = None
        # Every lease is renewed, even after one is found lost.
        for name in self.names:
            try:
                if not self.lock.renew(name, self.owner, lease_seconds):
                    lost.append(name)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                error = error or exc
        if lost:
            _LOG.error("locks lost: %s", ", ".join(lost))
            return False
        if error is not None:
            raise error
        return True

    def release(self) -> None:
        """Frees every lock; one that cannot be freed expires."""
        self._release(self.names)

    def _release(self, held: Sequence[str]) -> None:
        if self.lock is None:
            return
        for name in held:
            try:
                self.lock.release(name, self.owner)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                _LOG.warning("could not release %s, it expires: %s", name, exc)


@dataclasses.dataclass(frozen=True, slots=True)
class Locking:
    """How the kernel takes the locks of a run.

    Attributes:
        lock: The run lock, if any.
        lease_seconds: The lease of the locks; a run of a label waits up
            to it for the locks another try of the label holds.
        wait: The clock of that wait.
    """

    lock: contract.IRunLock | None
    lease_seconds: float
    wait: LockWait

    def lock_steps(
        self,
        plan: Sequence[contract.PipelineStep],
        ctx: contract.ETLContext,
        owner: str,
        *,
        label: str | None,
        ledger: contract.IRunLedger | None,
        resumed: str | None,
        resume_again: Callable[[], str | None] | None,
    ) -> Refusal | tuple[Leases, str | None, dict[str, runtime_steps.DoneStep]]:
        """Takes the locks of the steps a run runs, but those it carries
        over from the run it resumes.

        Args:
            plan: The steps of the pipeline.
            ctx: The run.
            owner: Who holds the locks (`owner_of`).
            label: The label of the run, if any.
            ledger: The run ledger, if any.
            resumed: The run it resumes, found before taking the locks.
            resume_again: Finds the run to resume again; None when the run
                resumes none.

        Returns:
            The refusal when they cannot be taken; else the leases, the
            run resumed and the steps carried over. After waiting for a try
            of the label, the run to resume is found again: that try may
            have finished steps meanwhile, whose locks are given back, or
            failed steps carried until then, whose locks are taken, by the
            same rules, `_RECOMPUTE_ROUNDS` times at most.
        """
        name = ctx.run.pipeline_name
        selected = [
            step.name
            for step in plan
            if not ctx.run.steps or step.name in ctx.run.steps
        ]
        carried = _carried(ledger, resumed)
        leases = Leases(
            self.lock, name, _locks_of(name, selected, carried), owner
        )
        for recompute in range(_RECOMPUTE_ROUNDS + 1):
            refused, waited = self.take(leases, ctx, label)
            if refused is not None:
                return refused
            if not (waited and resume_again):
                break
            if recompute == _RECOMPUTE_ROUNDS:
                _LOG.warning(
                    "%s waited %d times for the locks of its label: the"
                    " steps a try finished during the last wait run again",
                    name,
                    recompute + 1,
                )
                break
            resumed = resume_again()
            carried = _carried(ledger, resumed)
            needed = _locks_of(name, selected, carried)
            if set(needed) <= set(leases.names):
                leases = leases.without(set(leases.names) - set(needed))
                break
            # A step carried before the wait, failed since by the try waited
            # for: its lock was never taken. Nothing is held while waiting
            # for it, as for any lock, so two retries never deadlock.
            leases.release()
            leases = Leases(self.lock, name, needed, owner)
        if resumed:
            _LOG.info("%s resumes the run %s", name, resumed)
        return leases, resumed, carried

    def take(
        self, leases: Leases, ctx: contract.ETLContext, label: str | None
    ) -> tuple[Refusal | None, bool]:
        """Takes the locks of a run.

        Returns:
            The refusal when it cannot, None once taken; and whether it
            waited for them.

        A run of a label waits, up to the lease, for the locks another run of
        that label holds (a try that died, or still alive): they are free
        once the lease of a dead one expires. A holder of another label makes it
        give up at once, as any holder does an unlabelled run.
        """
        poll = self.lease_seconds / 10
        deadline = self.wait.now() + self.lease_seconds + poll
        waiting = False
        while (held := leases.acquire(self.lease_seconds)) is not None:
            holder = leases.holder(held)
            if (
                label is None
                or (holder is not None and _label_of(holder) != label)
                or self.wait.now() >= deadline
            ):
                _LOG.warning(
                    "%s is held by %s: skipped",
                    held,
                    holder or "another run",
                )
                return Refusal.LOCKED, waiting
            if not waiting:
                _LOG.warning(
                    "%s is held by %s, of the same label: waiting up to"
                    " %.0fs for it",
                    held,
                    holder or "another run",
                    self.lease_seconds,
                )
                waiting = True
            if self.wait.pause(ctx, poll):
                _LOG.warning("cancelled while waiting for %s", held)
                return Refusal.CANCELLED, waiting
        return None, waiting


def owner_of(host: str, label: str | None, run_id: str) -> str:
    """Who holds the locks of a run: this process, never another."""
    if label is None:
        return f"{host[:_MAX_HOST_IN_OWNER]}:{run_id}"
    return f"{_LABELLED}{label}@{run_id}"


def check_label(label: str | None) -> None:
    """Raises PipelineConfigError if a label does not fit the owner of a
    lock."""
    if label is not None and not 0 < len(label) <= _MAX_LABEL_LENGTH:
        raise contract.PipelineConfigError(
            f"A label has 1 to {_MAX_LABEL_LENGTH} characters"
        )


def _carried(
    ledger: contract.IRunLedger | None, resumed: str | None
) -> dict[str, runtime_steps.DoneStep]:
    """The steps carried over from the run resumed, if any."""
    if ledger is None or resumed is None:
        return {}
    return runtime_steps.done_steps(ledger, resumed)


def _locks_of(
    name: str, selected: Sequence[str], carried: Collection[str]
) -> tuple[str, ...]:
    """The locks of the steps a run runs: those selected, less those carried
    over."""
    return tuple(f"{name}/{step}" for step in selected if step not in carried)


def _label_of(owner: str) -> str | None:
    """The label of the run owning a lock; None for an unlabelled run.

    The owner is `label:<label>@<run id>`. Earlier versions wrote
    `label:<label>` for every run of a label, the label alone, which may hold
    an '@' too: such a lock is taken as held by that label.
    """
    if not owner.startswith(_LABELLED):
        return None
    labelled = owner.removeprefix(_LABELLED)
    label, at, run_id = labelled.rpartition("@")
    return label if at and _RUN_ID.fullmatch(run_id) else labelled
