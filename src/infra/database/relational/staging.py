"""Choosing online how a batch is staged: as parameters of an INSERT, or by
the driver's bulk loader (SQL Server's bulk copy).

Which is faster depends on the server, the network, the row width and the
batch size: a laptop's threshold is wrong elsewhere. So, per target table,
the first batches alternate the two ways, each timed per row; the faster
one is kept, and tried against the other again every `RECHECK_EVERY`
batches, switching only when the other is `HYSTERESIS` faster, so that
noise does not flip the choice. Batches too small to measure go by
parameters.
"""

from __future__ import annotations

from collections.abc import Callable
import dataclasses
import logging
import threading
import time

from infra.database.relational import types as relational_types

_LOG = logging.getLogger(__name__)

PARAMETERS: relational_types.Staging = "parameters"
BULK_COPY: relational_types.Staging = "bulk copy"
# The batches each table alternates before the first choice.
TRIAL_BATCHES = 3
RECHECK_EVERY = 200
HYSTERESIS = 0.2
# Below it, a batch goes by parameters, unmeasured: too short to time, and
# the bulk loader's fixed cost would show whatever the server.
SMALL_BATCH = 100


@dataclasses.dataclass(slots=True)
class _Table:
    """What is known of one target table."""

    batches: int = 0
    # Seconds a row, the latest measured, by strategy.
    seconds: dict[relational_types.Staging, float] = dataclasses.field(
        default_factory=dict[relational_types.Staging, float]
    )
    chosen: relational_types.Staging | None = None


class StagingChooser:
    """Picks, per target table, between staging by parameters and by bulk
    copy, from measurements of both.

    `pick` says how the next batch goes; the caller times that batch and
    `record`s it. Thread-safe: steps writing one table at once share its
    measurements.

    Attributes:
        trials: The batches a table alternates before the first choice.
        recheck_every: The batches between two tries of the other way.
        hysteresis: The share by which the other way must be faster, per
            row, to be chosen instead.
    """

    def __init__(
        self,
        *,
        trials: int = TRIAL_BATCHES,
        recheck_every: int = RECHECK_EVERY,
        hysteresis: float = HYSTERESIS,
        clock: Callable[[], float] = time.perf_counter,
    ) -> None:
        """Initializes the chooser.

        Args:
            trials: The batches a table alternates before the first choice.
            recheck_every: The batches between two tries of the other way.
            hysteresis: The share by which the other way must be faster to
                be chosen instead.
            clock: Seconds, for the timings.

        Raises:
            ValueError: If `trials` is below 2, `recheck_every` below 1 or
                `hysteresis` not a share.
        """
        if trials < 2 or recheck_every < 1 or not 0 <= hysteresis < 1:
            raise ValueError(
                "trials must be >= 2, recheck_every >= 1, hysteresis in [0, 1)"
            )
        self.trials = trials
        self.recheck_every = recheck_every
        self.hysteresis = hysteresis
        self.clock = clock
        self._tables: dict[str, _Table] = {}
        self._lock = threading.Lock()

    def chosen(self, table: str) -> relational_types.Staging | None:
        """The strategy kept for `table`; None while it is being tried."""
        with self._lock:
            state = self._tables.get(table)
            return state.chosen if state else None

    def pick(self, table: str, rows: int) -> relational_types.Staging:
        """How the next batch of `rows` rows of `table` is staged.

        A batch below `SMALL_BATCH` rows goes by parameters, and is not
        measured: `record` it anyway, it is ignored.
        """
        if rows < SMALL_BATCH:
            return PARAMETERS
        with self._lock:
            state = self._tables.setdefault(table, _Table())
            if state.chosen is None:
                return (PARAMETERS, BULK_COPY)[state.batches % 2]
            if state.batches % self.recheck_every == self.recheck_every - 1:
                return _other(state.chosen)
            return state.chosen

    def record(
        self,
        table: str,
        strategy: relational_types.Staging,
        rows: int,
        seconds: float,
    ) -> None:
        """Keeps the time one batch of `rows` rows took by `strategy`, and
        chooses when the measurements say so."""
        if rows < SMALL_BATCH:
            return
        with self._lock:
            state = self._tables.setdefault(table, _Table())
            state.batches += 1
            state.seconds[strategy] = seconds / rows
            if state.chosen is None:
                if state.batches >= self.trials and len(state.seconds) == 2:
                    state.chosen = self._faster(state)
                    self._log(table, state, "chosen")
            elif strategy != state.chosen:
                current = state.seconds[state.chosen]
                if state.seconds[strategy] < current * (1 - self.hysteresis):
                    state.chosen = strategy
                    self._log(table, state, "chosen instead")

    def _faster(self, state: _Table) -> relational_types.Staging:
        return min(state.seconds, key=lambda name: state.seconds[name])

    def _log(self, table: str, state: _Table, what: str) -> None:
        timings = ", ".join(
            f"{name} {seconds * 1000:.3f} ms a row"
            for name, seconds in sorted(state.seconds.items())
        )
        _LOG.info(
            "%s: staging by %s %s after %d batch(es): %s",
            table,
            state.chosen,
            what,
            state.batches,
            timings,
        )


def _other(strategy: relational_types.Staging) -> relational_types.Staging:
    return BULK_COPY if strategy == PARAMETERS else PARAMETERS
