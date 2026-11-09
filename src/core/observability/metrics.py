"""Metrics of runs: rows, durations, lags.

    metrics.meter().add("etl.rows.loaded", 1000, {"etl.pipeline": "crm.sync"})

The composition root installs a meter exporting to OpenTelemetry, or none.
Names are dotted, the unit apart (`etl.step.duration`, in `s`).
"""

from __future__ import annotations

import abc
import threading

from core import contract


class IMeter(abc.ABC):
    """Records measurements; instruments are created on first use."""

    @abc.abstractmethod
    def add(
        self,
        name: str,
        value: float,
        attributes: contract.Attributes | None = None,
        *,
        unit: str = "1",
        description: str = "",
    ) -> None:
        """Adds to a counter, e.g. rows loaded."""

    @abc.abstractmethod
    def record(
        self,
        name: str,
        value: float,
        attributes: contract.Attributes | None = None,
        *,
        unit: str = "1",
        description: str = "",
    ) -> None:
        """Records a value of a histogram, e.g. a duration."""

    @abc.abstractmethod
    def gauge(
        self,
        name: str,
        value: float,
        attributes: contract.Attributes | None = None,
        *,
        unit: str = "1",
        description: str = "",
    ) -> None:
        """Sets a gauge, e.g. the lag of a change log."""


class NoopMeter(IMeter):
    """Records nothing: the default."""

    def add(
        self,
        name: str,
        value: float,
        attributes: contract.Attributes | None = None,
        *,
        unit: str = "1",
        description: str = "",
    ) -> None:
        del name, value, attributes, unit, description  # Unused.

    def record(
        self,
        name: str,
        value: float,
        attributes: contract.Attributes | None = None,
        *,
        unit: str = "1",
        description: str = "",
    ) -> None:
        del name, value, attributes, unit, description  # Unused.

    def gauge(
        self,
        name: str,
        value: float,
        attributes: contract.Attributes | None = None,
        *,
        unit: str = "1",
        description: str = "",
    ) -> None:
        del name, value, attributes, unit, description  # Unused.


class InMemoryMeter(IMeter):
    """Keeps the measurements, for tests.

    Attributes:
        counters: Sums, by name and attributes.
        histograms: Values recorded, by name and attributes.
        gauges: Last values, by name and attributes.
    """

    def __init__(self) -> None:
        self.counters: dict[tuple[str, frozenset[object]], float] = {}
        self.histograms: dict[tuple[str, frozenset[object]], list[float]] = {}
        self.gauges: dict[tuple[str, frozenset[object]], float] = {}
        self._lock = threading.Lock()

    def add(
        self,
        name: str,
        value: float,
        attributes: contract.Attributes | None = None,
        *,
        unit: str = "1",
        description: str = "",
    ) -> None:
        key = (name, frozenset((attributes or {}).items()))
        with self._lock:
            self.counters[key] = self.counters.get(key, 0) + value

    def record(
        self,
        name: str,
        value: float,
        attributes: contract.Attributes | None = None,
        *,
        unit: str = "1",
        description: str = "",
    ) -> None:
        key = (name, frozenset((attributes or {}).items()))
        with self._lock:
            self.histograms.setdefault(key, []).append(value)

    def gauge(
        self,
        name: str,
        value: float,
        attributes: contract.Attributes | None = None,
        *,
        unit: str = "1",
        description: str = "",
    ) -> None:
        key = (name, frozenset((attributes or {}).items()))
        with self._lock:
            self.gauges[key] = value

    def total(self, name: str, **attributes: object) -> float:
        """The sum of a counter over the series having these attributes."""
        wanted = set(attributes.items())
        return sum(
            value
            for (found, labels), value in self.counters.items()
            if found == name and wanted <= set(labels)
        )


_meter: IMeter = NoopMeter()


def set_meter(installed: IMeter | None) -> None:
    """Installs the process's meter; None for none."""
    global _meter  # pylint: disable=global-statement
    _meter = installed if installed is not None else NoopMeter()


def meter() -> IMeter:
    """The process's meter."""
    return _meter
