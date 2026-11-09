"""The cutover report of a module: its entities, its last runs, a verdict.

The verdict is one of:

- `PASSED`: every entity reconciles, every missing row is a parked orphan.
- `PASSED-WITH-REJECTS`: likewise, but rejected rows are left behind, all of
  them accepted (`AcceptedRejections`).
- `FAILED`: something is missing, extra, different or rejected that nothing
  explains; or an entity is empty on both sides (`empty`), unless the BAs
  accept it (`AcceptedRejections.empty`).
- `COUNTED`: the rows were only counted (`--counts-only`): no verdict.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
import dataclasses
import datetime

from core import contract
from core.cutover import acceptance
from core.cutover import checksum
from core.cutover import diff

PASSED: contract.Verdict = "PASSED"
PASSED_WITH_REJECTS: contract.Verdict = "PASSED-WITH-REJECTS"
FAILED: contract.Verdict = "FAILED"
COUNTED: contract.Verdict = "COUNTED"
_FAILING: frozenset[contract.EntityStatus] = frozenset({"mismatch", "empty"})


@dataclasses.dataclass(slots=True)
class CutoverReport:
    """Whether the target holds what the source says, entity by entity.

    Attributes:
        module: The module reconciled.
        generated_at: When.
        entities: The reconciliation of each entity.
        runs: The last run of each pipeline of the module.
        provenance: What produced the report, for the auditors: the
            environment, the version of the code, the hash of the settings,
            the module's fingerprint (mappings, corrections, keys), the
            databases compared (never their credentials), the options, the
            rejected rows accepted.
    """

    module: str
    generated_at: datetime.datetime
    entities: list[diff.EntityReport]
    runs: list[contract.RunRecord] = dataclasses.field(
        default_factory=list[contract.RunRecord]
    )
    provenance: dict[str, object] = dataclasses.field(
        default_factory=dict[str, object]
    )

    @property
    def passed(self) -> bool:
        """Whether no entity has a difference left unexplained."""
        return all(entity.status not in _FAILING for entity in self.entities)

    @property
    def verdict(self) -> contract.Verdict:
        """The verdict: see the module's documentation."""
        statuses = {entity.status for entity in self.entities}
        if statuses & _FAILING:
            return FAILED
        if "counted" in statuses:
            return COUNTED
        if "match-with-rejects" in statuses:
            return PASSED_WITH_REJECTS
        return PASSED

    def as_dict(self) -> dict[str, object]:
        """The report as JSON-compatible data."""
        return {
            "module": self.module,
            "generated_at": self.generated_at.isoformat(),
            "verdict": self.verdict,
            "passed": self.passed,
            "provenance": self.provenance,
            "entities": [_entity(entity) for entity in self.entities],
            "runs": [
                {
                    "pipeline": run.pipeline,
                    "status": run.status.value,
                    "started_at": run.started_at.isoformat(),
                    "completed_at": (
                        run.completed_at.isoformat()
                        if run.completed_at
                        else None
                    ),
                    "stats": dict(run.stats),
                    "message": run.message,
                    "run_id": run.run_id,
                }
                for run in self.runs
            ],
        }


def last_runs(
    records: Iterable[contract.RunRecord], module: str
) -> list[contract.RunRecord]:
    """The last run of each pipeline of `module`, by pipeline name."""
    last: dict[str, contract.RunRecord] = {}
    for record in records:
        if not record.pipeline.startswith(f"{module}."):
            continue
        known = last.get(record.pipeline)
        if known is None or record.started_at > known.started_at:
            last[record.pipeline] = record
    return [last[name] for name in sorted(last)]


def _entity(entity: diff.EntityReport) -> dict[str, object]:
    """An entity's reconciliation as JSON-compatible data."""
    fields = dataclasses.asdict(entity)
    fields["samples"] = [
        {
            "kind": sample.kind,
            "key": [checksum.shown(value) for value in sample.key],
            "columns": {
                column: {
                    "expected": checksum.shown(want),
                    "actual": checksum.shown(got),
                }
                for column, (want, got) in sample.columns.items()
            },
        }
        for sample in entity.samples
    ]
    fields["status"] = entity.status
    fields["unexplained_missing"] = entity.unexplained_missing
    fields["unaccepted"] = entity.unaccepted
    return fields


# The options of `diff.reconcile`, keyword-only, next to the report's own.
def build(  # pylint: disable=too-many-arguments
    module: str,
    reconciliations: Sequence[contract.Reconciliation],
    *,
    parking: contract.IParkingLot | None = None,
    ledger: contract.IRunLedger | None = None,
    counted_only: bool = False,
    buckets: int | None = None,
    samples: int = 20,
    accepted: acceptance.AcceptedRejections | None = None,
    resolution: int = checksum.MICROSECONDS,
    provenance: Mapping[str, object] | None = None,
    spill_rows: int = diff.SPILL_ROWS,
) -> CutoverReport:
    """Reconciles every entity of a module, and reports.

    Args:
        module: The module.
        reconciliations: Its entities.
        parking: The parking lot, to tell parked orphans from missing rows.
        ledger: The run ledger, for the last runs of the module.
        counted_only: Whether to count the rows only, in the databases.
        buckets: See `diff.reconcile`.
        samples: See `diff.reconcile`.
        accepted: The rejected rows the BAs accept to leave behind, and the
            entities they accept empty.
        resolution: See `diff.reconcile`.
        provenance: What produces the report (`CutoverReport.provenance`).
        spill_rows: See `diff.reconcile`.
    """
    entities: list[diff.EntityReport] = []
    for reconciliation in reconciliations:
        parked, keys = _parked(parking, reconciliation, resolution)
        if counted_only:
            entities.append(diff.count(reconciliation, parked=parked))
            continue
        entity = diff.reconcile(
            reconciliation,
            buckets=buckets,
            samples=samples,
            parked=parked,
            parked_keys=keys,
            accepted=accepted.of(reconciliation.entity) if accepted else {},
            resolution=resolution,
            spill_rows=spill_rows,
        )
        entity.accepted_empty = bool(
            accepted and accepted.accepts_empty(entity.entity)
        )
        entities.append(entity)
    runs = last_runs(ledger.recent(None, 1000), module) if ledger else []
    return CutoverReport(
        module=module,
        generated_at=datetime.datetime.now(datetime.UTC),
        entities=entities,
        runs=runs,
        provenance=dict(provenance or {}),
    )


def _parked(
    parking: contract.IParkingLot | None,
    reconciliation: contract.Reconciliation,
    resolution: int,
) -> tuple[int | None, Collection[str]]:
    """How many orphans are parked, and the key texts of those whose target
    row the reconciliation tells (`Reconciliation.parked_row`)."""
    queue = reconciliation.parking_queue
    if parking is None or queue is None:
        return None, frozenset()
    found = 0
    keys: set[str] = set()
    for entry in parking.entries(queue):
        found += 1
        row = (
            reconciliation.parked_row(entry)
            if reconciliation.parked_row is not None
            else None
        )
        if row is not None and all(
            row.get(column) is not None for column in reconciliation.key
        ):
            keys.add(
                checksum.text(
                    checksum.canonical_row(row, reconciliation.key, resolution)
                )
            )
    return found, keys
