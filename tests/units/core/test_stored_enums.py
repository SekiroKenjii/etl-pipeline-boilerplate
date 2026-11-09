"""The values the ETL writes down, recorded: they are a contract.

A ledger, a state store, a quarantine, a compiled catalog, a settings file or
a sealed cutover report holds the values of these enums and literals, and
every later version reads them back. Renaming a member or changing a value
fails here: add a reader for the old value (or a migration of what was
written), then record the new one.
"""

from __future__ import annotations

import enum
import typing

import pydantic
import pytest

import config
from core import contract

STORED_VALUES_ARE_A_CONTRACT = (
    "stored values are a contract: add a reader for the old value or a"
    " migration, then record the new one here"
)

# Enum -> {member: value}, as the ledger, the stores and the reports hold.
STORED_ENUMS: dict[type[enum.Enum], dict[str, str]] = {
    # Runs and steps of the ledger, `etl runs`, the cutover report.
    contract.ETLStatus: {
        "PENDING": "pending",
        "IN_PROGRESS": "in_progress",
        "SUCCESS": "success",
        "PARTIAL_SUCCESS": "partial_success",
        "FAILED": "failed",
        "SKIPPED": "skipped",
        "CANCELLED": "cancelled",
    },
    # The phase of an error, in the ledger.
    contract.ETLPhase: {
        "EXTRACT": "extract",
        "TRANSFORM": "transform",
        "LOAD": "load",
    },
    # Quarantine entries and quality issues.
    contract.Checkpoint: {
        "BEFORE_EXTRACT": "before_extract",
        "AFTER_EXTRACT": "after_extract",
        "AFTER_TRANSFORM": "after_transform",
        "AFTER_LOAD": "after_load",
    },
    contract.QualityAction: {
        "WARN": "warn",
        "REJECT": "reject",
        "FAIL": "fail",
    },
    contract.QualitySeverity: {"WARNING": "warning", "ERROR": "error"},
    contract.QuarantineStatus: {
        "OPEN": "open",
        "REPLAYED": "replayed",
        "CLOSED": "closed",
    },
    # Change events parked or quarantined.
    contract.ChangeOperation: {
        "INSERT": "insert",
        "UPDATE": "update",
        "DELETE": "delete",
    },
    # Settings and module code.
    contract.WriteMode: {
        "INSERT": "insert",
        "UPSERT": "upsert",
        "INSERT_MISSING": "insert_missing",
        "UPDATE": "update",
    },
    contract.SamplingStrategy: {"FIRST": "first", "RESERVOIR": "reservoir"},
    # The compiled catalogs; the keys derive from the hashing and the case.
    contract.KeyCase: {
        "PRESERVE": "preserve",
        "UPPER": "upper",
        "LOWER": "lower",
    },
    contract.KeyHashing: {"UUID5": "uuid5", "HMAC": "hmac"},
    contract.MappingStatus: {"DRAFT": "draft", "APPROVED": "approved"},
    contract.UnmappedPolicy: {"ERROR": "error", "IGNORE": "ignore"},
}

# The enums of `core.contract` whose values nothing writes down, and why.
UNSTORED_ENUMS: dict[type[enum.Enum], str] = {
    contract.StepFailurePolicy: "set in the code of a pipeline",
    contract.Operator: "conditions built by code, rendered per database",
    contract.TypeFamily: "described from a live schema, never saved",
}


def _literal_values(annotation: object) -> frozenset[object]:
    """The values of the `Literal` in `annotation`, through aliases, tuples
    and unions."""
    if isinstance(annotation, typing.TypeAliasType):
        return _literal_values(annotation.__value__)
    if typing.get_origin(annotation) is typing.Literal:
        return frozenset(typing.get_args(annotation))
    found: frozenset[object] = frozenset()
    for argument in typing.get_args(annotation):
        found |= _literal_values(argument)
    return found


def _field(model: type[pydantic.BaseModel], name: str) -> object:
    return model.model_fields[name].annotation


# Literals without an enum: the reports and the settings files hold them.
BOUNDARY_LITERALS: dict[str, tuple[object, frozenset[str]]] = {
    "contract.DifferenceKind": (
        contract.DifferenceKind,
        frozenset({"missing", "extra", "different", "duplicate"}),
    ),
    "contract.EntityStatus": (
        contract.EntityStatus,
        frozenset(
            {"match", "match-with-rejects", "mismatch", "counted", "empty"}
        ),
    ),
    "contract.Verdict": (
        contract.Verdict,
        frozenset({"PASSED", "PASSED-WITH-REJECTS", "FAILED", "COUNTED"}),
    ),
    "config.ChangesSettings.kind": (
        _field(config.ChangesSettings, "kind"),
        frozenset({"native", "debezium"}),
    ),
    "config.LoggingSettings.format": (
        _field(config.LoggingSettings, "format"),
        frozenset({"json", "console"}),
    ),
    "config.OtlpSettings.signals": (
        _field(config.OtlpSettings, "signals"),
        frozenset({"logs", "traces", "metrics"}),
    ),
    "config.StoreSettings.kind": (
        _field(config.StoreSettings, "kind"),
        frozenset({"local", "gcs"}),
    ),
    "config.MappingModuleSettings.orphans": (
        _field(config.MappingModuleSettings, "orphans"),
        frozenset({"reject", "park"}),
    ),
    "config.MappingModuleSettings.snapshot": (
        _field(config.MappingModuleSettings, "snapshot"),
        frozenset({"table", "log"}),
    ),
    # `"auto"`: derived from the budgets (`docs/tuning.md`).
    **{
        f"config.TuningSettings.{name}": (
            _field(config.TuningSettings, name),
            frozenset({"auto"}),
        )
        for name in (
            "memory_budget",
            "cpu_budget",
            "key_cache_memory",
            "key_cache_disk",
            "spill_rows",
            "max_parallel_steps",
            "bulk_copy_rows",
            "in_chunk",
        )
    },
}

# The Literal fields of the settings that mirror an enum of the core, whose
# values `tests/units/config/test_literals_match_core.py` checks.
MIRRORING_A_CORE_ENUM = frozenset({"config.MappingModuleSettings.deletes"})


@pytest.mark.parametrize(
    ("kind", "recorded"),
    list(STORED_ENUMS.items()),
    ids=[kind.__qualname__ for kind in STORED_ENUMS],
)
def test_stored_enum_values_are_as_recorded(
    kind: type[enum.Enum], recorded: dict[str, str]
) -> None:
    values = {member.name: member.value for member in kind}

    assert (
        values == recorded
    ), f"{kind.__qualname__}: {STORED_VALUES_ARE_A_CONTRACT}"


@pytest.mark.parametrize("name", list(BOUNDARY_LITERALS))
def test_boundary_literal_values_are_as_recorded(name: str) -> None:
    annotation, recorded = BOUNDARY_LITERALS[name]

    assert (
        _literal_values(annotation) == recorded
    ), f"{name}: {STORED_VALUES_ARE_A_CONTRACT}"


def test_every_enum_of_the_contract_is_classified() -> None:
    """A new enum of `core.contract` is recorded here, or said unstored."""
    enums = {
        value
        for value in vars(contract).values()
        if isinstance(value, type) and issubclass(value, enum.Enum)
    }

    assert enums == set(STORED_ENUMS) | set(UNSTORED_ENUMS), sorted(
        kind.__qualname__
        for kind in enums ^ (set(STORED_ENUMS) | set(UNSTORED_ENUMS))
    )


def test_every_literal_of_the_settings_is_recorded() -> None:
    """A new `Literal` field of `config` is recorded, or mirrors an enum."""
    found = {
        f"config.{name}.{field}"
        for name in config.__all__
        if isinstance(model := getattr(config, name), type)
        and issubclass(model, pydantic.BaseModel)
        for field, info in model.model_fields.items()
        if _literal_values(info.annotation)
    }
    recorded = {
        name for name in BOUNDARY_LITERALS if name.startswith("config.")
    }

    assert found == recorded | MIRRORING_A_CORE_ENUM, sorted(
        found ^ (recorded | MIRRORING_A_CORE_ENUM)
    )
