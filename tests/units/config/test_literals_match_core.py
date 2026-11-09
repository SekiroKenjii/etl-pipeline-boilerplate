"""The `Literal` fields of the settings and the values of the core they
stand for: `config` may not import `core`, so they are written twice, and
must not drift apart."""

from __future__ import annotations

import enum
import typing

import pydantic
import pytest

import config
from core import mapping_pipelines
from core import operators


def _literal_values(annotation: object) -> frozenset[object]:
    if typing.get_origin(annotation) is typing.Literal:
        return frozenset(typing.get_args(annotation))
    found: frozenset[object] = frozenset()
    for argument in typing.get_args(annotation):
        found |= _literal_values(argument)
    return found


def _field(model: type[pydantic.BaseModel], name: str) -> frozenset[object]:
    return _literal_values(model.model_fields[name].annotation)


def _property(protocol: type, name: str) -> frozenset[object]:
    """The `Literal` a property of a protocol of the core returns."""
    getter = getattr(protocol, name).fget
    return _literal_values(typing.get_type_hints(getter)["return"])


# (settings model, field, core enum it mirrors)
ENUM_MIRRORS: list[tuple[type[pydantic.BaseModel], str, type[enum.Enum]]] = [
    (config.MappingModuleSettings, "deletes", operators.Deletes),
]

# The settings fields the core reads through a protocol, by the same name.
PROTOCOL_MIRRORS: list[tuple[type[pydantic.BaseModel], type, str]] = [
    (config.MappingModuleSettings, mapping_pipelines.MappingSettings, name)
    for name in ("orphans", "deletes", "snapshot")
]


@pytest.mark.parametrize(
    ("model", "field", "kind"),
    ENUM_MIRRORS,
    ids=[f"{m.__name__}.{f}" for m, f, _ in ENUM_MIRRORS],
)
def test_a_literal_has_the_values_of_its_enum(
    model: type[pydantic.BaseModel], field: str, kind: type[enum.Enum]
) -> None:
    assert _field(model, field) == {
        member.value for member in kind
    }, f"config.{model.__name__}.{field} drifted from {kind.__qualname__}"


@pytest.mark.parametrize(
    ("model", "protocol", "name"),
    PROTOCOL_MIRRORS,
    ids=[f"{m.__name__}.{n}" for m, _, n in PROTOCOL_MIRRORS],
)
def test_a_literal_has_the_values_the_core_reads(
    model: type[pydantic.BaseModel], protocol: type, name: str
) -> None:
    assert _field(model, name) == _property(protocol, name), (
        f"config.{model.__name__}.{name} drifted from"
        f" {protocol.__qualname__}.{name}"
    )
