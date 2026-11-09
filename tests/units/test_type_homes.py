"""
Where types and enums are defined: a tiered home, so that a reader knows
where to look and the docs (`docs/reference/types.md`) show them all.

1. Cross-layer contracts in `core.contract`; generic value types
   (`JsonValue`) in `shared.utils`.
2. A package's own: one file of each kind, `documents.py` (stored or
   serialized shapes: Mongo documents, the compiled mappings' JSON),
   `clients.py` (what is used of a library's objects: pymongo, google,
   azure, confluent, openpyxl), `types.py` (anything else its modules share,
   or its callers pass). Each lists what it defines in `__all__`.
3. A module's own `type` aliases stay in it, private (`_Name`).

A public enum is defined in `core.contract` (its values are stored or cross
a layer) or `cli.exit_codes`; the few package-local ones are listed, with
their reason, in `LOCAL_ENUMS`. The layering rules are in
`test_architecture.py`.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
import pathlib

import pytest

SRC = pathlib.Path(__file__).resolve().parents[2] / "src"
SOURCES = sorted(SRC.rglob("*.py"))


def _module_name(path: pathlib.Path) -> str:
    parts = path.relative_to(SRC).with_suffix("").parts
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _matches(name: str, prefix: str) -> bool:
    return name == prefix or name.startswith(prefix + ".")


# Where a `TypedDict`, a `Protocol` or a public `type` alias may be defined:
# the cross-layer contracts, the generic helpers, and the one file of each
# kind a package has for its own (`documents.py`: stored or serialized
# shapes; `clients.py`: what is used of a library's objects; `types.py`:
# anything else its modules share). Elsewhere, an alias is private.
TYPE_HOMES: tuple[str, ...] = ("core.contract", "shared.utils")
BOUNDARY_FILES: frozenset[str] = frozenset(
    {"documents.py", "clients.py", "types.py"}
)

# Definitions that stay in their module, with the reason: moving them makes
# an import cycle with the classes of that module they name.
LOCAL_TYPES: dict[str, str] = {
    "core.runtime.resources.Declaration": (
        "takes the `Resources` and returns the `Scope` of its module"
    ),
    "core.cutover.checksum.Canonical": "names the `Number` of its module",
}


def _base_names(node: ast.ClassDef) -> list[str]:
    """The bases of a class, by their last name: `typing.Protocol[T]`
    gives `Protocol`."""
    names = []
    for base in node.bases:
        target = base.value if isinstance(base, ast.Subscript) else base
        names.append(ast.unparse(target).rpartition(".")[2])
    return names


def _type_definitions(source: str) -> Iterator[tuple[int, str, str]]:
    """The (line, name, kind) of the `TypedDict`s, `Protocol`s and `type`
    aliases at the top of `source`, private ones included."""
    typed_dicts = {"TypedDict"}
    for node in ast.parse(source).body:
        if isinstance(node, ast.ClassDef):
            bases = _base_names(node)
            if typed_dicts.intersection(bases):
                typed_dicts.add(node.name)
                yield node.lineno, node.name, "TypedDict"
            elif "Protocol" in bases:
                yield node.lineno, node.name, "Protocol"
        elif isinstance(node, ast.TypeAlias):
            yield node.lineno, node.name.id, "alias"
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and ast.unparse(node.annotation).endswith("TypeAlias")
        ):
            yield node.lineno, node.target.id, "alias"
        elif (
            isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Call)
            and ast.unparse(node.value.func).rpartition(".")[2] == "TypedDict"
        ):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    yield node.lineno, target.id, "TypedDict"


def _is_type_home(module: str, file_name: str) -> bool:
    return file_name in BOUNDARY_FILES or any(
        _matches(module, home) for home in TYPE_HOMES
    )


def _misplaced_types(source: str, module: str, file_name: str) -> list[str]:
    """The definitions of `source` out of their home, each with where to."""
    if _is_type_home(module, file_name):
        return []
    package = module.rpartition(".")[0].replace(".", "/")
    homes = {
        "TypedDict": "documents.py (a stored or serialized shape) or types.py",
        "Protocol": "clients.py (what is used of a library) or types.py",
        "alias": "types.py, or make it private",
    }
    return [
        f"{module}.{name} ({kind}, line {line}): move it to"
        f" src/{package}/{homes[kind]}"
        for line, name, kind in _type_definitions(source)
        if f"{module}.{name}" not in LOCAL_TYPES
        and (kind != "alias" or not name.startswith("_"))
    ]


def test_types_are_defined_in_their_homes() -> None:
    misplaced = [
        found
        for path in SOURCES
        for found in _misplaced_types(
            path.read_text(encoding="utf-8"), _module_name(path), path.name
        )
    ]

    assert not misplaced, "\n".join(misplaced)


def test_every_local_type_is_still_one() -> None:
    """An entry of `LOCAL_TYPES` names a definition still outside a home."""
    found = {
        f"{_module_name(path)}.{name}"
        for path in SOURCES
        if not _is_type_home(_module_name(path), path.name)
        for _, name, _ in _type_definitions(path.read_text(encoding="utf-8"))
    }

    assert set(LOCAL_TYPES) <= found, sorted(set(LOCAL_TYPES) - found)


def _public_names(source: str) -> set[str]:
    """The public classes, functions, aliases and constants of `source`."""
    names: set[str] = set()
    for node in ast.parse(source).body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef)):
            names.add(node.name)
        elif isinstance(node, ast.TypeAlias):
            names.add(node.name.id)
        elif isinstance(node, ast.Assign):
            names.update(
                target.id
                for target in node.targets
                if isinstance(target, ast.Name)
            )
        elif isinstance(node, ast.AnnAssign) and isinstance(
            node.target, ast.Name
        ):
            names.add(node.target.id)
    return {name for name in names if not name.startswith("_")}


def _declared_all(source: str) -> set[str] | None:
    """The names of the `__all__` of `source`; None without one."""
    for node in ast.parse(source).body:
        if (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "__all__"
                for target in node.targets
            )
            and isinstance(node.value, (ast.List, ast.Tuple))
        ):
            return {
                item.value
                for item in node.value.elts
                if isinstance(item, ast.Constant)
                and isinstance(item.value, str)
            }
    return None


BOUNDARY_SOURCES = [path for path in SOURCES if path.name in BOUNDARY_FILES]


@pytest.mark.parametrize(
    "path", BOUNDARY_SOURCES, ids=lambda p: str(p.relative_to(SRC))
)
def test_every_boundary_file_lists_what_it_defines(path: pathlib.Path) -> None:
    """`__all__` is what `docs/reference/types.md` shows of the file."""
    source = path.read_text(encoding="utf-8")

    declared = _declared_all(source)

    assert declared is not None, f"{path.relative_to(SRC)} has no __all__"
    assert declared == _public_names(source), sorted(
        declared ^ _public_names(source)
    )


# (module, file name, source) the type rules must refuse; synthetic.
MISPLACED: list[tuple[str, str, str]] = [
    ("app.probe", "probe.py", "class A(typing.TypedDict):\n    x: int\n"),
    ("app.probe", "probe.py", "class A(TypedDict, total=False):\n    x: int\n"),
    (
        "app.probe",
        "probe.py",
        "class A(TypedDict):\n    x: int\nclass B(A):\n    y: int\n",
    ),
    ("app.probe", "probe.py", "A = TypedDict('A', {'x': int})\n"),
    ("app.probe", "probe.py", "class _A(Protocol):\n    x: int\n"),
    ("app.probe", "probe.py", "class A[T](typing.Protocol):\n    x: T\n"),
    ("app.probe", "probe.py", "class A(Protocol[T]):\n    x: T\n"),
    ("app.probe", "probe.py", "type A = int\n"),
    ("app.probe", "probe.py", "A: typing.TypeAlias = int\n"),
    ("core.mapping.probe", "probe.py", "type Outcome = tuple[int, str]\n"),
    ("core.contractual", "contractual.py", "type A = int\n"),
    ("core.mapping.casts", "casts.py", "type Caster = int\n"),
]

# (module, file name, source) the type rules must accept.
WELL_PLACED: list[tuple[str, str, str]] = [
    ("app.probe", "probe.py", "type _A = int\n"),
    ("app.probe", "probe.py", "class A(Base):\n    x: int\n"),
    ("app.probe", "probe.py", "class A(contract.IPort):\n    x: int\n"),
    ("app.clients", "clients.py", "class A(Protocol):\n    x: int\n"),
    ("app.documents", "documents.py", "class A(TypedDict):\n    x: int\n"),
    ("core.mapping.types", "types.py", "type A = int\n"),
    ("core.contract.tables", "tables.py", "class A(Protocol):\n    x: int\n"),
    ("shared.utils.probe", "probe.py", "type JsonValue = int\n"),
]


@pytest.mark.parametrize(("module", "file_name", "source"), MISPLACED)
def test_the_type_rules_refuse(
    module: str, file_name: str, source: str
) -> None:
    assert _misplaced_types(source, module, file_name), source


@pytest.mark.parametrize(("module", "file_name", "source"), WELL_PLACED)
def test_the_type_rules_accept(
    module: str, file_name: str, source: str
) -> None:
    assert not _misplaced_types(source, module, file_name), source


# Where a public enum is defined: its values are stored, or cross a layer.
ENUM_HOMES: tuple[str, ...] = ("core.contract", "cli.exit_codes")
_ENUM_BASES = frozenset({"Enum", "StrEnum", "IntEnum", "Flag", "IntFlag"})

# The public enums that stay in their package, with the reason: nothing
# stores their values, and none but `ExitCode` leaves the package.
LOCAL_ENUMS: dict[str, str] = {
    "core.runtime.resources.Scope": "how long a resource lives, in the runtime",
    "core.operators.changes.Deletes": (
        "what a change loader does with a delete; the settings' `deletes`"
        " mirrors its values (test_literals_match_core.py)"
    ),
    "core.mapping.casts.TypeKind": "what a cast makes of a value",
    "core.keys.relationships.Evidence": (
        "why a key or a relationship is suggested, in the key discovery"
    ),
    "core.runtime.locks.Refusal": (
        "why a run took none of its locks; the kernel turns it into its"
        " exit code"
    ),
    "core.runtime.kernel.ExitCode": (
        "what a run tells the scheduler; `cli.exit_codes.ExitCode` has each"
        " member, by name and value (test_operations_cli.py)"
    ),
}


def _public_enums(source: str) -> Iterator[tuple[int, str]]:
    """The (line, name) of the public enums at the top of `source`."""
    for node in ast.parse(source).body:
        if (
            isinstance(node, ast.ClassDef)
            and not node.name.startswith("_")
            and _ENUM_BASES.intersection(_base_names(node))
        ):
            yield node.lineno, node.name


def _misplaced_enums(source: str, module: str) -> list[str]:
    """The public enums of `source` out of their homes and not listed."""
    if any(_matches(module, home) for home in ENUM_HOMES):
        return []
    return [
        f"{module}.{name} (line {line}): move it to core.contract (its"
        " values are stored or cross a layer), or list it in LOCAL_ENUMS"
        " with the reason it stays"
        for line, name in _public_enums(source)
        if f"{module}.{name}" not in LOCAL_ENUMS
    ]


def test_public_enums_are_defined_in_their_homes() -> None:
    misplaced = [
        found
        for path in SOURCES
        for found in _misplaced_enums(
            path.read_text(encoding="utf-8"), _module_name(path)
        )
    ]

    assert not misplaced, "\n".join(misplaced)


def test_every_local_enum_is_still_one() -> None:
    """An entry of `LOCAL_ENUMS` names a public enum out of the homes."""
    found = {
        f"{_module_name(path)}.{name}"
        for path in SOURCES
        for _, name in _public_enums(path.read_text(encoding="utf-8"))
        if not any(_matches(_module_name(path), home) for home in ENUM_HOMES)
    }

    assert set(LOCAL_ENUMS) <= found, sorted(set(LOCAL_ENUMS) - found)


@pytest.mark.parametrize(
    ("module", "source", "refused"),
    [
        ("app.probe", "class Color(enum.Enum):\n    RED = 1\n", True),
        ("core.quality.probe", "class Mode(StrEnum):\n    A = 'a'\n", True),
        ("infra.probe", "class Code(enum.IntEnum):\n    A = 1\n", True),
        ("core.runtime.probe", "class Bits(enum.Flag):\n    A = 1\n", True),
        ("core.mapping.probe", "class _Role(enum.Enum):\n    A = 1\n", False),
        (
            "core.contract.probe",
            "class Mode(enum.StrEnum):\n    A = 'a'\n",
            False,
        ),
        ("cli.exit_codes", "class ExitCode(enum.IntEnum):\n    A = 1\n", False),
        (
            "core.runtime.resources",
            "class Scope(enum.StrEnum):\n    A = 'a'\n",
            False,
        ),
        ("app.probe", "class Thing(Base):\n    A = 1\n", False),
    ],
)
def test_the_enum_rule(module: str, source: str, refused: bool) -> None:
    assert bool(_misplaced_enums(source, module)) is refused, source


# A package's `documents.py`, `clients.py` and `types.py` are its own: used
# by its modules (and subpackages), by the composition root (`app`), by the
# benchmarks and the tests (neither under `src`). A type other packages use
# belongs in `core.contract`; the few that stay are listed, with the reason.
BOUNDARY_IMPORTERS: tuple[str, ...] = ("app",)
CROSS_PACKAGE_TYPES: dict[str, str] = {
    "core.runtime.types.PipelineFactory": (
        "what a module gives the runtime to build a pipeline; it names"
        " `Resources`, which `core.contract` cannot import"
    ),
    "core.operators.types.SourceRecord": (
        "what the operators read, which the mapping pipelines correct and"
        " key; the operators' own vocabulary, not a port"
    ),
}

_MODULES: frozenset[str] = frozenset(_module_name(path) for path in SOURCES)
_BOUNDARY_MODULES: frozenset[str] = frozenset(
    _module_name(path) for path in SOURCES if path.name in BOUNDARY_FILES
)


def _reexports() -> dict[str, dict[str, str]]:
    """By package: the names its `__init__` imports from one of its
    boundary files, with the boundary module."""
    found: dict[str, dict[str, str]] = {}
    for path in SOURCES:
        if path.name != "__init__.py":
            continue
        for node in ast.parse(path.read_text(encoding="utf-8")).body:
            if (
                isinstance(node, ast.ImportFrom)
                and node.module in _BOUNDARY_MODULES
            ):
                for alias in node.names:
                    found.setdefault(_module_name(path), {})[
                        alias.asname or alias.name
                    ] = node.module
    return found


_REEXPORTS = _reexports()


def _imported_modules(tree: ast.Module) -> dict[str, str]:
    """The local names of `tree` bound to a module of `src`."""
    bound: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    bound[alias.asname] = alias.name
                else:
                    root = alias.name.partition(".")[0]
                    bound[root] = root
        elif isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                full = f"{node.module}.{alias.name}"
                if full in _MODULES:
                    bound[alias.asname or alias.name] = full
    return bound


def _resolved(module: str, name: str) -> str | None:
    """`module.name` when it is a boundary file's, directly or through its
    package's re-export; None otherwise."""
    if module in _BOUNDARY_MODULES:
        return f"{module}.{name}"
    home = _REEXPORTS.get(module, {}).get(name)
    return None if home is None else f"{home}.{name}"


def _attribute_uses(
    tree: ast.Module, bound: dict[str, str]
) -> Iterator[tuple[int, str]]:
    """The boundary definitions `tree` reaches as `package.module.Name`."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Attribute):
            continue
        chain: list[str] = []
        current: ast.expr = node
        while isinstance(current, ast.Attribute):
            chain.append(current.attr)
            current = current.value
        if not isinstance(current, ast.Name) or current.id not in bound:
            continue
        base = bound[current.id]
        for attr in reversed(chain):
            found = _resolved(base, attr)
            if found is not None:
                yield node.lineno, found
                break
            if f"{base}.{attr}" not in _MODULES:
                break
            base = f"{base}.{attr}"


def _boundary_uses(source: str) -> set[tuple[int, str]]:
    """The (line, `module.Name`) of every boundary definition `source`
    uses, however imported."""
    tree = ast.parse(source)
    uses: set[tuple[int, str]] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                found = _resolved(node.module, alias.name)
                if found is not None:
                    uses.add((node.lineno, found))
    uses.update(_attribute_uses(tree, _imported_modules(tree)))
    return uses


def _foreign_boundary_uses(source: str, module: str) -> list[str]:
    """The boundary definitions `source` uses from another package, and
    not listed in `CROSS_PACKAGE_TYPES`."""
    if any(_matches(module, home) for home in BOUNDARY_IMPORTERS):
        return []
    return [
        f"{module}:{line} uses {name}: move it to core.contract, or list it"
        " in CROSS_PACKAGE_TYPES with the reason it stays"
        for line, name in sorted(_boundary_uses(source))
        if not _matches(module, name.rsplit(".", 2)[0])
        and name not in CROSS_PACKAGE_TYPES
    ]


def test_boundary_files_are_used_by_their_own_package() -> None:
    foreign = [
        found
        for path in SOURCES
        for found in _foreign_boundary_uses(
            path.read_text(encoding="utf-8"), _module_name(path)
        )
    ]

    assert not foreign, "\n".join(foreign)


def test_every_cross_package_type_is_still_used_so() -> None:
    """An entry of `CROSS_PACKAGE_TYPES` is still used out of its package."""
    used = {
        name
        for path in SOURCES
        for _, name in _boundary_uses(path.read_text(encoding="utf-8"))
        if not _matches(_module_name(path), name.rsplit(".", 2)[0])
        and not any(
            _matches(_module_name(path), home) for home in BOUNDARY_IMPORTERS
        )
    }

    assert set(CROSS_PACKAGE_TYPES) <= used, sorted(
        set(CROSS_PACKAGE_TYPES) - used
    )


# (importing module, source, refused): synthetic, on real boundary files.
_BOUNDARY_CASES: list[tuple[str, str, bool]] = [
    (
        "infra.probe",
        "from core.operators import types\nx: types.Fetch[int]\n",
        True,
    ),
    (
        "infra.probe",
        "from core.operators.types import Fetch\n",
        True,
    ),
    ("infra.probe", "from core import operators\nx: operators.Fetch\n", True),
    (
        "infra.probe",
        "import core.operators.types\nx: core.operators.types.Fetch\n",
        True,
    ),
    (
        "infra.probe",
        "from core.operators import types as t\nx: t.Params\n",
        True,
    ),
    (
        "core.operators.probe",
        "from core.operators import types\nx: types.Fetch[int]\n",
        False,
    ),
    (
        "app.probe",
        "from infra.storage import clients\nx: clients.GcsClient\n",
        False,
    ),
    (
        "infra.probe",
        "from core import operators\nx: operators.SourceRecord\n",
        False,
    ),
    ("infra.probe", "from core import operators\nx: operators.Job\n", False),
]


@pytest.mark.parametrize(("module", "source", "refused"), _BOUNDARY_CASES)
def test_the_boundary_rule(module: str, source: str, refused: bool) -> None:
    assert bool(_foreign_boundary_uses(source, module)) is refused, source
