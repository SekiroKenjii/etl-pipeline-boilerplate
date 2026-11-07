"""
The layering rules of the project, checked on every import of `src/`:

    shared            <- generic helpers, standard library only
    core              <- the ETL, standard library (and shared) only
    core.contract     <- ports, DTOs, exceptions: imports no other core part
    core.pipeline     <- the engine (job, orchestrator)
    core.quality      <- quality gate          (contract only)
    core.state        <- state stores          (contract only)
    core.operators    <- reusable ETL operators (contract + pipeline)
    core.observability <- log context, redaction (contract), run logging
    core.keys         <- surrogate keys, key registries (contract + pipeline)
    core.mapping      <- source-to-target mappings (contract + pipeline + keys)
    core.cutover      <- reconciliation, data diff, cutover reports (contract)
    core.mapping_pipelines
                      <- mapping-driven pipelines: the top of core, which
                         no other part of core (nor infra) imports
    infra             <- adapters: implement the ports, know nothing of ETL
    config            <- settings (pydantic), for the application layer only
    app, modules, cli <- composition root, business pipelines, entry points
    cli               <- may drive the offline adapters (`etl-db`,
                         `etl-mappings`, report writers) itself: CLI_INFRA

`from core import mapping` counts as an import of `core.mapping`, the way
the project writes its imports. Adding a dependency that breaks a rule fails
here, not in a code review.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
import pathlib
import sys
import tomllib

import pytest

SRC = pathlib.Path(__file__).resolve().parents[2] / "src"

# package -> packages it must never import (prefix match)
FORBIDDEN: dict[str, tuple[str, ...]] = {
    "shared": ("core", "infra", "modules", "config", "cli", "app"),
    "core": ("infra", "modules", "cli", "config", "app"),
    "config": ("core", "infra", "modules", "cli", "app"),
    # Business modules get adapters as ports, from the resources.
    "modules": ("infra", "app", "cli"),
    "app": ("cli",),
    "core.pipeline": (
        "core.quality",
        "core.state",
        "core.operators",
        "core.mapping",
        "core.keys",
    ),
    "core.quality": (
        "core.pipeline",
        "core.state",
        "core.operators",
        "core.mapping",
        "core.keys",
    ),
    "core.state": (
        "core.pipeline",
        "core.quality",
        "core.operators",
        "core.mapping",
        "core.keys",
    ),
    "core.operators": ("core.quality", "core.state", "core.mapping"),
    # Resources, modules and the kernel run pipelines, knowing ports only.
    "core.runtime": (
        "core.quality",
        "core.state",
        "core.operators",
        "core.mapping",
        "core.keys",
        "core.cutover",
    ),
    # A diff compares rows through the ports: the module maps them.
    "core.cutover": (
        "core.pipeline",
        "core.quality",
        "core.state",
        "core.operators",
        "core.mapping",
        "core.keys",
        "core.runtime",
        "core.observability",
    ),
    # Its context needs the contracts only: the engine logs through it.
    "core.observability": (
        "core.quality",
        "core.state",
        "core.operators",
        "core.mapping",
        "core.keys",
    ),
    "core.mapping": ("core.quality", "core.state", "core.operators"),
    "core.keys": (
        "core.mapping",
        "core.quality",
        "core.state",
        "core.operators",
    ),
    "infra": (
        "core.pipeline",
        "core.quality",
        "core.state",
        "core.operators",
        "core.mapping",
        "core.keys",
        "core.cutover",
        "modules",
        "cli",
        "config",
        "app",
    ),
}
# The mapping-driven pipelines compose every other part of core.
for _package in FORBIDDEN:
    if _package == "infra" or _package.startswith("core."):
        FORBIDDEN[_package] += ("core.mapping_pipelines",)

# package -> the only packages it may import (prefix match), on top of the
# standard library: an allow-list, so a new dependency is refused before
# anyone thinks of forbidding it. The core gets its values as arguments.
ONLY: dict[str, tuple[str, ...]] = {
    "shared": ("shared",),
    "core": ("core", "shared"),
    "core.contract": ("core.contract", "shared"),
    "config": ("config", "shared", "pydantic", "pydantic_settings"),
}

# The parts of `infra` that `cli` may import. `etl-db` (DDL and discovery
# from `--url`) and `etl-mappings` (workbooks) run without settings, and the
# cutover report writes xlsx and describes a database from its connection
# string: each drives one adapter and composes nothing. Anything built from
# the settings (bookkeeping, sources, logging, lineage) comes from `app`.
CLI_INFRA: tuple[str, ...] = (
    "infra.database",
    "infra.mapping",
    "infra.reports",
)

STDLIB = frozenset(sys.stdlib_module_names)


def _module_name(path: pathlib.Path) -> str:
    parts = path.relative_to(SRC).with_suffix("").parts
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _imports(source: str, module: str, is_package: bool) -> set[str]:
    """Every name `source` imports, absolute.

    `from a import b` gives both `a` and `a.b`: `b` may be a module, and
    `from core import mapping` is how the project imports `core.mapping`.

    Args:
        source: Python source code.
        module: The dotted name of the module `source` is.
        is_package: Whether it is the `__init__` of a package (relative
            imports then start from the module itself).

    Returns:
        The imported names.
    """
    package = module if is_package else module.rpartition(".")[0]
    found: set[str] = set()

    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                base = node.module or ""
            else:
                parts = package.split(".")
                parts = parts[: len(parts) - (node.level - 1)]
                if node.module:
                    parts.append(node.module)
                base = ".".join(parts)
            found.add(base)
            found.update(
                f"{base}.{alias.name}"
                for alias in node.names
                if alias.name != "*"
            )

    return found


def _matches(name: str, prefix: str) -> bool:
    return name == prefix or name.startswith(prefix + ".")


def _forbidden_for(module: str) -> set[str]:
    """Returns the rules of every enclosing package, `core` for `core.x`."""
    return {
        forbidden
        for package, rules in FORBIDDEN.items()
        if _matches(module, package)
        for forbidden in rules
    }


def _allowed_for(module: str) -> list[tuple[str, ...]]:
    """The allow-lists of every enclosing package: an import passes each."""
    return [
        allowed
        for package, allowed in ONLY.items()
        if _matches(module, package)
    ]


def _violations(
    source: str, module: str, is_package: bool = False
) -> list[str]:
    """The imports of `source` that break a rule, as `module` sees them."""
    forbidden = _forbidden_for(module)
    allow_lists = _allowed_for(module)
    found = _imports(source, module, is_package)

    def breaks(name: str) -> bool:
        if any(_matches(name, rule) for rule in forbidden):
            return True
        if name.partition(".")[0] not in STDLIB and any(
            not any(_matches(name, prefix) for prefix in allowed)
            for allowed in allow_lists
        ):
            return True
        # `infra` alone is its empty namespace package: only its parts count.
        return (
            _matches(module, "cli")
            and name != "infra"
            and _matches(name, "infra")
            and not any(_matches(name, prefix) for prefix in CLI_INFRA)
        )

    return sorted(name for name in found if breaks(name))


SOURCES = sorted(SRC.rglob("*.py"))


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: str(p.relative_to(SRC)))
def test_imports_respect_the_layers(path: pathlib.Path) -> None:
    module = _module_name(path)

    violations = _violations(
        path.read_text(encoding="utf-8"), module, path.name == "__init__.py"
    )

    assert not violations, f"{module} must not import {violations}"


def _rule_cases() -> Iterator[tuple[str, str]]:
    """One module of each ruled package, and each import it must not make."""
    for package, rules in FORBIDDEN.items():
        for rule in rules:
            yield f"{package}.probe", rule
    for package in ONLY:
        yield f"{package}.probe", "sqlalchemy"
    yield "cli.probe", "infra.observability"


@pytest.mark.parametrize(
    ("module", "imported"),
    list(_rule_cases()),
    ids=str,
)
def test_every_rule_refuses_the_project_import_style(
    module: str, imported: str
) -> None:
    """Each rule catches `from a import b`, the style the project writes."""
    parent, _, name = imported.rpartition(".")
    source = f"from {parent} import {name}\n" if parent else f"import {name}\n"

    assert _violations(source, module), source


# (module, source) the rules must refuse; synthetic, no file is written.
REFUSED: list[tuple[str, str]] = [
    ("core.keys.probe", "from core import mapping\n"),
    ("core.keys.probe", "from core.mapping import catalog\n"),
    ("core.keys.probe", "import core.mapping.catalog\n"),
    ("core.keys.probe", "from .. import mapping\n"),
    ("core.keys.probe", "from ..mapping import catalog\n"),
    ("core.cutover.probe", "from core import runtime\n"),
    ("core.contract.probe", "from core import observability\n"),
    ("core.contract.probe", "from core import runtime\n"),
    ("core.pipeline.probe", "from core import keys as key_registry\n"),
    ("core.mapping.probe", "import sqlalchemy\n"),
    ("core.mapping.probe", "import sqlalchemy as sa\n"),
    ("core.mapping.probe", "from sqlalchemy import orm\n"),
    ("core.probe", "import pydantic\n"),
    ("core.runtime.probe", "from config import settings\n"),
    ("core.runtime.probe", "import config\n"),
    ("shared.utils.probe", "import yaml\n"),
    ("shared.utils.probe", "from core import contract\n"),
    ("config.probe", "import sqlalchemy\n"),
    ("config.probe", "from core import contract\n"),
    ("modules.crm.probe", "from infra import database\n"),
    ("modules.crm.probe", "from infra.database import relational\n"),
    ("modules.crm.probe", "import infra.mapping\n"),
    ("infra.probe", "from core import pipeline\n"),
    ("infra.probe", "from core import mapping\n"),
    ("infra.probe", "import config\n"),
    ("app.probe", "from cli import run\n"),
    ("cli.probe", "from infra import observability\n"),
    ("cli.probe", "from infra.messaging import kafka\n"),
    ("cli.probe", "import infra.storage\n"),
    ("core.keys.probe", "def f():\n    from core import mapping\n"),
    (
        "core.keys.probe",
        "import typing\nif typing.TYPE_CHECKING:\n    import sqlalchemy\n",
    ),
]

# (module, source) the rules must accept.
ACCEPTED: list[tuple[str, str]] = [
    ("core.keys.probe", "from core import contract\n"),
    ("core.keys.probe", "from core import pipeline\n"),
    ("core.keys.probe", "from . import registry\n"),
    ("core.mapping.probe", "from core import keys\n"),
    ("core.mapping.probe", "from core.keys import registry\n"),
    ("core.probe", "from __future__ import annotations\n"),
    ("core.probe", "import json\nfrom collections.abc import Mapping\n"),
    ("core.probe", "from concurrent import futures\n"),
    ("core.probe", "from shared import utils\n"),
    ("core.contract.probe", "from core.contract import etl\n"),
    ("core.contract.probe", "from shared import utils\n"),
    ("shared.utils.probe", "import datetime\nfrom shared import utils\n"),
    ("config.probe", "import pydantic\nimport pydantic_settings\n"),
    ("config.probe", "from config import secrets\nimport tomllib\n"),
    ("modules.crm.probe", "from core import runtime\nimport config\n"),
    ("infra.probe", "from core import contract\nimport sqlalchemy\n"),
    ("app.probe", "from infra.observability import lineage\n"),
    ("cli.probe", "from infra.database import relational\n"),
    ("cli.probe", "from infra import mapping as infra_mapping\n"),
    ("cli.probe", "from infra.reports import xlsx\n"),
    ("cli.probe", "from app import bootstrap\n"),
]


@pytest.mark.parametrize(("module", "source"), REFUSED)
def test_the_rules_refuse(module: str, source: str) -> None:
    assert _violations(source, module), source


@pytest.mark.parametrize(("module", "source"), ACCEPTED)
def test_the_rules_accept(module: str, source: str) -> None:
    assert not _violations(source, module), source


def test_relative_imports_resolve_from_the_package() -> None:
    assert _imports("from . import b\n", "core.keys.a", False) == {
        "core.keys",
        "core.keys.b",
    }
    assert _imports("from .. import b\n", "core.keys", True) == {
        "core",
        "core.b",
    }
    assert _imports("from ..c import d\n", "core.keys.a", False) == {
        "core.c",
        "core.c.d",
    }


def test_the_rules_see_every_layer() -> None:
    """Every ruled package exists, and every rule names a real target."""
    modules = {_module_name(path) for path in SOURCES}

    def exists(name: str) -> bool:
        return any(_matches(module, name) for module in modules)

    for package in [*FORBIDDEN, *ONLY]:
        assert exists(package), package
    targets = [rule for rules in FORBIDDEN.values() for rule in rules]
    targets += [
        prefix
        for prefixes in ONLY.values()
        for prefix in prefixes
        if not prefix.startswith("pydantic")
    ]
    for target in [*targets, *CLI_INFRA]:
        assert exists(target), target


def test_test_files_have_unique_names() -> None:
    """pytest imports test files by name: two alike fail to collect."""
    tests = SRC.parent / "tests"
    seen: dict[str, pathlib.Path] = {}
    for path in sorted(tests.rglob("test_*.py")):
        assert path.name not in seen, f"{path} and {seen[path.name]}"
        seen[path.name] = path


def test_every_package_ships_in_the_wheel() -> None:
    """A package missing from the wheel works from the source tree only."""
    pyproject = tomllib.loads(
        (SRC.parent / "pyproject.toml").read_text(encoding="utf-8")
    )
    shipped = set(
        pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]
    )
    packages = {f"src/{path.parent.name}" for path in SRC.glob("*/__init__.py")}

    assert packages <= shipped, sorted(packages - shipped)
