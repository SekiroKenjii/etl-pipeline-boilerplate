"""
The layering rules of the project, checked on every import of `src/`:

    shared            <- generic helpers, standard library only
    core              <- the ETL, standard library (and shared) only
    infra             <- adapters: implement the ports, know nothing of ETL
    config            <- settings (pydantic), for the application layer only
    app, modules, cli <- composition root, business pipelines, entry points
    cli               <- may drive the offline adapters (`etl-db`,
                         `etl-mappings`, report writers) itself: CLI_INFRA

Inside `core`, each part imports only the parts below it (`CORE_LAYERS`, an
allow-list: a new edge is refused until it is added there), and no cycle:

    core.contract      <- ports, DTOs, exceptions: no other part of core
    core.quality       <- quality gate                      (contract)
    core.state         <- state stores                      (contract)
    core.cutover       <- reconciliation, data diff, reports (contract)
    core.observability <- log context, redaction, run listeners (contract)
    core.pipeline      <- the engine: job, orchestrator (contract,
                          observability)
    core.keys          <- surrogate keys, key registries (contract, pipeline)
    core.mapping       <- source-to-target mappings (contract, pipeline, keys)
    core.operators     <- reusable ETL operators (contract, pipeline; keys
                          for the reconciliation's canonical keys,
                          observability for the metrics of change feeds)
    core.runtime       <- resources, kernel (contract, pipeline,
                          observability)
    core.mapping_pipelines
                       <- mapping-driven pipelines: the top of core, which
                          no other part of core (nor infra) imports

No module of `src/` is on an import cycle, and no package, at any level,
depends on itself through its parts: every import counts, those of a
`TYPE_CHECKING` block or of a function body too, since a cycle they hide is
still one. And there are none: every import is at the top of its module,
never in a function nor behind `TYPE_CHECKING`, so what a module needs is
what it imports, read at the top.

`from core import mapping` counts as an import of `core.mapping`, the way
the project writes its imports; `from x import *` is refused, since it hides
what it imports. Adding a dependency that breaks a rule fails here, not in a
code review.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator, Mapping
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
    "infra": (
        "core.pipeline",
        "core.quality",
        "core.state",
        "core.operators",
        "core.mapping",
        "core.keys",
        "core.cutover",
        "core.mapping_pipelines",
        "modules",
        "cli",
        "config",
        "app",
    ),
}

# part of core -> the only other parts of core it may import, on top of the
# standard library and `shared`. Lower parts first: each names parts listed
# before it only, so there is no cycle.
CORE_LAYERS: dict[str, tuple[str, ...]] = {
    "core.contract": (),
    "core.quality": ("core.contract",),
    "core.state": ("core.contract",),
    "core.cutover": ("core.contract",),
    "core.observability": ("core.contract",),
    "core.pipeline": ("core.contract", "core.observability"),
    "core.keys": ("core.contract", "core.pipeline"),
    "core.mapping": ("core.contract", "core.pipeline", "core.keys"),
    "core.operators": (
        "core.contract",
        "core.pipeline",
        "core.keys",
        "core.observability",
    ),
    "core.runtime": ("core.contract", "core.pipeline", "core.observability"),
}
CORE_LAYERS["core.mapping_pipelines"] = tuple(CORE_LAYERS)

# package -> the only packages it may import (prefix match), on top of the
# standard library: an allow-list, so a new dependency is refused before
# anyone thinks of forbidding it. The core gets its values as arguments.
ONLY: dict[str, tuple[str, ...]] = {
    "shared": ("shared",),
    "core": ("core", "shared"),
    **{part: (part, *below, "shared") for part, below in CORE_LAYERS.items()},
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
    `from a import *` gives `a` and `a.*`.

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
            found.update(f"{base}.{alias.name}" for alias in node.names)

    return found


def _dependencies(
    source: str, module: str, is_package: bool, modules: frozenset[str]
) -> set[str]:
    """The modules of `modules` that `source` imports, itself excluded.

    `from a import b` depends on `a.b` when it is a module, else on `a`:
    a module importing a submodule of its own package does not depend on
    the package's `__init__`, one importing a name from it does.
    """
    package = module if is_package else module.rpartition(".")[0]
    found: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                base = node.module or ""
            else:
                parts = package.split(".")
                parts = parts[: len(parts) - (node.level - 1)]
                if node.module:
                    parts.append(node.module)
                base = ".".join(parts)
            names = [f"{base}.{alias.name}" for alias in node.names]
        else:
            continue
        for name in names:
            while name and name not in modules:
                name = name.rpartition(".")[0]
            if name and name != module:
                found.add(name)
    return found


def _module_graph(
    sources: Mapping[str, tuple[str, bool]],
) -> dict[str, set[str]]:
    """module -> the modules it imports, from {module: (source, package)}."""
    modules = frozenset(sources)
    return {
        module: _dependencies(source, module, is_package, modules)
        for module, (source, is_package) in sources.items()
    }


def _part_of(package: str, module: str) -> str | None:
    """The direct part of `package` (`""`: the root) holding `module`."""
    prefix = f"{package}." if package else ""
    if not module.startswith(prefix) or module == package:
        return None
    return prefix + module.removeprefix(prefix).split(".")[0]


def _package_graph(
    graph: Mapping[str, set[str]], package: str
) -> dict[str, set[str]]:
    """part of `package` -> the other parts its modules import."""
    parts: dict[str, set[str]] = {}
    for module, imported in graph.items():
        part = _part_of(package, module)
        if part is None:
            continue
        parts.setdefault(part, set()).update(
            other
            for name in imported
            if (other := _part_of(package, name)) not in (None, part)
        )
    return parts


def _cycle(graph: Mapping[str, set[str]]) -> list[str]:
    """One cycle of `graph`, as the path that closes it; [] when none."""
    done: set[str] = set()
    path: list[str] = []

    def visit(node: str) -> list[str]:
        if node in path:
            return [*path[path.index(node) :], node]
        if node in done:
            return []
        path.append(node)
        for nxt in sorted(graph.get(node, ())):
            if found := visit(nxt):
                return found
        path.pop()
        done.add(node)
        return []

    for node in sorted(graph):
        if found := visit(node):
            return found
    return []


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
        if name.endswith(".*") or any(
            _matches(name, rule) for rule in forbidden
        ):
            return True
        # `core` of `from core import x`, in core: only `core.x` counts.
        if module.startswith(f"{name}."):
            return False
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
    for part, below in CORE_LAYERS.items():
        for other in CORE_LAYERS:
            if other != part and other not in below:
                yield f"{part}.probe", other
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
    # Each sub-package of core imports the parts below it only.
    ("core.quality.probe", "from core import runtime\n"),
    ("core.quality.probe", "from core import cutover\n"),
    ("core.quality.probe", "from core.observability import context\n"),
    ("core.state.probe", "import core.runtime as kernel\n"),
    ("core.state.probe", "from .. import observability\n"),
    ("core.keys.probe", "from core import runtime\n"),
    ("core.keys.probe", "from core.cutover import diff\n"),
    ("core.mapping.probe", "from core import runtime\n"),
    ("core.mapping.probe", "from core import observability\n"),
    ("core.cutover.probe", "from core import observability\n"),
    ("core.operators.probe", "from core import runtime\n"),
    ("core.observability.probe", "from core import pipeline\n"),
    ("core.observability.probe", "from core.pipeline import orchestrator\n"),
    # No cycle: the kernel runs the engine, never the other way round.
    ("core.pipeline.probe", "from core import runtime\n"),
    ("core.pipeline.probe", "from core.runtime import kernel\n"),
    # A star import hides what it imports from the rules.
    ("core.keys.probe", "from core import *\n"),
    ("core.keys.probe", "from core.contract import *\n"),
    ("infra.probe", "from core.contract import *\n"),
    ("modules.crm.probe", "def f():\n    from core.runtime import *\n"),
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
    ("core.runtime.probe", "from core import pipeline\n"),
    ("core.runtime.probe", "from core.observability import metrics\n"),
    ("core.operators.probe", "from core import keys\n"),
    ("core.operators.probe", "from core.observability import metrics\n"),
    ("core.observability.probe", "from core import contract\n"),
    ("core.mapping_pipelines.probe", "from core import runtime\n"),
    ("core.quality.probe", "from shared import utils\n"),
]


@pytest.mark.parametrize(("module", "source"), REFUSED)
def test_the_rules_refuse(module: str, source: str) -> None:
    assert _violations(source, module), source


@pytest.mark.parametrize(("module", "source"), ACCEPTED)
def test_the_rules_accept(module: str, source: str) -> None:
    assert not _violations(source, module), source


def test_the_parts_of_core_have_no_cycle() -> None:
    """Each part of core may import only parts listed before it."""
    seen: set[str] = set()
    for part, below in CORE_LAYERS.items():
        assert set(below) <= seen, (part, sorted(set(below) - seen))
        seen.add(part)


def _project_graph() -> dict[str, set[str]]:
    return _module_graph({
        _module_name(path): (
            path.read_text(encoding="utf-8"),
            path.name == "__init__.py",
        )
        for path in SOURCES
    })


def test_no_module_is_on_an_import_cycle() -> None:
    assert not (cycle := _cycle(_project_graph())), " -> ".join(cycle)


def test_no_package_depends_on_itself_through_its_parts() -> None:
    graph = _project_graph()
    packages = {""} | {
        _module_name(path) for path in SOURCES if path.name == "__init__.py"
    }
    packages |= {name.rpartition(".")[0] for name in graph} - {""}

    cycles = {
        package or "src": cycle
        for package in sorted(packages)
        if (cycle := _cycle(_package_graph(graph, package)))
    }

    assert not cycles, cycles


@pytest.mark.parametrize(
    "hidden",
    [
        "from core.contract import b\n",
        (
            "import typing\nif typing.TYPE_CHECKING:\n    from core.contract"
            " import b\n"
        ),
        "def f():\n    from core.contract import b\n",
        "from . import b\n",
        "import core.contract.b\n",
    ],
    ids=["plain", "type-checking", "function", "relative", "dotted"],
)
def test_every_import_counts_for_a_cycle(hidden: str) -> None:
    graph = _module_graph({
        "core.contract": ("", True),
        "core.contract.a": (hidden, False),
        "core.contract.b": ("from core.contract import a\n", False),
    })

    assert _cycle(graph) == [
        "core.contract.a",
        "core.contract.b",
        "core.contract.a",
    ]


def test_a_cycle_across_packages_counts() -> None:
    """Two packages importing each other through different modules."""
    graph = _module_graph({
        "core.x.one": ("from core.y import two\n", False),
        "core.y.two": ("", False),
        "core.y.three": ("from core.x import four\n", False),
        "core.x.four": ("", False),
    })

    assert not _cycle(graph)
    assert _cycle(_package_graph(graph, "core")) == [
        "core.x",
        "core.y",
        "core.x",
    ]


def test_importing_a_part_of_its_own_package_is_no_cycle() -> None:
    graph = _module_graph({
        "core.contract": ("from core.contract.a import A\n", True),
        "core.contract.a": ("from core.contract import b\n", False),
        "core.contract.b": ("", False),
    })

    assert not _cycle(graph)


def test_importing_a_name_of_its_own_package_is_a_cycle() -> None:
    graph = _module_graph({
        "core.contract": ("from core.contract.a import A\n", True),
        "core.contract.a": ("from core.contract import ETLResult\n", False),
    })

    assert _cycle(graph) == [
        "core.contract",
        "core.contract.a",
        "core.contract",
    ]


def _late_imports(source: str) -> list[int]:
    """The lines of the imports of `source` that are not at its top, and
    of its uses of `TYPE_CHECKING`."""
    tree = ast.parse(source)
    top = {id(node) for node in tree.body}
    return sorted(
        node.lineno
        for node in ast.walk(tree)
        if (
            isinstance(node, (ast.Import, ast.ImportFrom))
            and id(node) not in top
        )
        or (isinstance(node, ast.Name) and node.id == "TYPE_CHECKING")
        or (isinstance(node, ast.Attribute) and node.attr == "TYPE_CHECKING")
    )


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: str(p.relative_to(SRC)))
def test_every_import_is_at_the_top_of_its_module(path: pathlib.Path) -> None:
    lines = _late_imports(path.read_text(encoding="utf-8"))

    assert not lines, f"{path.relative_to(SRC)}: lines {lines}"


@pytest.mark.parametrize(
    "source",
    [
        "def f():\n    import json\n",
        "class C:\n    from core import contract\n",
        "try:\n    import json\nexcept ImportError:\n    pass\n",
        (
            "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    import"
            " json\n"
        ),
        "import typing\nif typing.TYPE_CHECKING:\n    pass\n",
    ],
    ids=["function", "class", "try", "type-checking", "typing-dot"],
)
def test_a_late_import_is_refused(source: str) -> None:
    assert _late_imports(source)


def test_imports_at_the_top_are_accepted() -> None:
    assert not _late_imports(
        "from __future__ import annotations\n\nimport json\n"
        "from core import contract\n\nX = 1\n"
    )


def test_every_part_of_core_has_its_layer() -> None:
    """A new sub-package of core is placed in `CORE_LAYERS` before use."""
    parts = {
        f"core.{path.parent.name}"
        for path in (SRC / "core").glob("*/__init__.py")
    }

    assert parts == set(CORE_LAYERS), sorted(parts ^ set(CORE_LAYERS))


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
