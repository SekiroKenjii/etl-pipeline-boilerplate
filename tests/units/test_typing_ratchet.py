"""The uses of `typing.Any` in `src/`, held at or below a ceiling.

`Any` turns type checking off wherever it flows. It stays only where a
third-party signature forces it, each use at the narrowest scope. A use is
the name `Any` (or its alias) imported from `typing`, or `typing.Any` itself,
in code or in a string annotation; the word in a comment or a docstring is
not one. Removing a use lowers the count: lower its ceiling with it.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator, Mapping
import pathlib
from typing import TypeIs

import pytest

SRC = pathlib.Path(__file__).resolve().parents[2] / "src"
_TYPING_MODULES = frozenset({"typing", "typing_extensions"})

# Top-level package (or module) -> the most uses it may have.
CEILINGS: dict[str, int] = {
    "app": 0,
    "cli": 0,
    "config": 1,
    "core": 0,
    "infra": 3,
    "main": 0,
    "modules": 0,
    "shared": 0,
}
# The files holding them, to name the one that grew.
KNOWN_USES: dict[str, int] = {
    # pydantic types `__init__` field by field.
    "config/settings.py": 1,
    # pymongo types `MongoClient`'s options one by one.
    "infra/database/mongodb/client.py": 1,
    # SQLAlchemy's `is_disconnect` takes its own DBAPI error class.
    "infra/database/relational/bulk_loading.py": 1,
    # confluent-kafka types `AdminClient`'s config narrower than librdkafka.
    "infra/messaging/kafka.py": 1,
}


def any_uses(source: str) -> list[int]:
    """The lines of the uses of `typing.Any` in a module's source."""
    tree = ast.parse(source)
    names, modules = _aliases(tree)
    lines = [
        node.lineno for node in ast.walk(tree) if _is_any(node, names, modules)
    ]
    for text, line in _string_annotations(tree):
        try:
            annotation = ast.parse(text, mode="eval")
        except SyntaxError:
            continue
        lines.extend(
            line
            for node in ast.walk(annotation)
            if _is_any(node, names, modules)
        )
    return sorted(lines)


def count_uses(root: pathlib.Path) -> dict[str, dict[str, list[int]]]:
    """The uses of `typing.Any` under `root`, by top-level package, then
    by file (relative to `root`)."""
    found: dict[str, dict[str, list[int]]] = {}
    for path in sorted(root.rglob("*.py")):
        relative = path.relative_to(root)
        package = relative.parts[0].removesuffix(".py")
        lines = any_uses(path.read_text(encoding="utf-8"))
        files = found.setdefault(package, {})
        if lines:
            files[relative.as_posix()] = lines
    return found


def over_ceiling(
    uses: Mapping[str, Mapping[str, list[int]]],
    ceilings: Mapping[str, int] | None = None,
    known: Mapping[str, int] | None = None,
) -> list[str]:
    """One message per package above its ceiling, naming the files that
    grew, with the lines of their uses; `CEILINGS` and `KNOWN_USES` by
    default."""
    ceilings = CEILINGS if ceilings is None else ceilings
    known = KNOWN_USES if known is None else known
    problems = []
    for package, files in sorted(uses.items()):
        count = sum(len(lines) for lines in files.values())
        ceiling = ceilings.get(package, 0)
        if count <= ceiling:
            continue
        grown = [
            f"{name} ({len(lines)}, was {known.get(name, 0)}: lines"
            f" {', '.join(map(str, lines))})"
            for name, lines in sorted(files.items())
            if len(lines) > known.get(name, 0)
        ]
        problems.append(
            f"src/{package}: {count} uses of typing.Any, ceiling {ceiling};"
            f" grew in {'; '.join(grown) or 'files known to use it'}. Use a"
            " precise type (object, a Protocol, a TypedDict, a TypeVar...)"
        )
    return problems


def _aliases(tree: ast.Module) -> tuple[set[str], set[str]]:
    """The names `Any` is imported as, and those of the typing modules."""
    names: set[str] = set()
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in _TYPING_MODULES:
            names.update(
                alias.asname or alias.name
                for alias in node.names
                if alias.name == "Any"
            )
        elif isinstance(node, ast.Import):
            modules.update(
                alias.asname or alias.name
                for alias in node.names
                if alias.name in _TYPING_MODULES
            )
    return names, modules


def _is_any(
    node: ast.AST, names: set[str], modules: set[str]
) -> TypeIs[ast.Name | ast.Attribute]:
    if isinstance(node, ast.Name):
        return node.id in names
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "Any"
        and isinstance(node.value, ast.Name)
        and node.value.id in modules
    )


def _string_annotations(tree: ast.Module) -> Iterator[tuple[str, int]]:
    """The texts of the strings in annotations and in `cast` types."""
    roots: list[ast.expr] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.arg) and node.annotation is not None:
            roots.append(node.annotation)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.returns is not None:
                roots.append(node.returns)
        elif isinstance(node, ast.AnnAssign):
            roots.append(node.annotation)
        elif isinstance(node, ast.Call) and node.args and _is_cast(node.func):
            roots.append(node.args[0])
    for root in roots:
        for found in ast.walk(root):
            if isinstance(found, ast.Constant) and isinstance(found.value, str):
                yield found.value, found.lineno


def _is_cast(func: ast.expr) -> bool:
    return (isinstance(func, ast.Name) and func.id == "cast") or (
        isinstance(func, ast.Attribute) and func.attr == "cast"
    )


def test_no_package_uses_typing_any_more_than_its_ceiling() -> None:
    assert not over_ceiling(count_uses(SRC))


def test_the_ceilings_are_the_counts_reached() -> None:
    uses = count_uses(SRC)
    counts = {
        package: sum(len(lines) for lines in files.values())
        for package, files in uses.items()
    }
    by_file = {
        name: len(lines)
        for files in uses.values()
        for name, lines in files.items()
    }

    # A use removed: lower the ceiling, so that it cannot come back.
    assert {p: counts.get(p, 0) for p in CEILINGS} == CEILINGS
    assert set(counts) <= set(CEILINGS)
    assert by_file == KNOWN_USES


@pytest.mark.parametrize(
    ("source", "lines"),
    [
        ("from typing import Any\nx: Any = 1\n", [2]),
        ("from typing import Any as A\ndef f(x: A) -> A: ...\n", [2, 2]),
        ("import typing\ny: dict[str, typing.Any] = {}\n", [2]),
        ("import typing as t\nz = t.cast(t.Any, 1)\n", [2]),
        ('from typing import Any\ndef f(x: "list[Any]") -> None: ...\n', [2]),
        ('import typing\nv = typing.cast("typing.Any", 1)\n', [2]),
        ("from typing_extensions import Any\nw: Any\n", [2]),
    ],
)
def test_uses_are_found_however_written(source: str, lines: list[int]) -> None:
    assert any_uses(source) == lines


@pytest.mark.parametrize(
    "source",
    [
        '"""Any value."""\n# Any other comment.\nx = "Any"\n',
        "from typing import Any\n",  # Imported, not used.
        "Any = object\nx: Any = 1\n",  # Not typing's.
        "import other\nx: other.Any = 1\n",
        'from typing import Any\nprint("Any other thing")\n',
    ],
)
def test_the_word_alone_is_no_use(source: str) -> None:
    assert not any_uses(source)


def test_a_package_above_its_ceiling_names_the_file_that_grew(
    tmp_path: pathlib.Path,
) -> None:
    (tmp_path / "infra").mkdir()
    (tmp_path / "infra" / "old.py").write_text(
        "from typing import Any\nx: Any\n", encoding="utf-8"
    )
    (tmp_path / "infra" / "new.py").write_text(
        "import typing\n\n\ndef f(x: typing.Any) -> None: ...\n",
        encoding="utf-8",
    )

    problems = over_ceiling(
        count_uses(tmp_path),
        ceilings={"infra": 1},
        known={"infra/old.py": 1},
    )

    assert problems == [
        "src/infra: 2 uses of typing.Any, ceiling 1; grew in infra/new.py"
        " (1, was 0: lines 4). Use a precise type (object, a Protocol, a"
        " TypedDict, a TypeVar...)"
    ]
