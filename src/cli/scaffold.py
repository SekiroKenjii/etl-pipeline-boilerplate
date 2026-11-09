"""`etl new-module <name>`: a mapping-driven module and its workbook.

The module is a `core.mapping_pipelines.MappingModule`: its settings, its custom
transforms and `create`, in one file. The workbook to fill is the one of
`etl-mappings template`, whose example mappings run as they are.
"""

from __future__ import annotations

import io
import keyword
import pathlib
import re

from cli import mappings as mappings_cli

MAX_NAME_LENGTH = 20

_MODULE = '''"""The {name} module: <what it migrates, from where to where>.

A `core.mapping_pipelines.MappingModule`: its pipelines (`full_load`, `sync`,
`parked`, `replay`, `deletes`) run the mappings of the workbook
`mappings/{name}.xlsx`, compiled with the custom transforms of
`registry` (`etl-mappings --registry modules.{name}:registry
compile mappings/{name}.xlsx`).
"""

from __future__ import annotations

import pathlib

import config
from core import mapping
from core import mapping_pipelines

NAME = "{name}"
MAPPINGS = pathlib.Path("mappings/compiled/{name}.json")

# The custom transforms of the mappings, which the workbook then lists:
#
#     @registry.register("{name}.code")
#     def code(value: str | None) -> str | None:
#         """A code without its blanks, in upper case."""
#         return value.strip().upper() if value else None
registry = mapping.TransformRegistry()


class {cls}Settings(config.MappingModuleSettings):
    """The settings of the module: see `config.MappingModuleSettings`."""

    source: str = "legacy"
    target: str = "{name}"
    mappings: pathlib.Path = MAPPINGS


def create(settings: config.AppSettings) -> mapping_pipelines.MappingModule:
    """Builds the module from the application's settings.

    Raises:
        ConfigError: If its settings are invalid.
    """
    secret = settings.key_secret
    return mapping_pipelines.MappingModule(
        NAME,
        settings.module_settings(NAME, {cls}Settings),
        registry,
        key_secret=(
            secret.get_secret_value().encode() if secret is not None else None
        ),
        anonymize=settings.anonymization_secret(),
    )
'''

SNIPPET = """
Next:
  1. Fill mappings/{name}.xlsx: one sheet per target table, in place of
     its two example mappings. A mapping runs once its Status is approved.
  2. uv run etl-mappings --registry modules.{name}:registry compile \\
       mappings/{name}.xlsx
  3. Add to config/base.toml:

     [modules.{name}]
     module = "modules.{name}:create"

     [modules.{name}.settings]
     source = "legacy"
     target = "{name}"
     watermark = "UPDATED_AT"   # The source column tracking changes.

  4. uv run etl run {name}.full_load
"""


def scaffold(
    name: str,
    root: pathlib.Path,
    mappings: pathlib.Path = pathlib.Path("mappings"),
) -> list[pathlib.Path]:
    """Writes a new module, `root/modules/<name>`, and its workbook,
    `mappings/<name>.xlsx`.

    Args:
        name: The module.
        root: Where the `modules` package is.
        mappings: Where the workbooks are.

    Returns:
        The files written.

    Raises:
        ValueError: If the name is not a module name, the module or its
            workbook exists already.
    """
    if (
        not re.fullmatch(r"[a-z][a-z0-9_]*", name)
        or keyword.iskeyword(name)
        or len(name) > MAX_NAME_LENGTH
    ):
        raise ValueError(
            f"{name!r} is not a module name: lowercase letters, digits, _,"
            f" {MAX_NAME_LENGTH} at most"
        )
    directory = root / "modules" / name
    workbook = mappings / f"{name}.xlsx"
    for path in (directory, workbook):
        if path.exists():
            raise ValueError(f"{path} exists already")
    out = io.StringIO()
    if mappings_cli.main(["template", str(workbook), "--module", name], out):
        raise ValueError(out.getvalue().strip())
    directory.mkdir(parents=True)
    module = directory / "__init__.py"
    module.write_text(
        _MODULE.format(
            name=name,
            cls=_class_name(name),
        ),
        encoding="utf-8",
    )
    return [module, workbook]


def _class_name(name: str) -> str:
    return "".join(part.capitalize() for part in name.split("_"))
