"""Every numeric threshold of `src` is in `docs/tuning.md`.

The inventory below is the list of the numeric module constants (`NAME =
number`, at the top of a module) and of the numeric setting defaults (the
fields of `config` with a number as default), each with the text a row of
the page must hold. A constant added to `src` fails here until it is listed
and documented; one removed fails until it is dropped.
"""

from __future__ import annotations

import ast
import pathlib

SRC = pathlib.Path(__file__).resolve().parents[2] / "src"
DOC = pathlib.Path(__file__).resolve().parents[2] / "docs" / "tuning.md"

_RELATIONAL = "infra.database.relational"

# qualified name -> what `docs/tuning.md` names it (within backticks).
INVENTORY: dict[str, str] = {
    "cli.calibrate.REPEATS": "REPEATS",
    "cli.calibrate.DEFAULT_BATCH_SIZE": "DEFAULT_BATCH_SIZE",
    "cli.scaffold.MAX_NAME_LENGTH": "MAX_NAME_LENGTH",
    "config.modules.MappingModuleSettings.batch_size": "batch_size",
    "config.modules.MappingModuleSettings.max_reject_rate": "max_reject_rate",
    "config.modules.MappingModuleSettings.max_delete_share": "max_delete_share",
    "config.modules.MappingModuleSettings.partitions": "partitions",
    "config.modules.MappingModuleSettings.step_attempts": "step_attempts",
    "config.modules.MappingModuleSettings.retry_backoff_seconds": (
        "retry_backoff_seconds"
    ),
    "config.modules.MappingModuleSettings.spill_rows": (
        "MappingModuleSettings.spill_rows"
    ),
    "config.settings.BOOKKEEPING_CONNECTIONS": "BOOKKEEPING_CONNECTIONS",
    "config.settings.OtlpSettings.timeout_seconds": (
        "OtlpSettings.timeout_seconds"
    ),
    "config.settings.OtlpSettings.export_seconds": "export_seconds",
    "config.settings.KafkaSettings.timeout_seconds": (
        "KafkaSettings.timeout_seconds"
    ),
    "config.settings.EngineOptions.DEFAULT_POOL_SIZE": (
        "EngineOptions.DEFAULT_POOL_SIZE"
    ),
    "config.settings.EngineOptions.DEFAULT_MAX_OVERFLOW": (
        "DEFAULT_MAX_OVERFLOW"
    ),
    "config.settings.MongoOptions.DEFAULT_POOL_SIZE": (
        "MongoOptions.DEFAULT_POOL_SIZE"
    ),
    "config.settings.DatabaseSettings.page_size": "DatabaseSettings.page_size",
    "config.settings.BookkeepingSettings.retention_days": "retention_days",
    "config.settings.LineageSettings.timeout_seconds": (
        "LineageSettings.timeout_seconds"
    ),
    "config.settings.RunsSettings.lease_seconds": "lease_seconds",
    "config.tuning.MIB": "MIB",
    "config.tuning.GIB": "GIB",
    "config.tuning.MEMORY_BUDGET_SHARE": "MEMORY_BUDGET_SHARE",
    "config.tuning.MIN_MEMORY_BUDGET": "MIN_MEMORY_BUDGET",
    "config.tuning.KEY_CACHE_MEMORY_SHARE": "KEY_CACHE_MEMORY_SHARE",
    "config.tuning.KEY_CACHE_DISK_SHARE": "KEY_CACHE_DISK_SHARE",
    "config.tuning.MAX_KEY_CACHE_DISK": "MAX_KEY_CACHE_DISK",
    "config.tuning.SPILL_SHARE": "SPILL_SHARE",
    "config.tuning.SPILL_ROW_BYTES": "SPILL_ROW_BYTES",
    "config.tuning.MIN_SPILL_ROWS": "MIN_SPILL_ROWS",
    "config.tuning.MAX_SPILL_ROWS": "MAX_SPILL_ROWS",
    "config.tuning.IN_CHUNK_CAP": "IN_CHUNK_CAP",
    "config.tuning.TuningSettings.queue_size": "queue_size",
    "core.contract.quality.DEFAULT_MAX_SAMPLES_PER_RULE": (
        "DEFAULT_MAX_SAMPLES_PER_RULE"
    ),
    "core.cutover.checksum.FORM": "checksum.FORM",
    "core.cutover.checksum.MICROSECONDS": "MICROSECONDS",
    "core.cutover.diff._MASK": "diff._MASK",
    "core.cutover.diff.ROWS_PER_PARTITION": "ROWS_PER_PARTITION",
    "core.cutover.diff.MAX_PARTITIONS": "MAX_PARTITIONS",
    "core.cutover.diff.SPILL_ROWS": "core.cutover.diff.SPILL_ROWS",
    "core.cutover.rules.DEFAULT_CHUNK": "rules.DEFAULT_CHUNK",
    "core.keys.tiered.SAMPLE_SIZE": "SAMPLE_SIZE",
    "core.keys.tiered.RESAMPLE_EVERY": "RESAMPLE_EVERY",
    "core.keys.tiered._SLOT_BYTES": "_SLOT_BYTES",
    "core.keys.tiered._IN_CHUNK": "_IN_CHUNK",
    "core.mapping.catalog.FORMAT_VERSION": "FORMAT_VERSION",
    "core.mapping.privacy._MAX_SHIFT_DAYS": "_MAX_SHIFT_DAYS",
    "core.mapping.privacy._EMAIL_HEX_DIGITS": "_EMAIL_HEX_DIGITS",
    "core.mapping.privacy._PHONE_DIGITS": "_PHONE_DIGITS",
    "core.mapping.privacy._HASH_HEX_DIGITS": "_HASH_HEX_DIGITS",
    "core.operators.reconciliation.SPILL_ROWS": (
        "core.operators.reconciliation.SPILL_ROWS"
    ),
    "core.pipeline.async_bridge._PUT_RETRY_SECONDS": "_PUT_RETRY_SECONDS",
    "core.pipeline.async_bridge._GET_POLL_SECONDS": "_GET_POLL_SECONDS",
    "core.runtime.locks._RECOMPUTE_ROUNDS": "_RECOMPUTE_ROUNDS",
    "infra.database.mongodb.bookkeeping.DEFAULT_IN_CHUNK": (
        "mongodb.bookkeeping.DEFAULT_IN_CHUNK"
    ),
    "infra.database.mongodb.bookkeeping._PAGE_SIZE": (
        "mongodb.bookkeeping._PAGE_SIZE"
    ),
    "infra.database.mongodb.bookkeeping._INDEX_NOT_FOUND": "_INDEX_NOT_FOUND",
    "infra.database.mongodb.client._LONGEST_MESSAGE": "_LONGEST_MESSAGE",
    "infra.database.mongodb.client._LONGEST": "_LONGEST",
    "infra.database.mongodb.inspector._MAX_FIELD_PATH_DEPTH": (
        "_MAX_FIELD_PATH_DEPTH"
    ),
    "infra.database.relational.base_dialect.BULK_INSERT_MISSING": (
        "BULK_INSERT_MISSING"
    ),
    "infra.database.relational.base_dialect.IN_CHUNK_CAP": "IN_CHUNK_CAP",
    "infra.database.relational.base_dialect._RESERVED_PARAMETERS": (
        "_RESERVED_PARAMETERS"
    ),
    "infra.database.relational.dialects.MSSQL_IN_CHUNK": "MSSQL_IN_CHUNK",
    f"{_RELATIONAL}.bookkeeping._base._CONCURRENT_KEY_WRITE_ATTEMPTS": (
        "_CONCURRENT_KEY_WRITE_ATTEMPTS"
    ),
    "infra.database.relational.bookkeeping.stores._PAGE_SIZE": (
        "stores._PAGE_SIZE"
    ),
    "infra.database.relational.bookkeeping.stores._KEY_PAGE_SIZE": (
        "stores._KEY_PAGE_SIZE"
    ),
    "infra.database.relational.changes._PAGE_SIZE": "changes._PAGE_SIZE",
    "infra.database.relational.connection_strings._SQL_ATTR_LOGIN_TIMEOUT": (
        "_SQL_ATTR_LOGIN_TIMEOUT"
    ),
    "infra.database.relational.migrations._RACING_UPGRADE_ATTEMPTS": (
        "_RACING_UPGRADE_ATTEMPTS"
    ),
    "infra.database.relational.migrations._UPGRADE_LOCK_WAIT_SECONDS": (
        "_UPGRADE_LOCK_WAIT_SECONDS"
    ),
    f"{_RELATIONAL}.migrations.versions.v0003_quarantine_keys._PAGE": (
        "v0003_quarantine_keys._PAGE"
    ),
    "infra.database.relational.staging.TRIAL_BATCHES": "TRIAL_BATCHES",
    "infra.database.relational.staging.RECHECK_EVERY": "RECHECK_EVERY",
    "infra.database.relational.staging.HYSTERESIS": "HYSTERESIS",
    "infra.database.relational.staging.SMALL_BATCH": "SMALL_BATCH",
    "infra.messaging.kafka._MAX_REASON": "_MAX_REASON",
    "shared.utils.budgets._V1_UNLIMITED": "_V1_UNLIMITED",
    "shared.utils.external_sort.FAN_IN": "FAN_IN",
}


def _module_name(path: pathlib.Path) -> str:
    parts = path.relative_to(SRC).with_suffix("").parts
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _is_number(node: ast.expr, known: set[str]) -> bool:
    """Whether `node` is a number: a literal, or arithmetic of literals and
    of the module's numeric constants (`256 * MIB`)."""
    if isinstance(node, ast.Constant):
        return isinstance(node.value, (int, float)) and not isinstance(
            node.value, bool
        )
    if isinstance(node, ast.Name):
        return node.id in known
    if isinstance(node, ast.UnaryOp):
        return _is_number(node.operand, known)
    if isinstance(node, ast.BinOp):
        return _is_number(node.left, known) and _is_number(node.right, known)
    return False


def _constant_name(target: ast.expr) -> str | None:
    """`NAME` of `NAME = ...` or `_NAME = ...`; None for other targets."""
    if isinstance(target, ast.Name) and target.id.lstrip("_").isupper():
        return target.id
    return None


def _field_default(node: ast.AnnAssign, known: set[str]) -> bool:
    """Whether a settings field has a numeric default, bare or in
    `pydantic.Field(default=...)`."""
    value = node.value
    if value is None:
        return False
    if _is_number(value, known):
        return True
    if isinstance(value, ast.Call) and ast.unparse(value.func).endswith(
        "Field"
    ):
        return any(
            keyword.arg == "default" and _is_number(keyword.value, known)
            for keyword in value.keywords
        )
    return False


def numeric_names(path: pathlib.Path) -> set[str]:
    """The qualified names of the numeric constants and setting defaults
    of a module."""
    module = _module_name(path)
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: set[str] = set()
    known: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names = [_constant_name(target) for target in node.targets]
            if all(names) and _is_number(node.value, known):
                for name in names:
                    if name is not None:
                        known.add(name)
                        found.add(f"{module}.{name}")
        elif isinstance(node, ast.AnnAssign):
            name = _constant_name(node.target)
            if (
                name is not None
                and node.value is not None
                and _is_number(node.value, known)
            ):
                known.add(name)
                found.add(f"{module}.{name}")
        elif isinstance(node, ast.ClassDef) and module.startswith("config."):
            for item in node.body:
                if (
                    isinstance(item, ast.AnnAssign)
                    and isinstance(item.target, ast.Name)
                    and _field_default(item, known)
                ):
                    found.add(f"{module}.{node.name}.{item.target.id}")
    return found


def test_the_inventory_is_the_numeric_constants_of_src() -> None:
    found = set().union(*(numeric_names(path) for path in SRC.rglob("*.py")))

    missing = sorted(found - set(INVENTORY))
    gone = sorted(set(INVENTORY) - found)

    assert not missing, f"list and document in docs/tuning.md: {missing}"
    assert not gone, f"no longer in src, drop from the inventory: {gone}"


def test_every_threshold_has_a_row_in_the_tuning_page() -> None:
    page = DOC.read_text(encoding="utf-8")

    undocumented = sorted(
        name for name, text in INVENTORY.items() if f"`{text}`" not in page
    )

    assert not undocumented, f"name them in docs/tuning.md: {undocumented}"


def test_the_scan_sees_numbers_however_written(tmp_path: pathlib.Path) -> None:
    module = tmp_path / "src" / "probe.py"
    module.parent.mkdir()
    module.write_text(
        "MIB = 1024 ** 2\n"
        "FLOOR = 256 * MIB\n"
        "_SHARE = 0.6\n"
        "NEGATIVE = -1\n"
        "MASK = (1 << 64) - 1\n"
        "TEXT = 'no'\n"
        "lower = 3\n"
        "FLAG = True\n"
        "LENGTH = 200 - len('x')\n",
        encoding="utf-8",
    )
    tree = ast.parse(module.read_text(encoding="utf-8"))

    known: set[str] = set()
    numbers = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            name = _constant_name(node.targets[0])
            if name is not None and _is_number(node.value, known):
                known.add(name)
                numbers.append(name)

    assert numbers == ["MIB", "FLOOR", "_SHARE", "NEGATIVE", "MASK"]
