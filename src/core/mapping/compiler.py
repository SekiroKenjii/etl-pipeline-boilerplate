"""Validates mapping specs and compiles them into `CompiledMapping`s.

Every problem is reported at its spreadsheet location, all at once, before
any row is mapped.
"""

from __future__ import annotations

from collections.abc import Sequence
import datetime
import difflib
import hashlib
import hmac
import json
import logging
import uuid

from core import contract
from core import keys as core_keys
from core.mapping import casts
from core.mapping import compiled
from core.mapping import privacy
from core.mapping import results
from core.mapping import transforms
from core.mapping import types

_LOG = logging.getLogger(__name__)

DEFAULT_LABELS: dict[str, str] = {
    "target": "Target column",
    "target_type": "Target type",
    "required": "Required",
    "sources": "Source columns",
    "transform": "Transform",
    "params": "Parameters",
    "format": "Format",
    "default": "Default",
    "status": "Status",
    "ignored": "Ignored source fields",
    "row_filter": "Row filter",
    "row_split": "Row split",
    "key": "Key",
    "references": "References",
    "personal": "Personal data",
    "key_hashing": "Key hashing",
    "key_case": "Key case",
    "key_namespace": "Key namespace",
}


def compile_catalog(
    catalog: contract.MappingCatalog,
    registry: transforms.TransformRegistry,
    *,
    allow_drafts: bool = False,
    key_secret: bytes | None = None,
    anonymize: bytes | None = None,
) -> dict[str, compiled.CompiledMapping]:
    """Compiles every mapping of a catalog.

    Draft mappings are skipped, with a warning, unless `allow_drafts`; so are
    the mappings referencing them, whose foreign keys would point to rows
    never loaded. The others run.

    Args:
        catalog: The mappings.
        registry: The transforms and row hooks they may refer to.
        allow_drafts: Whether draft mappings may run, e.g. in development.
        key_secret: Keys the HMAC surrogate keys; from a secret store, never
            from a workbook. Mappings that do not choose their key hashing
            use HMAC when it is set.
        anonymize: The secret anonymizing the personal columns (`privacy`);
            None keeps their values, as in production.

    Returns:
        The compiled mappings, by name; the skipped ones are left out.

    Raises:
        MappingDefinitionError: With every problem of the catalog.
    """
    mappings: dict[str, compiled.CompiledMapping] = {}
    problems = _cycle_problems(catalog)
    skipped = {} if allow_drafts else skipped_mappings(catalog)
    for name, reason in skipped.items():
        _LOG.warning("Mapping %r is skipped: %s", name, reason)

    for spec in catalog.mappings:
        if spec.name in skipped:
            continue
        compiler = _Compiler(
            spec,
            catalog,
            registry,
            allow_drafts=allow_drafts,
            key_secret=key_secret,
            require_secret=True,
            anonymize=anonymize,
        )
        found = compiler.problems()
        if found:
            problems.extend(found)
        else:
            mappings[spec.name] = compiler.build()

    if problems:
        raise contract.MappingDefinitionError(problems)
    return mappings


def validate_catalog(
    catalog: contract.MappingCatalog,
    registry: transforms.TransformRegistry,
    *,
    allow_drafts: bool = True,
) -> list[contract.MappingProblem]:
    """Returns every problem of a catalog, for tooling and CI.

    HMAC secrets are not required: CI does not have them.

    Args:
        catalog: The mappings.
        registry: The transforms and row hooks they may refer to.
        allow_drafts: Whether draft mappings are acceptable.

    Returns:
        The problems; empty when the catalog is valid.
    """
    names = [spec.name for spec in catalog.mappings]
    problems = [
        contract.MappingProblem(f"mapping {name!r} is defined more than once")
        for name in sorted({n for n in names if names.count(n) > 1})
    ]
    problems.extend(_cycle_problems(catalog))
    for spec in catalog.mappings:
        compiler = _Compiler(
            spec,
            catalog,
            registry,
            allow_drafts=allow_drafts,
            key_secret=None,
            require_secret=False,
        )
        problems.extend(compiler.problems())
    return problems


def catalog_warnings(
    catalog: contract.MappingCatalog,
) -> list[contract.MappingProblem]:
    """Returns what is legal but likely wrong in a catalog, for tooling.

    The draft mappings, and those referencing them, which production skips;
    the dates read with 2-digit years.
    """
    labels = {**DEFAULT_LABELS, **catalog.labels}
    warnings: list[contract.MappingProblem] = []
    for name, reason in skipped_mappings(catalog).items():
        spec = catalog.get(name)
        location = _at(spec.location, labels["status"])
        warnings.append(
            contract.MappingProblem(
                f"mapping {name!r} is skipped where drafts do not run:"
                f" {reason}",
                location,
            )
        )
    for spec in catalog.mappings:
        for f in spec.fields:
            if f.format is not None and casts.two_digit_year(f.format):
                location = _at(f.location, labels["format"])
                warnings.append(
                    contract.MappingProblem(
                        f"{f.format!r} reads 2-digit years: 69-99 are"
                        " 1969-1999, 00-68 are 2000-2068 (a birth in 1960"
                        " becomes 2060); read 4 digits if the source has them",
                        location,
                    )
                )
    return warnings


def skipped_mappings(catalog: contract.MappingCatalog) -> dict[str, str]:
    """Returns the mappings that do not run where drafts do not, and why:
    the drafts, and the mappings referencing one, directly or not."""
    dependencies = mapping_dependencies(catalog)
    skipped = {
        spec.name: "it is a draft"
        for spec in catalog.mappings
        if spec.status is contract.MappingStatus.DRAFT
    }
    changed = True
    while changed:
        changed = False
        for name, parents in dependencies.items():
            drafts = sorted(parents & skipped.keys())
            if name not in skipped and drafts:
                skipped[name] = "it references " + ", ".join(
                    f"{p!r} (skipped)" for p in drafts
                )
                changed = True
    return skipped


def key_hashing(
    spec: contract.MappingSpec, key_secret: bytes | None
) -> contract.KeyHashing:
    """Returns how the keys of a mapping are derived: as it chooses, else
    HMAC when a key secret is configured, else UUIDv5."""
    if spec.key_hashing is not None:
        return spec.key_hashing
    if key_secret is not None:
        return contract.KeyHashing.HMAC
    return contract.KeyHashing.UUID5


def namespace_name(spec: contract.MappingSpec) -> str:
    """Returns the name of the namespace of a mapping's keys: its Key
    namespace, else `<source>:<target>`."""
    return spec.key_namespace or f"{spec.source}:{spec.target}"


def key_namespace(spec: contract.MappingSpec) -> uuid.UUID:
    """Returns the namespace of the surrogate keys of a mapping."""
    return core_keys.named_namespace(namespace_name(spec), spec.key_version)


def key_fingerprint(
    catalog: contract.MappingCatalog, key_secret: bytes | None
) -> str:
    """Returns what the surrogate keys of a catalog are derived from, hashed:
    `customer:1a2b3c4d5e6f,customer:natural_key:...,order:...`.

    Two entries per mapping. `<mapping>` changes when its namespace (name
    and key version), hashing, case, or, for HMAC keys, the key secret
    change. `<mapping>:natural_key` changes when the source columns of its
    primary key do, or their order. The secret is only in it through an
    HMAC of a constant: the fingerprint tells nothing of it. Compare the
    entries of the mappings in both fingerprints: a new mapping, or a new
    entry, changes no existing key.
    """
    secret = (
        hmac.new(
            key_secret, b"etl:keys:fingerprint", hashlib.sha256
        ).hexdigest()[:16]
        if key_secret is not None
        else None
    )
    entries: list[str] = []
    for spec in sorted(catalog.mappings, key=lambda m: m.name):
        hashing = key_hashing(spec, key_secret)
        # Its own entry: adding the natural key to the facts would change
        # the entry every run recorded before it, and refuse their next run.
        entries.append(
            _entry(
                spec.name,
                [
                    str(key_namespace(spec)),
                    hashing.value,
                    spec.key_case.value,
                    secret if hashing is contract.KeyHashing.HMAC else None,
                ],
            )
        )
        sources = _key_sources(spec)
        if sources:
            entries.append(_entry(f"{spec.name}:natural_key", sources))
    return ",".join(entries)


def _key_sources(spec: contract.MappingSpec) -> list[str]:
    """The source columns of the primary key of a mapping, in order."""
    key = next((f for f in spec.fields if f.primary_key), None)
    return list(key.sources) if key is not None else []


def _entry(name: str, facts: Sequence[str | None]) -> str:
    digest = hashlib.sha256(json.dumps(facts).encode()).hexdigest()[:12]
    return f"{name}:{digest}"


def mapping_dependencies(
    catalog: contract.MappingCatalog,
) -> dict[str, frozenset[str]]:
    """Returns the mappings each mapping references, self-references aside.

    Parents must be loaded first: use this for the `dependencies` of the
    pipeline steps.
    """
    names = {spec.name for spec in catalog.mappings}
    return {
        spec.name: frozenset(
            f.references
            for f in spec.fields
            if f.references in names and f.references != spec.name
        )
        for spec in catalog.mappings
    }


def _cycle_problems(
    catalog: contract.MappingCatalog,
) -> list[contract.MappingProblem]:
    """Returns one problem per cycle of references between mappings."""
    dependencies = mapping_dependencies(catalog)
    cycles: list[list[str]] = []
    seen: set[frozenset[str]] = set()

    def visit(name: str, path: list[str]) -> None:
        for parent in sorted(dependencies.get(name, ())):
            if parent in path:
                cycle = path[path.index(parent) :]
                if frozenset(cycle) not in seen:
                    seen.add(frozenset(cycle))
                    cycles.append([*cycle, parent])
            else:
                visit(parent, [*path, parent])

    for name in sorted(dependencies):
        visit(name, [name])

    return [
        contract.MappingProblem(
            "references form a cycle: "
            + " -> ".join(cycle)
            + ". Load one side in two passes (reference empty first, then"
            " updated)."
        )
        for cycle in cycles
    ]


class _Compiler:
    """Validates and compiles one mapping of a catalog."""

    def __init__(
        self,
        spec: contract.MappingSpec,
        catalog: contract.MappingCatalog,
        registry: transforms.TransformRegistry,
        *,
        allow_drafts: bool,
        key_secret: bytes | None,
        require_secret: bool,
        anonymize: bytes | None = None,
    ) -> None:
        self.spec = spec
        self.anonymize = anonymize
        self.catalog = catalog
        self.registry = registry
        self.allow_drafts = allow_drafts
        self.key_secret = key_secret
        self.require_secret = require_secret
        self.labels = {**DEFAULT_LABELS, **catalog.labels}

    def problems(self) -> list[contract.MappingProblem]:
        """Returns every problem of the mapping."""
        spec = self.spec
        problems: list[contract.MappingProblem] = []

        if (
            spec.status is contract.MappingStatus.DRAFT
            and not self.allow_drafts
        ):
            problems.append(
                contract.MappingProblem(
                    f"mapping {spec.name!r} is a draft: set its"
                    f" {self.labels['status']} to approved to run it here",
                    self._at(spec.location, "status"),
                )
            )
        if not spec.fields:
            problems.append(
                contract.MappingProblem(
                    f"mapping {spec.name!r} has no field", spec.location
                )
            )

        seen: dict[str, contract.FieldSpec] = {}
        for field_spec in spec.fields:
            first = seen.setdefault(field_spec.target, field_spec)
            if first is not field_spec:
                problems.append(
                    contract.MappingProblem(
                        f"target column {field_spec.target!r} is already mapped"
                        f" at {first.location}",
                        self._at(field_spec.location, "target"),
                    )
                )
            problems.extend(self._field_problems(field_spec))

        problems.extend(self._hook_problems())
        problems.extend(self._key_problems())

        used: set[str] = set()
        for field_spec in spec.fields:
            used.update(field_spec.sources)
        for column in sorted(used & set(spec.ignored_source_fields)):
            problems.append(
                contract.MappingProblem(
                    f"source column {column!r} is both used and ignored",
                    self._at(spec.location, "ignored"),
                )
            )

        return problems

    def build(self) -> compiled.CompiledMapping:
        """Compiles the mapping; only called once `problems()` is empty."""
        fields: list[compiled.CompiledField] = []
        for f in self.spec.fields:
            transform = self._transform(f)
            target_type = (
                casts.parse_type(f.target_type) if f.target_type else None
            )
            fields.append(
                compiled.CompiledField(
                    spec=f,
                    transform=transform,
                    fn=self._bind(f, transform),
                    target_type=target_type,
                    is_key=f.primary_key,
                    maker=self._key_maker(f),
                    anonymize=(
                        privacy.anonymizer(f.personal, self.anonymize)
                        if self.anonymize and f.personal
                        else None
                    ),
                )
            )

        references = {
            f.target: results.Reference(
                field=f.target,
                mapping=f.references,
                entity=self._parent(f.references).target,
                self_reference=f.references == self.spec.name,
            )
            for f in self.spec.fields
            if f.references is not None
        }
        row_filter = (
            self.registry.get_filter(self.spec.row_filter)
            if self.spec.row_filter
            else None
        )
        row_split = (
            self.registry.get_splitter(self.spec.row_split)
            if self.spec.row_split
            else None
        )
        return compiled.CompiledMapping(
            self.spec, tuple(fields), row_filter, row_split, references
        )

    def _key_problems(self) -> list[contract.MappingProblem]:
        """Checks the mapping-wide key settings."""
        spec = self.spec
        problems: list[contract.MappingProblem] = []

        primary_keys = [f for f in spec.fields if f.primary_key]
        for extra in primary_keys[1:]:
            problems.append(
                contract.MappingProblem(
                    "a mapping has one primary key: list every natural key"
                    " column in the source columns of"
                    f" {primary_keys[0].target!r} instead",
                    self._at(extra.location, "key"),
                )
            )

        makes_keys = bool(primary_keys) or any(
            f.references == spec.name for f in spec.fields
        )
        hashing = key_hashing(spec, self.key_secret)
        uses_hmac = hashing is contract.KeyHashing.HMAC and makes_keys
        if uses_hmac and self.require_secret and self.key_secret is None:
            problems.append(
                contract.MappingProblem(
                    f"mapping {spec.name!r} uses HMAC keys: pass the key secret"
                    " when compiling (from a secret store, never in the"
                    " workbook)",
                    self._at(spec.location, "key_hashing"),
                )
            )

        # A UUIDv5 of a small value space (national id) is brute-forced.
        personal = self._personal_key_columns(primary_keys[:1])
        if personal and spec.key_hashing is contract.KeyHashing.UUID5:
            problems.append(
                contract.MappingProblem(
                    f"the natural key of {spec.name!r} holds personal data"
                    f" ({personal}): use hmac key hashing, as uuid5 keys can"
                    " be reversed",
                    self._at(spec.location, "key_hashing"),
                )
            )
        elif (
            personal
            and spec.key_hashing is None
            and self.require_secret
            and self.key_secret is None
        ):
            problems.append(
                contract.MappingProblem(
                    f"the natural key of {spec.name!r} holds personal data"
                    f" ({personal}): its keys are HMACs, pass the key secret"
                    " when compiling",
                    self._at(spec.location, "key_hashing"),
                )
            )

        if spec.key_namespace is not None and not spec.key_namespace.strip():
            problems.append(
                contract.MappingProblem(
                    "the key namespace is blank",
                    self._at(spec.location, "key_namespace"),
                )
            )
        return problems

    def _personal_key_columns(
        self, primary_keys: list[contract.FieldSpec]
    ) -> str:
        """Returns the natural key columns that personal columns read, as
        `NID (national_id: mask)`; empty when none."""
        columns = {c for f in primary_keys for c in f.sources}
        found = [
            f"{column} ({f.target}: {f.personal})"
            for f in self.spec.fields
            if f.personal is not None
            for column in f.sources
            if column in columns
        ]
        return ", ".join(sorted(set(found)))

    def _hook_problems(self) -> list[contract.MappingProblem]:
        """Checks that the row hooks exist and take the row only."""
        problems: list[contract.MappingProblem] = []
        hooks = [
            (
                "row filter",
                "row_filter",
                self.spec.row_filter,
                self.registry.get_filter,
                self.registry.filter_names,
            ),
            (
                "row splitter",
                "row_split",
                self.spec.row_split,
                self.registry.get_splitter,
                self.registry.splitter_names,
            ),
        ]
        for kind, column, name, get, names in hooks:
            if name is None:
                continue
            location = self._at(self.spec.location, column)
            hook = get(name)
            if hook is None:
                problems.append(
                    contract.MappingProblem(
                        f"unknown {kind} {name!r}.{_suggest(name, names())}",
                        location,
                    )
                )
                continue
            problems.extend(
                contract.MappingProblem(message, location)
                for message in transforms.check_row_hook(hook, kind)
            )
        return problems

    def _field_problems(
        self, f: contract.FieldSpec
    ) -> list[contract.MappingProblem]:
        """Returns every problem of one field."""
        found: list[tuple[str, str]] = []  # (message, label key)

        target_type: casts.TargetType | None = None
        if f.target_type:
            try:
                target_type = casts.parse_type(f.target_type)
            except ValueError as exc:
                found.append((str(exc), "target_type"))

        if f.primary_key or f.references is not None:
            found.extend(self._key_field_problems(f, target_type))
        else:
            found.extend(self._transform_problems(f))
        found.extend(_format_problems(f, target_type))
        found.extend(_default_problems(f, target_type))
        found.extend(_personal_problems(f, target_type))

        return [
            contract.MappingProblem(message, self._at(f.location, column))
            for message, column in found
        ]

    def _transform_problems(
        self, f: contract.FieldSpec
    ) -> list[tuple[str, str]]:
        """Checks the sources, transform and parameters of a non-key field."""
        if f.transform is None:
            if len(f.sources) > 1:
                return [(
                    (
                        "several source columns need a transform (concat,"
                        " coalesce or a custom one)"
                    ),
                    "transform",
                )]
            if not f.sources and f.default is None:
                return [(
                    (
                        "nothing to map: give a source column, a transform or a"
                        " default"
                    ),
                    "sources",
                )]
            return []

        transform = self.registry.get(f.transform)
        if transform is None:
            suggestion = _suggest(f.transform, self.registry.names())
            return [
                (f"unknown transform {f.transform!r}.{suggestion}", "transform")
            ]

        found = [
            (message, "params" if "parameter" in message else "sources")
            for message in transforms.check_call(
                transform, len(f.sources), f.params
            )
        ]
        if transform.name == "value_map" and not f.params:
            found.append((
                "value_map needs its translations, e.g. A=active; I=inactive",
                "params",
            ))
        table = f.params.get("table")
        if (
            transform.name == "lookup"
            and table is not None
            and table not in self.catalog.lookups
        ):
            known = ", ".join(sorted(self.catalog.lookups)) or "none"
            found.append(
                (f"no lookup table {table!r} (known: {known})", "params")
            )
        return found

    def _key_field_problems(
        self, f: contract.FieldSpec, target_type: casts.TargetType | None
    ) -> list[tuple[str, str]]:
        """Checks a primary key or foreign key field."""
        found: list[tuple[str, str]] = []
        kind = "a primary key" if f.primary_key else "a reference"

        if f.transform is not None:
            found.append((
                (
                    f"{kind} takes no transform: its value comes from the"
                    " source columns"
                ),
                "transform",
            ))
        if not f.sources:
            found.append((
                f"{kind} needs its natural key column(s) as source columns",
                "sources",
            ))
        if f.default is not None:
            found.append((f"{kind} takes no default", "default"))
        if target_type is not None and not _holds_uuid(target_type):
            found.append((
                "keys are UUIDs: use the UUID or VARCHAR(36) type",
                "target_type",
            ))

        if f.references is not None:
            found.extend(self._reference_problems(f))
        return found

    def _reference_problems(
        self, f: contract.FieldSpec
    ) -> list[tuple[str, str]]:
        """Checks what a foreign key points to."""
        names = [m.name for m in self.catalog.mappings]
        reference = f.references or ""
        if reference not in names:
            suggestion = _suggest(reference, names)
            return [
                (f"unknown mapping {reference!r}.{suggestion}", "references")
            ]

        parent = self._parent(reference)
        parent_key = next((pf for pf in parent.fields if pf.primary_key), None)
        if parent_key is None:
            return [(
                f"mapping {parent.name!r} has no primary key to reference",
                "references",
            )]
        if f.sources and len(parent_key.sources) != len(f.sources):
            columns = ", ".join(parent_key.sources)
            return [(
                (
                    f"the key of {parent.name!r} has {len(parent_key.sources)}"
                    f" column(s) ({columns}), {len(f.sources)} given"
                ),
                "sources",
            )]
        return []

    def _parent(self, name: str) -> contract.MappingSpec:
        return next(m for m in self.catalog.mappings if m.name == name)

    def _maker(self, spec: contract.MappingSpec) -> core_keys.KeyMaker:
        """Returns the key maker of the entity of `spec`."""
        uses_hmac = (
            key_hashing(spec, self.key_secret) is contract.KeyHashing.HMAC
        )
        return core_keys.KeyMaker(
            key_namespace(spec),
            self.key_secret if uses_hmac else None,
            spec.key_case,
        )

    def _key_maker(self, f: contract.FieldSpec) -> core_keys.KeyMaker | None:
        """Returns the key maker of a key field's entity: its parent's for a
        foreign key, so that both normalize natural keys the same way."""
        if f.references is not None:
            return self._maker(self._parent(f.references))
        if f.primary_key:
            return self._maker(self.spec)
        return None

    def _transform(self, f: contract.FieldSpec) -> transforms.Transform:
        if f.transform is None:
            return transforms.IDENTITY
        transform = self.registry.get(f.transform)
        if transform is None:
            raise KeyError(f.transform)
        return transform

    def _bind(
        self, f: contract.FieldSpec, transform: transforms.Transform
    ) -> types.TransformFn:
        """Returns the function computing the value of `f`."""
        if f.references is not None:
            parent = self._parent(f.references)
            return compiled.key_function(self._maker(parent), f.sources)
        if f.primary_key:
            return compiled.key_function(self._maker(self.spec), f.sources)
        if f.transform is None and not f.sources:
            # A constant: the default provides the value.
            return lambda: None
        if transform.name == "lookup":
            table = self.catalog.lookups[f.params["table"]]
            return compiled.lookup_function(table)
        return transform.fn

    def _at(self, location: str | None, key: str) -> str | None:
        """Returns `location` narrowed to a column, named as in the sheet."""
        return _at(location, self.labels[key])


def _at(location: str | None, column: str) -> str | None:
    """A location narrowed to a column: `... › row 12 › Transform`."""
    return None if location is None else f"{location} › {column}"


def _format_problems(
    f: contract.FieldSpec, target_type: casts.TargetType | None
) -> list[tuple[str, str]]:
    """Checks the source date format of a field."""
    if f.format is None:
        return []
    if target_type is None or not target_type.is_temporal:
        return [(
            "a format only applies to DATE / TIMESTAMP target types",
            "format",
        )]
    problems = casts.format_problems(f.format)
    if problems:
        return [(problem, "format") for problem in problems]
    if not _format_round_trips(f.format):
        return [(
            f"{f.format!r} is not a usable date format (e.g. dd/MM/yyyy)",
            "format",
        )]
    return []


def _default_problems(
    f: contract.FieldSpec, target_type: casts.TargetType | None
) -> list[tuple[str, str]]:
    if f.default is None or target_type is None:
        return []
    try:
        _cast_default(target_type, f.default, f.format)
    except casts.CastError as exc:
        return [(f"the default {f.default!r} is not valid: {exc}", "default")]
    return []


def _holds_uuid(target_type: casts.TargetType) -> bool:
    return target_type.kind is casts.TypeKind.STRING and (
        target_type.max_length is None or target_type.max_length >= 36
    )


def _suggest(name: str, candidates: list[str]) -> str:
    hint = difflib.get_close_matches(name, candidates, n=1)
    return f" Did you mean {hint[0]!r}?" if hint else ""


def _format_round_trips(source_format: str) -> bool:
    pattern = casts.to_strftime(source_format)
    sample = datetime.datetime(2001, 2, 3, 4, 5, 6)
    try:
        datetime.datetime.strptime(sample.strftime(pattern), pattern)
    except ValueError:
        return False
    return True


def _cast_default(
    target_type: casts.TargetType, default: str, source_format: str | None
) -> object:
    """Casts a default, written in the source format or in ISO 8601."""
    try:
        return target_type.cast(default, source_format)
    except casts.CastError:
        if source_format is None:
            raise
        return target_type.cast(default)


def _personal_problems(
    f: contract.FieldSpec, target_type: casts.TargetType | None
) -> list[tuple[str, str]]:
    """Checks the kind of personal data of a field, and that its fake values
    fit the target type."""
    if f.personal is None:
        return []
    if f.personal not in privacy.KINDS:
        return [(
            (
                f"personal data kind {f.personal!r} is unknown:"
                f" {', '.join(sorted(privacy.KINDS))}"
            ),
            "personal",
        )]
    if f.primary_key or f.references is not None:
        return [(
            (
                "a key is not personal data: its HMAC surrogate already hides"
                " the natural key"
            ),
            "personal",
        )]
    if target_type is None:
        return []
    kind = f.personal
    is_text = target_type.kind is casts.TypeKind.STRING
    if kind in privacy.TEXT_LENGTHS:
        length = privacy.TEXT_LENGTHS[kind]
        too_short = (
            target_type.max_length is not None
            and target_type.max_length < length
        )
        if not is_text or too_short:
            return [(
                (
                    f"{kind} values are anonymized into text of up to"
                    f" {length} characters: {target_type.raw} cannot hold"
                    f" them (use VARCHAR({length}) or more)"
                ),
                "personal",
            )]
    elif kind in privacy.SAME_LENGTH_KINDS and not is_text:
        return [(
            (
                f"{kind} values are anonymized into text: {target_type.raw}"
                " cannot hold them (use a text type, or another kind)"
            ),
            "personal",
        )]
    elif kind in privacy.TEMPORAL_KINDS and not target_type.is_temporal:
        return [(
            (
                f"{kind} shifts dates: {target_type.raw} is no DATE or"
                " TIMESTAMP (use mask or hash for text)"
            ),
            "personal",
        )]
    return []
