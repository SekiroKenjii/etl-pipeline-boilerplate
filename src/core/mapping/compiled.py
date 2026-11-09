"""Compiled mappings: applying a validated mapping to source rows."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
import dataclasses
import enum
import hashlib
import threading
import typing

from core import contract
from core import keys as core_keys
from core.mapping import casts
from core.mapping import results
from core.mapping import transforms
from core.mapping import types
from shared import utils

# What a field makes of one row: the value (None on an issue), the issue,
# and, for a key field, the canonical natural key the value derives from.
type _Outcome = tuple[
    object, results.MappingIssue | None, tuple[str, ...] | None
]
# Maps one row; the sequence identifies the record, for anonymizing.
type _Runner = Callable[[Mapping[str, object], Sequence[str]], _Outcome]
type _Reader = Callable[[Mapping[str, object]], list[object]]
type _Settler = Callable[[object, list[object], Sequence[str]], _Outcome]


class IncompleteKeyError(ValueError):
    """Some, but not all, components of a natural key are empty."""


@dataclasses.dataclass(frozen=True, slots=True)
class CompiledField:
    """A validated field, ready to produce its target value.

    Attributes:
        spec: The field as defined.
        transform: The transform it runs.
        fn: The function computing the value from the source values.
        target_type: The type values are cast to, if any.
        is_key: Whether it is the primary key: an empty value is an issue.
        maker: For a primary or foreign key, the key maker of its entity.
        anonymize: Replaces the value of a personal column, when its data
            must not leave production.
        run: Maps one row, compiled once from the above: the sources, the
            transform, the default, the cast and the anonymizer are chosen
            here, not at every row. A key field derives its natural key
            once and returns it with the value.

    The issues of a personal column (`spec.personal`) never carry its value,
    in their value or their message: they go to the quarantine, the issue
    sinks, the exports and the cutover reports.
    """

    spec: contract.FieldSpec
    transform: transforms.Transform
    fn: types.TransformFn
    target_type: casts.TargetType | None
    is_key: bool = False
    maker: core_keys.KeyMaker | None = None
    anonymize: types.Anonymizer | None = None
    run: _Runner = dataclasses.field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "run", _runner(self))

    def natural_key(self, row: Mapping[str, object]) -> tuple[str, ...]:
        """Returns the canonical natural key a key field reads in `row`."""
        values = [source_value(row, s) for s in self.spec.sources]
        if self.maker is None:
            return core_keys.canonical(values)
        return self.maker.canonical(values)

    def apply(
        self, row: Mapping[str, object], record: Sequence[str] = ()
    ) -> tuple[object, results.MappingIssue | None]:
        """Produces the value of the field for one source row.

        Args:
            row: The source row.
            record: Identifies the record (its canonical natural key), for
                anonymizing: the dates of a record shift together.

        Returns:
            The value (None on an issue), and the issue, if any.
        """
        value, issue, _ = self.run(row, record)
        return value, issue

    def issue(
        self, message: str, value: object, redacted: str | None = None
    ) -> results.MappingIssue:
        """Returns an issue of the field; without the value for a personal
        column."""
        if self.spec.personal is not None:
            return results.MappingIssue(
                self.spec.target, message if redacted is None else redacted
            )
        return results.MappingIssue(self.spec.target, message, value)


class _Role(enum.Enum):
    """What the natural key of a field serves in the record."""

    NONE = enum.auto()
    PRIMARY = enum.auto()
    REFERENCE = enum.auto()


class CompiledMapping:
    """A validated mapping, ready to map rows. Thread-safe.

    Attributes:
        spec: The mapping as defined.
        references: The foreign keys, by target column.
        sources: Every source column a field reads.
    """

    def __init__(
        self,
        spec: contract.MappingSpec,
        fields: tuple[CompiledField, ...],
        row_filter: transforms.Transform | None = None,
        row_split: transforms.Transform | None = None,
        references: dict[str, results.Reference] | None = None,
    ) -> None:
        self.spec = spec
        self._fields = fields
        self._row_filter = row_filter
        self._row_split = row_split
        self.references = references or {}
        self._key_field = next((f for f in fields if f.spec.primary_key), None)
        self._plan = tuple(
            (f.spec.target, f.run, self._role(f), f) for f in fields
        )
        sources: set[str] = set()
        for f in fields:
            sources.update(f.spec.sources)
        self.sources = frozenset(sources)
        self._anonymizes = any(f.anonymize is not None for f in fields)
        self._checked: set[frozenset[str]] = set()
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def entity(self) -> str:
        """The target entity, as registered in the key crosswalk."""
        return self.spec.target

    @property
    def key_field(self) -> str | None:
        """The target column of the primary key, if any."""
        return self._key_field.spec.target if self._key_field else None

    def check_columns(self, columns: Iterable[str]) -> None:
        """Checks that a source has the shape the mapping expects.

        Checked once per distinct set of columns.

        Args:
            columns: The columns of a source row.

        Raises:
            FatalError: If a used column is missing, or if (policy `error`) a
                column is neither used nor ignored: no row of the source can
                be trusted.
        """
        present = frozenset(columns)
        with self._lock:
            if present in self._checked:
                return

        missing = sorted(
            source
            for source in self.sources
            if source not in present and _root(source) not in present
        )
        if missing:
            raise contract.FatalError(
                f"Mapping {self.name!r}: the source has no column(s) {missing}"
            )

        if self.spec.unmapped_source_fields is contract.UnmappedPolicy.ERROR:
            used = self.sources | {_root(source) for source in self.sources}
            unmapped = sorted(
                present - used - set(self.spec.ignored_source_fields)
            )
            if unmapped:
                raise contract.FatalError(
                    f"Mapping {self.name!r}: source column(s) {unmapped} are"
                    " not mapped. Map them, or list them in the ignored source"
                    " fields."
                )

        with self._lock:
            self._checked.add(present)

    def map(
        self, row: Mapping[str, object], source_key: str | None = None
    ) -> list[results.MappedRecord]:
        """Maps one source row into its target rows.

        A row hook failing on a row yields one record carrying the failure as
        an issue, so that the quality gate handles it as any other bad row.

        Args:
            row: The source row.
            source_key: Identifies the source row, for lineage.

        Returns:
            None if the row filter drops the row, several rows if the row
            splitter makes several, else one.

        Raises:
            FatalError: If the source does not have the expected shape.
        """
        if self._row_filter is not None:
            # Row hooks are custom code: their failure is an issue of the row.
            try:
                keep = self._row_filter.fn(row)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                return [
                    self._failed(
                        "row filter", self._row_filter, exc, source_key
                    )
                ]
            if not keep:
                return []

        parts: list[Mapping[str, object]] = [row]
        if self._row_split is not None:
            try:
                parts = _rows(self._row_split.fn(row))
            except Exception as exc:  # pylint: disable=broad-exception-caught
                return [
                    self._failed(
                        "row splitter", self._row_split, exc, source_key
                    )
                ]

        mapped: list[results.MappedRecord] = []
        for index, part in enumerate(parts):
            self.check_columns(part.keys())
            mapped.append(self.apply(part, source_key, index))
        return mapped

    def apply(
        self,
        row: Mapping[str, object],
        source_key: str | None = None,
        part: int = 0,
    ) -> results.MappedRecord:
        """Maps one row with the fields only: no row hook, no shape check.

        Args:
            row: The source row.
            source_key: Identifies the source row, for lineage.
            part: The position of the row among those split from one source
                row.

        Returns:
            The target record, with its issues.
        """
        data: dict[str, object] = {}
        issues: list[results.MappingIssue] = []
        key: str | None = None
        natural: tuple[str, ...] | None = None
        # Set foreign keys were derived from complete natural keys.
        foreign_keys: dict[str, tuple[str, ...]] = {}

        record = self._record(row, source_key) if self._anonymizes else ()
        for target, run, role, field in self._plan:
            value, issue, derived = run(row, record)
            data[target] = value
            if issue is not None:
                issues.append(issue)
            elif value is not None and role is not _Role.NONE:
                if derived is None:
                    derived = field.natural_key(row)
                if role is _Role.PRIMARY:
                    key, natural = str(value), derived
                else:
                    foreign_keys[target] = derived

        return results.MappedRecord(
            data,
            tuple(issues),
            self.name,
            self.spec.version,
            source_key,
            part,
            self.entity,
            key,
            natural,
            foreign_keys,
        )

    def deletion(
        self, row: Mapping[str, object], source_key: str | None = None
    ) -> results.MappedRecord:
        """Returns the deletion of the target row of a deleted source row.

        Only the natural key columns of `row` are read: a change log gives
        little more about a deleted row.

        Args:
            row: The deleted source row, its key columns at least.
            source_key: Identifies the source row, for lineage.

        Raises:
            FatalError: If the mapping has no primary key, or splits rows:
                the target rows of a deleted source row cannot be told
                apart; use a scope (`MappingTransformer(scope=...)`).
        """
        if self._key_field is None or self._row_split is not None:
            raise contract.FatalError(
                f"Mapping {self.name!r} cannot apply deletes by key: it"
                + (
                    " splits rows; delete them by scope"
                    if self._key_field is not None
                    else " has no primary key"
                )
            )
        return self.scope_deletion(row, self._key_field.spec.target, source_key)

    def scope_deletion(
        self,
        row: Mapping[str, object],
        target: str,
        source_key: str | None = None,
    ) -> results.MappedRecord:
        """Returns the deletion of the target rows whose `target` column has
        the value the field maps from `row`.

        A row split into child rows (`explode(addresses)`) deletes its
        children by their parent key before they are written again: children
        gone from the source are gone from the target.

        Raises:
            FatalError: If `target` is not a column of the mapping.
        """
        found = next((f for f in self._fields if f.spec.target == target), None)
        if found is None:
            raise contract.FatalError(
                f"Mapping {self.name!r} has no target column {target!r}"
            )
        value, issue = found.apply(row)
        key = str(value) if found.is_key and value is not None else None
        return results.MappedRecord(
            data={target: value},
            issues=() if issue is None else (issue,),
            mapping=self.name,
            version=self.spec.version,
            source_key=source_key,
            entity=self.entity,
            key=key,
            deleted=True,
        )

    @property
    def splits_rows(self) -> bool:
        return self._row_split is not None

    def _role(self, field: CompiledField) -> _Role:
        if field is self._key_field:
            return _Role.PRIMARY
        if field.spec.target in self.references:
            return _Role.REFERENCE
        return _Role.NONE

    def _record(
        self, row: Mapping[str, object], source_key: str | None
    ) -> tuple[str, ...]:
        """Identifies the record a row maps to, for anonymizing: its natural
        key, else the source key, else the row itself.

        Without the last, the rows of a mapping without identity would all
        share one date shift: one real date known, every other follows. A
        row is its own identity only while it stays the same: changed, its
        dates get another shift.
        """
        if self._key_field is not None:
            try:
                return self._key_field.natural_key(row)
            except core_keys.MissingKeyComponentError:
                return _row_identity(row)
        return _row_identity(row) if source_key is None else (source_key,)

    def _failed(
        self,
        kind: str,
        hook: transforms.Transform,
        exc: Exception,
        source_key: str | None,
    ) -> results.MappedRecord:
        """The record of a row whose row filter or splitter failed: an issue
        of the whole row, on one line."""
        message = (
            f"{kind} '{hook.name}' failed: {type(exc).__name__}:"
            f" {utils.one_line(str(exc))}"
        )
        issue = results.MappingIssue(results.ROW, message)
        return results.MappedRecord(
            {},
            (issue,),
            self.name,
            self.spec.version,
            source_key,
            entity=self.entity,
        )


def key_function(
    maker: core_keys.KeyMaker, sources: tuple[str, ...]
) -> types.TransformFn:
    """Returns the function deriving a surrogate key from `sources`.

    The function returns None when every component is empty, and raises
    `IncompleteKeyError` when only some are.
    """

    def key(*values: object) -> object:
        natural = _natural_key(maker, sources, list(values))
        return None if natural is None else maker.key(natural)

    return key


def lookup_function(table: Mapping[str, str]) -> types.TransformFn:
    """Returns the function translating values through a lookup table.

    The function raises `UnmappedValueError` for a value not in the table.
    """

    def lookup(value: object, **params: str) -> object:
        del params  # The table is already bound.
        if transforms.is_empty(value):
            return None
        key = str(value).strip()
        if key in table:
            return table[key]
        raise transforms.UnmappedValueError(value)

    return lookup


def _runner(field: CompiledField) -> _Runner:
    """Compiles a field into the function mapping one row."""
    read = _reader(field.spec.sources)
    settle = _settler(field)
    if field.maker is not None:
        return _key_runner(field, read, settle)
    return _value_runner(field, read, settle)


def _value_runner(
    field: CompiledField, read: _Reader, settle: _Settler
) -> _Runner:
    """The runner of a field computing its value with its transform."""
    fn, params = field.fn, field.spec.params
    default = field.spec.default

    if fn is transforms.IDENTITY.fn and len(field.spec.sources) == 1:

        def run_source(
            row: Mapping[str, object], record: Sequence[str]
        ) -> _Outcome:
            values = read(row)
            return settle(values[0], values, record)

        return run_source

    def run(row: Mapping[str, object], record: Sequence[str]) -> _Outcome:
        values = read(row)
        try:
            value = fn(*values, **params) if params else fn(*values)
        except transforms.UnmappedValueError as exc:
            if default is not None:
                return settle(default, values, record)
            return (
                None,
                field.issue(
                    str(exc), exc.value, redacted="no translation for the value"
                ),
                None,
            )
        except IncompleteKeyError as exc:
            return None, field.issue(str(exc), _first(values)), None
        # A custom transform choking on legacy data is an issue of that row.
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return None, _transform_failed(field, exc, values), None
        if transforms.is_empty(value):
            value = None
        return settle(value, values, record)

    return run


def _key_runner(
    field: CompiledField, read: _Reader, settle: _Settler
) -> _Runner:
    """The runner of a primary or foreign key: the natural key is derived
    once, for the value and for the record."""
    maker = field.maker
    if maker is None:
        raise ValueError(f"{field.spec.target!r} has no key maker")
    sources = field.spec.sources

    def run(row: Mapping[str, object], record: Sequence[str]) -> _Outcome:
        values = read(row)
        try:
            natural = _natural_key(maker, sources, values)
            key = None if natural is None else maker.key(natural)
        except IncompleteKeyError as exc:
            return None, field.issue(str(exc), _first(values)), None
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return None, _transform_failed(field, exc, values), None
        value, issue, _ = settle(key, values, record)
        return value, issue, natural

    return run


def _settler(field: CompiledField) -> _Settler:
    """The function finishing a value: the default of an empty one, the
    cast to the target type, the anonymizing."""
    default = field.spec.default
    required = field.spec.required or field.is_key
    target_type, anonymize = field.target_type, field.anonymize
    cast = (
        target_type.caster(field.spec.format)
        if target_type is not None
        else None
    )
    recast = target_type.caster() if target_type is not None else None

    def settle(
        value: object, values: list[object], record: Sequence[str]
    ) -> _Outcome:
        if value is None:
            if default is None:
                if required:
                    return (
                        None,
                        field.issue("is required but empty", _first(values)),
                        None,
                    )
                return None, None, None
            value = default
        if cast is not None:
            try:
                value = cast(value)
            except casts.CastError as exc:
                return None, field.issue(str(exc), value, exc.redacted), None
        if anonymize is not None:
            value = anonymize(value, record)
            if recast is not None:
                # The fake value must fit the column as the real one did.
                try:
                    value = recast(value)
                except casts.CastError as exc:
                    return (
                        None,
                        field.issue(
                            "the anonymized value does not fit:"
                            f" {exc.redacted}",
                            None,
                        ),
                        None,
                    )
        return value, None, None

    return settle


def _reader(sources: tuple[str, ...]) -> _Reader:
    """The function reading the source values of a field in a row, an empty
    value (None, blank text) as None."""
    is_empty = transforms.is_empty
    if any("." in source for source in sources):

        def read_paths(row: Mapping[str, object]) -> list[object]:
            return [
                None if is_empty(value) else value
                for value in (source_value(row, s) for s in sources)
            ]

        return read_paths

    if len(sources) == 1:
        (source,) = sources

        def read_one(row: Mapping[str, object]) -> list[object]:
            value = row[source]
            return [None if is_empty(value) else value]

        return read_one

    def read(row: Mapping[str, object]) -> list[object]:
        return [
            None if is_empty(value) else value
            for value in (row[s] for s in sources)
        ]

    return read


def _natural_key(
    maker: core_keys.KeyMaker, sources: tuple[str, ...], values: list[object]
) -> tuple[str, ...] | None:
    """The canonical natural key of `values`: None when every component is
    empty.

    Raises:
        IncompleteKeyError: When only some components are empty.
    """
    if None in values:
        missing = [s for s, value in zip(sources, values) if value is None]
        if len(missing) == len(values):
            return None
        raise IncompleteKeyError(
            f"incomplete natural key: {', '.join(missing)} empty"
        )
    return maker.canonical(values)


def _transform_failed(
    field: CompiledField, exc: Exception, values: list[object]
) -> results.MappingIssue:
    kind = type(exc).__name__
    failed = f"transform '{field.transform.name}' failed: {kind}"
    return field.issue(
        f"{failed}: {utils.one_line(str(exc))}", _first(values), redacted=failed
    )


def _row_identity(row: Mapping[str, object]) -> tuple[str, ...]:
    """A row as an identity: the digest of its columns and values."""
    text = repr(sorted((str(name), repr(value)) for name, value in row.items()))
    return ("row", hashlib.sha256(text.encode()).hexdigest())


def source_value(row: Mapping[str, object], source: str) -> object:
    """Returns the value of a source column, or of a nested field.

    A source column named `a.b` is read as it is if the row has it, else as
    the field `b` of the document `a` (MongoDB rows); a missing nested field
    is None, as documents need not all have the same fields.
    """
    if source in row or "." not in source:
        return row[source]
    return utils.get_path(row, source)


def _root(source: str) -> str:
    return source.split(".")[0]


def _rows(produced: object) -> list[Mapping[str, object]]:
    """Checks what a row splitter returned: an iterable of rows."""
    if not isinstance(produced, Iterable) or isinstance(
        produced, (str, bytes, Mapping)
    ):
        raise TypeError("it must return an iterable of rows")
    items: list[object] = list(typing.cast(Iterable[object], produced))
    rows = [
        typing.cast(Mapping[str, object], item)
        for item in items
        if isinstance(item, Mapping)
    ]
    if len(rows) != len(items):
        raise TypeError("every row it returns must be a mapping")
    return rows


def _first(values: Sequence[object]) -> object:
    return values[0] if len(values) == 1 else (values or None)
