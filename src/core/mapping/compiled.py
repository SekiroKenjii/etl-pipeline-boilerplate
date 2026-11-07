"""Compiled mappings: applying a validated mapping to source rows."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import dataclasses
import threading
import typing

from core import contract
from core import keys as core_keys
from core.mapping import privacy
from core.mapping import results
from core.mapping import transforms
from core.mapping import types
from shared import utils


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

    The issues of a personal column (`spec.personal`) never carry its value,
    in their value or their message: they go to the quarantine, the issue
    sinks, the exports and the cutover reports.
    """

    spec: contract.FieldSpec
    transform: transforms.Transform
    fn: transforms.TransformFn
    target_type: types.TargetType | None
    is_key: bool = False
    maker: core_keys.KeyMaker | None = None
    anonymize: privacy.Anonymizer | None = None

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
        values = [
            None if transforms.is_empty(value) else value
            for value in (
                source_value(row, source) for source in self.spec.sources
            )
        ]
        value, issue = self._transformed(values)
        if issue is not None:
            return None, issue
        if value is None:
            if self.spec.required or self.is_key:
                return None, self._issue(
                    "is required but empty", _first(values)
                )
            return None, None
        return self._typed(value, record)

    def _transformed(
        self, values: Sequence[object]
    ) -> tuple[object, results.MappingIssue | None]:
        """The value of the transform, or the default when it is empty."""
        spec = self.spec
        untranslated: transforms.UnmappedValueError | None = None
        try:
            value = self.fn(*values, **spec.params)
        except transforms.UnmappedValueError as exc:
            value, untranslated = None, exc
        except IncompleteKeyError as exc:
            return None, self._issue(str(exc), _first(values))
        # A custom transform choking on legacy data is an issue of that row.
        except Exception as exc:  # pylint: disable=broad-exception-caught
            kind = type(exc).__name__
            failed = f"transform '{self.transform.name}' failed: {kind}"
            return None, self._issue(
                f"{failed}: {exc}", _first(values), redacted=failed
            )
        if not transforms.is_empty(value):
            return value, None
        if spec.default is not None:
            return spec.default, None
        if untranslated is not None:
            return None, self._issue(
                str(untranslated),
                untranslated.value,
                redacted="no translation for the value",
            )
        return None, None

    def _typed(
        self, value: object, record: Sequence[str]
    ) -> tuple[object, results.MappingIssue | None]:
        """The value cast to the target type, then anonymized."""
        if self.target_type is not None:
            try:
                value = self.target_type.cast(value, self.spec.format)
            except types.CastError as exc:
                return None, self._issue(str(exc), value, exc.redacted)

        if self.anonymize is not None:
            value = self.anonymize(value, record)
            if self.target_type is not None:
                # The fake value must fit the column as the real one did.
                try:
                    value = self.target_type.cast(value)
                except types.CastError as exc:
                    return None, self._issue(
                        f"the anonymized value does not fit: {exc.redacted}",
                        None,
                    )
        return value, None

    def _issue(
        self, message: str, value: object, redacted: str | None = None
    ) -> results.MappingIssue:
        """Returns an issue of the field; without the value for a personal
        column."""
        if self.spec.personal is not None:
            return results.MappingIssue(
                self.spec.target, message if redacted is None else redacted
            )
        return results.MappingIssue(self.spec.target, message, value)


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
        self._reference_fields = [
            f for f in fields if f.spec.target in self.references
        ]
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

        record = self._record(row, source_key) if self._anonymizes else ()
        for f in self._fields:
            value, issue = f.apply(row, record)
            data[f.spec.target] = value
            if issue is not None:
                issues.append(issue)

        key: str | None = None
        natural: tuple[str, ...] | None = None
        if (
            self._key_field is not None
            and data[self._key_field.spec.target] is not None
        ):
            key = str(data[self._key_field.spec.target])
            natural = self._key_field.natural_key(row)

        # Set foreign keys were derived from complete natural keys.
        foreign_keys = {
            f.spec.target: f.natural_key(row)
            for f in self._reference_fields
            if data[f.spec.target] is not None
        }

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

    def _record(
        self, row: Mapping[str, object], source_key: str | None
    ) -> tuple[str, ...]:
        """Identifies the record a row maps to, for anonymizing: its natural
        key, else the source key."""
        if self._key_field is not None:
            try:
                return self._key_field.natural_key(row)
            except core_keys.MissingKeyComponentError:
                return ()
        return () if source_key is None else (source_key,)

    def _failed(
        self,
        kind: str,
        hook: transforms.Transform,
        exc: Exception,
        source_key: str | None,
    ) -> results.MappedRecord:
        message = f"{kind} '{hook.name}' failed: {type(exc).__name__}: {exc}"
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
) -> transforms.TransformFn:
    """Returns the function deriving a surrogate key from `sources`.

    The function returns None when every component is empty, and raises
    `IncompleteKeyError` when only some are.
    """

    def key(*values: object) -> object:
        if all(value is None for value in values):
            return None
        missing = [
            source for source, value in zip(sources, values) if value is None
        ]
        if missing:
            raise IncompleteKeyError(
                f"incomplete natural key: {', '.join(missing)} empty"
            )
        return maker.key(maker.canonical(values))

    return key


def lookup_function(table: Mapping[str, str]) -> transforms.TransformFn:
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
