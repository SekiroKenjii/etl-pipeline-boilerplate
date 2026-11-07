"""Transforms: N source values, and text parameters from the sheet, to 1 value.

A small closed set is built in. Anything else is a Python function written by
a developer and registered by name (`customer.full_address`): no expression is
ever evaluated from a spreadsheet. A sheet refers to a transform by name, and
the signature of the function is what validation checks the sheet against.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
import dataclasses
import inspect
import re
import typing

from shared import utils

type TransformFn = Callable[..., object]

_CUSTOM_NAME = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$")


class UnmappedValueError(ValueError):
    """A value has no translation (value_map, lookup): the default applies.

    Attributes:
        value: The value without translation.
    """

    def __init__(self, value: object) -> None:
        super().__init__(f"no translation for {value!r}")
        self.value = value


def is_empty(value: object) -> bool:
    """Returns whether `value` is None or blank text.

    That is how legacy exports and spreadsheets write "no value".
    """
    return value is None or (isinstance(value, str) and not value.strip())


def _identity(value: object) -> object:
    """The source value as it is (when no transform is given)."""
    return value


def _concat(*values: object, separator: str = "") -> object:
    """Joins the non-empty values with `separator`."""
    parts = [str(value) for value in values if not is_empty(value)]
    return separator.join(parts) or None


def _coalesce(*values: object) -> object:
    """The first non-empty value."""
    return next((value for value in values if not is_empty(value)), None)


def _trim(value: object) -> object:
    """Removes leading and trailing spaces."""
    return None if value is None else str(value).strip()


def _upper(value: object) -> object:
    """UPPER CASE."""
    return None if value is None else str(value).upper()


def _lower(value: object) -> object:
    """lower case."""
    return None if value is None else str(value).lower()


def _value_map(value: object, **entries: str) -> object:
    """Translates the codes listed in the parameters: A=active; I=inactive."""
    if is_empty(value):
        return None
    key = str(value).strip()
    if key in entries:
        return entries[key]
    raise UnmappedValueError(value)


def _lookup(value: object, *, table: str) -> object:
    """Translates through a lookup sheet: table=countries reads that sheet."""
    del value, table  # Bound to the table by the compiler.
    raise NotImplementedError("bound to its table by the compiler")


@dataclasses.dataclass(frozen=True, slots=True)
class Transform:
    """A transform or a row hook, registered by name.

    Attributes:
        name: The name sheets refer to it by.
        fn: The function.
        builtin: Whether it ships with the boilerplate.
    """

    name: str
    fn: TransformFn
    builtin: bool

    @property
    def description(self) -> str:
        """The first line of the function's docstring, for sheet authors."""
        doc = inspect.getdoc(self.fn) or ""
        return doc.splitlines()[0] if doc else ""


_EXPLODE = re.compile(r"^explode\(\s*([A-Za-z_$][\w$]*(?:\.[\w$]+)*)\s*\)$")


def explode(path: str) -> TransformFn:
    """Returns the row splitter making one row per element of an array.

    Each row is the source row with the array at `path` replaced by one of
    its elements, and `<path>#` holding the element's position (0, 1...):
    fields read `addresses.city`, and a child key `_id, addresses#`. A row
    whose array is empty or missing makes no row (it is counted as
    filtered).

    Args:
        path: The field path of the array, e.g. `addresses` or
            `profile.phones`.
    """

    def split(row: Mapping[str, object]) -> Iterator[dict[str, object]]:
        items = utils.get_path(row, path)
        if items is None:
            return
        if not isinstance(items, list):
            raise TypeError(f"{path} is not an array")
        for index, item in enumerate(typing.cast(list[object], items)):
            exploded = _replaced(row, path.split("."), item)
            exploded[f"{path}#"] = index
            yield exploded

    split.__doc__ = f"One row per element of {path}."
    return split


def _replaced(
    row: Mapping[str, object], parts: list[str], value: object
) -> dict[str, object]:
    """Returns a copy of `row` with the value at `parts` replaced.

    Only the documents on the path are copied: the rest is shared.
    """
    copy = dict(row)
    if len(parts) == 1:
        copy[parts[0]] = value
        return copy
    nested = copy.get(parts[0])
    if not isinstance(nested, Mapping):
        raise TypeError(f"{parts[0]} is not a document")
    copy[parts[0]] = _replaced(
        typing.cast(Mapping[str, object], nested), parts[1:], value
    )
    return copy


BUILTIN_TRANSFORMS: dict[str, TransformFn] = {
    "concat": _concat,
    "coalesce": _coalesce,
    "trim": _trim,
    "upper": _upper,
    "lower": _lower,
    "value_map": _value_map,
    "lookup": _lookup,
}
IDENTITY = Transform("identity", _identity, builtin=True)


class TransformRegistry:
    """The transforms and row hooks a mapping may refer to.

        transforms = TransformRegistry()

        @transforms.register("customer.full_address")
        def full_address(street, ward, district, *, separator=", "): ...

        @transforms.register_filter("customer.not_deleted")
        def not_deleted(row) -> bool: ...

        @transforms.register_splitter("customer.phones")
        def phones(row) -> Iterable[Mapping[str, object]]: ...

    Transforms: positional parameters receive the source columns, in the
    sheet's order (`*args` accepts any number); keyword-only parameters
    receive the sheet's parameters, as text.

    Row hooks take the whole source row: a filter returns whether to keep it, a
    splitter returns the rows it becomes (0 to n, e.g. one per phone number),
    each then mapped by the fields.

    Custom names need a namespace (`domain.name`), so that they never clash
    with a future built-in.
    """

    def __init__(self, *, builtins: bool = True) -> None:
        self._transforms: dict[str, Transform] = {}
        self._filters: dict[str, Transform] = {}
        self._splitters: dict[str, Transform] = {}
        if builtins:
            for name, fn in BUILTIN_TRANSFORMS.items():
                self._transforms[name] = Transform(name, fn, builtin=True)

    def register[F: TransformFn](self, name: str) -> Callable[[F], F]:
        """Returns a decorator registering a custom transform as `name`."""

        def decorator(fn: F) -> F:
            self.add(name, fn)
            return fn

        return decorator

    def register_filter[F: TransformFn](self, name: str) -> Callable[[F], F]:
        """Returns a decorator registering a row filter as `name`."""

        def decorator(fn: F) -> F:
            _add(self._filters, "row filter", name, fn)
            return fn

        return decorator

    def register_splitter[F: TransformFn](self, name: str) -> Callable[[F], F]:
        """Returns a decorator registering a row splitter as `name`."""

        def decorator(fn: F) -> F:
            _add(self._splitters, "row splitter", name, fn)
            return fn

        return decorator

    def add(self, name: str, fn: TransformFn) -> None:
        """Registers a custom transform.

        Raises:
            ValueError: If `name` has no namespace or is already registered.
        """
        _add(self._transforms, "transform", name, fn)

    def include(self, other: TransformRegistry) -> None:
        """Adds the custom transforms and hooks of `other`.

        For example, one registry per module, merged for the tooling.

        Raises:
            ValueError: If a name is registered in both.
        """
        for mine, theirs, kind in [
            (self._transforms, other.transforms(), "transform"),
            (self._filters, other.filters(), "row filter"),
            (self._splitters, other.splitters(), "row splitter"),
        ]:
            for item in theirs:
                if not item.builtin:
                    _add(mine, kind, item.name, item.fn)

    def get(self, name: str) -> Transform | None:
        return self._transforms.get(name)

    def get_filter(self, name: str) -> Transform | None:
        return self._filters.get(name)

    def get_splitter(self, name: str) -> Transform | None:
        """Returns a registered row splitter, or the built-in `explode`.

        `explode(<field path>)` makes one row per element of an array field
        of the source (a document's `addresses`): see `explode`.
        """
        match = _EXPLODE.match(name)
        if match is not None:
            return Transform(name, explode(match.group(1)), builtin=True)
        return self._splitters.get(name)

    def names(self) -> list[str]:
        return sorted(self._transforms)

    def filter_names(self) -> list[str]:
        return sorted(self._filters)

    def splitter_names(self) -> list[str]:
        return sorted(self._splitters)

    def transforms(self) -> list[Transform]:
        """Returns every transform, built-in or custom, by name."""
        return [self._transforms[name] for name in self.names()]

    def filters(self) -> list[Transform]:
        """Returns every row filter, by name."""
        return [self._filters[name] for name in self.filter_names()]

    def splitters(self) -> list[Transform]:
        """Returns every row splitter, by name."""
        return [self._splitters[name] for name in self.splitter_names()]


def _add(
    target: dict[str, Transform], kind: str, name: str, fn: TransformFn
) -> None:
    if not _CUSTOM_NAME.match(name):
        raise ValueError(
            f"Custom {kind} {name!r} must be namespaced, e.g. 'customer.{name}'"
        )
    if name in target:
        raise ValueError(f"{kind.capitalize()} {name!r} is already registered")
    target[name] = Transform(name, fn, builtin=False)


def check_row_hook(hook: Transform, kind: str) -> list[str]:
    """Returns what is wrong with a row hook's signature.

    A row hook is called with the row only: one positional parameter, and no
    other required one.
    """
    if check_call(hook, 1, ()):
        return [
            f"{kind} '{hook.name}' must take the source row as its only"
            " argument"
        ]
    return []


def check_call(
    transform: Transform, sources: int, params: Iterable[str]
) -> list[str]:
    """Returns what is wrong with calling a transform as a sheet does.

    Args:
        transform: The transform.
        sources: The number of source columns the sheet gives.
        params: The names of the parameters the sheet gives.

    Returns:
        One message per problem; empty when the call is fine.
    """
    signature = inspect.signature(transform.fn)
    parameters = list(signature.parameters.values())
    problems: list[str] = []

    positional = [
        p
        for p in parameters
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    variadic = any(p.kind is p.VAR_POSITIONAL for p in parameters)
    least = sum(1 for p in positional if p.default is p.empty)
    most = None if variadic else len(positional)

    if sources < least or (most is not None and sources > most):
        problems.append(
            f"'{transform.name}' takes {_expected(least, most)} source"
            f" column(s), {sources} given"
        )

    keywords = {p.name: p for p in parameters if p.kind is p.KEYWORD_ONLY}
    open_keywords = any(p.kind is p.VAR_KEYWORD for p in parameters)
    given = set(params)

    if not open_keywords:
        for unknown in sorted(given - keywords.keys()):
            accepted = ", ".join(sorted(keywords)) or "none"
            problems.append(
                f"'{transform.name}' has no parameter {unknown!r} (accepted:"
                f" {accepted})"
            )
    for name, parameter in keywords.items():
        if parameter.default is parameter.empty and name not in given:
            problems.append(f"'{transform.name}' needs the parameter {name!r}")

    return problems


def _expected(least: int, most: int | None) -> str:
    if most is None:
        return f"at least {least}"
    if least == most:
        return f"exactly {least}"
    return f"{least} to {most}"
