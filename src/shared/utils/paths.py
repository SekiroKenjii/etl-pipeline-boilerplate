"""Reading nested values with dotted paths."""

from __future__ import annotations

from collections.abc import Mapping, Sequence


def get_path(data: object, path: str, default: object = None) -> object:
    """Reads a nested value with a dotted path.

        get_path(body, "data.items.0.id")

    Args:
        data: Nested mappings and sequences, e.g. a decoded JSON body.
        path: Keys separated by dots; integer segments index sequences.
        default: The value returned when a segment is missing.

    Returns:
        The value at `path`, or `default`.
    """
    current = data
    for segment in path.split("."):
        if isinstance(current, Mapping):
            if segment not in current:
                return default
            current = current[segment]
        elif (
            isinstance(current, Sequence)
            and not isinstance(current, (str, bytes))
            and segment.lstrip("-").isdigit()
        ):
            index = int(segment)
            if not -len(current) <= index < len(current):
                return default
            current = current[index]
        else:
            return default

    return current
