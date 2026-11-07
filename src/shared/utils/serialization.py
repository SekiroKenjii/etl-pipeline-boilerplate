"""Turning Python values into JSON-friendly ones."""

from __future__ import annotations

import base64
from collections.abc import Callable
import dataclasses
import datetime
import decimal
import enum
import pathlib
from typing import Any
import uuid


def to_jsonable(value: object) -> object:
    """Turns a value `json` cannot encode into one it can.

    Meant as `json.dumps(default=to_jsonable)`: `json` calls it again on
    whatever it returns, so nested values are handled too.

    Args:
        value: A value `json` cannot encode.

    Returns:
        A dict for dataclasses, ISO text for dates, the value of enums, text
        for paths, a list for sets and tuples, and `repr(value)` for anything
        else, so that serializing never fails.
    """
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, pathlib.PurePath):
        return str(value)
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)

    return repr(value)


_DECODER_BY_TAG: dict[str, Callable[[str], object]] = {
    "datetime": datetime.datetime.fromisoformat,
    "date": datetime.date.fromisoformat,
    "time": datetime.time.fromisoformat,
    "decimal": decimal.Decimal,
    "uuid": uuid.UUID,
    "bytes": base64.b64decode,
}
_TAG = "$type"
_ADAPTER_TYPE_BY_TAG: dict[str, tuple[type[Any], Callable[[Any], str]]] = {}


def register_tag(
    tag: str,
    type_: type[Any],
    encode: Callable[[Any], str],
    decode: Callable[[str], object],
) -> None:
    """Teaches `to_tagged` and `from_tagged` a type of a library.

    For the values an adapter returns that must survive a state store, e.g.
    MongoDB's ObjectId as a watermark. Registering the same tag again
    replaces it.

    Args:
        tag: The tag of the type, e.g. `objectid`.
        type_: The type.
        encode: Turns a value into text.
        decode: Turns the text back into a value.

    Raises:
        ValueError: If `tag` is one of the built-in tags.
    """
    if (
        tag in _DECODER_BY_TAG
        and tag not in _ADAPTER_TYPE_BY_TAG
        or tag == "dict"
    ):
        raise ValueError(f"{tag!r} is a built-in tag")
    _ADAPTER_TYPE_BY_TAG[tag] = (type_, encode)
    _DECODER_BY_TAG[tag] = decode


def to_tagged(value: object) -> object:
    """Encodes a value into JSON-compatible data that `from_tagged` restores.

    Unlike `to_jsonable`, it is lossless for the values a database returns:
    datetimes, dates, times, decimals, UUIDs and bytes (e.g. a SQL Server
    `rowversion`) become `{"$type": ..., "value": ...}`. Lists and tuples
    become lists, dicts keep string keys.

    Args:
        value: None, a bool, int, float, str, one of the tagged types, or a
            list, tuple or dict of those.

    Returns:
        JSON-compatible data.

    Raises:
        TypeError: If a value has no lossless JSON form.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    # Before `date`: a datetime is a date.
    if isinstance(value, datetime.datetime):
        return {_TAG: "datetime", "value": value.isoformat()}
    if isinstance(value, datetime.date):
        return {_TAG: "date", "value": value.isoformat()}
    if isinstance(value, datetime.time):
        return {_TAG: "time", "value": value.isoformat()}
    if isinstance(value, decimal.Decimal):
        return {_TAG: "decimal", "value": str(value)}
    if isinstance(value, uuid.UUID):
        return {_TAG: "uuid", "value": str(value)}
    if isinstance(value, (bytes, bytearray, memoryview)):
        text = base64.b64encode(bytes(value)).decode("ascii")
        return {_TAG: "bytes", "value": text}
    if isinstance(value, (list, tuple)):
        return [to_tagged(item) for item in value]
    if isinstance(value, dict):
        encoded = {str(key): to_tagged(item) for key, item in value.items()}
        # A plain dict with a `$type` key must not pass for a tagged value.
        return {_TAG: "dict", "value": encoded} if _TAG in value else encoded
    for tag, (type_, encode) in _ADAPTER_TYPE_BY_TAG.items():
        if isinstance(value, type_):
            return {_TAG: tag, "value": encode(value)}
    raise TypeError(f"{type(value).__name__} has no lossless JSON form")


def from_tagged(data: object) -> object:
    """Decodes what `to_tagged` encoded.

    Raises:
        ValueError: If a tag is unknown.
    """
    if isinstance(data, list):
        return [from_tagged(item) for item in data]
    if not isinstance(data, dict):
        return data

    tag = data.get(_TAG)
    if tag is None:
        return {key: from_tagged(item) for key, item in data.items()}
    if tag == "dict":
        return {key: from_tagged(item) for key, item in data["value"].items()}
    parse = _DECODER_BY_TAG.get(tag)
    if parse is None:
        raise ValueError(f"Unknown tag {tag!r}")
    return parse(data["value"])
