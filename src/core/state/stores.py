"""State stores: in memory, and JSON files."""

from __future__ import annotations

import copy
import json
import pathlib
import threading
import typing

from core import contract
from shared import utils


class InMemoryStateStore(contract.IStateStore):
    """Keeps the state in memory, for tests and local development."""

    def __init__(self) -> None:
        self._data: dict[str, dict[str, contract.StateValue]] = {}
        self._lock = threading.Lock()

    def get(self, namespace: str, key: str) -> contract.StateValue:
        with self._lock:
            return copy.deepcopy(self._data.get(namespace, {}).get(key))

    def set(self, namespace: str, key: str, value: contract.StateValue) -> None:
        with self._lock:
            self._data.setdefault(namespace, {})[key] = copy.deepcopy(value)

    def delete(self, namespace: str, key: str) -> None:
        with self._lock:
            self._data.get(namespace, {}).pop(key, None)


class JsonFileStateStore(contract.IStateStore):
    """Keeps one JSON file per namespace: `{directory}/{namespace}.state.json`.

    Every write rewrites the file atomically, so that a crash never leaves a
    corrupted state behind. Good for single-host deployments; use a database
    backed store when several hosts run the same pipeline.

    Attributes:
        directory: Where the files are written.
    """

    def __init__(self, directory: pathlib.Path | str) -> None:
        self.directory = pathlib.Path(directory)
        self._lock = threading.Lock()

    def path_for(self, namespace: str) -> pathlib.Path:
        """Returns the file of `namespace`."""
        return self.directory / f"{namespace}.state.json"

    def get(self, namespace: str, key: str) -> contract.StateValue:
        with self._lock:
            return self._read(namespace).get(key)

    def set(self, namespace: str, key: str, value: contract.StateValue) -> None:
        with self._lock:
            state = self._read(namespace)
            state[key] = value
            self._write(namespace, state)

    def delete(self, namespace: str, key: str) -> None:
        with self._lock:
            state = self._read(namespace)
            if state.pop(key, None) is not None:
                self._write(namespace, state)

    def _read(self, namespace: str) -> dict[str, contract.StateValue]:
        """Returns the state of `namespace`.

        Raises:
            ValueError: If the file does not hold a JSON object.
        """
        path = self.path_for(namespace)
        if not path.exists():
            return {}

        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"State file {path} does not hold a JSON object")
        return typing.cast(dict[str, contract.StateValue], data)

    def _write(
        self, namespace: str, state: dict[str, contract.StateValue]
    ) -> None:
        text = json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True)
        utils.atomic_write_text(self.path_for(namespace), text + "\n")
