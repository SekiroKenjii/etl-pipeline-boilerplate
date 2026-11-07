"""Secrets in settings: `${provider:reference}` placeholders, resolved once.

    url = "postgresql+psycopg://etl:${env:CRM_DB_PASSWORD}@db/crm"
    url = "${file:/run/secrets/crm_db_url}"     # Docker and Kubernetes secrets

Settings files hold placeholders, never the secrets themselves. `env` and
`file` come built in; register a provider for a secret manager (Vault, GCP
Secret Manager, Azure Key Vault...) with `register_provider`.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
import contextlib
import contextvars
import os
import pathlib
import re
import threading

# `${provider:reference}`; `$${` writes a literal `${`.
_PLACEHOLDER = re.compile(r"\$\$\{|\$\{([a-z][a-z0-9_]*):([^}]*)\}")

type SecretProvider = Callable[[str], str]


class SecretError(ValueError):
    """A placeholder that cannot be resolved."""


def _from_env(name: str) -> str:
    try:
        return os.environ[name]
    except KeyError:
        raise SecretError(f"environment variable {name} is not set") from None


def _from_file(path: str) -> str:
    try:
        return pathlib.Path(path).read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise SecretError(f"cannot read secret file {path}: {exc}") from None


_PROVIDERS: dict[str, SecretProvider] = {"env": _from_env, "file": _from_file}
_BUILT_IN = frozenset(_PROVIDERS)
_LOCK = threading.Lock()
_KEEP_PLACEHOLDERS = contextvars.ContextVar("unresolved", default=False)


@contextlib.contextmanager
def unresolved() -> Iterator[None]:
    """Leaves the placeholders of registered providers as they are, within.

    To show settings (`etl config show`) without reaching a secret manager
    (Vault, GCP, Azure): their placeholders are shown anyway. `env` and
    `file` are still read. A setting validated by the secret itself (a
    MongoDB URL held whole by a manager) may fail.
    """
    token = _KEEP_PLACEHOLDERS.set(True)
    try:
        yield
    finally:
        _KEEP_PLACEHOLDERS.reset(token)


def register_provider(name: str, provider: SecretProvider) -> None:
    """Adds a source of secrets, e.g. `vault` for `${vault:crm/db#password}`.

    Args:
        name: The provider of the placeholders.
        provider: Returns the secret of a reference; raises `SecretError`
            when there is none.
    """
    with _LOCK:
        _PROVIDERS[name] = provider


def resolve(text: str) -> str:
    """Replaces the placeholders of `text` with their secrets.

    Raises:
        SecretError: If a provider is unknown, or has no such secret.
    """

    def replace(match: re.Match[str]) -> str:
        if match.group(0) == "$${":
            return "${"
        name, reference = match.group(1), match.group(2)
        if _KEEP_PLACEHOLDERS.get() and name not in _BUILT_IN:
            return match.group(0)
        with _LOCK:
            provider = _PROVIDERS.get(name)
        if provider is None:
            raise SecretError(
                f"unknown secret provider {name!r}; known: {sorted(_PROVIDERS)}"
            )
        return provider(reference)

    return _PLACEHOLDER.sub(replace, text)


def resolve_all(data: object, path: str = "") -> object:
    """Resolves the placeholders of every text in nested settings data.

    Raises:
        SecretError: With the settings path of the first failing value.
    """
    if isinstance(data, str):
        try:
            return resolve(data)
        except SecretError as exc:
            raise SecretError(f"{path or '<root>'}: {exc}") from None
    if isinstance(data, Mapping):
        return {
            key: resolve_all(value, f"{path}.{key}" if path else str(key))
            for key, value in data.items()
        }
    if isinstance(data, list):
        return [
            resolve_all(value, f"{path}[{i}]") for i, value in enumerate(data)
        ]
    return data


def placeholders(
    data: object, path: tuple[str | int, ...] = ()
) -> dict[tuple[str | int, ...], str]:
    """Finds the texts of nested settings data holding a placeholder.

    Returns:
        Each text holding one, unresolved, by its path (keys and indexes):
        what may be shown instead of the secret it resolves to.
    """
    if isinstance(data, str):
        found = any(m.group(1) for m in _PLACEHOLDER.finditer(data))
        return {path: data} if found else {}
    texts: dict[tuple[str | int, ...], str] = {}
    if isinstance(data, Mapping):
        for key, value in data.items():
            texts.update(placeholders(value, (*path, str(key))))
    elif isinstance(data, list):
        for i, value in enumerate(data):
            texts.update(placeholders(value, (*path, i)))
    return texts
