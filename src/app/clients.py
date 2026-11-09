"""What the composition root uses of the secret managers' SDKs: Google's
Secret Manager and Azure's Key Vault, faked in the tests."""

from __future__ import annotations

from typing import Protocol

__all__ = [
    "KeyVaultClient",
    "KeyVaultSecretLike",
    "SecretManagerClient",
    "SecretPayloadLike",
    "SecretVersionLike",
]


class SecretPayloadLike(Protocol):
    """What is used of a Secret Manager `SecretPayload`."""

    @property
    def data(self) -> bytes:
        ...


class SecretVersionLike(Protocol):
    """What is used of an `AccessSecretVersionResponse`."""

    @property
    def payload(self) -> SecretPayloadLike:
        ...


class SecretManagerClient(Protocol):
    """What is used of a `SecretManagerServiceClient`."""

    def access_secret_version(
        self, request: dict[str, str]
    ) -> SecretVersionLike:
        ...


class KeyVaultSecretLike(Protocol):
    """What is used of a Key Vault `KeyVaultSecret`."""

    @property
    def value(self) -> str | None:
        ...


class KeyVaultClient(Protocol):
    """What is used of a `SecretClient`."""

    def get_secret(self, name: str, version: str | None) -> KeyVaultSecretLike:
        ...
