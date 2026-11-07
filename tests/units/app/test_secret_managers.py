"""Secret managers' placeholders: Vault, GCP Secret Manager, Azure Key Vault."""

from __future__ import annotations

import dataclasses
import os
import pathlib
import re
import ssl
from typing import Any

from azure.core import exceptions as azure_errors
import certifi
from google.api_core import exceptions as google_errors
import httpx
import pytest

from app import bootstrap
from app import secret_managers
import config


def _vault(responses: dict[str, httpx.Response]) -> httpx.Client:
    def handle(request: httpx.Request) -> httpx.Response:
        assert request.headers["X-Vault-Token"] == "t0ken"
        return responses.get(request.url.path, httpx.Response(404))

    return httpx.Client(transport=httpx.MockTransport(handle))


class TestVault:

    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VAULT_ADDR", "http://vault:8200/")
        monkeypatch.setenv("VAULT_TOKEN", "t0ken")

    def test_a_field_of_a_kv2_secret(self) -> None:
        http = _vault({
            "/v1/secret/data/crm/db": httpx.Response(
                200, json={"data": {"data": {"password": "s3cret"}}}
            )
        })

        assert secret_managers.vault("secret/crm/db#password", http) == "s3cret"

    @pytest.mark.parametrize(
        ("reference", "message"),
        [
            ("secret/crm/db", "expected <mount>/<path>#<field>"),
            ("secret/nope#password", "HTTP 404 (no such secret)"),
            ("secret/crm/db#user", "has no field 'user'"),
        ],
    )
    def test_what_cannot_be_read(self, reference: str, message: str) -> None:
        http = _vault({
            "/v1/secret/data/crm/db": httpx.Response(
                200, json={"data": {"data": {"password": "s3cret"}}}
            )
        })

        with pytest.raises(config.SecretError, match=re.escape(message)):
            secret_managers.vault(reference, http)

    def test_a_token_file(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        monkeypatch.delenv("VAULT_TOKEN")
        token = tmp_path / "token"
        token.write_text("t0ken\n", encoding="utf-8")
        monkeypatch.setenv("VAULT_TOKEN_FILE", str(token))
        http = _vault({
            "/v1/kv/data/app": httpx.Response(
                200, json={"data": {"data": {"key": "v"}}}
            ),
        })

        assert secret_managers.vault("kv/app#key", http) == "v"

    def test_unconfigured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("VAULT_ADDR")

        with pytest.raises(config.SecretError, match="VAULT_ADDR"):
            secret_managers.vault("secret/crm/db#password")

    @pytest.mark.parametrize(
        "answer",
        [
            httpx.Response(200, text="<html>login</html>"),
            httpx.Response(200, json=["not", "a", "secret"]),
            httpx.Response(200, json={"data": "nope"}),
        ],
    )
    def test_an_answer_that_is_not_vaults(self, answer: httpx.Response) -> None:
        http = _vault({"/v1/secret/data/crm/db": answer})

        with pytest.raises(config.SecretError, match="vault:secret/crm/db"):
            secret_managers.vault("secret/crm/db#password", http)

    def test_a_private_ca_is_trusted_and_redirects_not_followed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clients: list[dict[str, Any]] = []

        class Client(httpx.Client):

            def __init__(self, **options: Any) -> None:
                clients.append(options)
                super().__init__(
                    transport=httpx.MockTransport(
                        lambda _: httpx.Response(
                            200, json={"data": {"data": {"password": "p"}}}
                        )
                    )
                )

        monkeypatch.setattr(httpx, "Client", Client)
        monkeypatch.setenv("VAULT_CACERT", certifi.where())

        assert secret_managers.vault("secret/crm/db#password") == "p"
        (options,) = clients
        assert isinstance(options["verify"], ssl.SSLContext)
        assert options["follow_redirects"] is False

    def test_a_missing_ca_file(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        monkeypatch.setenv("VAULT_CACERT", str(tmp_path / "nope.pem"))

        with pytest.raises(config.SecretError, match="VAULT_CACERT"):
            secret_managers.vault("secret/crm/db#password")


@dataclasses.dataclass
class _Gcp:
    secrets: dict[str, str]

    def access_secret_version(self, request: dict[str, str]) -> Any:
        name = request["name"]
        if name not in self.secrets:
            raise google_errors.NotFound(name)  # type: ignore[no-untyped-call]
        payload = dataclasses.make_dataclass("P", ["data"])(
            self.secrets[name].encode()
        )
        return dataclasses.make_dataclass("R", ["payload"])(payload)


def test_gcp_secret_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    name = "projects/acme/secrets/crm-db/versions/latest"
    client = _Gcp({name: "postgresql://x"})
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "acme")

    assert secret_managers.gcp(name, client) == "postgresql://x"
    assert secret_managers.gcp("crm-db", client) == "postgresql://x"
    with pytest.raises(config.SecretError, match="gcp:projects/acme"):
        secret_managers.gcp("nope", client)
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT")
    with pytest.raises(config.SecretError, match="GOOGLE_CLOUD_PROJECT"):
        secret_managers.gcp("crm-db", client)


@dataclasses.dataclass
class _Azure:
    secrets: dict[tuple[str, str | None], str]

    def get_secret(self, name: str, version: str | None) -> Any:
        if (name, version) not in self.secrets:
            raise azure_errors.ResourceNotFoundError(name)
        return dataclasses.make_dataclass("S", ["value"])(
            self.secrets[(name, version)]
        )


def test_azure_key_vault() -> None:
    client = _Azure({("crm-db", None): "latest", ("crm-db", "v1"): "first"})

    assert secret_managers.azure("acme-kv/crm-db", client) == "latest"
    assert secret_managers.azure("acme-kv/crm-db/v1", client) == "first"
    with pytest.raises(config.SecretError, match="azure:acme-kv/nope"):
        secret_managers.azure("acme-kv/nope", client)
    with pytest.raises(config.SecretError, match="<vault>/<secret>"):
        secret_managers.azure("crm-db", client)


@pytest.mark.parametrize(
    ("vault", "suffix", "url"),
    [
        ("acme-kv", None, "https://acme-kv.vault.azure.net"),
        ("acme-kv", "vault.azure.cn", "https://acme-kv.vault.azure.cn"),
        (
            "acme-kv",
            ".vault.usgovcloudapi.net",
            "https://acme-kv.vault.usgovcloudapi.net",
        ),
        ("acme-kv.vault.azure.cn", None, "https://acme-kv.vault.azure.cn"),
    ],
)
def test_azure_clouds(
    monkeypatch: pytest.MonkeyPatch, vault: str, suffix: str | None, url: str
) -> None:
    if suffix is None:
        monkeypatch.delenv("AZURE_KEYVAULT_DNS_SUFFIX", raising=False)
    else:
        monkeypatch.setenv("AZURE_KEYVAULT_DNS_SUFFIX", suffix)

    assert secret_managers._azure_vault_url(vault) == url  # pylint: disable=protected-access


def test_settings_resolve_the_managers(tmp_path: pathlib.Path) -> None:
    """Providers are installed before the settings load, and read once."""
    reads: list[str] = []

    def vault(reference: str) -> str:
        reads.append(reference)
        return "s3cret"

    settings_url = "postgresql://etl:${vault:secret/crm#pw}@db/crm"
    bootstrap.load_settings(
        "test", directory=tmp_path / "none", overrides={}
    )  # Installs the managers.
    config.register_provider("vault", secret_managers._cached(vault))  # pylint: disable=protected-access
    overrides = {"databases": {"crm": {"url": settings_url}}}
    try:
        settings = bootstrap.load_settings(
            "test", directory=tmp_path / "none", overrides=overrides
        )
        bootstrap.load_settings(  # Read again: from the cache.
            "test", directory=tmp_path / "none", overrides=overrides
        )
    finally:
        config.register_provider(
            "vault",
            secret_managers._cached(secret_managers.vault),  # pylint: disable=protected-access
        )

    url = settings.databases["crm"].url.get_secret_value()
    assert url == "postgresql://etl:s3cret@db/crm"
    assert reads == ["secret/crm#pw"]
    assert "s3cret" not in str(settings.redacted())


def test_a_real_vault(monkeypatch: pytest.MonkeyPatch) -> None:
    """With `ETL_TEST_VAULT_URL` (a dev Vault, root token `root`)."""
    url = os.environ.get("ETL_TEST_VAULT_URL")
    if not url:
        pytest.skip("set ETL_TEST_VAULT_URL to run the Vault test")
    headers = {"X-Vault-Token": "root"}
    httpx.post(
        f"{url}/v1/secret/data/etl-test/db",
        headers=headers,
        json={"data": {"password": "from-vault"}},
    ).raise_for_status()
    monkeypatch.setenv("VAULT_ADDR", url)
    monkeypatch.setenv("VAULT_TOKEN", "root")

    assert secret_managers.vault("secret/etl-test/db#password") == "from-vault"
