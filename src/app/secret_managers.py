"""Secret managers as placeholders of the settings: Vault, GCP, Azure.

    url = "postgresql://etl:${vault:secret/crm/db#password}@db/crm"
    url = "${gcp:projects/acme/secrets/crm-db-url/versions/latest}"
    url = "${azure:acme-kv/crm-db-url}"

Each manager is reached with its own standard credentials, never with the
settings (which it resolves):

- `vault`: KV version 2, `<mount>/<path>#<field>`; `VAULT_ADDR`, and
  `VAULT_TOKEN` (or a token file, `VAULT_TOKEN_FILE`), `VAULT_NAMESPACE`,
  `VAULT_CACERT` (a PEM file) or `VAULT_CAPATH` (a directory of them) for
  a private CA. `VAULT_SKIP_VERIFY` is not supported: certificates are
  always verified. Redirects (a standby node) are not followed: point
  `VAULT_ADDR` at the active node or the load balancer.
- `gcp`: Secret Manager, the full version name, or `<secret>` (its latest
  version in `$GOOGLE_CLOUD_PROJECT`); Application Default Credentials.
- `azure`: Key Vault, `<vault>/<secret>[/<version>]`;
  `DefaultAzureCredential` (managed identity, environment, CLI). The vault
  is `<vault>.vault.azure.net`; in another cloud, set
  `AZURE_KEYVAULT_DNS_SUFFIX` (`vault.azure.cn`, `vault.usgovcloudapi.net`)
  and `AZURE_AUTHORITY_HOST`, or give the vault's host name as `<vault>`.

A secret is read once per process, whatever the number of placeholders.
"""

from __future__ import annotations

from collections.abc import Callable
import functools
import os
import pathlib
import ssl
import threading

from azure import identity as azure_identity
from azure.core import exceptions as azure_errors
from azure.keyvault import secrets as azure_secrets
from google.api_core import exceptions as google_errors
from google.auth import exceptions as auth_errors
from google.cloud import secretmanager
import httpx

from app import clients
import config

_INSTALLED = threading.Event()


def install() -> None:
    """Registers the managers' providers; idempotent."""
    if _INSTALLED.is_set():
        return
    config.register_provider("vault", _cached(vault))
    config.register_provider("gcp", _cached(gcp))
    config.register_provider("azure", _cached(azure))
    _INSTALLED.set()


def _cached(read: Callable[[str], str]) -> Callable[[str], str]:
    """Reads each reference once, per process."""
    return functools.cache(read)


def vault(reference: str, http: httpx.Client | None = None) -> str:
    """Reads `<mount>/<path>#<field>` from Vault's KV version 2 engine.

    Args:
        reference: The secret, e.g. `secret/crm/db#password`.
        http: The client (tests); a new one by default.

    Raises:
        SecretError: If Vault is not configured, or has no such secret.
    """
    path, _, field = reference.partition("#")
    mount, _, name = path.partition("/")
    if not field or not name:
        raise config.SecretError(
            f"vault:{reference}: expected <mount>/<path>#<field>"
        )
    address = os.environ.get("VAULT_ADDR")
    if not address:
        raise config.SecretError("vault: VAULT_ADDR is not set")
    headers = {"X-Vault-Token": _vault_token()}
    if namespace := os.environ.get("VAULT_NAMESPACE"):
        headers["X-Vault-Namespace"] = namespace
    url = f"{address.rstrip('/')}/v1/{mount}/data/{name}"
    try:
        if http is not None:
            response = http.get(url, headers=headers)
        else:
            with httpx.Client(
                timeout=10, verify=_vault_tls(), follow_redirects=False
            ) as client:
                response = client.get(url, headers=headers)
    except httpx.HTTPError as exc:
        raise config.SecretError(f"vault:{path}: {exc}") from None
    if response.status_code != 200:
        raise config.SecretError(
            f"vault:{path}: HTTP {response.status_code}"
            + (" (no such secret)" if response.status_code == 404 else "")
        )
    try:
        body = response.json()
    except ValueError:
        body = None
    if not isinstance(body, dict):
        raise config.SecretError(
            f"vault:{path}: the answer is not Vault's JSON"
            f" ({response.headers.get('content-type', 'no content type')}):"
            " is VAULT_ADDR the address of Vault?"
        )
    data = body.get("data")
    data = data.get("data") if isinstance(data, dict) else None
    if not isinstance(data, dict) or field not in data:
        raise config.SecretError(f"vault:{path} has no field {field!r}")
    return str(data[field])


def _vault_tls() -> ssl.SSLContext:
    """The TLS context of Vault: the system's CAs, or `VAULT_CACERT`'s."""
    cafile = os.environ.get("VAULT_CACERT") or None
    capath = os.environ.get("VAULT_CAPATH") or None
    try:
        return ssl.create_default_context(cafile=cafile, capath=capath)
    except OSError as exc:  # ssl.SSLError included
        raise config.SecretError(
            f"vault: cannot load VAULT_CACERT/VAULT_CAPATH: {exc}"
        ) from None


def _vault_token() -> str:
    token = os.environ.get("VAULT_TOKEN")
    if token:
        return token
    path = os.environ.get("VAULT_TOKEN_FILE")
    if path:
        try:
            return pathlib.Path(path).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise config.SecretError(f"vault: {exc}") from None
    raise config.SecretError(
        "vault: VAULT_TOKEN or VAULT_TOKEN_FILE is not set"
    )


def gcp(
    reference: str, client: clients.SecretManagerClient | None = None
) -> str:
    """Reads a secret version of Google Cloud Secret Manager.

    Args:
        reference: `projects/<p>/secrets/<s>/versions/<v>`, or `<s>` for
            the latest version in `$GOOGLE_CLOUD_PROJECT`.
        client: A `SecretManagerServiceClient`-like object (tests).

    Raises:
        SecretError: If the secret cannot be read.
    """
    name = reference
    if "/" not in reference:
        project = os.environ.get("GOOGLE_CLOUD_PROJECT")
        if not project:
            raise config.SecretError(
                f"gcp:{reference}: give the full name, or set"
                " GOOGLE_CLOUD_PROJECT"
            )
        name = f"projects/{project}/secrets/{reference}/versions/latest"
    try:
        client = client or secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(request={"name": name})
    except (google_errors.GoogleAPIError, auth_errors.GoogleAuthError) as exc:
        raise config.SecretError(f"gcp:{name}: {exc}") from None
    return str(response.payload.data.decode("utf-8"))


def azure(reference: str, client: clients.KeyVaultClient | None = None) -> str:
    """Reads a secret of Azure Key Vault.

    Args:
        reference: `<vault>/<secret>`, or `<vault>/<secret>/<version>`.
        client: A `SecretClient`-like object (tests), for that vault.

    Raises:
        SecretError: If the secret cannot be read.
    """
    parts = reference.split("/")
    if len(parts) not in (2, 3) or not all(parts):
        raise config.SecretError(
            f"azure:{reference}: expected <vault>/<secret>[/<version>]"
        )
    vault_name, secret = parts[0], parts[1]
    version = parts[2] if len(parts) == 3 else None
    try:
        client = client or azure_secrets.SecretClient(
            _azure_vault_url(vault_name),
            azure_identity.DefaultAzureCredential(),
        )
        found = client.get_secret(secret, version)
    except azure_errors.AzureError as exc:
        raise config.SecretError(f"azure:{reference}: {exc}") from None
    if found.value is None:
        raise config.SecretError(f"azure:{reference} has no value")
    return str(found.value)


def _azure_vault_url(vault_name: str) -> str:
    """The URL of a vault: its name in `AZURE_KEYVAULT_DNS_SUFFIX`, or its
    host name as given."""
    if "." in vault_name:
        return f"https://{vault_name}"
    suffix = os.environ.get("AZURE_KEYVAULT_DNS_SUFFIX") or "vault.azure.net"
    return f"https://{vault_name}.{suffix.strip('.')}"
