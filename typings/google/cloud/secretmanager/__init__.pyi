# The part of google-cloud-secret-manager the application uses. Its own
# types live beside google.cloud's namespace, which the storage stub covers.
from typing import Any

class SecretManagerServiceClient:
    def __init__(self) -> None: ...
    def access_secret_version(
        self, request: dict[str, str] | None = None
    ) -> Any: ...
