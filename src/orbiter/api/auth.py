from __future__ import annotations

import hmac
from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class AuthConfig:
    api_key: str | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)


def extract_api_key(headers: Mapping[str, str]) -> str | None:
    direct = headers.get("x-orbiter-key")
    if direct:
        return direct
    auth = headers.get("authorization")
    if not auth:
        return None
    prefix = "Bearer "
    if auth.startswith(prefix):
        return auth[len(prefix):]
    return None


def is_public_path(path: str) -> bool:
    return path in {"/", "/healthz", "/auth/config"} or path.startswith("/ui")


def is_authorised(config: AuthConfig, headers: Mapping[str, str], path: str) -> bool:
    if not config.enabled:
        return True
    if is_public_path(path):
        return True
    candidate = extract_api_key(headers)
    if candidate is None or config.api_key is None:
        return False
    return hmac.compare_digest(candidate, config.api_key)
