from __future__ import annotations

from typing import Any


def build_app(*args: Any, **kwargs: Any):
    from orbiter.api.server import build_app as _build_app

    return _build_app(*args, **kwargs)


__all__ = ["build_app"]
