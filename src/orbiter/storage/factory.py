from __future__ import annotations

from pathlib import Path

from orbiter.storage.postgres_store import PostgresStateStore
from orbiter.storage.sqlite_store import SQLiteStateStore


def create_state_store(target: str | Path = ":memory:") -> SQLiteStateStore | PostgresStateStore:
    value = str(target)
    if value.startswith("postgres://") or value.startswith("postgresql://"):
        return PostgresStateStore(value)
    return SQLiteStateStore(value)
