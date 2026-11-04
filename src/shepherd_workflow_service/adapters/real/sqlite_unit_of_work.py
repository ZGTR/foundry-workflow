"""A real `UnitOfWork` backed by SQLite.

SQLite rather than Postgres/MySQL on purpose: the template ships no database
by default (plan Q5) and a service that needs a real relational store swaps
this for its own `UnitOfWork` without touching `services/` or `ports/`. What
this adapter demonstrates -- table ownership, lazy connection, explicit
commit/rollback -- carries over unchanged.

The connection opens lazily, on first `session()` call, never in
`__init__`. `tests/unit/test_container.py` asserts the container graph builds
with zero I/O; a constructor that touched the filesystem would break that.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


class SqliteUnitOfWork:
    def __init__(self, db_path: str, *, owned_tables: frozenset[str]) -> None:
        self._db_path = db_path
        self._owned_tables = owned_tables
        self._connection: sqlite3.Connection | None = None

    def session(self) -> sqlite3.Connection:
        if self._connection is None:
            if self._db_path != ":memory:":
                Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
            self._connection = sqlite3.connect(self._db_path)
            self._connection.execute(
                "CREATE TABLE IF NOT EXISTS outbox ("
                "outbox_id TEXT PRIMARY KEY, event_type TEXT NOT NULL, org_id TEXT NOT NULL, "
                "occurred_at TEXT NOT NULL, payload_json TEXT NOT NULL, "
                "relayed_at TEXT)"
            )
        return self._connection

    def owned_tables(self) -> frozenset[str]:
        return self._owned_tables

    def commit(self) -> None:
        if self._connection is not None:
            self._connection.commit()

    def rollback(self) -> None:
        if self._connection is not None:
            self._connection.rollback()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
