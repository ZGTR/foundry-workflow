"""The fake for `shepherd_contracts.unit_of_work.UnitOfWork`.

Backs `OutboxEventBus` in tests: `commit()`/`rollback()` are not part of the
shared `UnitOfWork` Protocol (a blackbox never commits its caller's
transaction), but this fake exposes them so a test can drive both outcomes
and assert what the outbox publisher did in each.
"""

from __future__ import annotations


class InMemoryUnitOfWork:
    """A transaction double: a dict "session" plus explicit commit/rollback."""

    def __init__(self, *, owned_tables: frozenset[str]) -> None:
        self._owned_tables = owned_tables
        self._session: dict[str, list[dict[str, object]]] = {t: [] for t in owned_tables}
        self.committed = False
        self.rolled_back = False

    def session(self) -> dict[str, list[dict[str, object]]]:
        return self._session

    def owned_tables(self) -> frozenset[str]:
        return self._owned_tables

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True
        for rows in self._session.values():
            rows.clear()
