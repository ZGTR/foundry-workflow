"""The fake for `ports.ledger_outbox.LedgerOutboxDrain`.

The rows live in a list instead of a table; everything else is the real thing.
In particular this drives the package's own `OutboxDispatcher` rather than
looping over the rows itself, so the publish-then-mark order -- the one
property the port exists to protect -- is the SAME code here as in the SQL
adapter. A fake that marked rows sent before publishing them would pass every
test written against it while describing a mechanism this service does not have.

`sent` and `ambiguous` are exposed so a test can assert what a drain did without
reaching into privates, and `pending()` is the same projection the SQL adapter
builds with a WHERE clause.
"""

from __future__ import annotations

from shepherd_contracts.clock import Clock, SystemClock
from shepherd_workflow.dispatcher import Broker, DispatchReport, OutboxDispatcher


class InMemoryLedgerOutbox:
    """An in-memory `LedgerOutboxDrain`, de-duplicating by key as the PK does."""

    def __init__(self, *, clock: Clock | None = None) -> None:
        self._clock = clock or SystemClock()
        self.rows: list[dict] = []
        self.sent: list[str] = []
        self.ambiguous: list[str] = []
        self.duplicates_suppressed = 0

    def enqueue(self, *, key: str, event_class: str, payload: dict) -> None:
        if any(row["key"] == key for row in self.rows):
            self.duplicates_suppressed += 1
            return
        self.rows.append({"key": key, "event_class": event_class, "payload": payload})

    def pending(self, limit: int = 100) -> list[dict]:
        delivered = set(self.sent) | set(self.ambiguous)
        return [row for row in self.rows if row["key"] not in delivered][:limit]

    def drain_pending(self, *, broker: Broker, batch_size: int = 100) -> DispatchReport:
        dispatcher = OutboxDispatcher(
            broker=broker,
            clock=self._clock,
            mark_sent=self.sent.append,
            mark_ambiguous=self.ambiguous.append,
        )
        return dispatcher.dispatch(self.pending(batch_size))
