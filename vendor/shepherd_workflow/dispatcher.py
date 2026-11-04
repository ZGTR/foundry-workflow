"""Moving outbox rows to a broker, at-least-once and never at-most-once.

The order of operations is the entire content of this file, and it is chosen
against the failure that actually happens rather than the one that reads worst.

    publish, THEN mark sent      -> a crash between them re-publishes. Duplicate.
    mark sent, THEN publish      -> a crash between them loses the event. Silent.

Duplicates are recoverable because every event carries a stable key; a lost
event is not recoverable at all, because nothing records that it should have
existed. So this publishes first and accepts that a consumer must be idempotent
-- which the stable key from `StageRunner._event_key` is there to make possible.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from shepherd_contracts import Clock, PossibleSend


@runtime_checkable
class Broker(Protocol):
    def publish(self, *, key: str, event_class: str, payload: dict) -> None:
        ...


@dataclass(frozen=True, slots=True)
class DispatchReport:
    published: int
    ambiguous: int
    failed: int

    @property
    def total(self) -> int:
        return self.published + self.ambiguous + self.failed


class OutboxDispatcher:
    """Drains rows to a broker and reports what it could not be sure about."""

    def __init__(
        self,
        *,
        broker: Broker,
        clock: Clock,
        mark_sent: Callable[[str], None],
        mark_ambiguous: Callable[[str], None] | None = None,
    ) -> None:
        self._broker = broker
        self._clock = clock
        self._mark_sent = mark_sent
        self._mark_ambiguous = mark_ambiguous or mark_sent

    def dispatch(self, rows: list[dict]) -> DispatchReport:
        published = ambiguous = failed = 0
        for row in rows:
            try:
                self._broker.publish(
                    key=row["key"], event_class=row["event_class"], payload=row["payload"]
                )
            except PossibleSend:
                # The send may have landed. Marking it sent risks losing it;
                # leaving it pending risks a duplicate. We choose the duplicate,
                # and record the ambiguity so an operator can see it happened.
                ambiguous += 1
                self._mark_ambiguous(row["key"])
            except Exception:
                # Left pending deliberately: the next drain retries it.
                failed += 1
            else:
                published += 1
                self._mark_sent(row["key"])
        return DispatchReport(published=published, ambiguous=ambiguous, failed=failed)
