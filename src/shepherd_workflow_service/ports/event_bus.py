"""The Pub/Sub port: one `Event`, one `Publisher`, one `Subscriber`.

Three adapters implement `Publisher` (`adapters/real/in_process_event_bus.py`,
`adapters/real/sqs_event_bus.py`, `adapters/real/outbox_event_bus.py`); only
the in-process one also implements `Subscriber`, because SQS and outbox
delivery happen out of process -- a consumer subscribes to them by running a
separate handler, not by registering an in-memory callback.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class Event:
    """An immutable fact this service published."""

    event_type: str
    org_id: str
    occurred_at: datetime
    payload: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        if not self.event_type:
            raise ValueError("Event.event_type must be non-empty")
        if self.occurred_at.tzinfo is None:
            raise ValueError("Event.occurred_at must be timezone-aware")
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))


EventHandler = Callable[[Event], None]


@runtime_checkable
class Publisher(Protocol):
    def publish(self, event: Event) -> None:
        """Publish `event`. What "published" means is the adapter's choice."""
        ...


@runtime_checkable
class Subscriber(Protocol):
    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        """Register `handler` to be called for every future `event_type`."""
        ...
