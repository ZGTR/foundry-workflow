"""The fake for `ports.event_bus.Publisher`."""

from __future__ import annotations

from shepherd_workflow_service.ports.event_bus import Event


class RecordingPublisher:
    """Captures every published event in order. Never raises, never delivers."""

    def __init__(self) -> None:
        self.published: list[Event] = []

    def publish(self, event: Event) -> None:
        self.published.append(event)
