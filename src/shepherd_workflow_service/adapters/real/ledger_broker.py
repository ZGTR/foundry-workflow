"""`PublisherBroker` -- the ledger dispatcher's downstream, in this service's terms.

`shepherd_workflow.dispatcher.OutboxDispatcher` publishes through a `Broker`,
whose call is `(key, event_class, payload)`. This service already owns a
`Publisher` port with three adapters, whose call is one `Event`. This adapter is
the translation between the two, and it exists so that the ledger drain reuses
`SqsPublisher` rather than growing a second, parallel way to reach SQS.

THE KEY IS CARRIED INTO THE PAYLOAD, deliberately. The dispatcher publishes
at-least-once and says so: a crash between the send and the mark re-sends the
row. That is only recoverable if the consumer can recognise a repeat, and the
stable key from `StageRunner._event_key` is what makes it possible. A translation
that dropped the key would turn a recoverable duplicate into an
indistinguishable one.

`org_id` is read from the payload rather than passed in, because the payload
`StageRunner` writes always carries it and a second source for the same fact is
how two answers to one question get created. A payload without it raises
`KeyError`, which `OutboxDispatcher` counts as a failed row and leaves PENDING
for the next drain -- the right failure, since a row nobody can attribute to a
tenant must not be published as if it belonged to one.
"""

from __future__ import annotations

from shepherd_contracts.clock import Clock

from shepherd_workflow_service.ports.event_bus import Event, Publisher


class PublisherBroker:
    """Implements the package's `Broker` by delegating to a `Publisher`."""

    def __init__(self, publisher: Publisher, *, clock: Clock) -> None:
        self._publisher = publisher
        self._clock = clock

    def publish(self, *, key: str, event_class: str, payload: dict) -> None:
        self._publisher.publish(
            Event(
                event_type=event_class,
                org_id=str(payload["org_id"]),
                # The DELIVERY time, not the decision time. The row already
                # carries `created_at`, and overwriting that with "now" would
                # erase how long a row waited -- the one number that says
                # whether the drain is keeping up.
                occurred_at=self._clock.now(),
                payload={**payload, "key": key},
            )
        )
