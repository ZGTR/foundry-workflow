"""`PublisherBroker`: the translation between the package's `Broker` and this
service's `Publisher`.

The three properties here are the ones a wrong translation would quietly lose:
the stable key must survive into the payload (it is what lets a consumer
recognise an at-least-once repeat), the tenant must come from the row rather
than from anywhere else, and a row that cannot name its tenant must FAIL rather
than be published as if it belonged to one.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from shepherd_contracts.clock import FrozenClock
from shepherd_workflow.dispatcher import Broker, OutboxDispatcher

from shepherd_workflow_service.adapters.fake.event_bus import RecordingPublisher
from shepherd_workflow_service.adapters.real.ledger_broker import PublisherBroker

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)


def _broker(publisher: RecordingPublisher) -> PublisherBroker:
    return PublisherBroker(publisher, clock=FrozenClock(NOW))


def test_it_satisfies_the_packages_broker_protocol() -> None:
    assert isinstance(_broker(RecordingPublisher()), Broker)


def test_the_stable_key_survives_into_the_published_payload() -> None:
    """Without this an at-least-once repeat is indistinguishable from a new event."""
    publisher = RecordingPublisher()

    _broker(publisher).publish(
        key="evt_abc", event_class="stage.succeeded", payload={"org_id": "org_1", "job_id": "j"}
    )

    (event,) = publisher.published
    assert event.event_type == "stage.succeeded"
    assert event.org_id == "org_1"
    assert event.payload["key"] == "evt_abc"
    assert event.payload["job_id"] == "j"
    assert event.occurred_at == NOW


def test_a_row_that_cannot_name_its_tenant_is_left_pending_rather_than_published() -> None:
    """The dispatcher counts it failed, so the next drain retries it. What must
    NOT happen is a publish under some default org."""
    publisher = RecordingPublisher()
    sent: list[str] = []
    report = OutboxDispatcher(
        broker=_broker(publisher),
        clock=FrozenClock(NOW),
        mark_sent=sent.append,
    ).dispatch([{"key": "evt_1", "event_class": "stage.failed", "payload": {}}])

    assert report.failed == 1
    assert report.published == 0
    assert publisher.published == []
    assert sent == []


def test_an_empty_event_class_is_refused_by_the_event_itself() -> None:
    """A positive control for the two arms above: the translation is not simply
    swallowing everything it is handed."""
    with pytest.raises(ValueError, match="event_type"):
        _broker(RecordingPublisher()).publish(key="k", event_class="", payload={"org_id": "o"})
