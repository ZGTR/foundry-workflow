"""Proves the outbox transactional guarantee: a rolled-back unit of work
publishes nothing. `publish()` only writes the row inside the caller's
transaction; `drain_outbox()` relays whatever survived it.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from shepherd_contracts.unit_of_work import TableOwnershipViolation

from shepherd_workflow_service.adapters.fake.event_bus import RecordingPublisher
from shepherd_workflow_service.adapters.fake.unit_of_work import InMemoryUnitOfWork
from shepherd_workflow_service.adapters.real.outbox_event_bus import OutboxEventBus, drain_outbox
from shepherd_workflow_service.ports.event_bus import Event

EVENT = Event(
    event_type="stage_run.advanced", org_id="org_1", occurred_at=datetime(2026, 1, 1, tzinfo=UTC)
)


def test_committed_write_is_relayed() -> None:
    uow = InMemoryUnitOfWork(owned_tables=frozenset({"outbox"}))
    outbox = OutboxEventBus(uow)
    downstream = RecordingPublisher()

    outbox.publish(EVENT)
    uow.commit()
    relayed = drain_outbox(uow, downstream)

    assert relayed == 1
    assert len(downstream.published) == 1
    assert downstream.published[0].event_type == "stage_run.advanced"


def test_rolled_back_write_publishes_nothing() -> None:
    uow = InMemoryUnitOfWork(owned_tables=frozenset({"outbox"}))
    outbox = OutboxEventBus(uow)
    downstream = RecordingPublisher()

    outbox.publish(EVENT)
    uow.rollback()
    relayed = drain_outbox(uow, downstream)

    assert relayed == 0
    assert downstream.published == []


def test_draining_twice_relays_each_row_once() -> None:
    uow = InMemoryUnitOfWork(owned_tables=frozenset({"outbox"}))
    outbox = OutboxEventBus(uow)
    downstream = RecordingPublisher()

    outbox.publish(EVENT)
    uow.commit()

    first_pass = drain_outbox(uow, downstream)
    second_pass = drain_outbox(uow, downstream)

    assert first_pass == 1
    assert second_pass == 0
    assert len(downstream.published) == 1


def test_publish_only_writes_the_outbox_table() -> None:
    uow = InMemoryUnitOfWork(owned_tables=frozenset({"stage_runs"}))  # outbox NOT granted

    with pytest.raises(TableOwnershipViolation):
        OutboxEventBus(uow)
