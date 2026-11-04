"""What the composition root binds the ledger drain to -- and what it REFUSES.

The refusals carry the weight in this file. A drain that publishes to nowhere
and then marks every row sent is worse than no drain at all: the undrained table
at least still holds the events. So the two ways to get one are both tested as
errors, not as configuration.
"""

from __future__ import annotations

import socket
from datetime import UTC, datetime

import pytest
from shepherd_contracts.clock import FrozenClock
from shepherd_contracts.errors import ConfigurationError

from shepherd_workflow_service.adapters.fake.event_bus import RecordingPublisher
from shepherd_workflow_service.adapters.fake.ledger_outbox import InMemoryLedgerOutbox
from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import build_container
from shepherd_workflow_service.ports.ledger_outbox import LedgerOutboxDrain
from tests.conftest import WORKFLOW_DEFINITION_PATH

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)


def _config(tmp_path, **overrides) -> ServiceConfig:
    base = dict(
        env="local",
        port=8080,
        runtime_mode="http",
        issuer="test",
        db_path=str(tmp_path / "workflow.db"),
        event_bus_mode="in_process",
        sqs_queue_url=None,
        aws_region="eu-west-1",
        workflow_definition_path=WORKFLOW_DEFINITION_PATH,
    )
    base.update(overrides)
    return ServiceConfig(**base)


def test_the_fake_satisfies_the_port() -> None:
    assert isinstance(InMemoryLedgerOutbox(), LedgerOutboxDrain)


def test_a_drain_with_no_durable_downstream_refuses_instead_of_publishing_to_nowhere(
    tmp_path,
) -> None:
    """The failure this rules out: `InProcessEventBus` has no subscribers in any
    deployed process, so a drain wired to it would empty the outbox into a list
    nobody reads and report a clean run."""
    outbox = InMemoryLedgerOutbox(clock=FrozenClock(NOW))
    outbox.enqueue(key="evt_1", event_class="stage.leased", payload={"org_id": "org_1"})
    container = build_container(_config(tmp_path), ledger_outbox=outbox)

    with pytest.raises(ConfigurationError, match="SQS_QUEUE_URL"):
        container.drain_ledger_outbox()

    # The row is STILL PENDING. A refusal that had already marked rows sent
    # would be the exact data loss this refusal exists to prevent.
    assert [row["key"] for row in outbox.pending()] == ["evt_1"]
    assert outbox.sent == []


def test_an_engine_that_cannot_drain_is_named_rather_than_silently_doing_nothing(
    tmp_path,
) -> None:
    class _EngineWithoutADrain:
        pass

    container = build_container(
        _config(tmp_path), ledger_outbox=_EngineWithoutADrain()  # type: ignore[arg-type]
    )

    with pytest.raises(ConfigurationError, match="_EngineWithoutADrain"):
        container.drain_ledger_outbox()


def test_an_injected_publisher_is_used_and_the_rows_leave(tmp_path) -> None:
    """The positive control for both refusals above: with a real downstream the
    same call publishes and marks, so the refusals are about the downstream and
    not about the drain being unreachable."""
    outbox = InMemoryLedgerOutbox(clock=FrozenClock(NOW))
    outbox.enqueue(key="evt_1", event_class="stage.leased", payload={"org_id": "org_1"})
    publisher = RecordingPublisher()
    container = build_container(_config(tmp_path), publisher=publisher, ledger_outbox=outbox)

    report = container.drain_ledger_outbox()

    assert (report.published, report.ambiguous, report.failed) == (1, 0, 0)
    assert [event.payload["key"] for event in publisher.published] == ["evt_1"]
    assert outbox.pending() == []


def test_building_the_container_never_builds_the_broker(tmp_path, monkeypatch) -> None:
    """The broker factory must stay unevaluated at build time: a boto3 client
    resolves credentials and a region, and the container graph is asserted to
    build with the network unavailable."""

    def _no_network(*args, **kwargs):
        raise AssertionError("socket.socket() called during build_container()")

    monkeypatch.setattr(socket, "socket", _no_network)

    container = build_container(_config(tmp_path, sqs_queue_url="https://sqs.example/q"))

    assert callable(container.ledger_broker_factory)
