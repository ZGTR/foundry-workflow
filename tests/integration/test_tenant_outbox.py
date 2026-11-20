"""Exercise owner-scoped relay through the ledger and independent delivery rows."""

from dataclasses import dataclass, field

import pytest
from shepherd_contracts.clock import FrozenClock
from shepherd_contracts.errors import ConfigurationError
from sqlalchemy import Column, String, Table, select

from shepherd_workflow_service.adapters.real.tenant_outbox import TenantOutboxDelivery
from tests.integration.test_ledger_outbox_drain import NOW, _engine, _lease_one_stage


class Broker:
    def __init__(self):
        self.sent = []

    def publish(self, *, key, event_class, payload):
        self.sent.append((key, event_class, payload))


@dataclass
class Destination:
    tenant_id: str
    routing_version: str = "one"
    destination_id: str = "callback"
    endpoint: str = "https://example.com/hooks"
    event_types: frozenset[str] = frozenset({"stage.leased"})
    sent: list = field(default_factory=list)
    unavailable: bool = False

    def send(self, event):
        if self.unavailable:
            raise OSError("network failure")
        self.sent.append(event)


def _delivery(engine, clock, broker, *destinations, bind=()):
    metadata = engine.tables["jobs"].metadata
    bindings = metadata.tables.get("test_bindings")
    if bind and bindings is None:
        bindings = Table(
            "test_bindings",
            metadata,
            Column("job_id", String(64), primary_key=True),
            Column("org_id", String(64), nullable=False),
        )
        with engine.open_connection() as connection, connection.begin():
            bindings.create(connection)
            connection.execute(
                bindings.insert(),
                [{"job_id": f"job_{owner}", "org_id": owner} for owner in bind],
            )
    return TenantOutboxDelivery(
        engine=engine,
        clock=clock,
        producer="foundry",
        private_broker=broker,
        destinations=destinations,
        bindings=bindings,
    )


def _records(delivery):
    with delivery.engine.open_connection() as connection:
        return connection.execute(select(delivery.deliveries)).mappings().all()


def test_rows_envelopes_and_destinations_are_owner_bound(tmp_path):
    engine = _engine(tmp_path)
    _lease_one_stage(engine, org="alpha", job="job_alpha")
    _lease_one_stage(engine, org="beta", job="job_beta")
    _lease_one_stage(engine, org="gamma", job="job_gamma")

    outbox = engine.tables["outbox"]
    with engine.open_connection() as connection, connection.begin():
        source = connection.execute(select(outbox).where(outbox.c.org_id == "gamma")).one()
        connection.execute(
            outbox.update()
            .where(outbox.c.key == source.key)
            .values(
                payload={
                    **source.payload,
                    "job_id": "job_alpha",
                }
            )
        )

    broker = Broker()
    callback = Destination("alpha")
    other_callback = Destination("beta", endpoint="https://beta.example.com/hooks")
    delivery = _delivery(
        engine, FrozenClock(NOW), broker, callback, other_callback, bind=("alpha", "beta")
    )
    assert delivery.drain().published == 4
    assert {payload["org_id"] for _, _, payload in broker.sent} == {"alpha", "beta"}
    assert [event.tenant_id for event in callback.sent] == ["alpha"]
    assert [event.tenant_id for event in other_callback.sent] == ["beta"]
    assert [
        (row["org_id"], row["reason"])
        for row in _records(delivery)
        if row["state"] == "quarantined"
    ] == [("gamma", "job_owner_mismatch")]
    assert delivery.drain().total == 0
    with pytest.raises(ConfigurationError):
        engine.drain_pending(broker=broker)


def test_fenced_ack_cannot_close_reclaimed_and_foreign_deliveries(tmp_path):
    engine = _engine(tmp_path)
    _lease_one_stage(engine, org="alpha", job="job_alpha")
    broker = Broker()
    clock = FrozenClock(NOW)
    delivery = _delivery(engine, clock, broker)
    delivery.check_ready()
    delivery._plan_pending(10)
    obsolete = delivery._claim()
    assert obsolete is not None
    clock.advance(61)
    current = delivery._claim()
    assert current is not None
    assert current["fence"] > obsolete["fence"]
    delivery._finish(obsolete, state="sent")
    delivery._finish({**current, "org_id": "beta"}, state="sent")
    assert [row["state"] for row in _records(delivery)] == ["leased"]
    delivery._send(current)
    delivery._finish(current, state="sent")
    assert [row["state"] for row in _records(delivery)] == ["sent"]
    assert delivery.replay(owner="beta", event_id=current["event_id"]) is False
    assert delivery.replay(owner="alpha", event_id=current["event_id"]) is True
    assert delivery.drain().published == 1
    assert broker.sent[0][0] == broker.sent[1][0]


def test_backoff_and_revocation_keep_independent_progress(tmp_path):
    engine = _engine(tmp_path)
    _lease_one_stage(engine, org="alpha", job="job_alpha")
    _lease_one_stage(engine, org="beta", job="job_beta")
    broker = Broker()
    clock = FrozenClock(NOW)
    alpha = Destination("alpha", unavailable=True)
    beta = Destination("beta", endpoint="https://beta.example.com/hooks")
    delivery = _delivery(engine, clock, broker, alpha, beta, bind=("alpha", "beta"))
    report = delivery.drain()
    assert (report.published, report.failed) == (3, 1)
    assert delivery.drain().total == 0
    assert len(beta.sent) == 1
    clock.advance(10)
    updated = _delivery(engine, clock, broker, beta)
    assert updated.drain().failed == 1
    assert [
        (row["org_id"], row["reason"])
        for row in _records(delivery)
        if row["state"] == "quarantined"
    ] == [("alpha", "destination_inactive")]


def test_mismatched_envelope_is_quarantined(tmp_path):
    engine = _engine(tmp_path)
    _lease_one_stage(engine, org="alpha", job="job_alpha")
    outbox = engine.tables["outbox"]
    with engine.open_connection() as connection, connection.begin():
        source = connection.execute(select(outbox)).one()
        connection.execute(
            outbox.update()
            .where(outbox.c.key == source.key)
            .values(
                payload={
                    **source.payload,
                    "envelope": {
                        "event_id": source.key,
                        "tenant_id": "beta",
                        "producer": "foundry",
                        "type": source.event_class,
                        "occurred_at": NOW.isoformat(),
                    },
                }
            )
        )
    broker = Broker()
    delivery = _delivery(engine, FrozenClock(NOW), broker)
    delivery.drain()
    assert [row["reason"] for row in _records(delivery)] == ["envelope_binding_mismatch"]
    assert broker.sent == []


def test_legacy_owned_rows_relay_only_to_private_bus(tmp_path):
    engine = _engine(tmp_path)
    _lease_one_stage(engine, org="alpha", job="job_alpha")
    _lease_one_stage(engine, org="beta", job="job_beta")
    broker = Broker()
    callback = Destination("alpha")
    delivery = _delivery(engine, FrozenClock(NOW), broker, callback, bind=("beta",))
    assert delivery.drain().published == 2
    assert {payload["org_id"] for _, _, payload in broker.sent} == {"alpha", "beta"}
    assert callback.sent == []


def test_v2_envelope_without_execution_binding_is_quarantined(tmp_path):
    engine = _engine(tmp_path)
    _lease_one_stage(engine, org="alpha", job="job_alpha")
    outbox = engine.tables["outbox"]
    with engine.open_connection() as connection, connection.begin():
        source = connection.execute(select(outbox)).one()
        connection.execute(
            outbox.update()
            .where(outbox.c.key == source.key)
            .values(
                payload={
                    **source.payload,
                    "envelope": {
                        "event_id": source.key,
                        "tenant_id": "alpha",
                        "producer": "foundry",
                        "type": source.event_class,
                        "occurred_at": NOW.isoformat(),
                    },
                }
            )
        )
    broker = Broker()
    delivery = _delivery(engine, FrozenClock(NOW), broker)
    assert delivery.drain().published == 0
    assert [row["reason"] for row in _records(delivery)] == ["missing_execution_binding"]
