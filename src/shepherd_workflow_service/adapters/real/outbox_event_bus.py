"""`OutboxEventBus` -- the publisher for anything that must not be lost.

`publish()` writes one row into the `outbox` table **inside the caller's
`UnitOfWork`** -- the same transaction as whatever domain write triggered the
event. It does not deliver anything itself. A separate `drain_outbox()` call,
run on its own schedule against a *committed* `UnitOfWork`, relays every
unrelayed row through a downstream `Publisher` (typically `SqsPublisher`) and
marks it relayed.

The transactional guarantee this buys: if the caller's unit of work rolls
back, the outbox row rolls back with it, and `drain_outbox()` finds nothing to
relay. `tests/integration/test_outbox_event_bus.py` proves exactly that, using
`adapters.fake.unit_of_work.InMemoryUnitOfWork`.

This is the same mechanism the host application it was extracted from runs in
production; this module is the template-generic version of it.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from shepherd_contracts.ids import new_id
from shepherd_contracts.unit_of_work import UnitOfWork, assert_writes_within

from shepherd_workflow_service.ports.event_bus import Event, Publisher

_OWNED_TABLES = frozenset({"outbox"})


def _is_sql_session(session: Any) -> bool:
    return hasattr(session, "execute") and not isinstance(session, dict)


class OutboxEventBus:
    """Implements `Publisher`. Writes only; never relays."""

    def __init__(self, uow: UnitOfWork) -> None:
        assert_writes_within(uow, _OWNED_TABLES, package="foundry-workflow")
        self._uow = uow

    def publish(self, event: Event) -> None:
        session = self._uow.session()
        outbox_id = new_id("obx")
        payload_json = json.dumps(dict(event.payload))
        if _is_sql_session(session):
            session.execute(
                "INSERT INTO outbox "
                "(outbox_id, event_type, org_id, occurred_at, payload_json, relayed_at) "
                "VALUES (?, ?, ?, ?, ?, NULL)",
                (
                    outbox_id,
                    event.event_type,
                    event.org_id,
                    event.occurred_at.isoformat(),
                    payload_json,
                ),
            )
        else:
            session["outbox"].append(
                {
                    "outbox_id": outbox_id,
                    "event_type": event.event_type,
                    "org_id": event.org_id,
                    "occurred_at": event.occurred_at.isoformat(),
                    "payload_json": payload_json,
                    "relayed_at": None,
                }
            )


def drain_outbox(uow: UnitOfWork, downstream: Publisher, *, batch_size: int = 100) -> int:
    """Relay every unrelayed outbox row through `downstream`. Returns the count relayed.

    Call this from a scheduled worker, never from the request path -- "drained
    separately" is the point: the request that wrote the row must not block on
    delivery.
    """
    session = uow.session()
    relayed = 0
    if _is_sql_session(session):
        rows = session.execute(
            "SELECT outbox_id, event_type, org_id, occurred_at, payload_json FROM outbox "
            "WHERE relayed_at IS NULL ORDER BY occurred_at ASC LIMIT ?",
            (batch_size,),
        ).fetchall()
        for outbox_id, event_type, org_id, occurred_at, payload_json in rows:
            downstream.publish(
                Event(
                    event_type=event_type,
                    org_id=org_id,
                    occurred_at=datetime.fromisoformat(occurred_at),
                    payload=json.loads(payload_json),
                )
            )
            session.execute(
                "UPDATE outbox SET relayed_at = ? WHERE outbox_id = ?",
                (datetime.now(UTC).isoformat(), outbox_id),
            )
            relayed += 1
    else:
        for row in session["outbox"]:
            if row["relayed_at"] is not None or relayed >= batch_size:
                continue
            downstream.publish(
                Event(
                    event_type=row["event_type"],
                    org_id=row["org_id"],
                    occurred_at=datetime.fromisoformat(row["occurred_at"]),
                    payload=json.loads(row["payload_json"]),
                )
            )
            row["relayed_at"] = datetime.now(UTC).isoformat()
            relayed += 1
    return relayed
