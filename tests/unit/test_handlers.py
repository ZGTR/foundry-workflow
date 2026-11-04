"""The TEMPLATE-inherited outbox drain -- the `Publisher`/`OutboxEventBus` pair.

READ THE NAME CAREFULLY: this is NOT the workflow ledger's outbox. This service
has two, and this is the one nothing in `services/` or `controllers/` publishes
through, so its table is always empty. The ledger's is `wf_outbox`, and its drain
is proved in `tests/integration/test_ledger_outbox_drain.py`.

This docstring used to say the handler below was wired by `iac/serverless.yml` as
its own Lambda entrypoint, and that `main.py` builds the bound callable. Neither
was ever true of a running deployment, and both stopped being true of the config
when that function was deleted. What the file still proves is
worth keeping -- the transactional write/relay pair behaves -- but it proves it
about a mechanism this service does not currently invoke anywhere.
"""

from __future__ import annotations

import functools
from datetime import UTC, datetime

from shepherd_workflow_service.adapters.fake.event_bus import RecordingPublisher
from shepherd_workflow_service.adapters.fake.unit_of_work import InMemoryUnitOfWork
from shepherd_workflow_service.adapters.real.outbox_event_bus import OutboxEventBus, drain_outbox
from shepherd_workflow_service.handlers.outbox_drain import handle_scheduled_drain
from shepherd_workflow_service.ports.event_bus import Event


def test_handle_scheduled_drain_relays_committed_rows() -> None:
    uow = InMemoryUnitOfWork(owned_tables=frozenset({"outbox"}))
    outbox = OutboxEventBus(uow)
    outbox.publish(
        Event(
            event_type="stage_run.advanced",
            org_id="org_1",
            occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    )
    uow.commit()
    downstream = RecordingPublisher()
    # The shape a composition root WOULD bind: the handler itself never imports
    # the outbox adapter. No composition root in this service builds one today.
    drain = functools.partial(drain_outbox, uow, downstream)

    relayed = handle_scheduled_drain(drain)

    assert relayed == 1
    assert len(downstream.published) == 1
