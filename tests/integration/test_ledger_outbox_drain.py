"""The ledger outbox, drained against a real database.

This is the arm that distinguishes the mechanism from its description. The rows
are produced the way production produces them -- by submitting a real command
through `SqlWorkflowEngine`, which writes them inside the engine's own
transaction -- and the assertions are made against the `<prefix>outbox` table
itself, not against a return value the drain computed.

SQLite rather than MySQL for the same reason the sibling engine test gives: the
statements involved are dialect-neutral, and a test that needs a server is a
test nobody runs.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from shepherd_contracts.clock import FrozenClock
from shepherd_contracts.errors import PossibleSend
from shepherd_contracts.principal import PrincipalMinter
from shepherd_workflow.sql.schema import build_tables
from sqlalchemy import MetaData, create_engine, select

from shepherd_workflow_service.adapters.fake.event_bus import RecordingPublisher
from shepherd_workflow_service.adapters.real.json_workflow_definition import (
    load_workflow_definition,
)
from shepherd_workflow_service.adapters.real.ledger_broker import PublisherBroker
from shepherd_workflow_service.adapters.real.sql_workflow_engine import SqlWorkflowEngine
from tests.conftest import WORKFLOW_DEFINITION_PATH

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
PREFIX = "wf_"


def _principal(org: str = "org_test"):
    return PrincipalMinter(issuer="test").mint(
        org_id=org, subject_id="sub_1", scopes=frozenset({"jobs:write"})
    )


def _engine(tmp_path: Path) -> SqlWorkflowEngine:
    tmp_path.mkdir(parents=True, exist_ok=True)
    return SqlWorkflowEngine(
        url_provider=lambda: f"sqlite+pysqlite:///{tmp_path / 'workflow.db'}",
        workflow_provider=lambda: load_workflow_definition(WORKFLOW_DEFINITION_PATH),
        clock=FrozenClock(NOW),
        job_deadline=timedelta(hours=1),
        table_prefix=PREFIX,
    )


def _outbox_rows(tmp_path: Path) -> list[tuple[str, str, object]]:
    """Read the table DIRECTLY. The drain's own report is not evidence about the
    database; the point of this file is to look at the rows."""
    tables = build_tables(MetaData(), prefix=PREFIX)
    sql_engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'workflow.db'}", future=True)
    with sql_engine.connect() as connection:
        outbox = tables["outbox"]
        return [
            (row.key, row.event_class, row.sent_at)
            for row in connection.execute(
                select(outbox).order_by(outbox.c.created_at)
            ).all()
        ]


def _lease_one_stage(engine: SqlWorkflowEngine, *, org: str = "org_test", job: str = "job_1"):
    principal = _principal(org)
    view = engine.start_job(job_id=job, input_digest="dig_1", principal=principal)
    outcome = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="lease",
        state_version=0,
        fence=None,
        payload={},
        principal=principal,
    )
    assert outcome.accepted, outcome.refusal
    return view


def test_a_committed_command_leaves_a_PENDING_row_and_nothing_drains_it_on_its_own(
    tmp_path: Path,
) -> None:
    """The control for every arm below, and the defect this slice was opened for.

    Submitting a command writes the outbox row. Running the service does not
    move it. Until `drain_pending` existed, this was the terminal state of every
    event this service ever decided.
    """
    engine = _engine(tmp_path)
    _lease_one_stage(engine)

    rows = _outbox_rows(tmp_path)

    assert [(key.startswith("evt_"), event_class, sent_at) for key, event_class, sent_at in rows] \
        == [(True, "stage.leased", None)]


def test_draining_publishes_the_row_and_marks_it_sent_in_the_table(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _lease_one_stage(engine)
    publisher = RecordingPublisher()

    report = engine.drain_pending(broker=PublisherBroker(publisher, clock=FrozenClock(NOW)))

    assert (report.published, report.ambiguous, report.failed) == (1, 0, 0)
    assert [event.event_type for event in publisher.published] == ["stage.leased"]
    # SQLite returns the column naive, so identity with NOW is asserted on the
    # value the driver can round-trip rather than on the tzinfo it drops.
    assert [sent_at for _, _, sent_at in _outbox_rows(tmp_path)] == [NOW.replace(tzinfo=None)]


def test_a_second_drain_finds_nothing_because_sent_rows_are_no_longer_pending(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _lease_one_stage(engine)
    first = RecordingPublisher()
    engine.drain_pending(broker=PublisherBroker(first, clock=FrozenClock(NOW)))

    second = RecordingPublisher()
    report = engine.drain_pending(broker=PublisherBroker(second, clock=FrozenClock(NOW)))

    assert report.total == 0
    assert second.published == []


def test_a_broker_that_raises_leaves_the_row_PENDING_for_the_next_drain(
    tmp_path: Path,
) -> None:
    """At-least-once is the contract. A failed publish must not mark the row."""

    class _BrokenBroker:
        def publish(self, *, key: str, event_class: str, payload: dict) -> None:
            raise RuntimeError("broker down")

    engine = _engine(tmp_path)
    _lease_one_stage(engine)

    report = engine.drain_pending(broker=_BrokenBroker())

    assert (report.published, report.failed) == (0, 1)
    assert [sent_at for _, _, sent_at in _outbox_rows(tmp_path)] == [None]


def test_an_ambiguous_send_is_marked_rather_than_retried_forever(tmp_path: Path) -> None:
    """`PossibleSend` means the send may have landed. The package chooses a
    possible duplicate over a possible loss and records the ambiguity in the row,
    so this asserts the row LEAVES pending -- the opposite of the arm above."""

    class _AmbiguousBroker:
        def publish(self, *, key: str, event_class: str, payload: dict) -> None:
            raise PossibleSend("connection died after send", idempotency_key=key)

    engine = _engine(tmp_path)
    _lease_one_stage(engine)

    report = engine.drain_pending(broker=_AmbiguousBroker())

    assert (report.published, report.ambiguous, report.failed) == (0, 1, 0)
    assert [sent_at for _, _, sent_at in _outbox_rows(tmp_path)] == [NOW.replace(tzinfo=None)]


def test_the_drain_covers_EVERY_tenant_not_just_the_one_that_happens_to_be_first(
    tmp_path: Path,
) -> None:
    """The package's outbox reader is org-scoped because the request path always
    has exactly one tenant. A drain has none, so it must enumerate them. A drain
    that used a single hardcoded or first-seen org would pass every single-tenant
    test in this file and silently strand every other tenant's events.
    """
    engine = _engine(tmp_path)
    _lease_one_stage(engine, org="org_a", job="job_a")
    _lease_one_stage(engine, org="org_b", job="job_b")
    publisher = RecordingPublisher()

    report = engine.drain_pending(broker=PublisherBroker(publisher, clock=FrozenClock(NOW)))

    assert report.published == 2
    assert sorted(event.org_id for event in publisher.published) == ["org_a", "org_b"]
    naive = NOW.replace(tzinfo=None)
    assert [sent_at for _, _, sent_at in _outbox_rows(tmp_path)] == [naive, naive]
