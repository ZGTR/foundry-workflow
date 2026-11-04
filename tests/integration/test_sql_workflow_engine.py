"""The SQL engine against a real database, including the race it exists to lose.

SQLite here rather than MySQL for the same reason the package's own suite uses
it: the compare-and-set is a conditional UPDATE, which both dialects execute the
same way, and requiring a server to run this test would mean nobody runs it.
`tests/integration/test_workflow_repository_contract.py` is where a MySQL target
belongs when one is available.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from shepherd_contracts.clock import FrozenClock
from shepherd_contracts.principal import PrincipalMinter
from shepherd_workflow.ports import ConcurrentUpdate
from shepherd_workflow.sql.repository import SqlAlchemyStageRepository
from shepherd_workflow.state import StageStatus
from shepherd_workflow.workflow import StageDefinitionV1, WorkflowDefinitionV1

from shepherd_workflow_service.adapters.real.json_workflow_definition import (
    load_workflow_definition,
)
from shepherd_workflow_service.adapters.real.sql_workflow_engine import SqlWorkflowEngine
from tests.conftest import WORKFLOW_DEFINITION_PATH

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)


def _principal(org: str = "org_test", scopes: tuple[str, ...] = ("jobs:write",)):
    return PrincipalMinter(issuer="test").mint(
        org_id=org, subject_id="sub_1", scopes=frozenset(scopes)
    )


def _engine(tmp_path: Path, *, prefix: str = "wf_") -> SqlWorkflowEngine:
    tmp_path.mkdir(parents=True, exist_ok=True)
    return SqlWorkflowEngine(
        url_provider=lambda: f"sqlite+pysqlite:///{tmp_path / 'workflow.db'}",
        workflow_provider=lambda: load_workflow_definition(WORKFLOW_DEFINITION_PATH),
        clock=FrozenClock(NOW),
        job_deadline=timedelta(hours=1),
        table_prefix=prefix,
    )


def test_a_started_job_sits_at_the_workflows_first_stage(tmp_path: Path) -> None:
    view = _engine(tmp_path).start_job(
        job_id="job_1", input_digest="dig_1", principal=_principal()
    )

    assert view.stage == "plan"
    assert view.status == str(StageStatus.PENDING)
    assert view.state_version == 0


def test_starting_the_same_job_over_the_same_input_twice_is_one_stage_run(
    tmp_path: Path,
) -> None:
    """Idempotent by dispatch key, resolved by the database rather than a check."""
    engine = _engine(tmp_path)
    first = engine.start_job(job_id="job_1", input_digest="dig_1", principal=_principal())
    second = engine.start_job(job_id="job_1", input_digest="dig_1", principal=_principal())

    assert first.stage_run_id == second.stage_run_id
    assert second.state_version == 0


def test_two_callers_at_the_SAME_state_version_do_not_both_win(tmp_path: Path) -> None:
    """The arm that distinguishes a wired repository from a blind one.

    Both callers submit at `state_version=0`. If the second were applied too, the
    ledger would end at version 2 with the loser's transition silently on top of
    the winner's. It ends at version 1, and the loser is told which class of
    refusal it hit so it knows to re-read rather than to retry blindly.
    """
    engine = _engine(tmp_path)
    principal = _principal()
    view = engine.start_job(job_id="job_1", input_digest="dig_1", principal=principal)

    first = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="lease",
        state_version=0,
        fence=None,
        payload={},
        principal=principal,
    )
    second = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="lease",
        state_version=0,
        fence=None,
        payload={},
        principal=principal,
    )

    assert first.accepted is True
    assert second.accepted is False
    assert second.refusal == "state_version_conflict"

    final = engine.get(view.stage_run_id, principal=principal)
    assert final is not None
    assert final.state_version == 1, "the loser overwrote the winner"


def test_the_underlying_UPDATE_is_conditional_on_the_version_it_read(
    tmp_path: Path,
) -> None:
    """The test above is refused by the pure reducer, which re-reads before deciding.

    That is the caller-visible guarantee, but it does not prove the WRITE is
    conditional -- two callers can pass the reducer simultaneously, and only the
    UPDATE's WHERE clause separates them then. So this drives the repository the
    engine actually wires, with two handles holding the SAME v0 snapshot, and
    asserts the second is refused by the database rather than applied.
    """
    from shepherd_workflow.reducer import TransitionPlanV1

    engine = _engine(tmp_path)
    principal = _principal()
    view = engine.start_job(job_id="job_1", input_digest="dig_1", principal=principal)

    plan = TransitionPlanV1(accepted=True, next_status=StageStatus.LEASED, next_fence=1)
    with engine._connect() as connection:  # noqa: SLF001 -- driving the real wiring
        with connection.begin():
            repository = SqlAlchemyStageRepository(connection, engine._tables)  # noqa: SLF001
            snapshot_a = repository.load(view.stage_run_id)
            snapshot_b = repository.load(view.stage_run_id)
            assert snapshot_a is not None and snapshot_b is not None
            assert snapshot_a.state_version == snapshot_b.state_version == 0

            repository.apply(snapshot_a, plan, principal=principal)
            with pytest.raises(ConcurrentUpdate):
                repository.apply(snapshot_b, plan, principal=principal)


def test_a_lost_race_reaches_the_caller_as_a_refusal_not_a_500(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`ConcurrentUpdate` escaping the engine would be a 500 for routine contention.

    A worker losing a race is expected, frequent and not a fault, so the engine
    must translate it into the same refusal class the reducer produces for the
    same situation. Monkeypatched because the interleaving that raises it
    naturally cannot be produced from one process against SQLite.
    """
    engine = _engine(tmp_path)
    principal = _principal()
    view = engine.start_job(job_id="job_1", input_digest="dig_1", principal=principal)

    def _always_raced(*_args: object, **_kwargs: object) -> None:
        raise ConcurrentUpdate("simulated interleaving")

    monkeypatch.setattr(SqlAlchemyStageRepository, "apply", _always_raced)

    outcome = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="lease",
        state_version=0,
        fence=None,
        payload={},
        principal=principal,
    )

    assert outcome.accepted is False
    assert outcome.refusal == "state_version_conflict"


def test_a_refusal_writes_nothing(tmp_path: Path) -> None:
    """A refused command must leave no outbox row behind.

    The events are enqueued inside the same transaction as the transition, so a
    refusal that committed would publish events for a transition that never
    happened. The engine leaves the transaction by raising, which rolls it back.
    """
    engine = _engine(tmp_path)
    principal = _principal()
    view = engine.start_job(job_id="job_1", input_digest="dig_1", principal=principal)

    outcome = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="complete",
        state_version=0,
        fence=None,
        payload={"payload_digest": "out_1"},
        principal=principal,
    )
    assert outcome.accepted is False
    assert outcome.refusal == "lease_not_held"

    with engine._connect() as connection:  # noqa: SLF001
        rows = connection.execute(engine._tables["outbox"].select()).fetchall()  # noqa: SLF001
    assert rows == [], "a refused command enqueued events"


def test_another_orgs_stage_run_is_invisible_rather_than_forbidden(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    mine = _principal("org_a")
    theirs = _principal("org_b")
    view = engine.start_job(job_id="job_1", input_digest="dig_1", principal=mine)

    assert engine.get(view.stage_run_id, principal=mine) is not None
    assert engine.get(view.stage_run_id, principal=theirs) is None

    outcome = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="lease",
        state_version=0,
        fence=None,
        payload={},
        principal=theirs,
    )
    assert outcome.refusal == "unknown_stage"


def test_a_caller_without_the_scope_cannot_start_a_job(tmp_path: Path) -> None:
    from shepherd_contracts.errors import Unauthorized

    with pytest.raises(Unauthorized):
        _engine(tmp_path).start_job(
            job_id="job_1", input_digest="dig_1", principal=_principal(scopes=("jobs:read",))
        )


@pytest.mark.parametrize("prefix", ["wf_", "app_", "shepherd_wf_"])
def test_the_tables_it_creates_are_the_tables_it_is_allowed_to_write(
    tmp_path: Path, prefix: str
) -> None:
    """The prefix rule, exercised at three prefixes rather than asserted at one.

    `build_tables(prefix=P)` names the tables and `owned_tables(P)` names the set
    `assert_writes_within` permits. If the engine held a literal `wf_` set, the
    `app_` case would create `app_jobs` and then be refused it by the guard --
    a TableOwnershipViolation against tables it had just created. Only running a
    NON-default prefix can catch that, which is why every arm below is a write,
    not a name comparison.
    """
    engine = _engine(tmp_path / prefix, prefix=prefix)
    principal = _principal()
    view = engine.start_job(job_id="job_1", input_digest="dig_1", principal=principal)

    outcome = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="lease",
        state_version=0,
        fence=None,
        payload={},
        principal=principal,
    )

    assert outcome.accepted is True, f"prefix {prefix!r} was refused its own tables"
    with engine._connect() as connection:  # noqa: SLF001
        names = {
            row[0]
            for row in connection.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert {f"{prefix}jobs", f"{prefix}stage_runs", f"{prefix}outbox"} <= names


def test_a_workflow_this_service_does_not_run_is_a_digest_mismatch(tmp_path: Path) -> None:
    """A ledger row written under one workflow is refused by a service running another.

    This is what the workflow digest is for: a job that began before a stage was
    inserted is refused rather than silently resumed against the new shape.
    """
    engine = _engine(tmp_path)
    principal = _principal()
    view = engine.start_job(job_id="job_1", input_digest="dig_1", principal=principal)

    engine._workflow = WorkflowDefinitionV1(  # noqa: SLF001 -- a redeployment, in one line
        workflow_key="shepherd.agent_run.v1",
        stages=(
            StageDefinitionV1(key="plan", input_schema="a", output_schema="b"),
            StageDefinitionV1(key="extra", input_schema="c", output_schema="d"),
        ),
    )

    outcome = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="lease",
        state_version=0,
        fence=None,
        payload={},
        principal=principal,
    )

    assert outcome.refusal == "workflow_digest_mismatch"


class _MovableClock:
    """A clock a test can push forward. `FrozenClock` cannot express "later".

    Every other arm in this file is a single instant, so a frozen clock is right
    for them. The lease arm below is the one property in the ledger that is only
    observable across two instants: the lease is granted at one and read back at
    another, and no single-instant test can tell a persisted expiry from a NULL.
    """

    def __init__(self, at: datetime) -> None:
        self._at = at

    def now(self) -> datetime:
        return self._at

    def advance(self, delta: timedelta) -> None:
        self._at = self._at + delta


def test_an_expired_lease_is_refused_as_lease_expired_under_SQL(tmp_path: Path) -> None:
    """A fixed lease-expiry defect, and the arm that keeps it fixed in THIS repository.

    `SqlAlchemyStageRepository.apply` used not to persist `lease_expires_at`, so
    the column stayed NULL under SQL however far the clock moved, and
    `RefusalClass.LEASE_EXPIRED` was unreachable through the deployed engine
    while the in-memory reference produced it happily. Nothing here would have
    gone red: the fake and the SQL engine agree on every arm in
    `tests/contract/test_engine_conformance.py`, because none of them advances a
    clock, and a refusal class that cannot be produced is indistinguishable from
    one nobody happened to test.

    The fix landed in the upstream `shepherd-workflow` package and was
    re-vendored (`f0d9903`). This arm asserts the SERVICE observes it, which is
    the only claim this repository is entitled to make.

    Note the ordering the reducer imposes and why the numbers below respect it:
    `now >= deadline_at` is checked BEFORE the lease is, so a job deadline
    shorter than the lease duration would surface as `deadline_passed` and this
    test would pass for the wrong reason. One hour against five minutes keeps the
    two apart.
    """
    clock = _MovableClock(NOW)
    engine = SqlWorkflowEngine(
        url_provider=lambda: f"sqlite+pysqlite:///{tmp_path / 'workflow.db'}",
        workflow_provider=lambda: load_workflow_definition(WORKFLOW_DEFINITION_PATH),
        clock=clock,
        job_deadline=timedelta(hours=1),
        table_prefix="wf_",
    )
    principal = _principal()
    view = engine.start_job(job_id="job_1", input_digest="dig_1", principal=principal)

    leased = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="lease",
        state_version=0,
        fence=None,
        payload={},
        principal=principal,
    )
    assert leased.accepted is True, leased.refusal
    assert leased.view is not None
    assert leased.view.fence == 1

    # Past the five-minute lease, well inside the one-hour job deadline.
    clock.advance(timedelta(minutes=5, seconds=1))

    outcome = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="complete",
        state_version=leased.view.state_version,
        fence=leased.view.fence,
        payload={"payload_digest": "sha256:result"},
        principal=principal,
    )

    assert outcome.accepted is False
    assert outcome.refusal == "lease_expired", (
        "the SQL engine did not observe the lease expiring -- if this reads "
        "'stale_fence' or the call was accepted, lease_expires_at is not being "
        "persisted again"
    )


def test_the_lease_arm_above_is_not_passing_because_of_the_DEADLINE(tmp_path: Path) -> None:
    """The control for the arm above, and the reason its numbers are what they are.

    `lease_expired` and `deadline_passed` are both refusals produced by moving a
    clock forward, and the reducer checks the deadline FIRST. A job deadline
    shorter than the lease would make the arm above green while proving nothing
    about `lease_expires_at`. This drives the same script with a THREE-MINUTE job
    deadline and asserts the refusal changes -- so the two are demonstrably
    distinguishable in this wiring, not merely different names in an enum.
    """
    clock = _MovableClock(NOW)
    engine = SqlWorkflowEngine(
        url_provider=lambda: f"sqlite+pysqlite:///{tmp_path / 'workflow.db'}",
        workflow_provider=lambda: load_workflow_definition(WORKFLOW_DEFINITION_PATH),
        clock=clock,
        job_deadline=timedelta(minutes=3),
        table_prefix="wf_",
    )
    principal = _principal()
    view = engine.start_job(job_id="job_1", input_digest="dig_1", principal=principal)
    leased = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="lease",
        state_version=0,
        fence=None,
        payload={},
        principal=principal,
    )
    assert leased.accepted is True, leased.refusal
    assert leased.view is not None

    clock.advance(timedelta(minutes=5, seconds=1))

    outcome = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="complete",
        state_version=leased.view.state_version,
        fence=leased.view.fence,
        payload={"payload_digest": "sha256:result"},
        principal=principal,
    )

    assert outcome.accepted is False
    assert outcome.refusal == "deadline_passed"
