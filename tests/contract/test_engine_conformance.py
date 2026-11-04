"""The fake and the SQL engine answer the same questions the same way.

Why this exists: every other test that uses the fake is only evidence about the
deployed service if the two agree. `shepherd_workflow.memory` states the rule the
other way round -- "when a real adapter and these disagree, the real adapter is
wrong" -- and this is where that claim is checked for THIS host's wiring rather
than assumed from the package's own suite.

Each arm runs the identical script against both engines and compares the answers,
so a divergence names the scenario rather than a diff of two implementations.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from shepherd_contracts.clock import FrozenClock
from shepherd_contracts.principal import PrincipalMinter

from shepherd_workflow_service.adapters.fake.workflow_engine import InMemoryWorkflowEngine
from shepherd_workflow_service.adapters.real.json_workflow_definition import (
    load_workflow_definition,
)
from shepherd_workflow_service.adapters.real.sql_workflow_engine import SqlWorkflowEngine
from shepherd_workflow_service.ports.workflow_engine import WorkflowEnginePort
from tests.conftest import WORKFLOW_DEFINITION_PATH

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
PRINCIPAL = PrincipalMinter(issuer="test").mint(
    org_id="org_1", subject_id="sub_1", scopes=frozenset({"jobs:write"})
)


@pytest.fixture(params=["fake", "sql"])
def engine(request: pytest.FixtureRequest, tmp_path: Path) -> WorkflowEnginePort:
    workflow = load_workflow_definition(WORKFLOW_DEFINITION_PATH)
    if request.param == "fake":
        return InMemoryWorkflowEngine(
            workflow=workflow, clock=FrozenClock(NOW), job_deadline=timedelta(hours=1)
        )
    return SqlWorkflowEngine(
        url_provider=lambda: f"sqlite+pysqlite:///{tmp_path / 'conformance.db'}",
        workflow_provider=lambda: workflow,
        clock=FrozenClock(NOW),
        job_deadline=timedelta(hours=1),
        table_prefix="wf_",
    )


def test_both_engines_satisfy_the_port(engine: WorkflowEnginePort) -> None:
    assert isinstance(engine, WorkflowEnginePort)


def test_a_started_job_looks_the_same_from_either_engine(engine: WorkflowEnginePort) -> None:
    view = engine.start_job(job_id="job_1", input_digest="dig_1", principal=PRINCIPAL)

    assert (view.stage, view.status, view.state_version, view.fence) == (
        "plan",
        "pending",
        0,
        None,
    )
    assert view.workflow == "shepherd.agent_run.v1"


def test_a_stale_command_is_refused_identically(engine: WorkflowEnginePort) -> None:
    view = engine.start_job(job_id="job_1", input_digest="dig_1", principal=PRINCIPAL)
    engine.submit(
        stage_run_id=view.stage_run_id,
        kind="lease",
        state_version=0,
        fence=None,
        payload={},
        principal=PRINCIPAL,
    )

    outcome = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="lease",
        state_version=0,
        fence=None,
        payload={},
        principal=PRINCIPAL,
    )

    assert (outcome.accepted, outcome.refusal) == (False, "state_version_conflict")


def test_a_completion_without_a_lease_is_refused_identically(
    engine: WorkflowEnginePort,
) -> None:
    view = engine.start_job(job_id="job_1", input_digest="dig_1", principal=PRINCIPAL)

    outcome = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="complete",
        state_version=0,
        fence=None,
        payload={"payload_digest": "out_1"},
        principal=PRINCIPAL,
    )

    assert (outcome.accepted, outcome.refusal) == (False, "lease_not_held")


def test_the_lease_then_complete_path_agrees(engine: WorkflowEnginePort) -> None:
    view = engine.start_job(job_id="job_1", input_digest="dig_1", principal=PRINCIPAL)
    leased = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="lease",
        state_version=0,
        fence=None,
        payload={},
        principal=PRINCIPAL,
    )
    assert leased.view is not None

    completed = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="complete",
        state_version=leased.view.state_version,
        fence=leased.view.fence,
        payload={"payload_digest": "out_1"},
        principal=PRINCIPAL,
    )

    assert completed.accepted is True
    assert completed.view is not None
    assert completed.view.status == "succeeded"
    assert completed.view.attempts == 2, "one lease granted, plus the initial attempt"
