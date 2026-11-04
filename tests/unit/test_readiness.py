"""`EngineReadinessCheck` must answer BOTH ways, and neither answer is free.

A readiness check that returns True because it never asked anything is the
failure this file exists to rule out: it is indistinguishable from a healthy one
until the ledger is down, which is the only moment it was ever needed. So the
green arm carries a positive control -- the engine records that it was called --
and the red arm proves an exception becomes False rather than a 500 out of
`/readyz`.

The third arm is about what the probe is ALLOWED to see, not about whether it
works. `WorkflowEnginePort.get` answers None for both "no such stage run" and
"not yours", so a probe principal with no scopes and an org id no tenant holds
cannot read a row even if its id collided with a real one.
"""

from __future__ import annotations

from typing import Any

from shepherd_contracts.principal import CallerPrincipalV1

from shepherd_workflow_service.adapters.real.readiness import EngineReadinessCheck
from shepherd_workflow_service.ports.workflow_engine import (
    CommandOutcome,
    StageRunView,
    WorkflowEnginePort,
)


class _RecordingEngine:
    """Answers `get` the way a reachable ledger does for an id that is not there."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, CallerPrincipalV1]] = []

    def start_job(
        self, *, job_id: str, input_digest: str, principal: CallerPrincipalV1
    ) -> StageRunView:
        raise AssertionError("readiness must not start a job")

    def submit(
        self,
        *,
        stage_run_id: str,
        kind: str,
        state_version: int,
        fence: int | None,
        payload: dict[str, Any],
        principal: CallerPrincipalV1,
    ) -> CommandOutcome:
        raise AssertionError("readiness must not submit a command")

    def stages(self, job_id, *, principal):
        return []

    def get(self, stage_run_id: str, *, principal: CallerPrincipalV1) -> StageRunView | None:
        self.calls.append((stage_run_id, principal))
        return None


class _UnreachableEngine(_RecordingEngine):
    def stages(self, job_id, *, principal):
        return []

    def get(self, stage_run_id: str, *, principal: CallerPrincipalV1) -> StageRunView | None:
        self.calls.append((stage_run_id, principal))
        raise RuntimeError("could not connect to the ledger")


def test_a_reachable_ledger_is_ready_and_was_actually_asked() -> None:
    engine = _RecordingEngine()

    ready = EngineReadinessCheck(engine).check()

    assert ready is True
    # The positive control. Without it, a check that returned True without
    # touching the engine would pass this test and fail in production silently.
    assert len(engine.calls) == 1


def test_an_unreachable_ledger_is_not_ready_rather_than_an_exception() -> None:
    engine = _UnreachableEngine()

    ready = EngineReadinessCheck(engine).check()

    assert ready is False
    assert len(engine.calls) == 1


def test_the_probe_principal_carries_no_scopes_and_no_tenants_org() -> None:
    engine = _RecordingEngine()

    EngineReadinessCheck(engine).check()

    _stage_run_id, principal = engine.calls[0]
    assert principal.scopes == frozenset()
    assert principal.org_id == "__readyz_probe__"


def test_the_check_satisfies_nothing_more_than_the_port() -> None:
    """It depends on `WorkflowEnginePort`, not on `SqlWorkflowEngine`: the fakes
    above are enough to construct it, which is what makes `/readyz` testable
    without a database."""
    assert isinstance(_RecordingEngine(), WorkflowEnginePort)
