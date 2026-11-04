"""The application service: authenticate, ask the engine, translate the answer.

It holds NO state machine. Every decision belongs to `reduce()` inside the
vendored package, and duplicating even one branch here would put the same rule in
two places where only one of them is tested against the ledger.

What it does own is the translation from a REFUSAL to an ERROR. That boundary is
the interesting one: inside the engine a refusal is an ordinary answer, and at
the HTTP edge it has to become a status code, because an HTTP caller has no other
channel for "understood, and no". Which code depends on the refusal class, so
the mapping lives here rather than in a controller that must stay four lines.
"""

from __future__ import annotations

from typing import Any

from shepherd_contracts.principal import CallerPrincipalV1

from shepherd_workflow_service.errors import StageRunConflict, StageRunNotFound, StageRunRejected
from shepherd_workflow_service.ports.workflow_engine import StageRunView, WorkflowEnginePort

#: Refusals that mean "the ledger is not in the state your command assumed".
#: They are 409: the request was well formed, the world moved, and RETRYING AFTER
#: RE-READING is the correct client behaviour for every one of them.
_CONFLICT_REFUSALS = frozenset(
    {
        "state_version_conflict",
        "stale_fence",
        "terminal_state",
        "cancelled",
        "lease_not_held",
        "lease_held",
        "retry_not_due",
        "attempts_exhausted",
        "lease_expired",
        "deadline_passed",
        "feature_disabled",
        "dispatch_key_mismatch",
    }
)

#: Refusals that mean "no such stage run, for you". 404, and deliberately the
#: same answer for another tenant's id as for one that does not exist.
_NOT_FOUND_REFUSALS = frozenset({"unknown_stage"})


class WorkflowService:
    def __init__(self, *, engine: WorkflowEnginePort) -> None:
        self._engine = engine

    def start_job(
        self, principal: CallerPrincipalV1, *, job_id: str, input_digest: str
    ) -> StageRunView:
        return self._engine.start_job(job_id=job_id, input_digest=input_digest, principal=principal)

    def get(self, principal: CallerPrincipalV1, *, stage_run_id: str) -> StageRunView:
        view = self._engine.get(stage_run_id, principal=principal)
        if view is None:
            raise StageRunNotFound(stage_run_id)
        return view

    def stages(self, principal: CallerPrincipalV1, *, job_id: str) -> list[StageRunView]:
        views = self._engine.stages(job_id, principal=principal)
        if not views:
            raise StageRunNotFound(job_id)
        return views

    def submit(
        self,
        principal: CallerPrincipalV1,
        *,
        stage_run_id: str,
        kind: str,
        state_version: int,
        fence: int | None,
        payload: dict[str, Any],
    ) -> StageRunView:
        """Apply one command, or raise the error its refusal maps to."""
        outcome = self._engine.submit(
            stage_run_id=stage_run_id,
            kind=kind,
            state_version=state_version,
            fence=fence,
            payload=payload,
            principal=principal,
        )
        if outcome.accepted:
            assert outcome.view is not None  # an accepted command has a stage run
            return outcome.view

        refusal = outcome.refusal or "refused"
        if refusal in _NOT_FOUND_REFUSALS:
            raise StageRunNotFound(stage_run_id)
        if refusal in _CONFLICT_REFUSALS:
            raise StageRunConflict(refusal)
        # An unrecognised refusal class is 422 rather than 500 or a silent 409.
        # The package can add one, and mapping the unknown to a conflict would
        # tell the caller to retry something that may never succeed.
        raise StageRunRejected(refusal)
