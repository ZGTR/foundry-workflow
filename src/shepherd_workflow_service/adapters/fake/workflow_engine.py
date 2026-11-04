"""The in-memory `WorkflowEnginePort`, for tests and for a local run with no DB.

This is a fake in the sense that its STORAGE is a dict. It is not a fake in the
sense that matters: it drives the same `StageRunner`, the same pure `reduce()`
and the same `InMemoryStageRepository` compare-and-set that the package calls its
reference semantics, so a refusal seen here is the refusal the SQL engine must
also produce. A hand-written stand-in that returned canned outcomes would agree
with every test written against it and prove nothing about the deployed service.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from typing import Any

from shepherd_contracts import CallerPrincipalV1, Clock, derive_id
from shepherd_workflow.commands import LedgerSnapshotV1
from shepherd_workflow.memory import InMemoryOutbox, InMemoryStageRepository, InMemoryUnitOfWork
from shepherd_workflow.ports import ConcurrentUpdate, owned_tables
from shepherd_workflow.reducer import dispatch_key as derive_dispatch_key
from shepherd_workflow.runner import StageRunner
from shepherd_workflow.state import RefusalClass, StageStatus
from shepherd_workflow.workflow import WorkflowDefinitionV1

from shepherd_workflow_service.errors import StageRunConflict
from shepherd_workflow_service.models.workflow_command import build_command, to_view
from shepherd_workflow_service.ports.workflow_engine import CommandOutcome, StageRunView


class InMemoryWorkflowEngine:
    """Ledger in a dict; decisions by the real reducer."""

    def __init__(
        self,
        *,
        workflow: WorkflowDefinitionV1,
        clock: Clock,
        job_deadline: timedelta,
        table_prefix: str = "",
    ) -> None:
        self._workflow = workflow
        self._clock = clock
        self._job_deadline = job_deadline
        self._repository = InMemoryStageRepository()
        self._outbox = InMemoryOutbox()
        # The SAME parameter builds the ownership set here as names the tables,
        # for the same reason the SQL engine derives it: a literal set would be
        # correct only at one prefix. There are no physical tables in memory, so
        # the set is purely the write-guard's input.
        self._uow = InMemoryUnitOfWork(owned_tables(table_prefix))
        self._runner = StageRunner(
            workflow=workflow,
            repository=self._repository,
            outbox=self._outbox,
            clock=clock,
            tables=owned_tables(table_prefix),
        )
        self._by_dispatch_key: dict[str, str] = {}
        self._attempts: dict[str, int] = {}
        self._org_of: dict[str, str] = {}
        self._jobs: dict[str, tuple[str, str]] = {}

    def start_job(
        self, *, job_id: str, input_digest: str, principal: CallerPrincipalV1
    ) -> StageRunView:
        principal.require("jobs:write")
        identity = (principal.org_id, input_digest)
        if job_id in self._jobs and self._jobs[job_id] != identity:
            raise StageRunConflict("job_identity_conflict")
        self._jobs[job_id] = identity
        stage = self._workflow.first
        key = derive_dispatch_key(self._workflow.digest, job_id, stage.key, input_digest)
        existing = self._by_dispatch_key.get(key)
        if existing is not None:
            view = self._view(existing)
            assert view is not None
            return view

        stage_run_id = derive_id("srun", key)
        now = self._clock.now()
        self._repository.put(
            LedgerSnapshotV1(
                workflow_digest=self._workflow.digest,
                job_id=job_id,
                stage_run_id=stage_run_id,
                stage_key=stage.key,
                state_version=0,
                fence=None,
                attempt_id=derive_id("att", stage_run_id, 0),
                dispatch_key=key,
                input_digest=input_digest,
                status=str(StageStatus.PENDING),
                deadline_at=now + self._job_deadline,
            )
        )
        self._by_dispatch_key[key] = stage_run_id
        self._attempts[stage_run_id] = 1
        self._org_of[stage_run_id] = principal.org_id
        view = self._view(stage_run_id)
        assert view is not None
        return view

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
        if self._org_of.get(stage_run_id) != principal.org_id:
            # Another org's stage run is refused as UNKNOWN, never as forbidden:
            # a distinguishable refusal confirms the id exists.
            return CommandOutcome(False, str(RefusalClass.UNKNOWN_STAGE), None)
        snapshot = self._repository.load(stage_run_id)
        if snapshot is None:
            return CommandOutcome(False, str(RefusalClass.UNKNOWN_STAGE), None)

        command = build_command(
            snapshot=snapshot,
            kind=kind,
            state_version=state_version,
            fence=fence,
            payload=payload,
            now=self._clock.now(),
        )
        try:
            result = self._runner.apply(command, principal=principal, uow=self._uow)
        except ConcurrentUpdate:
            return CommandOutcome(
                False, str(RefusalClass.STATE_VERSION_CONFLICT), self._view(stage_run_id)
            )
        if not result.accepted:
            return CommandOutcome(False, str(result.refusal), self._view(stage_run_id))
        if command.kind.value == "lease":
            self._attempts[stage_run_id] = self._attempts.get(stage_run_id, 1) + 1
        plan = result.plan
        if plan.next_stage_run_id is not None:
            assert plan.next_stage_key is not None
            assert plan.artifact_digest is not None
            assert plan.successor_dispatch_key is not None
            successor = replace(
                snapshot,
                stage_run_id=plan.next_stage_run_id,
                stage_key=plan.next_stage_key,
                input_digest=plan.artifact_digest,
                dispatch_key=plan.successor_dispatch_key,
                status=str(StageStatus.PENDING),
                state_version=0,
                fence=None,
                lease_expires_at=None,
                next_attempt_at=None,
                artifact_digest=None,
                output_ref=None,
                next_stage_run_id=None,
            )
            self._repository.put(successor)
            self._org_of[successor.stage_run_id] = principal.org_id
            self._attempts[successor.stage_run_id] = 1
        return CommandOutcome(True, None, self._view(stage_run_id))

    def stages(self, job_id: str, *, principal: CallerPrincipalV1) -> list[StageRunView]:
        views = [
            self._view(stage_id)
            for stage_id, org in self._org_of.items()
            if org == principal.org_id
        ]
        order = {stage.key: index for index, stage in enumerate(self._workflow.stages)}
        return sorted(
            (view for view in views if view is not None and view.job_id == job_id),
            key=lambda view: order[view.stage],
        )

    def get(self, stage_run_id: str, *, principal: CallerPrincipalV1) -> StageRunView | None:
        if self._org_of.get(stage_run_id) != principal.org_id:
            return None
        return self._view(stage_run_id)

    def _view(self, stage_run_id: str) -> StageRunView | None:
        snapshot = self._repository.load(stage_run_id)
        if snapshot is None:
            return None
        return to_view(
            snapshot,
            workflow_key=self._workflow.workflow_key,
            attempts=self._attempts.get(stage_run_id, 1),
        )
