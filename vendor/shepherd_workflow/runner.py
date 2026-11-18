"""The one place a decision becomes a durable fact.

`reduce()` decides and `StageRepository` stores; this joins them and is the only
code in the package that does both. Keeping it this small is deliberate -- a
runner that grows business rules has started to duplicate the reducer somewhere
untestable.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from shepherd_contracts import CallerPrincipalV1, Clock, ReceiptV1, derive_id
from shepherd_contracts.unit_of_work import UnitOfWork, assert_writes_within

from shepherd_workflow.commands import CommandV1, LedgerSnapshotV1
from shepherd_workflow.ports import OWNED_TABLES, OutboxSink, StageRepository
from shepherd_workflow.reducer import TransitionPlanV1, reduce
from shepherd_workflow.state import RefusalClass
from shepherd_workflow.workflow import WorkflowDefinitionV1


@dataclass(frozen=True, slots=True)
class ApplyResult:
    """The outcome of one command, refusal included.

    A refusal is returned, not raised. It is an ordinary, expected outcome -- a
    worker losing a race refuses many times an hour -- and raising would push
    routine contention into exception handling where it reads as breakage.
    """

    plan: TransitionPlanV1
    snapshot: LedgerSnapshotV1 | None
    receipt: ReceiptV1

    @property
    def accepted(self) -> bool:
        return self.plan.accepted

    @property
    def refusal(self) -> RefusalClass | None:
        return self.plan.refusal_class


class StageRunner:
    """Applies commands to stage runs. Constructed once, per workflow."""

    def __init__(
        self,
        *,
        workflow: WorkflowDefinitionV1,
        repository: StageRepository,
        outbox: OutboxSink,
        clock: Clock,
        tables: frozenset[str] = OWNED_TABLES,
    ) -> None:
        self._workflow = workflow
        self._repository = repository
        self._outbox = outbox
        self._clock = clock
        # The tables THIS deployment writes. Defaults to the default prefix, but a
        # host that built its schema with a custom prefix -- a multi-tenant host may give
        # each app its own -- must pass `owned_tables(its_prefix)`, or the ownership check
        # below would refuse it the very tables it just created.
        self._tables = tables

    @property
    def workflow(self) -> WorkflowDefinitionV1:
        return self._workflow

    @property
    def repository(self) -> StageRepository:
        """Exposed so a worker can claim work without reaching into privates."""
        return self._repository

    @property
    def clock(self) -> Clock:
        return self._clock

    def apply(
        self,
        command: CommandV1,
        *,
        principal: CallerPrincipalV1,
        uow: UnitOfWork,
        required_scope: str = "jobs:write",
    ) -> ApplyResult:
        """Decide ``command``, and if accepted, persist it and enqueue its events."""
        principal.require(required_scope)
        assert_writes_within(uow, self._tables, package="shepherd-workflow")

        now: datetime = self._clock.now()
        snapshot = self._repository.load(command.stage_run_id)
        if snapshot is None:
            plan = TransitionPlanV1.refuse(RefusalClass.UNKNOWN_STAGE)
            return ApplyResult(plan, None, self._receipt(command, plan, principal, now))

        plan = reduce(self._workflow, snapshot, command, now=now)
        if not plan.accepted:
            return ApplyResult(plan, snapshot, self._receipt(command, plan, principal, now))

        updated = self._repository.apply(snapshot, plan, principal=principal)
        for event_class in plan.event_classes:
            self._outbox.enqueue(
                key=self._event_key(command, event_class),
                event_class=event_class,
                payload={
                    "org_id": principal.org_id,
                    "occurred_at": now.isoformat(),
                    "workflow_digest": snapshot.workflow_digest,
                    "fence": updated.fence,
                    "state_version": updated.state_version,
                    "job_id": snapshot.job_id,
                    "stage_run_id": snapshot.stage_run_id,
                    "stage_key": snapshot.stage_key,
                    "status": str(plan.next_status),
                    "artifact_digest": plan.artifact_digest,
                    "output_ref": plan.output_ref,
                    "next_attempt_at": (
                        plan.next_attempt_at.isoformat() if plan.next_attempt_at else None
                    ),
                    "next_stage_key": plan.next_stage_key,
                    "successor_dispatch_key": plan.successor_dispatch_key,
                },
            )
        return ApplyResult(plan, updated, self._receipt(command, plan, principal, now))

    def _event_key(self, command: CommandV1, event_class: str) -> str:
        """Name an event by the transition that produced it, not by when it fired.

        A retry of the same command produces the same key, so a dispatcher that
        delivers at-least-once can be de-duplicated downstream into effectively
        once.
        """
        return derive_id(
            "evt",
            command.stage_run_id,
            command.expected_state_version,
            str(command.kind),
            event_class,
        )

    def _receipt(
        self,
        command: CommandV1,
        plan: TransitionPlanV1,
        principal: CallerPrincipalV1,
        now: datetime,
    ) -> ReceiptV1:
        return ReceiptV1(
            receipt_id=derive_id(
                "rcpt", command.stage_run_id, command.expected_state_version, str(command.kind)
            ),
            org_id=principal.org_id,
            kind="jobs.transition",
            occurred_at=now,
            idempotency_key=derive_id(
                "idem", command.stage_run_id, command.expected_state_version, str(command.kind)
            ),
            outcome="succeeded" if plan.accepted else "refused",
            detail={
                "stage_key": command.stage_key,
                "command": str(command.kind),
                "refusal_class": str(plan.refusal_class) if plan.refusal_class else None,
            },
        )
