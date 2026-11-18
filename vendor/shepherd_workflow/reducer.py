"""The pure decision: given a snapshot and a command, what should happen?

Nothing here touches a database, a clock or a network. It is a function from
(workflow, snapshot, command, now) to a plan, so every refusal branch below is
reachable in a unit test with three literals and no fixtures.

The separation is not stylistic. In the system this was extracted from, the
equivalent logic lives in a pure domain reducer module while the code that
persists its output lives in a separate transition service. That split is the
reason the state machine can be exhaustively tested at all, and it is the single
most valuable thing the original design has to teach; it is preserved here
deliberately.

Order of checks matters and is asserted by tests. Identity is checked before
freshness, freshness before authority, authority before time -- so a command
naming the wrong workflow is refused as a workflow mismatch even if it is also
stale, and an operator reading the refusal learns the root fault rather than a
downstream symptom.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import MappingProxyType

from shepherd_contracts import canonical_digest, derive_id

from shepherd_workflow.commands import CommandKind, CommandV1, LedgerSnapshotV1
from shepherd_workflow.state import RefusalClass, StageStatus, is_terminal
from shepherd_workflow.workflow import WorkflowDefinitionV1

#: How long a granted lease is valid. Five minutes: long enough for a stage
#: executor to finish a provider call, short enough that a crashed worker
#: does not hold a stage past a human's patience. A caller with a different
#: shape passes its own to `reduce()`; the FENCE, not this number, is what
#: makes a late write safe.
DEFAULT_LEASE_DURATION = timedelta(minutes=5)


@dataclass(frozen=True, slots=True)
class TransitionPlanV1:
    """What the caller should durably write, or why it should write nothing.

    A refused plan is a complete answer, not an error: it names its reason and
    guarantees no side effect. Callers persist an accepted plan inside their own
    transaction and drop a refused one after recording the refusal class.
    """

    accepted: bool
    refusal_class: RefusalClass | None = None
    next_status: StageStatus | None = None
    event_classes: tuple[str, ...] = ()
    artifact_digest: str | None = None
    next_stage_key: str | None = None
    successor_dispatch_key: str | None = None
    next_fence: int | None = None
    #: When the lease this transition grants stops being valid, or None when the
    #: transition grants no lease. NOTHING WROTE THIS FIELD BEFORE:
    #: `LedgerSnapshotV1.lease_expires_at` was read by `_lease_check` and
    #: deserialised by the SQL adapter, but no code path ever SET it -- so it was
    #: always NULL, the expiry branch never fired, and "the lease expires and
    #: another worker picks the stage up" had no implementation. The guarantee
    #: was not merely untested; it was absent, which is why an untested guarantee
    #: is worth this much suspicion.
    next_lease_expires_at: datetime | None = None
    next_attempt_at: datetime | None = None
    output_ref: str | None = None
    next_stage_run_id: str | None = None
    provider_pin: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        object.__setattr__(self, "provider_pin", MappingProxyType(dict(self.provider_pin)))

    @classmethod
    def refuse(cls, refusal_class: RefusalClass) -> TransitionPlanV1:
        return cls(accepted=False, refusal_class=refusal_class)


def dispatch_key(workflow_digest: str, job_id: str, stage_key: str, input_digest: str) -> str:
    """Name a unit of work by what it is, not by when it was created.

    Two dispatches of the same stage of the same job over the same input are the
    same work, so they get the same key and the second is recognised as a
    duplicate rather than executed twice. This is the idempotency boundary for
    the whole package.
    """
    return canonical_digest({"w": workflow_digest, "j": job_id, "s": stage_key, "i": input_digest})


def artifact_digest(
    workflow_digest: str,
    stage_key: str,
    input_digest: str,
    provider_pin: Mapping[str, object],
    payload_digest: str,
) -> str:
    """Bind an artifact's identity to what actually produced it.

    Both the provider pin and the returned bytes are in the hash, so two
    artifacts differ exactly when their production differed. A digest computed
    from the stage definition alone would be constant across every run of that
    stage, which makes it useless for deciding whether anything changed.
    """
    return canonical_digest(
        {
            "w": workflow_digest,
            "s": stage_key,
            "i": input_digest,
            "p": dict(provider_pin),
            "b": payload_digest,
        }
    )


def reduce(
    workflow: WorkflowDefinitionV1,
    snapshot: LedgerSnapshotV1,
    command: CommandV1,
    *,
    now: datetime,
    lease_duration: timedelta = DEFAULT_LEASE_DURATION,
) -> TransitionPlanV1:
    """Decide what ``command`` does to ``snapshot``. Pure; safe to call twice."""
    if now.tzinfo is None:
        raise ValueError("reduce() requires a timezone-aware `now`")

    # --- identity: is this command even about this workflow and stage? -------
    if command.workflow_digest != workflow.digest or snapshot.workflow_digest != workflow.digest:
        return TransitionPlanV1.refuse(RefusalClass.WORKFLOW_DIGEST_MISMATCH)
    stage = workflow.stage(command.stage_key)
    if stage is None or command.stage_key != snapshot.stage_key:
        return TransitionPlanV1.refuse(RefusalClass.UNKNOWN_STAGE)

    # --- cancellation outranks everything that is not already terminal ------
    if snapshot.cancelled and command.kind is not CommandKind.CANCEL:
        return TransitionPlanV1.refuse(RefusalClass.CANCELLED)
    if is_terminal(snapshot.status) or snapshot.status == StageStatus.SUCCEEDED:
        return TransitionPlanV1.refuse(RefusalClass.TERMINAL_STATE)

    # --- freshness: is the caller acting on the state it thinks it read? -----
    if command.expected_state_version != snapshot.state_version:
        return TransitionPlanV1.refuse(RefusalClass.STATE_VERSION_CONFLICT)

    # A flag gates ADMISSION, not COMPLETION. Turning a feature off must stop the
    # next stage from starting; it must NOT discard a stage whose provider call
    # has already been made and already been billed. Allowing the completion is
    # safe because COMPLETE requires a held lease at a matching fence, and a
    # lease can only have been granted while the flag was on -- so a completion
    # is never a way past a disabled flag, only the tail of work it admitted.
    if not snapshot.feature_enabled and command.kind in (CommandKind.START, CommandKind.LEASE):
        return TransitionPlanV1.refuse(RefusalClass.FEATURE_DISABLED)

    if command.kind is CommandKind.CANCEL:
        return TransitionPlanV1(
            accepted=True,
            next_status=StageStatus.CANCELLED,
            event_classes=(
                ("stage.cancelled", "job.cancelled")
                if workflow.contract_digest
                else ("stage.cancelled",)
            ),
        )

    if command.kind is CommandKind.EXPIRE:
        if now < snapshot.deadline_at:
            return TransitionPlanV1.refuse(RefusalClass.DEADLINE_PASSED)
        return TransitionPlanV1(
            accepted=True,
            next_status=StageStatus.EXPIRED,
            event_classes=(
                ("stage.expired", "job.expired") if workflow.contract_digest else ("stage.expired",)
            ),
        )

    if now >= snapshot.deadline_at:
        return TransitionPlanV1.refuse(RefusalClass.DEADLINE_PASSED)

    if command.kind is CommandKind.LEASE:
        if (snapshot.fence or 0) >= stage.max_attempts:
            return TransitionPlanV1.refuse(RefusalClass.ATTEMPTS_EXHAUSTED)
        return _lease(
            snapshot,
            command,
            now,
            timedelta(seconds=stage.timeout_seconds)
            if stage.timeout_seconds != 300
            else lease_duration,
        )

    # COMPLETE and FAIL both require the caller to still hold the lease.
    if command.kind is CommandKind.TIMEOUT:
        if snapshot.status != StageStatus.LEASED or command.fence != snapshot.fence:
            return TransitionPlanV1.refuse(RefusalClass.STALE_FENCE)
        if snapshot.lease_expires_at is None or now < snapshot.lease_expires_at:
            return TransitionPlanV1.refuse(RefusalClass.LEASE_HELD)
        held = None
    else:
        held = _lease_check(snapshot, command, now)
    if held is not None:
        return held

    if command.kind in (CommandKind.FAIL, CommandKind.TIMEOUT):
        if command.retryable and (snapshot.fence or 0) < stage.max_attempts:
            return TransitionPlanV1(
                accepted=True,
                next_status=StageStatus.RETRY_WAIT,
                event_classes=("stage.retry_scheduled",),
                next_attempt_at=now + timedelta(seconds=stage.retry_delay_seconds),
            )
        return TransitionPlanV1(
            accepted=True,
            next_status=StageStatus.FAILED,
            event_classes=(
                ("stage.failed", "job.failed") if workflow.contract_digest else ("stage.failed",)
            ),
        )

    if command.kind is CommandKind.COMPLETE:
        return _complete(workflow, snapshot, command)

    return TransitionPlanV1.refuse(RefusalClass.UNKNOWN_STAGE)


def _lease(
    snapshot: LedgerSnapshotV1,
    command: CommandV1,
    now: datetime,
    lease_duration: timedelta,
) -> TransitionPlanV1:
    """Grant a lease by minting a strictly higher fence.

    The fence, not the lease expiry, is what makes this safe. A worker whose
    lease expired mid-work still holds its old fence, so when it eventually
    writes, its fence is lower than the current one and the write is refused --
    even though nothing about the clock told it to stop. Expiry decides when
    work may be *handed out* again; the fence decides whose write *counts*.
    """
    if snapshot.status == StageStatus.LEASED and (
        snapshot.lease_expires_at is None or now < snapshot.lease_expires_at
    ):
        return TransitionPlanV1.refuse(RefusalClass.LEASE_HELD)
    if snapshot.next_attempt_at is not None and now < snapshot.next_attempt_at:
        return TransitionPlanV1.refuse(RefusalClass.RETRY_NOT_DUE)
    if command.dispatch_key is not None and command.dispatch_key != snapshot.dispatch_key:
        return TransitionPlanV1.refuse(RefusalClass.DISPATCH_KEY_MISMATCH)
    return TransitionPlanV1(
        accepted=True,
        next_status=StageStatus.LEASED,
        event_classes=("stage.leased",),
        next_fence=(snapshot.fence or 0) + 1,
        next_lease_expires_at=min(snapshot.deadline_at, now + lease_duration),
    )


def _lease_check(
    snapshot: LedgerSnapshotV1, command: CommandV1, now: datetime
) -> TransitionPlanV1 | None:
    """Return a refusal if the caller does not hold a current lease, else None."""
    if snapshot.status != StageStatus.LEASED:
        return TransitionPlanV1.refuse(RefusalClass.LEASE_NOT_HELD)
    if command.fence is None or snapshot.fence is None or command.fence != snapshot.fence:
        return TransitionPlanV1.refuse(RefusalClass.STALE_FENCE)
    if snapshot.lease_expires_at is not None and now >= snapshot.lease_expires_at:
        return TransitionPlanV1.refuse(RefusalClass.LEASE_EXPIRED)
    return None


def _complete(
    workflow: WorkflowDefinitionV1, snapshot: LedgerSnapshotV1, command: CommandV1
) -> TransitionPlanV1:
    if command.payload_digest is None:
        return TransitionPlanV1.refuse(RefusalClass.UNKNOWN_STAGE)

    produced = artifact_digest(
        workflow.digest,
        snapshot.stage_key,
        snapshot.input_digest,
        command.provider_pin,
        command.payload_digest,
    )
    nxt = workflow.next_after(snapshot.stage_key)
    if nxt is None:
        return TransitionPlanV1(
            accepted=True,
            next_status=(
                StageStatus.REVIEW_WAIT
                if workflow.completion == "review"
                else StageStatus.SUCCEEDED
            ),
            event_classes=(
                "stage.succeeded",
                "job.review_wait" if workflow.completion == "review" else "job.succeeded",
            ),
            artifact_digest=produced,
            output_ref=command.output_ref,
            provider_pin=command.provider_pin,
        )
    return TransitionPlanV1(
        accepted=True,
        next_status=StageStatus.SUCCEEDED,
        event_classes=("stage.succeeded", "stage.dispatched"),
        artifact_digest=produced,
        output_ref=command.output_ref,
        next_stage_run_id=derive_id(
            "srun", dispatch_key(workflow.digest, snapshot.job_id, nxt.key, produced)
        ),
        next_stage_key=nxt.key,
        successor_dispatch_key=dispatch_key(workflow.digest, snapshot.job_id, nxt.key, produced),
        provider_pin=command.provider_pin,
    )
