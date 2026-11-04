"""Turning an HTTP request into a `CommandV1`, in one place both engines share.

The fake engine and the SQL engine must build the SAME command from the same
request, or the fake stops being evidence about the real one. Two copies of this
translation would drift silently: the copies only disagree on a field the tests
in front of the fake never exercise, which is exactly the field the real engine
then gets wrong in production.

Nothing here reads storage or a clock — `now` and the snapshot are arguments —
so this stays a pure function of its inputs and is testable with three literals.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from shepherd_workflow.commands import CommandKind, CommandV1, LedgerSnapshotV1

from shepherd_workflow_service.ports.workflow_engine import StageRunView, UnknownCommandKind


def build_command(
    *,
    snapshot: LedgerSnapshotV1,
    kind: str,
    state_version: int,
    fence: int | None,
    payload: dict[str, Any],
    now: datetime,
) -> CommandV1:
    """Build the command ``kind`` names, against the stage run as STORED.

    `job_id`, `stage_key` and `workflow_digest` come from the snapshot, never from
    the request. A caller restating them could only ever restate them wrongly: the
    reducer would refuse the mismatch, so the field would be a way to turn a valid
    request into a refusal and never a way to say anything new.
    """
    try:
        command_kind = CommandKind(kind)
    except ValueError as exc:
        raise UnknownCommandKind(
            f"{kind!r} is not a command; expected one of {sorted(k.value for k in CommandKind)}"
        ) from exc

    return CommandV1(
        kind=command_kind,
        job_id=snapshot.job_id,
        stage_run_id=snapshot.stage_run_id,
        stage_key=snapshot.stage_key,
        workflow_digest=snapshot.workflow_digest,
        issued_at=now,
        expected_state_version=state_version,
        fence=fence,
        dispatch_key=payload.get("dispatch_key"),
        payload_digest=payload.get("payload_digest"),
        provider_pin=payload.get("provider_pin") or {},
        reason=payload.get("reason"),
        retryable=payload.get("retryable") is True,
        output_ref=payload.get("output_ref"),
    )


def to_view(snapshot: LedgerSnapshotV1, *, workflow_key: str, attempts: int) -> StageRunView:
    """Project the ledger snapshot onto what a caller is allowed to see."""
    return StageRunView(
        stage_run_id=snapshot.stage_run_id,
        job_id=snapshot.job_id,
        workflow=workflow_key,
        stage=snapshot.stage_key,
        status=snapshot.status,
        state_version=snapshot.state_version,
        fence=snapshot.fence,
        attempts=attempts,
        workflow_digest=snapshot.workflow_digest,
        input_digest=snapshot.input_digest,
        artifact_digest=snapshot.artifact_digest,
        output_ref=snapshot.output_ref,
        next_stage_run_id=snapshot.next_stage_run_id,
        next_attempt_at=snapshot.next_attempt_at,
        lease_expires_at=snapshot.lease_expires_at,
    )
