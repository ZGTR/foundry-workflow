"""What this service needs from the durable workflow engine, as a protocol.

WHY A PORT AT ALL, when `shepherd_workflow` is already a package with its own
ports. Because the package's ports face the OTHER way: `StageRepository` and
`OutboxSink` are what the package needs from ITS host. This one is what the
HTTP layer needs from the engine — start a job, submit a command against a stage
run, read a stage run back — so the controller depends on a contract this
repository owns rather than on the package's concrete `StageRunner`.

The practical consequence is the one that matters: `tests/shape/test_layering.py`
already forbids a controller importing an adapter, and this keeps that true
without the controller having to know that a `UnitOfWork` exists, that commands
carry fences, or that the reducer is pure. The engine's vocabulary stays inside
the engine.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from shepherd_contracts.principal import CallerPrincipalV1


@dataclass(frozen=True, slots=True)
class StageRunView:
    """A stage run as a CALLER sees it: no fence arithmetic, no dispatch key.

    `state_version` IS exposed, because a caller that wants to submit a command
    must state which version it read — that is what makes two racing callers
    safe. `fence` is exposed for the same reason and no other: a worker that was
    granted a lease must echo the fence it was granted, or its late write would
    be indistinguishable from a current one.
    """

    stage_run_id: str
    job_id: str
    workflow: str
    stage: str
    status: str
    state_version: int
    fence: int | None
    attempts: int
    workflow_digest: str = ""
    input_digest: str = ""
    artifact_digest: str | None = None
    output_ref: str | None = None
    next_stage_run_id: str | None = None
    next_attempt_at: datetime | None = None
    lease_expires_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class CommandOutcome:
    """Accepted, or refused with the class that names WHY.

    A refusal is an ANSWER, not an error — the same rule governance follows for a
    deny. `refusal` is None exactly when `accepted` is True; a caller reading
    only `accepted` still cannot mistake a refusal for a failure, because the
    HTTP layer maps refusal to 409/422 and a real fault to 5xx.
    """

    accepted: bool
    refusal: str | None
    view: StageRunView | None


class UnknownCommandKind(ValueError):
    """The caller named a command this workflow engine does not implement.

    Distinct from a refusal on purpose. A refusal means the engine understood the
    command and declined it; this means the request never named a command at all,
    which is a malformed request (422) rather than a state conflict (409).
    """


@runtime_checkable
class WorkflowEnginePort(Protocol):
    """Start a job; submit a command; read a stage run."""

    def start_job(
        self,
        *,
        job_id: str,
        input_digest: str,
        principal: CallerPrincipalV1,
    ) -> StageRunView:
        """Create ``job_id`` at the workflow's first stage and return that stage run.

        Idempotent by the dispatch key, not by a caller-supplied flag: starting the
        same job over the same input twice returns the SAME stage run rather than
        creating a second one. Two API calls racing on a cold job is the case this
        is for, and it is resolved by the database, not by a check-then-insert.
        """
        ...

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
        """Apply one command. Raises `UnknownCommandKind` if ``kind`` is not a command."""
        ...

    def stages(self, job_id: str, *, principal: CallerPrincipalV1) -> list[StageRunView]:
        """Return this tenant's stages in definition order."""
        ...

    def get(self, stage_run_id: str, *, principal: CallerPrincipalV1) -> StageRunView | None:
        """Return the stage run, or None when this principal may not see it.

        None covers BOTH "no such stage run" and "it belongs to another org", and
        that is deliberate. Answering 403 for the second would confirm the id
        exists, which is the leak the scoping exists to prevent: an id is
        derivable from a dispatch key, so an attacker who can tell present from
        absent can enumerate another tenant's jobs without ever reading one.
        """
        ...
