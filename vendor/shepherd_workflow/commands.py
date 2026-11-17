"""What a caller can ask of a stage run, and the snapshot it is asked against.

Every command carries the state it believes it is acting on -- the fence it
holds and the state version it read. The reducer compares those against the
snapshot rather than trusting them, which is what makes two workers racing on
the same stage safe: the loser's fence is stale and its command is refused
without any effect.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType


class CommandKind(StrEnum):
    START = "start"
    LEASE = "lease"
    COMPLETE = "complete"
    FAIL = "fail"
    TIMEOUT = "timeout"
    CANCEL = "cancel"
    EXPIRE = "expire"


@dataclass(frozen=True, slots=True)
class LedgerSnapshotV1:
    """Everything the reducer is allowed to know about current state.

    Deliberately a flat value with no repository handle. The reducer cannot
    perform a query, so it cannot depend on anything not written here, so its
    decisions are reproducible from the snapshot alone -- which is what makes
    every branch below testable without a database.
    """

    workflow_digest: str
    job_id: str
    stage_run_id: str
    stage_key: str
    state_version: int
    fence: int | None
    attempt_id: str
    dispatch_key: str
    input_digest: str
    status: str
    deadline_at: datetime
    lease_expires_at: datetime | None = None
    feature_enabled: bool = True
    cancelled: bool = False
    next_attempt_at: datetime | None = None
    artifact_digest: str | None = None
    output_ref: str | None = None
    next_stage_run_id: str | None = None


@dataclass(frozen=True, slots=True)
class CommandV1:
    """A request to move a stage run, plus the caller's view of where it was."""

    kind: CommandKind
    job_id: str
    stage_run_id: str
    stage_key: str
    workflow_digest: str
    issued_at: datetime
    expected_state_version: int
    fence: int | None = None
    dispatch_key: str | None = None
    payload_digest: str | None = None
    provider_pin: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))
    reason: str | None = None
    retryable: bool = False
    output_ref: str | None = None

    def __post_init__(self) -> None:
        if self.issued_at.tzinfo is None:
            raise ValueError("CommandV1.issued_at must be timezone-aware")
        object.__setattr__(self, "provider_pin", MappingProxyType(dict(self.provider_pin)))
