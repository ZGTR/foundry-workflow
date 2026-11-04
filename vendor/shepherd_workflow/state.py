"""The states a stage run can be in, and what may follow what.

Kept separate from the reducer so the vocabulary can be read on its own. The
terminal set is the load-bearing part: a terminal state accepts no command, and
that single rule is what stops a cancelled job from being resumed by a worker
that was holding a stale lease when the cancellation landed.
"""

from __future__ import annotations

from enum import StrEnum


class StageStatus(StrEnum):
    PENDING = "pending"
    RETRY_WAIT = "retry_wait"
    LEASED = "leased"
    SUCCEEDED = "succeeded"
    REVIEW_WAIT = "review_wait"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


TERMINAL_STATES: frozenset[StageStatus] = frozenset(
    {
        StageStatus.REVIEW_WAIT,
        StageStatus.FAILED,
        StageStatus.CANCELLED,
        StageStatus.EXPIRED,
    }
)


def is_terminal(status: StageStatus | str) -> bool:
    return StageStatus(status) in TERMINAL_STATES


class RefusalClass(StrEnum):
    """Why a command was not accepted.

    Every refusal is named, and the names are part of the contract: an operator
    reading a refusal must be able to tell, without reading code, whether the
    command was wrong, stale, too late, or forbidden -- because the response to
    each differs. An unnamed refusal is an outage nobody can triage.
    """

    UNKNOWN_STAGE = "unknown_stage"
    WORKFLOW_DIGEST_MISMATCH = "workflow_digest_mismatch"
    TERMINAL_STATE = "terminal_state"
    STALE_FENCE = "stale_fence"
    STATE_VERSION_CONFLICT = "state_version_conflict"
    LEASE_NOT_HELD = "lease_not_held"
    LEASE_HELD = "lease_held"
    RETRY_NOT_DUE = "retry_not_due"
    ATTEMPTS_EXHAUSTED = "attempts_exhausted"
    LEASE_EXPIRED = "lease_expired"
    DEADLINE_PASSED = "deadline_passed"
    FEATURE_DISABLED = "feature_disabled"
    CANCELLED = "cancelled"
    DISPATCH_KEY_MISMATCH = "dispatch_key_mismatch"
