"""What shepherd-workflow needs from its host, stated as protocols.

The list is short on purpose. Every entry here is a thing the host must provide,
and each one is a place a host could get it wrong, so a port that could be
avoided is a liability rather than a feature.

Note what is *not* here: no connection string, no table names, no migration
runner, no environment. The host owns storage entirely; this package owns the
decision about what should be stored.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from shepherd_contracts import CallerPrincipalV1

from shepherd_workflow.commands import LedgerSnapshotV1
from shepherd_workflow.reducer import TransitionPlanV1

#: The LOGICAL names of the tables this package writes, without any prefix. This
#: tuple is the single source of truth: `sql.schema.build_tables` names its five
#: tables from it, and `owned_tables` derives the ownership set from it, so the
#: two can never drift apart.
LOGICAL_TABLES: tuple[str, ...] = ("jobs", "stage_runs", "attempts", "events", "outbox")

#: The default physical prefix.
#:
#: `wf_` rather than no prefix at all, and the reason is measured rather than
#: assumed. The original host's 59 tables are UNPREFIXED plain nouns --
#: `organisations`, `users`, `agent_runs`, `brain_nodes`, `artifacts`, `decisions`
#: -- so an unprefixed `jobs`, `events`, `outbox`, `attempts` or `stage_runs` from
#: this package would land in exactly that namespace, where a future domain table
#: named `events` or `jobs` is entirely plausible.
#:
#: An earlier version of this comment justified the choice by claiming the host
#: "already owns `shepherd_*` names". That is FALSE: zero host tables carry a
#: `shepherd_` prefix (`grep __tablename__` -> 59 tables, 0 matches, against a
#: positive control of 2 tables containing "agent"). The prefix is right; the
#: reason given for it was not.
DEFAULT_TABLE_PREFIX = "wf_"


def owned_tables(prefix: str = DEFAULT_TABLE_PREFIX) -> frozenset[str]:
    """The tables this package writes under ``prefix``.

    `assert_writes_within` checks a host's unit of work against exactly this set,
    which is what keeps "shares a database instance" from quietly becoming
    "shares a schema".

    It is a FUNCTION of the prefix, not a constant, because `build_tables` accepts
    any prefix -- a physically multi-tenant host can give each app its own
    table prefix. A hardcoded set is only correct for one deployment: a
    host running `build_tables(prefix="app_")` would be refused its own tables by
    an ownership check naming `wf_*`, and the failure would surface as a
    TableOwnershipViolation against tables the package itself had just created.
    """
    return frozenset(f"{prefix}{name}" for name in LOGICAL_TABLES)


#: The ownership set under the default prefix, for hosts that do not re-prefix.
OWNED_TABLES: frozenset[str] = owned_tables()


@runtime_checkable
class StageRepository(Protocol):
    """Reads a snapshot and durably applies a plan. The host implements this."""

    def load(self, stage_run_id: str) -> LedgerSnapshotV1 | None:
        ...

    def apply(
        self,
        snapshot: LedgerSnapshotV1,
        plan: TransitionPlanV1,
        *,
        principal: CallerPrincipalV1,
    ) -> LedgerSnapshotV1:
        """Persist ``plan`` against ``snapshot`` and return the new snapshot.

        Must be conditional on ``snapshot.state_version``: if the row moved since
        it was read, this raises ``ConcurrentUpdate`` rather than overwriting.
        The reducer already refused stale commands, but two callers can pass the
        reducer simultaneously with the same version -- the reducer is pure and
        cannot see the other one. The compare-and-set here is what closes that,
        and a repository that does a blind UPDATE has silently removed the
        package's concurrency guarantee while passing every test that does not
        run two writers.
        """
        ...

    def claim_next(self, *, dispatch_key: str) -> LedgerSnapshotV1 | None:
        """Return the stage run for ``dispatch_key``, or None if already taken."""
        ...


@runtime_checkable
class OutboxSink(Protocol):
    """Where accepted events go, written in the SAME transaction as the plan.

    That co-location is the whole point. An event published to a broker directly
    is published even when the transaction that produced it rolls back, and a
    transaction that commits without publishing loses the event. Writing the
    event to a table the caller is already committing makes the two atomic; a
    separate dispatcher then moves rows to the broker at-least-once.
    """

    def enqueue(self, *, key: str, event_class: str, payload: dict) -> None:
        ...


class ConcurrentUpdate(Exception):
    """The row moved between the read and the write. Re-read and re-decide."""
