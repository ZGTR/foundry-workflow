"""In-memory implementations of every port.

These are not toys and they are not only for tests. They are the reference
semantics: the compare-and-set in `InMemoryStageRepository.apply` is the exact
behaviour a SQL adapter must reproduce, written in eight readable lines instead
of a dialect. When a real adapter and these disagree, the real adapter is wrong.

They are also what makes the package demonstrable end to end with no database,
which is the difference between a library someone can evaluate in a minute and
one they have to provision infrastructure to try.
"""

from __future__ import annotations

from dataclasses import replace

from shepherd_contracts import CallerPrincipalV1

from shepherd_workflow.commands import LedgerSnapshotV1
from shepherd_workflow.ports import OWNED_TABLES, ConcurrentUpdate
from shepherd_workflow.reducer import TransitionPlanV1


class InMemoryUnitOfWork:
    """A unit of work that grants exactly the tables shepherd-workflow owns."""

    def __init__(self, tables: frozenset[str] = OWNED_TABLES) -> None:
        self._tables = tables

    def session(self) -> object:
        return self

    def owned_tables(self) -> frozenset[str]:
        return self._tables


class InMemoryStageRepository:
    """Reference storage semantics, including the compare-and-set."""

    def __init__(self, snapshots: dict[str, LedgerSnapshotV1] | None = None) -> None:
        self._rows: dict[str, LedgerSnapshotV1] = dict(snapshots or {})
        self._claimed: set[str] = set()
        self.applied: list[TransitionPlanV1] = []

    def put(self, snapshot: LedgerSnapshotV1) -> None:
        self._rows[snapshot.stage_run_id] = snapshot

    def load(self, stage_run_id: str) -> LedgerSnapshotV1 | None:
        return self._rows.get(stage_run_id)

    def apply(
        self,
        snapshot: LedgerSnapshotV1,
        plan: TransitionPlanV1,
        *,
        principal: CallerPrincipalV1,
    ) -> LedgerSnapshotV1:
        current = self._rows.get(snapshot.stage_run_id)
        if current is None:
            raise ConcurrentUpdate(f"{snapshot.stage_run_id} no longer exists")
        if current.state_version != snapshot.state_version:
            raise ConcurrentUpdate(
                f"{snapshot.stage_run_id} moved from version {snapshot.state_version} "
                f"to {current.state_version}"
            )
        updated = replace(
            current,
            status=str(plan.next_status) if plan.next_status else current.status,
            state_version=current.state_version + 1,
            fence=plan.next_fence if plan.next_fence is not None else current.fence,
            # A LEASE transition carries a new expiry; every other transition
            # clears it, because a stage that is no longer leased must not keep a
            # stale deadline that a later read would test against.
            lease_expires_at=plan.next_lease_expires_at,
            next_attempt_at=plan.next_attempt_at,
            artifact_digest=plan.artifact_digest or current.artifact_digest,
            output_ref=plan.output_ref or current.output_ref,
            next_stage_run_id=plan.next_stage_run_id or current.next_stage_run_id,
        )
        self._rows[snapshot.stage_run_id] = updated
        self.applied.append(plan)
        return updated

    def claim_next(self, *, dispatch_key: str) -> LedgerSnapshotV1 | None:
        if dispatch_key in self._claimed:
            return None
        for row in self._rows.values():
            if row.dispatch_key == dispatch_key:
                self._claimed.add(dispatch_key)
                return row
        return None


class InMemoryOutbox:
    """An outbox that de-duplicates by key, exactly as a UNIQUE index would."""

    def __init__(self) -> None:
        self.rows: list[dict] = []
        self._keys: set[str] = set()
        self.duplicates_suppressed = 0

    def enqueue(self, *, key: str, event_class: str, payload: dict) -> None:
        if key in self._keys:
            self.duplicates_suppressed += 1
            return
        self._keys.add(key)
        self.rows.append({"key": key, "event_class": event_class, "payload": payload})

    def drain(self) -> list[dict]:
        rows, self.rows = self.rows, []
        return rows
