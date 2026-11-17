"""SQL implementations of the ports, reproducing the in-memory reference exactly.

The one thing worth reading closely is `apply()`. Everything else is mechanical.
"""

from __future__ import annotations

from datetime import UTC, datetime

from shepherd_contracts import CallerPrincipalV1, Clock
from sqlalchemy import Table, select, update
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError

from shepherd_workflow.commands import LedgerSnapshotV1
from shepherd_workflow.ports import ConcurrentUpdate
from shepherd_workflow.reducer import TransitionPlanV1


class SqlAlchemyUnitOfWork:
    """Wraps a connection the CALLER opened, and never commits it.

    That restraint is the point of the whole port. The caller may be writing its
    own rows in the same transaction, and only the caller knows whether that
    work also succeeded, so only the caller may decide the outcome.
    """

    def __init__(self, connection: Connection, tables: dict[str, Table]) -> None:
        self._connection = connection
        self._tables = tables

    def session(self) -> Connection:
        return self._connection

    def owned_tables(self) -> frozenset[str]:
        return frozenset(t.name for t in self._tables.values())


class SqlAlchemyStageRepository:
    def __init__(self, connection: Connection, tables: dict[str, Table]) -> None:
        self._c = connection
        self._stage_runs = tables["stage_runs"]

    def load(self, stage_run_id: str) -> LedgerSnapshotV1 | None:
        row = (
            self._c.execute(
                select(self._stage_runs).where(self._stage_runs.c.stage_run_id == stage_run_id)
            )
            .mappings()
            .first()
        )
        return None if row is None else _to_snapshot(row)

    def apply(
        self,
        snapshot: LedgerSnapshotV1,
        plan: TransitionPlanV1,
        *,
        principal: CallerPrincipalV1,
    ) -> LedgerSnapshotV1:
        """Conditional UPDATE on state_version. One statement, no read-then-write.

        The WHERE clause carries the version the caller decided against, so the
        database resolves the race rather than the application: exactly one of
        two concurrent writers matches a row, and the other sees rowcount 0.

        Doing this as SELECT-then-UPDATE would reopen the window it closes, and
        would still pass every test that runs one writer -- which is why the test
        for this drives two.
        """
        t = self._stage_runs
        values: dict[str, object] = {"state_version": snapshot.state_version + 1}
        if plan.next_status is not None:
            values["status"] = str(plan.next_status)
        if plan.next_fence is not None:
            values["fence"] = plan.next_fence
        # UNCONDITIONAL, matching `InMemoryStageRepository.apply`: a LEASE carries a
        # new expiry and every other transition clears it, because a stage that is
        # no longer leased must not keep a stale deadline a later read tests against.
        #
        # This line was ABSENT until 2026-09-05, and its absence is
        # the shape worth remembering rather than the typo. `lease_expires_at` was
        # in the schema, deserialised by `_to_snapshot`, and read by
        # `reducer._lease_check` -- everything a reader needs to conclude it works --
        # but nothing ever WROTE it here, so the column was NULL forever and
        # `RefusalClass.LEASE_EXPIRED` was unreachable in every SQL deployment while
        # the in-memory reference refused correctly. A conditional write would
        # reintroduce half of it: the clear on a non-lease transition is what stops
        # a completed stage from carrying a lease deadline into its next lease.
        values["lease_expires_at"] = plan.next_lease_expires_at
        values["next_attempt_at"] = plan.next_attempt_at
        if plan.artifact_digest is not None:
            values["artifact_digest"] = plan.artifact_digest
            values["output_ref"] = plan.output_ref
            values["next_stage_run_id"] = plan.next_stage_run_id

        result = self._c.execute(
            update(t)
            .where(t.c.stage_run_id == snapshot.stage_run_id)
            .where(t.c.state_version == snapshot.state_version)
            .where(t.c.org_id == principal.org_id)
            .values(**values)
        )
        if result.rowcount == 0:
            raise ConcurrentUpdate(
                f"{snapshot.stage_run_id} was not at version {snapshot.state_version} "
                f"for org {principal.org_id}"
            )
        updated = self.load(snapshot.stage_run_id)
        assert updated is not None  # the UPDATE above matched, so the row exists
        return updated

    def claim_next(self, *, dispatch_key: str) -> LedgerSnapshotV1 | None:
        t = self._stage_runs
        row = self._c.execute(select(t).where(t.c.dispatch_key == dispatch_key)).mappings().first()
        return None if row is None else _to_snapshot(row)


class SqlAlchemyOutbox:
    """Writes events into the caller's transaction; a duplicate key is a no-op.

    The primary key does the de-duplication, so a retried enqueue is refused by
    the database rather than remembered by the application. `IntegrityError` here
    means "already enqueued", which is success, not failure.
    """

    def __init__(
        self, connection: Connection, tables: dict[str, Table], *, clock: Clock, org_id: str
    ) -> None:
        self._c = connection
        self._outbox = tables["outbox"]
        self._clock = clock
        self._org_id = org_id
        self.duplicates_suppressed = 0

    def enqueue(self, *, key: str, event_class: str, payload: dict) -> None:
        savepoint = self._c.begin_nested()
        try:
            self._c.execute(
                self._outbox.insert().values(
                    key=key,
                    org_id=self._org_id,
                    event_class=event_class,
                    payload=payload,
                    created_at=self._clock.now(),
                    sent_at=None,
                    ambiguous=False,
                )
            )
        except IntegrityError:
            savepoint.rollback()
            self.duplicates_suppressed += 1
        else:
            savepoint.commit()

    def pending(self, limit: int = 100) -> list[dict]:
        t = self._outbox
        rows = (
            self._c.execute(
                select(t)
                .where(t.c.sent_at.is_(None))
                .where(t.c.org_id == self._org_id)
                .order_by(t.c.created_at)
                .limit(limit)
            )
            .mappings()
            .all()
        )
        return [
            {
                "key": r["key"],
                "event_class": r["event_class"],
                "payload": {
                    "occurred_at": r["created_at"].replace(tzinfo=UTC).isoformat(),
                    **r["payload"],
                },
            }
            for r in rows
        ]

    def mark_sent(self, key: str) -> None:
        self._c.execute(
            update(self._outbox).where(self._outbox.c.key == key).values(sent_at=self._clock.now())
        )

    def mark_ambiguous(self, key: str) -> None:
        self._c.execute(
            update(self._outbox)
            .where(self._outbox.c.key == key)
            .values(sent_at=self._clock.now(), ambiguous=True)
        )


def _to_snapshot(row) -> LedgerSnapshotV1:
    return LedgerSnapshotV1(
        workflow_digest=row["workflow_digest"],
        job_id=row["job_id"],
        stage_run_id=row["stage_run_id"],
        stage_key=row["stage_key"],
        state_version=row["state_version"],
        fence=row["fence"],
        attempt_id=row["attempt_id"],
        dispatch_key=row["dispatch_key"],
        input_digest=row["input_digest"],
        status=row["status"],
        deadline_at=_aware(row["deadline_at"]),
        lease_expires_at=_aware(row["lease_expires_at"]),
        feature_enabled=bool(row["feature_enabled"]),
        cancelled=bool(row["cancelled"]),
        next_attempt_at=_aware(row["next_attempt_at"]),
        artifact_digest=row["artifact_digest"],
        output_ref=row["output_ref"],
        next_stage_run_id=row["next_stage_run_id"],
    )


def _aware(value: datetime | None) -> datetime | None:
    """SQLite returns naive datetimes even for timezone-aware columns.

    Left explicit rather than hidden in a type decorator, because a naive
    datetime reaching the reducer raises, and a reader tracing that error should
    find the reason in one hop.
    """
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
