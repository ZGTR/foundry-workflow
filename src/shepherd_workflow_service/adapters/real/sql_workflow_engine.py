"""The deployed `WorkflowEnginePort`: the vendored package, over a real database.

This adapter WIRES `shepherd_workflow`; it re-implements none of it. Every
decision is `reduce()`, every durable write is `SqlAlchemyStageRepository`, and
the compare-and-set that makes two racing callers safe is the package's single
conditional UPDATE. The code here is transaction boundaries, schema naming and
projection — the three things a host owns and a package cannot.

THE PREFIX RULE, which is the one thing to read closely. `build_tables(prefix=P)`
names the physical tables and `owned_tables(P)` names the set the write-guard
permits, and BOTH take the same `self._prefix`. A literal `wf_` set here would be
correct only at the default: a deployment on `WORKFLOW_TABLE_PREFIX=app_` would
create `app_jobs` and then be refused it by `assert_writes_within`, surfacing as
a TableOwnershipViolation against tables this service had just created. That is
not hypothetical — that exact defect once lived in this package's own
`OWNED_TABLES` constant.

A REFUSAL WRITES NOTHING. The transaction is opened before the reducer runs, and
a refusal leaves it by raising, so it rolls back. A refusal that committed would
still have bumped nothing visible today, but the outbox writes happen inside the
same transaction, and "the command was refused and its events were published" is
the failure this shape rules out structurally rather than by remembering to.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any

from shepherd_contracts import CallerPrincipalV1, Clock, derive_id
from shepherd_contracts.errors import ConfigurationError
from shepherd_workflow.commands import CommandKind, LedgerSnapshotV1
from shepherd_workflow.dispatcher import Broker, DispatchReport, OutboxDispatcher
from shepherd_workflow.ports import ConcurrentUpdate, owned_tables
from shepherd_workflow.reducer import dispatch_key as derive_dispatch_key
from shepherd_workflow.runner import StageRunner
from shepherd_workflow.sql.repository import (
    SqlAlchemyOutbox,
    SqlAlchemyStageRepository,
    SqlAlchemyUnitOfWork,
)
from shepherd_workflow.sql.schema import build_tables
from shepherd_workflow.state import RefusalClass, StageStatus
from shepherd_workflow.workflow import WorkflowDefinitionV1
from sqlalchemy import Engine, MetaData, create_engine, func, inspect, select
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError

from shepherd_workflow_service.errors import StageRunConflict
from shepherd_workflow_service.models.workflow_command import build_command, to_view
from shepherd_workflow_service.ports.workflow_engine import CommandOutcome, StageRunView


class _Refused(Exception):  # noqa: N818 -- not an error; a rollback signal
    """Carries a refusal out of the transaction block so the transaction rolls back."""

    def __init__(self, refusal: str) -> None:
        super().__init__(refusal)
        self.refusal = refusal


class SqlWorkflowEngine:
    """The engine the service actually deploys with."""

    def __init__(
        self,
        *,
        url_provider: Callable[[], str],
        workflow_provider: Callable[[], WorkflowDefinitionV1],
        clock: Clock,
        job_deadline: timedelta,
        table_prefix: str,
    ) -> None:
        # Both providers are CALLABLES, resolved on first use. The container must
        # build with zero I/O (`tests/unit/test_container.py`), and resolving a
        # database password through the secrets port or reading the workflow
        # definition off disk are both I/O.
        self._url_provider = url_provider
        self._workflow_provider = workflow_provider
        self._clock = clock
        self._job_deadline = job_deadline
        self._prefix = table_prefix
        self._metadata = MetaData()
        self._tables = build_tables(self._metadata, prefix=table_prefix)
        self._owned = owned_tables(table_prefix)
        self._engine: Engine | None = None
        self._workflow: WorkflowDefinitionV1 | None = None
        self._schema_ready = False

    # -- lazy resolution ----------------------------------------------------

    @property
    def workflow(self) -> WorkflowDefinitionV1:
        if self._workflow is None:
            self._workflow = self._workflow_provider()
        return self._workflow

    def _sql_engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(self._url_provider(), future=True)
        return self._engine

    def _connect(self) -> Connection:
        engine = self._sql_engine()
        connection = engine.connect()
        if not self._schema_ready:
            # `checkfirst` is the default, so this is a no-op against a schema a
            # migration already created. It exists so a local SQLite run and a
            # fresh container need no provisioning step; a deployed MySQL service
            # runs `scripts/migrate.py` and this finds the tables already there.
            for table in self._tables.values():
                inspector = inspect(connection)
                if inspector.has_table(table.name):
                    existing = {column["name"] for column in inspector.get_columns(table.name)}
                    missing = set(table.columns.keys()) - existing
                    if missing:
                        connection.close()
                        raise ConfigurationError(
                            f"Workflow schema migration required: {table.name} "
                            f"missing {sorted(missing)}"
                        )
            self._metadata.create_all(bind=connection, checkfirst=True)
            connection.commit()
            self._schema_ready = True
        return connection

    # -- WorkflowEnginePort -------------------------------------------------

    def start_job(
        self, *, job_id: str, input_digest: str, principal: CallerPrincipalV1
    ) -> StageRunView:
        principal.require("jobs:write")
        workflow = self.workflow
        stage = workflow.first
        key = derive_dispatch_key(workflow.digest, job_id, stage.key, input_digest)
        stage_run_id = derive_id("srun", key)
        now = self._clock.now()

        with self._connect() as connection:
            try:
                with connection.begin():
                    self._insert_job(connection, job_id, principal, now)
                    runs = self._tables["stage_runs"]
                    first = connection.execute(
                        select(runs.c.input_digest).where(
                            runs.c.job_id == job_id, runs.c.stage_key == stage.key
                        )
                    ).scalar_one_or_none()
                    if first is not None and first != input_digest:
                        raise StageRunConflict("job_identity_conflict")
                    self._insert_stage_run(
                        connection,
                        stage_run_id=stage_run_id,
                        job_id=job_id,
                        stage_key=stage.key,
                        workflow_digest=workflow.digest,
                        dispatch_key=key,
                        input_digest=input_digest,
                        principal=principal,
                        now=now,
                    )
            except IntegrityError:
                # The UNIQUE index on dispatch_key already held this work. Two API
                # calls racing on a cold job is the case; the database resolved it,
                # so the loser reads the winner's row rather than failing.
                pass
            if self._org_of(connection, stage_run_id) != principal.org_id:
                raise StageRunConflict("job_identity_conflict")
            view = self._view(connection, stage_run_id)
        assert view is not None  # inserted above, or already present by dispatch key
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
        refusal: str | None = None
        with self._connect() as connection:
            try:
                with connection.begin():
                    self._apply(
                        connection,
                        stage_run_id=stage_run_id,
                        kind=kind,
                        state_version=state_version,
                        fence=fence,
                        payload=payload,
                        principal=principal,
                    )
            except _Refused as refused:
                refusal = refused.refusal
            except ConcurrentUpdate:
                # Two callers passed the pure reducer at the same version; the
                # conditional UPDATE let exactly one through. The loser gets the
                # same refusal the reducer gives a caller that read a stale row,
                # because from the caller's side they are the same situation.
                refusal = str(RefusalClass.STATE_VERSION_CONFLICT)
            view = (
                self._view(connection, stage_run_id)
                if self._org_of(connection, stage_run_id) == principal.org_id
                else None
            )
        return CommandOutcome(refusal is None, refusal, view)

    def get(self, stage_run_id: str, *, principal: CallerPrincipalV1) -> StageRunView | None:
        with self._connect() as connection:
            if self._org_of(connection, stage_run_id) != principal.org_id:
                # Absent and not-yours answer the same, on purpose: a stage run id
                # is derived from a dispatch key, so a distinguishable answer would
                # let one tenant enumerate another's jobs without reading one.
                return None
            return self._view(connection, stage_run_id)

    def stages(self, job_id: str, *, principal: CallerPrincipalV1) -> list[StageRunView]:
        with self._connect() as connection:
            table = self._tables["stage_runs"]
            ids = (
                connection.execute(
                    select(table.c.stage_run_id).where(
                        table.c.job_id == job_id, table.c.org_id == principal.org_id
                    )
                )
                .scalars()
                .all()
            )
            views = [self._view(connection, stage_id) for stage_id in ids]
        order = {stage.key: index for index, stage in enumerate(self.workflow.stages)}
        return sorted(
            (view for view in views if view is not None),
            key=lambda view: order.get(view.stage, len(order)),
        )

    # -- LedgerOutboxDrain --------------------------------------------------

    def drain_pending(self, *, broker: Broker, batch_size: int = 100) -> DispatchReport:
        """Relay pending outbox rows to `broker`, one committed batch per org.

        WHY PER ORG. `SqlAlchemyOutbox` is constructed WITH an `org_id` and its
        `pending()` filters on it, because on the request path there is always
        exactly one tenant in scope. A drain has no principal and no tenant, so
        the set of orgs is READ from the pending rows and the package's own
        org-scoped reader is then used unchanged for each. The alternative -- a
        cross-tenant SELECT written here -- would be a second implementation of a
        query the package already owns, and the two would agree only until one
        of them changed.

        WHY THE DISPATCHER RATHER THAN A LOOP. Publish, THEN mark. A crash
        between them re-publishes, which the stable key makes recoverable; the
        other order loses the event, which nothing records and nothing can
        recover. That order is `OutboxDispatcher`'s entire content, so this
        calls it instead of restating it.

        WHY A COMMIT PER ORG rather than one at the end: every uncommitted mark
        is a row that will be published again, so committing per org bounds that
        window to one tenant's batch instead of the whole drain.
        """
        outbox = self._tables["outbox"]
        published = ambiguous = failed = 0
        with self._connect() as connection:
            # Read the tenant list in its OWN transaction and close it. A bare
            # SELECT autobegins one on this connection, and the per-org
            # `begin()` below would then raise rather than nest. A row written
            # for a NEW org after this read waits for the next drain, which is
            # the same at-least-once schedule every pending row is already on.
            with connection.begin():
                org_ids = [
                    row[0]
                    for row in connection.execute(
                        select(outbox.c.org_id).where(outbox.c.sent_at.is_(None)).distinct()
                    ).all()
                ]
            for org_id in org_ids:
                sink = SqlAlchemyOutbox(connection, self._tables, clock=self._clock, org_id=org_id)
                with connection.begin():
                    one = OutboxDispatcher(
                        broker=broker,
                        clock=self._clock,
                        mark_sent=sink.mark_sent,
                        mark_ambiguous=sink.mark_ambiguous,
                    ).dispatch(sink.pending(batch_size))
                published += one.published
                ambiguous += one.ambiguous
                failed += one.failed
        return DispatchReport(published=published, ambiguous=ambiguous, failed=failed)

    # -- internals ----------------------------------------------------------

    def _apply(
        self,
        connection: Connection,
        *,
        stage_run_id: str,
        kind: str,
        state_version: int,
        fence: int | None,
        payload: dict[str, Any],
        principal: CallerPrincipalV1,
    ) -> None:
        repository = SqlAlchemyStageRepository(connection, self._tables)
        snapshot = repository.load(stage_run_id)
        if snapshot is None or self._org_of(connection, stage_run_id) != principal.org_id:
            raise _Refused(str(RefusalClass.UNKNOWN_STAGE))

        command = build_command(
            snapshot=snapshot,
            kind=kind,
            state_version=state_version,
            fence=fence,
            payload=payload,
            now=self._clock.now(),
        )
        runner = StageRunner(
            workflow=self.workflow,
            repository=repository,
            outbox=SqlAlchemyOutbox(
                connection, self._tables, clock=self._clock, org_id=principal.org_id
            ),
            clock=self._clock,
            tables=self._owned,
        )
        result = runner.apply(
            command,
            principal=principal,
            uow=SqlAlchemyUnitOfWork(connection, self._tables),
        )
        if not result.accepted:
            raise _Refused(str(result.refusal))
        if command.kind is CommandKind.LEASE:
            self._insert_attempt(connection, snapshot, principal)
        if result.plan.next_stage_key is not None:
            assert result.plan.next_stage_run_id is not None
            assert result.plan.successor_dispatch_key is not None
            assert result.plan.artifact_digest is not None
            self._insert_stage_run(
                connection,
                stage_run_id=result.plan.next_stage_run_id,
                job_id=snapshot.job_id,
                stage_key=result.plan.next_stage_key,
                workflow_digest=snapshot.workflow_digest,
                dispatch_key=result.plan.successor_dispatch_key,
                input_digest=result.plan.artifact_digest,
                principal=principal,
                now=self._clock.now(),
                deadline_at=snapshot.deadline_at,
            )
        return None

    def _insert_job(
        self, connection: Connection, job_id: str, principal: CallerPrincipalV1, now
    ) -> None:
        jobs = self._tables["jobs"]
        existing = (
            connection.execute(select(jobs).where(jobs.c.job_id == job_id)).mappings().first()
        )
        if existing:
            if (
                existing["org_id"] != principal.org_id
                or existing["workflow_digest"] != self.workflow.digest
            ):
                raise StageRunConflict("job_identity_conflict")
            return
        connection.execute(
            jobs.insert().values(
                job_id=job_id,
                org_id=principal.org_id,
                workflow_key=self.workflow.workflow_key,
                workflow_digest=self.workflow.digest,
                status=str(StageStatus.PENDING),
                created_at=now,
            )
        )

    def _insert_stage_run(
        self,
        connection: Connection,
        *,
        stage_run_id: str,
        job_id: str,
        stage_key: str,
        workflow_digest: str,
        dispatch_key: str,
        input_digest: str,
        principal: CallerPrincipalV1,
        now,
        deadline_at=None,
    ) -> None:
        connection.execute(
            self._tables["stage_runs"]
            .insert()
            .values(
                stage_run_id=stage_run_id,
                job_id=job_id,
                org_id=principal.org_id,
                workflow_digest=workflow_digest,
                stage_key=stage_key,
                state_version=0,
                fence=None,
                attempt_id=derive_id("att", stage_run_id, 0),
                dispatch_key=dispatch_key,
                input_digest=input_digest,
                status=str(StageStatus.PENDING),
                deadline_at=deadline_at or now + self._job_deadline,
                lease_expires_at=None,
                feature_enabled=True,
                cancelled=False,
            )
        )

    def _insert_attempt(
        self, connection: Connection, snapshot: LedgerSnapshotV1, principal: CallerPrincipalV1
    ) -> None:
        """One row per lease granted. The count is what `StageRunView.attempts` reports."""
        connection.execute(
            self._tables["attempts"]
            .insert()
            .values(
                attempt_id=derive_id("att", snapshot.stage_run_id, (snapshot.fence or 0) + 1),
                stage_run_id=snapshot.stage_run_id,
                org_id=principal.org_id,
                fence=(snapshot.fence or 0) + 1,
                started_at=self._clock.now(),
                outcome=None,
            )
        )

    def _org_of(self, connection: Connection, stage_run_id: str) -> str | None:
        """Which org owns this stage run. Not on `LedgerSnapshotV1`, so read directly.

        The package deliberately keeps `org_id` off the snapshot -- the reducer
        must not be able to make a tenancy decision -- so tenancy is the HOST's
        job, and this is where the host does it.
        """
        stage_runs = self._tables["stage_runs"]
        return connection.execute(
            select(stage_runs.c.org_id).where(stage_runs.c.stage_run_id == stage_run_id)
        ).scalar_one_or_none()

    def _view(self, connection: Connection, stage_run_id: str) -> StageRunView | None:
        snapshot = SqlAlchemyStageRepository(connection, self._tables).load(stage_run_id)
        if snapshot is None:
            return None
        attempts = self._tables["attempts"]
        counted = connection.execute(
            select(func.count())
            .select_from(attempts)
            .where(attempts.c.stage_run_id == stage_run_id)
        ).scalar_one()
        jobs = self._tables["jobs"]
        workflow_key = connection.execute(
            select(jobs.c.workflow_key).where(jobs.c.job_id == snapshot.job_id)
        ).scalar_one()
        return to_view(snapshot, workflow_key=workflow_key, attempts=int(counted) + 1)
