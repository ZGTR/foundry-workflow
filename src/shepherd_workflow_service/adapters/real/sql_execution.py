"""Atomic inbox, workflow binding and dispatch, using the existing ledger reducer.

No network calls occur inside these transactions. The outbox is drained separately.
Stored definition snapshots allow old runs to finish after the catalog changes.
"""

import json
from datetime import UTC

from shepherd_contracts import PrincipalMinter, canonical_digest, derive_id
from shepherd_workflow.ports import ConcurrentUpdate
from shepherd_workflow.sql.repository import SqlAlchemyOutbox, SqlAlchemyStageRepository
from sqlalchemy import JSON, Column, MetaData, String, Table, select
from sqlalchemy.exc import IntegrityError

from shepherd_workflow_service.errors import StageRunConflict, StageRunRejected
from shepherd_workflow_service.models.definition import Definition
from shepherd_workflow_service.models.execution import Correlation, EventEnvelope


def utc(value):
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


class SqlExecutionRepository:
    def __init__(self, *, catalog, engine_factory, clock, prefix, producer):
        self.catalog = catalog
        self.engine_factory = engine_factory
        self.clock = clock
        self.producer = producer
        self.minter = PrincipalMinter(issuer=producer)
        metadata = MetaData()
        self.bindings = Table(
            prefix + "execution_bindings",
            metadata,
            Column("job_id", String(64), primary_key=True),
            Column("org_id", String(64), nullable=False),
            Column("definition", JSON, nullable=False),
            Column("input_ref", String(2048)),
        )
        self.inbox = Table(
            prefix + "execution_inbox",
            metadata,
            Column("key", String(64), primary_key=True),
            Column("digest", String(64), nullable=False),
            Column("job_ids", JSON, nullable=False),
        )
        self.metadata = metadata

    def _base(self):
        return self.engine_factory(self.catalog.load().workflows[0])

    def _connection(self):
        connection = self._base().open_connection()
        self.metadata.create_all(connection)
        connection.commit()
        return connection

    def check_ready(self):
        self.catalog.load()
        with self._connection():
            pass

    def receive(self, event, principal):
        key = derive_id("inbox", event.tenant_id, event.producer, event.event_id)
        digest = canonical_digest(event.model_dump(mode="json"))
        with self._connection() as connection:
            try:
                with connection.begin():
                    connection.execute(
                        self.inbox.insert().values(key=key, digest=digest, job_ids=[])
                    )
                    job_ids = (
                        self._result(connection, event, principal)
                        if event.correlation is not None
                        else self._trigger(connection, event, principal)
                    )
                    connection.execute(
                        self.inbox.update().where(self.inbox.c.key == key).values(job_ids=job_ids)
                    )
                    return job_ids
            except IntegrityError:
                # Only an existing identical inbox receipt is a successful duplicate.
                prior = (
                    connection.execute(select(self.inbox).where(self.inbox.c.key == key))
                    .mappings()
                    .first()
                )
                if prior is None:
                    raise
                if prior["digest"] != digest:
                    raise StageRunConflict("event_identity_conflict") from None
                return prior["job_ids"]
            except ConcurrentUpdate as exc:
                raise StageRunConflict("state_version_conflict") from exc

    def _trigger(self, connection, event, principal):
        matches = [
            w
            for w in self.catalog.load().workflows
            if w.trigger.event_type == event.type and w.trigger.producer == event.producer
        ]
        if not matches:
            raise StageRunRejected("unsubscribed_event")
        # Versions may coexist for old runs; new triggers select newest per workflow key.
        latest = {}
        for definition in matches:
            old = latest.get(definition.workflow_key)
            if old is None or definition.version > old.version:
                latest[definition.workflow_key] = definition
        jobs = []
        for definition in latest.values():
            engine = self.engine_factory(definition)
            job_id = derive_id("job", event.tenant_id, definition.digest, event.event_id)
            connection.execute(
                self.bindings.insert().values(
                    job_id=job_id,
                    org_id=event.tenant_id,
                    definition=definition.model_dump(mode="json"),
                    input_ref=event.input_ref or event.output_ref,
                )
            )
            run_id = engine.start_in_transaction(
                connection,
                job_id=job_id,
                input_digest=canonical_digest(
                    {"input_ref": event.input_ref, "event_id": event.event_id}
                ),
                principal=principal,
            )
            self._dispatch(
                connection,
                engine,
                definition,
                run_id,
                principal,
                event.input_ref or event.output_ref,
            )
            jobs.append(job_id)
        return jobs

    def _binding(self, connection, job_id, org_id):
        row = (
            connection.execute(
                select(self.bindings).where(
                    self.bindings.c.job_id == job_id, self.bindings.c.org_id == org_id
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise StageRunRejected("unknown_execution")
        definition = Definition.model_validate_json(json.dumps(row["definition"]))
        return row, definition, self.engine_factory(definition)

    def _result(self, connection, event, principal):
        correlation = event.correlation
        binding, definition, engine = self._binding(
            connection, correlation.job_id, principal.org_id
        )
        snapshot = SqlAlchemyStageRepository(connection, engine.tables).load(
            correlation.stage_run_id
        )
        if (
            snapshot is None
            or snapshot.job_id != correlation.job_id
            or definition.digest != correlation.workflow_digest
        ):
            raise StageRunRejected("correlation_mismatch")
        stage = next(s for s in definition.stages if s.key == snapshot.stage_key)
        selector = (event.type, event.producer)
        success = stage.completion.success
        failure = stage.completion.failure
        if selector not in {
            (success.event_type, success.producer),
            (failure.event_type, failure.producer),
        }:
            raise StageRunRejected("unexpected_result_producer_or_type")
        # Late responses, including different event IDs, cannot mutate a newer attempt.
        if snapshot.fence != correlation.fence or snapshot.status != "leased":
            return [correlation.job_id]
        if self.clock.now() >= snapshot.lease_expires_at:
            return [correlation.job_id]
        complete = selector == (success.event_type, success.producer)
        if complete and event.payload_digest is None:
            raise StageRunRejected("success_requires_payload_digest")
        engine.apply_in_transaction(
            connection,
            stage_run_id=snapshot.stage_run_id,
            kind="complete" if complete else "fail",
            state_version=snapshot.state_version,
            fence=correlation.fence,
            payload={
                "payload_digest": event.payload_digest,
                "output_ref": event.output_ref,
                "retryable": event.retryable,
            },
            principal=principal,
        )
        view = engine.view_in_transaction(connection, snapshot.stage_run_id)
        if view.next_stage_run_id:
            self._dispatch(
                connection, engine, definition, view.next_stage_run_id, principal, event.output_ref
            )
        return [correlation.job_id]

    def _dispatch(self, connection, engine, definition, run_id, principal, input_ref):
        view = engine.view_in_transaction(connection, run_id)
        engine.apply_in_transaction(
            connection,
            stage_run_id=run_id,
            kind="lease",
            state_version=view.state_version,
            fence=None,
            payload={},
            principal=principal,
        )
        view = engine.view_in_transaction(connection, run_id)
        stage = next(s for s in definition.stages if s.key == view.stage)
        key = derive_id("evt", run_id, view.fence, "dispatch")
        event = EventEnvelope(
            event_id=key,
            type=stage.dispatch.event_type,
            producer=self.producer,
            tenant_id=principal.org_id,
            occurred_at=self.clock.now(),
            expires_at=view.lease_expires_at,
            correlation=Correlation(
                job_id=view.job_id,
                stage_run_id=run_id,
                workflow_digest=definition.digest,
                fence=view.fence,
            ),
            input_ref=input_ref,
        )
        SqlAlchemyOutbox(
            connection, engine.tables, clock=self.clock, org_id=principal.org_id
        ).enqueue(
            key=key,
            event_class=event.type,
            payload={"org_id": principal.org_id, "envelope": event.model_dump(mode="json")},
        )

    def tick(self, limit=100):
        engine = self._base()
        runs = engine.tables["stage_runs"]
        now = self.clock.now()
        with self._connection() as connection:
            with connection.begin():
                # Lock due rows on MySQL; the ledger CAS also protects SQLite/racing ticks.
                rows = (
                    connection.execute(
                        select(runs)
                        .join(self.bindings, self.bindings.c.job_id == runs.c.job_id)
                        .where(
                            runs.c.status.in_(["pending", "retry_wait", "leased"]),
                            (runs.c.deadline_at <= now)
                            | ((runs.c.status == "leased") & (runs.c.lease_expires_at <= now))
                            | ((runs.c.status == "retry_wait") & (runs.c.next_attempt_at <= now))
                            | (runs.c.status == "pending"),
                        )
                        .order_by(runs.c.deadline_at, runs.c.stage_run_id)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                    )
                    .mappings()
                    .all()
                )
                for row in rows:
                    self._tick_run(connection, row)
                return len(rows)

    def _tick_run(self, connection, row):
        binding, definition, engine = self._binding(connection, row["job_id"], row["org_id"])
        principal = self.minter.mint(
            org_id=row["org_id"], subject_id=self.producer, scopes=["jobs:write"]
        )
        if utc(row["deadline_at"]) <= self.clock.now():
            kind = "expire"
        elif row["status"] == "leased":
            kind = "timeout"
        else:
            input_ref = binding["input_ref"]
            # A later stage receives the previous output, including after a retry/restart.
            index = next(i for i, s in enumerate(definition.stages) if s.key == row["stage_key"])
            if index:
                runs = engine.tables["stage_runs"]
                input_ref = connection.execute(
                    select(runs.c.output_ref).where(
                        runs.c.job_id == row["job_id"],
                        runs.c.stage_key == definition.stages[index - 1].key,
                    )
                ).scalar_one()
            self._dispatch(
                connection, engine, definition, row["stage_run_id"], principal, input_ref
            )
            return
        engine.apply_in_transaction(
            connection,
            stage_run_id=row["stage_run_id"],
            kind=kind,
            state_version=row["state_version"],
            fence=row["fence"],
            payload={"retryable": True},
            principal=principal,
        )
