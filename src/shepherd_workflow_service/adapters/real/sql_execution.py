"""Atomic inbox, workflow binding and dispatch, using the existing ledger reducer.

No network calls occur inside these transactions. The outbox is drained separately.
Stored definition snapshots allow old runs to finish after the catalog changes.
"""

import json
from dataclasses import replace
from datetime import UTC
from threading import Lock

from shepherd_contracts import PrincipalMinter, canonical_digest, derive_id
from shepherd_contracts.errors import ConfigurationError, Unauthorized
from shepherd_workflow.ports import ConcurrentUpdate
from shepherd_workflow.sql.repository import SqlAlchemyOutbox
from sqlalchemy import JSON, Column, MetaData, String, Table, case, func, select

from shepherd_workflow_service.adapters.real.sql_admission import AdmissionLedger
from shepherd_workflow_service.adapters.real.tenant_sql import (
    check_tenant_collations,
    tenant_id_column_type,
)
from shepherd_workflow_service.errors import (
    ExecutionBackpressure,
    StageRunConflict,
    StageRunRejected,
)
from shepherd_workflow_service.models.definition import Definition
from shepherd_workflow_service.models.execution import Correlation, EventEnvelope
from shepherd_workflow_service.models.scheduling import (
    EligibleHead,
    PoolSchedulingState,
    TenantSchedulingState,
)
from shepherd_workflow_service.models.tenancy import validate_tenant_id

LIVE = ("pending", "retry_wait", "leased")
MODES = {"weighted", "equal", "paused"}


def utc(value):
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


class SqlExecutionRepository:
    def __init__(
        self,
        *,
        catalog,
        engine_factory,
        clock,
        prefix,
        producer,
        scheduler=None,
        pool_capacity=100,
        tenant_registry=None,
        authorize_artifact=None,
    ):
        self.catalog = catalog
        self.prefix = prefix
        self.engine_factory = engine_factory
        self.clock = clock
        self.producer = producer
        self.scheduler = scheduler
        self.pool_capacity = pool_capacity
        self.tenant_registry = tenant_registry
        self.authorize_artifact = authorize_artifact
        self._schema_lock = Lock()
        self._schema_ready = False
        self.minter = PrincipalMinter(issuer=producer)
        metadata = MetaData()
        self.bindings = Table(
            prefix + "execution_bindings",
            metadata,
            Column("job_id", String(64), primary_key=True),
            Column("org_id", tenant_id_column_type(), nullable=False, index=True),
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
        self.admission = AdmissionLedger(metadata, prefix)
        self.metadata = metadata

    def _base(self):
        return self.engine_factory(self.catalog.load().workflows[0])

    def _connection(self):
        if self._schema_ready:
            return self._base().open_connection()
        with self._schema_lock:
            connection = self._base().open_connection()
            if not self._schema_ready:
                try:
                    self.metadata.create_all(connection)
                    if self.scheduler is not None or self.tenant_registry is not None:
                        check_tenant_collations(connection, self.prefix)
                    connection.commit()
                except BaseException:
                    connection.close()
                    raise
                self._schema_ready = True
        return connection

    def check_ready(self):
        self.catalog.load()
        with self._connection() as connection:
            mode = connection.scalar(select(self.admission.pool.c.mode)) or "legacy"
            if self.scheduler is not None and mode not in MODES:
                raise ConfigurationError("execution admission must be reconciled and enabled")
            if self.scheduler is None and mode != "legacy":
                raise ConfigurationError("scheduled execution requires an admission worker")

    def _verify_mode(self, pool):
        if self.scheduler is None and pool["mode"] == "legacy":
            return False
        if self.scheduler is not None and pool["mode"] in MODES:
            return True
        raise ExecutionBackpressure()

    def _verify_artifacts(self, event):
        for ref in (event.input_ref, event.output_ref):
            if ref is None:
                continue
            if self.tenant_registry is not None and (
                self.authorize_artifact is None or not self.authorize_artifact(event.tenant_id, ref)
            ):
                raise Unauthorized("artifact_access_denied")

    def _verify_grant(self, principal, event, definition):
        if self.tenant_registry is not None and not self.tenant_registry.allows_trigger(
            principal,
            event_type=event.type,
            workflow_digest=definition.digest,
        ):
            raise Unauthorized("trigger_access_denied")

    def receive(self, event, principal):
        if (event.tenant_id, event.producer) != (principal.org_id, principal.subject_id):
            raise Unauthorized("event tenant/producer does not match authenticated principal")
        if (
            self.tenant_registry is not None
            and self.tenant_registry.active_workload(
                principal,
            )
            is None
        ):
            raise Unauthorized("workload_access_denied")
        key = derive_id("inbox", event.tenant_id, event.producer, event.event_id)
        digest = canonical_digest(event.model_dump(mode="json"))
        try:
            with self._connection() as connection, self.admission.transaction(connection) as pool:
                scheduled = self._verify_mode(pool)
                prior = (
                    connection.execute(select(self.inbox).where(self.inbox.c.key == key))
                    .mappings()
                    .one_or_none()
                )
                if prior is not None and prior["digest"] != digest:
                    raise StageRunConflict("event_identity_conflict")
                self._verify_artifacts(event)
                if event.correlation is not None:
                    result = self._authorize_result(connection, event, principal)
                    if prior is not None:
                        return prior["job_ids"]
                    job_ids = self._result(connection, event, principal, result, scheduled)
                else:
                    if prior is not None:
                        for job_id in prior["job_ids"]:
                            _, definition, _ = self._binding(connection, job_id, principal.org_id)
                            self._verify_grant(principal, event, definition)
                        return prior["job_ids"]
                    definitions = self._trigger_definitions(event, principal)
                    if scheduled and (
                        pool["mode"] == "paused"
                        or not self.admission.check_backlog(
                            connection,
                            principal.org_id,
                            len(definitions),
                        )
                    ):
                        raise ExecutionBackpressure()
                    job_ids = self._trigger(connection, event, principal, definitions, scheduled)
                connection.execute(
                    self.inbox.insert().values(key=key, digest=digest, job_ids=job_ids)
                )
                return job_ids
        except ConcurrentUpdate as exc:
            raise StageRunConflict("state_version_conflict") from exc

    def _trigger_definitions(self, event, principal):
        matches = {}
        for workflow in self.catalog.load().workflows:
            if (workflow.trigger.event_type, workflow.trigger.producer) == (
                event.type,
                event.producer,
            ):
                matches.setdefault(workflow.workflow_key, []).append(workflow)
        if not matches:
            if self.tenant_registry is not None:
                raise Unauthorized("trigger_access_denied")
            raise StageRunRejected("unsubscribed_event")
        selected = []
        for versions in matches.values():
            for definition in sorted(versions, key=lambda workflow: workflow.version, reverse=True):
                if self.tenant_registry is None or self.tenant_registry.allows_trigger(
                    principal,
                    event_type=event.type,
                    workflow_digest=definition.digest,
                ):
                    selected.append(definition)
                    break
        if not selected:
            raise Unauthorized("trigger_access_denied")
        return selected

    def _ledger_principal(self, principal):
        return self.minter.mint(
            org_id=principal.org_id,
            subject_id=principal.subject_id,
            scopes=["jobs:write"],
        )

    def _trigger(self, connection, event, principal, definitions, scheduled):
        principal = self._ledger_principal(principal)
        jobs = []
        for definition in definitions:
            engine = self.engine_factory(definition)
            job_id = derive_id("job", event.tenant_id, definition.digest, event.event_id)
            input_ref = event.input_ref or event.output_ref
            connection.execute(
                self.bindings.insert().values(
                    job_id=job_id,
                    org_id=event.tenant_id,
                    definition=definition.model_dump(mode="json"),
                    input_ref=input_ref,
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
            if scheduled:
                self.admission.mark_ready(
                    connection,
                    org_id=principal.org_id,
                    job_id=job_id,
                    stage_run_id=run_id,
                    ready_at=self.clock.now(),
                )
            else:
                self._dispatch(connection, engine, definition, run_id, principal, input_ref)
            jobs.append(job_id)
        if scheduled:
            self.admission.charge_backlog(connection, principal.org_id, jobs)
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

    def _authorize_result(self, connection, event, principal):
        correlation = event.correlation
        _, definition, engine = self._binding(connection, correlation.job_id, principal.org_id)
        runs = engine.tables["stage_runs"]
        row = (
            connection.execute(
                select(runs).where(
                    runs.c.org_id == principal.org_id,
                    runs.c.job_id == correlation.job_id,
                    runs.c.stage_run_id == correlation.stage_run_id,
                )
            )
            .mappings()
            .one_or_none()
        )
        if (
            row is None
            or row["workflow_digest"] != correlation.workflow_digest
            or definition.digest != correlation.workflow_digest
        ):
            raise StageRunRejected("unknown_execution")
        stage = next(s for s in definition.stages if s.key == row["stage_key"])
        if self.tenant_registry is not None and not self.tenant_registry.allows_executor(
            principal,
            workflow_digest=definition.digest,
            stage=stage.key,
        ):
            raise Unauthorized("completion_access_denied")
        selector = (event.type, event.producer)
        success = stage.completion.success
        failure = stage.completion.failure
        if selector not in {
            (success.event_type, success.producer),
            (failure.event_type, failure.producer),
        }:
            raise StageRunRejected("unexpected_result_producer_or_type")
        return engine, definition, row, selector == (success.event_type, success.producer)

    def _result(self, connection, event, principal, result, scheduled):
        engine, definition, row, complete = result
        correlation = event.correlation
        now = self.clock.now()
        # Late responses, including different event IDs, cannot mutate a newer attempt.
        if (
            row["fence"] != correlation.fence
            or row["status"] != "leased"
            or now >= min(utc(row["lease_expires_at"]), utc(row["deadline_at"]))
        ):
            return [correlation.job_id]
        if complete and event.payload_digest is None:
            raise StageRunRejected("success_requires_payload_digest")
        principal = self._ledger_principal(principal)
        engine.apply_in_transaction(
            connection,
            stage_run_id=row["stage_run_id"],
            kind="complete" if complete else "fail",
            state_version=row["state_version"],
            fence=correlation.fence,
            payload={
                "payload_digest": event.payload_digest,
                "output_ref": event.output_ref,
                "retryable": event.retryable,
            },
            principal=principal,
        )
        view = engine.view_in_transaction(connection, row["stage_run_id"], org_id=principal.org_id)
        if scheduled:
            self._schedule_transition(connection, engine, row, view, now=now, timed_out=False)
        elif view.next_stage_run_id:
            self._dispatch(
                connection, engine, definition, view.next_stage_run_id, principal, event.output_ref
            )
        return [correlation.job_id]

    def _schedule_transition(self, connection, engine, row, view, *, now, timed_out):
        org_id, job_id, stage_run_id = row["org_id"], row["job_id"], row["stage_run_id"]
        if row["status"] == "leased":
            # Occupied duration is bounded by the active lease or the job deadline.
            finish = min(now, utc(row["lease_expires_at"]), utc(row["deadline_at"]))
            self.admission.record_finish(
                connection,
                org_id=org_id,
                stage_run_id=stage_run_id,
                fence=row["fence"],
                now=finish,
                timed_out=timed_out,
            )
        else:
            self.admission.clear_ready(connection, org_id, stage_run_id)
        if view.next_stage_run_id or view.status == "retry_wait":
            self.admission.mark_ready(
                connection,
                org_id=org_id,
                job_id=job_id,
                stage_run_id=view.next_stage_run_id or stage_run_id,
                ready_at=view.next_attempt_at or now,
            )
        else:
            jobs = engine.tables["jobs"]
            status = connection.execute(
                select(jobs.c.status).where(
                    jobs.c.org_id == org_id,
                    jobs.c.job_id == job_id,
                )
            ).scalar_one()
            if status not in LIVE:
                self.admission.release_backlog(connection, org_id, job_id)

    def _dispatch(self, connection, engine, definition, run_id, principal, input_ref):
        view = engine.view_in_transaction(connection, run_id, org_id=principal.org_id)
        engine.apply_in_transaction(
            connection,
            stage_run_id=run_id,
            kind="lease",
            state_version=view.state_version,
            fence=None,
            payload={},
            principal=principal,
        )
        view = engine.view_in_transaction(connection, run_id, org_id=principal.org_id)
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

        return view

    def tick(self, limit=100):
        engine = self._base()
        runs = engine.tables["stage_runs"]
        with self._connection() as connection, self.admission.transaction(connection) as pool:
            try:
                scheduled = self._verify_mode(pool)
            except ExecutionBackpressure:
                return 0
            now = self.clock.now()
            due = (runs.c.deadline_at <= now) | (
                (runs.c.status == "leased") & (runs.c.lease_expires_at <= now)
            )
            if not scheduled:
                due |= ((runs.c.status == "retry_wait") & (runs.c.next_attempt_at <= now)) | (
                    runs.c.status == "pending"
                )
            rows = (
                connection.execute(
                    select(runs)
                    .join(self.bindings, self._owner_join(runs))
                    .where(
                        runs.c.status.in_(LIVE),
                        due,
                    )
                    .order_by(runs.c.deadline_at, runs.c.stage_run_id)
                    .limit(limit)
                )
                .mappings()
                .all()
            )
            for row in rows:
                self._tick_run(connection, row, now, scheduled)
            return len(rows)

    def _owner_join(self, table):
        return (self.bindings.c.job_id == table.c.job_id) & (
            self.bindings.c.org_id == table.c.org_id
        )

    def _tick_run(self, connection, row, now, scheduled):
        binding, definition, engine = self._binding(connection, row["job_id"], row["org_id"])
        principal = self.minter.mint(
            org_id=row["org_id"], subject_id=self.producer, scopes=["jobs:write"]
        )
        if utc(row["deadline_at"]) <= now:
            kind = "expire"
        elif row["status"] == "leased":
            kind = "timeout"
        else:
            self._dispatch_work(connection, binding, definition, engine, row, principal)
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
        if scheduled:
            view = engine.view_in_transaction(
                connection,
                row["stage_run_id"],
                org_id=row["org_id"],
            )
            self._schedule_transition(connection, engine, row, view, now=now, timed_out=True)

    def _dispatch_work(self, connection, binding, definition, engine, row, principal):
        input_ref = binding["input_ref"]
        # A later stage receives the preceding output, including after a retry.
        if row["stage_key"] != definition.stages[0].key:
            runs = engine.tables["stage_runs"]
            input_ref = connection.execute(
                select(runs.c.output_ref).where(
                    runs.c.org_id == principal.org_id,
                    runs.c.job_id == row["job_id"],
                    runs.c.next_stage_run_id == row["stage_run_id"],
                )
            ).scalar_one()
        return self._dispatch(
            connection,
            engine,
            definition,
            row["stage_run_id"],
            principal,
            input_ref,
        )

    def _head(self, connection, runs, org_id, now):
        ready = self.admission.ready
        return (
            connection.execute(
                select(runs, ready.c.ready_at)
                .join(
                    runs,
                    (
                        (ready.c.org_id == runs.c.org_id)
                        & (ready.c.stage_run_id == runs.c.stage_run_id)
                        & (ready.c.job_id == runs.c.job_id)
                    ),
                )
                .where(
                    ready.c.org_id == org_id,
                    ready.c.ready_at <= now,
                    runs.c.deadline_at > now,
                    runs.c.status.in_(("pending", "retry_wait")),
                    runs.c.feature_enabled.is_(True),
                    runs.c.cancelled.is_(False),
                )
                .order_by(ready.c.ready_at, ready.c.stage_run_id)
                .limit(1)
            )
            .mappings()
            .one_or_none()
        )

    def admit(self, limit=100):
        if self.scheduler is None:
            return 0
        count = 0
        # Each selection takes one short transaction, even when a larger batch is requested.
        for _ in range(limit):
            if not self._admit_one():
                break
            count += 1
        return count

    def _admit_one(self):
        with self._connection() as connection, self.admission.transaction(connection) as pool:
            if pool["mode"] not in {"weighted", "equal"}:
                return False
            now = self.clock.now()
            active = self.admission.active_counts(connection)
            states = {org: TenantSchedulingState(active=count) for org, count in active.items()}
            runs = self._base().tables["stage_runs"]
            policies, heads, work = {}, [], {}
            stage_costs = {
                (row.workflow_digest, row.stage_key): row.seconds
                for row in connection.execute(select(self.admission.fallbacks))
            }
            for policy_row in self.admission.policy_rows(connection):
                org_id = policy_row["org_id"]
                policy = self.admission.policy(policy_row)
                policies[org_id] = replace(policy, weight=1) if pool["mode"] == "equal" else policy
                states[org_id] = TenantSchedulingState(
                    deficit=policy_row["deficit"],
                    active=active.get(org_id, 0),
                )
                row = self._head(connection, runs, org_id, now)
                if row is None:
                    continue
                binding, definition, engine = self._binding(connection, row["job_id"], org_id)
                stage = next(s for s in definition.stages if s.key == row["stage_key"])
                estimate = self.admission.estimate(connection, org_id, definition.digest, stage.key)
                cost = estimate.charge(
                    fallback=stage_costs.get((definition.digest, stage.key), 1),
                    maximum=min(stage.completion.timeout_seconds, self.scheduler.max_charge),
                )
                work[org_id] = (binding, definition, engine, row)
                heads.append(EligibleHead(org_id, row["stage_run_id"], utc(row["ready_at"]), cost))
            decision = self.scheduler.choose(
                heads=heads,
                tenant_policies=policies,
                tenant_states=states,
                pool_state=PoolSchedulingState(
                    cursor=pool["cursor"],
                    visit_open=pool["visit_open"],
                    admissions=pool["admissions"],
                ),
                pool_capacity=pool["capacity"],
                now=now,
            )
            state = decision.pool_state
            if (state.cursor, state.visit_open, state.admissions) != (
                pool["cursor"],
                pool["visit_open"],
                pool["admissions"],
            ):
                self.admission.set_pool(
                    connection,
                    cursor=state.cursor,
                    visit_open=state.visit_open,
                    admissions=state.admissions,
                )
            tenants = self.admission.tenants
            for org_id, tenant in decision.tenant_states.items():
                if tenant.deficit != states[org_id].deficit:
                    connection.execute(
                        tenants.update()
                        .where(
                            tenants.c.org_id == org_id,
                        )
                        .values(deficit=tenant.deficit)
                    )
            if decision.head is None:
                return False
            head = decision.head
            binding, definition, engine, row = work[head.tenant_id]
            principal = self.minter.mint(
                org_id=head.tenant_id,
                subject_id=self.producer,
                scopes=["jobs:write"],
            )
            view = self._dispatch_work(connection, binding, definition, engine, row, principal)
            self.admission.record_lease(
                connection,
                org_id=head.tenant_id,
                stage_run_id=head.stage_run_id,
                fence=view.fence,
                workflow_digest=definition.digest,
                stage_key=row["stage_key"],
                started_at=self.clock.now(),
                cost=head.cost,
                age_override=decision.age_override,
                ready_at=head.ready_at,
            )
            self.admission.clear_ready(connection, head.tenant_id, head.stage_run_id)
            # Preserve the strategy's open visit across batches only while work stays ready.
            if self._head(connection, runs, head.tenant_id, now) is None:
                if decision.tenant_states[head.tenant_id].deficit > 0:
                    connection.execute(
                        tenants.update()
                        .where(
                            tenants.c.org_id == head.tenant_id,
                        )
                        .values(deficit=0)
                    )
                if state.cursor == head.tenant_id:
                    self.admission.set_pool(connection, visit_open=False)
            return True

    def get(self, stage_run_id, principal):
        base = self._base()
        runs = base.tables["stage_runs"]
        with self._connection() as connection:
            row = connection.execute(
                select(runs.c.job_id)
                .join(self.bindings, self._owner_join(runs))
                .where(
                    runs.c.org_id == principal.org_id,
                    runs.c.stage_run_id == stage_run_id,
                )
            ).one_or_none()
            if row is None:
                return None
            return self._authorize_view(
                base.view_in_transaction(connection, stage_run_id, org_id=principal.org_id),
                principal.org_id,
            )

    def stages(self, job_id, principal):
        base = self._base()
        with self._connection() as connection:
            owned = connection.execute(
                select(self.bindings.c.job_id).where(
                    self.bindings.c.org_id == principal.org_id,
                    self.bindings.c.job_id == job_id,
                )
            ).one_or_none()
            if owned is None:
                return []
        return [
            self._authorize_view(view, principal.org_id)
            for view in base.stages(job_id, principal=principal)
        ]

    def _authorize_view(self, view, org_id):
        if (
            view is not None
            and view.output_ref is not None
            and self.tenant_registry is not None
            and (
                self.authorize_artifact is None
                or not self.authorize_artifact(org_id, view.output_ref)
            )
        ):
            return replace(view, output_ref=None)
        return view

    def owns_job(self, job_id, principal):
        with self._connection() as connection:
            return (
                connection.execute(
                    select(self.bindings.c.job_id).where(
                        self.bindings.c.org_id == principal.org_id,
                        self.bindings.c.job_id == job_id,
                    )
                ).one_or_none()
                is not None
            )

    def reconcile_and_enable(
        self,
        *,
        tenant_policies,
        mode="weighted",
        stage_cost_fallbacks=None,
    ):
        """Backfill admission from the ledger while all older writers are quiesced."""
        if mode not in MODES or self.scheduler is None:
            raise ValueError("a configured scheduler and valid mode are required")
        if type(self.pool_capacity) is not int or not 1 <= self.pool_capacity <= 2**31 - 1:
            raise ValueError("pool capacity must be a positive integer")
        self._verify_policy_owners(tenant_policies)
        stage_cost_fallbacks = stage_cost_fallbacks or {}
        stage_limits = {
            (workflow.digest, stage.key): stage.completion.timeout_seconds
            for workflow in self.catalog.load().workflows
            for stage in workflow.stages
        }
        for key, cost in stage_cost_fallbacks.items():
            if type(cost) is not int or not 1 <= cost <= stage_limits.get(key, 0):
                raise ValueError("stage cost fallback must fit an approved stage runtime")
        engine = self._base()
        jobs, runs, attempts, events, outbox = (
            engine.tables[key] for key in ("jobs", "stage_runs", "attempts", "events", "outbox")
        )
        with self._connection() as connection, self.admission.transaction(connection) as pool:
            if pool["mode"] != "legacy":
                raise ValueError("execution admission is already reconciled")
            if stage_cost_fallbacks:
                connection.execute(
                    self.admission.fallbacks.insert(),
                    [
                        {"workflow_digest": digest, "stage_key": key, "seconds": cost}
                        for (digest, key), cost in stage_cost_fallbacks.items()
                    ],
                )
            for org_id, policy in tenant_policies.items():
                self.admission.set_policy(connection, org_id, policy, initial=True)
            orphan = connection.execute(
                select(self.bindings.c.job_id)
                .outerjoin(jobs, self._owner_join(jobs))
                .where(jobs.c.job_id.is_(None))
                .limit(1)
            ).first()
            if orphan is not None:
                raise ConfigurationError("an execution binding has no owner-matching ledger job")
            if self.tenant_registry is not None:
                self._audit_artifact_references(connection, runs)
            live_jobs = (
                connection.execute(
                    select(jobs)
                    .join(
                        self.bindings,
                        self._owner_join(jobs),
                    )
                    .where(jobs.c.status.in_(LIVE))
                )
                .mappings()
                .all()
            )
            for job in live_jobs:
                org_id, job_id = job["org_id"], job["job_id"]
                if org_id not in tenant_policies:
                    raise ConfigurationError("a live execution has no tenant admission policy")
                row = (
                    connection.execute(
                        select(runs).where(
                            runs.c.org_id == org_id,
                            runs.c.job_id == job_id,
                            runs.c.status.in_(LIVE),
                        )
                    )
                    .mappings()
                    .one()
                )
                self.admission.charge_backlog(connection, org_id, [job_id])
                if row["status"] == "leased":
                    started = connection.execute(
                        select(attempts.c.started_at).where(
                            attempts.c.org_id == org_id,
                            attempts.c.stage_run_id == row["stage_run_id"],
                            attempts.c.fence == row["fence"],
                        )
                    ).scalar_one()
                    self.admission.record_lease(
                        connection,
                        org_id=org_id,
                        stage_run_id=row["stage_run_id"],
                        fence=row["fence"],
                        workflow_digest=row["workflow_digest"],
                        stage_key=row["stage_key"],
                        started_at=started,
                        cost=1,
                    )
                    continue
                ready_at = row["next_attempt_at"]
                if ready_at is None:
                    ready_at = (
                        connection.execute(
                            select(func.min(events.c.occurred_at)).where(
                                events.c.org_id == org_id,
                                events.c.stage_run_id == row["stage_run_id"],
                            )
                        ).scalar_one()
                        or job["created_at"]
                    )
                self.admission.mark_ready(
                    connection,
                    org_id=org_id,
                    job_id=job_id,
                    stage_run_id=row["stage_run_id"],
                    ready_at=ready_at,
                )
            # Pending dispatch rows remain available to the outbox relay after cutover.
            # Refuse inconsistent ownership on declarative envelopes before enabling it.
            rows = connection.execute(select(outbox).where(outbox.c.sent_at.is_(None))).mappings()
            for row in rows:
                raw = row["payload"].get("envelope")
                if raw is None:
                    continue
                event = EventEnvelope.model_validate_json(json.dumps(raw))
                corr = event.correlation
                if (
                    event.tenant_id != row["org_id"]
                    or row["payload"].get("org_id") != row["org_id"]
                    or event.event_id != row["key"]
                    or event.type != row["event_class"]
                    or corr is None
                ):
                    raise ConfigurationError("a pending dispatch has inconsistent ownership")
                _, definition, _ = self._binding(connection, corr.job_id, event.tenant_id)
                matching = connection.execute(
                    select(runs.c.stage_run_id).where(
                        runs.c.org_id == event.tenant_id,
                        runs.c.job_id == corr.job_id,
                        runs.c.stage_run_id == corr.stage_run_id,
                        runs.c.workflow_digest == corr.workflow_digest,
                    )
                ).scalar_one_or_none()
                if matching is None or definition.digest != corr.workflow_digest:
                    raise ConfigurationError("a pending dispatch has inconsistent ownership")
            self.admission.set_pool(connection, mode=mode, capacity=self.pool_capacity)

    def _audit_artifact_references(self, connection, runs):
        # Legacy v2 rows predate storage grants. Validate them before making
        # existing job views or queued dispatch available to enrolled tenants.
        for table, column in (
            (self.bindings, self.bindings.c.input_ref),
            (runs, runs.c.output_ref),
        ):
            statement = select(table.c.org_id, column).where(
                table.c.org_id.in_(self.tenant_registry.tenant_ids),
                column.is_not(None),
            )
            if table is runs:
                statement = statement.join(self.bindings, self._owner_join(runs))
            for org_id, ref in connection.execute(statement):
                if self.authorize_artifact is None or not self.authorize_artifact(org_id, ref):
                    raise ConfigurationError("an execution artifact has no matching tenant grant")

    def _verify_policy_owners(self, tenant_ids):
        for org_id in tenant_ids:
            validate_tenant_id(org_id)
            if self.tenant_registry is not None and org_id not in self.tenant_registry.tenant_ids:
                raise ValueError("admission policy owner must belong to the tenant registry")

    def update_policy(self, tenant_id, policy):
        self._verify_policy_owners((tenant_id,))
        with self._connection() as connection, self.admission.transaction(connection) as pool:
            if pool["mode"] == "legacy":
                raise ValueError("execution admission must be reconciled first")
            self.admission.set_policy(connection, tenant_id, policy)
            if pool["cursor"] == tenant_id:
                self.admission.set_pool(connection, visit_open=False)

    def set_mode(self, mode):
        if mode not in MODES:
            raise ValueError("invalid admission mode")
        with self._connection() as connection, self.admission.transaction(connection) as pool:
            if pool["mode"] == "legacy":
                raise ValueError("execution admission must be reconciled first")
            if pool["mode"] != mode:
                self.admission.set_pool(connection, mode=mode, cursor=None, visit_open=False)
                connection.execute(self.admission.tenants.update().values(deficit=0))

    def scheduler_metrics(self) -> dict[str, float | int]:
        if self.scheduler is None:
            return {}
        now = self.clock.now()
        audit = self.admission
        ready, attempts = audit.ready, audit.attempts
        runs = self._base().tables["stage_runs"]
        with self._connection() as connection:
            depth, oldest = connection.execute(
                select(func.count(), func.min(ready.c.ready_at))
                .join(
                    runs,
                    (ready.c.org_id == runs.c.org_id)
                    & (ready.c.stage_run_id == runs.c.stage_run_id),
                )
                .where(
                    ready.c.ready_at <= now,
                    runs.c.deadline_at > now,
                    runs.c.feature_enabled.is_(True),
                    runs.c.cancelled.is_(False),
                    runs.c.status.in_(("pending", "retry_wait")),
                )
            ).one()
            active, charged, observed, overrides, timeouts = connection.execute(
                select(
                    func.sum(case((attempts.c.finished_at.is_(None), 1), else_=0)),
                    func.sum(attempts.c.charged_seconds),
                    func.sum(attempts.c.observed_seconds),
                    func.sum(case((attempts.c.age_override.is_(True), 1), else_=0)),
                    func.sum(case((attempts.c.timed_out.is_(True), 1), else_=0)),
                )
            ).one()
            capacity = connection.execute(select(audit.pool.c.capacity)).scalar_one_or_none()
            backlog = connection.execute(
                select(func.count()).select_from(audit.backlog)
            ).scalar_one()
        return {
            "EligibleDepth": depth,
            "OldestEligibleSeconds": max(0, (now - utc(oldest)).total_seconds()) if oldest else 0,
            "InFlight": int(active or 0),
            "Backlog": backlog,
            "PoolSaturation": int(active or 0) / capacity if capacity else 0,
            "ChargedSlotSeconds": int(charged or 0),
            "ObservedSlotSeconds": float(observed or 0),
            "AgeOverrides": int(overrides or 0),
            "Timeouts": int(timeouts or 0),
        }
