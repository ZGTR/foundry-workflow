"""Durable state and transaction lock shared by declarative execution writers."""

from contextlib import contextmanager
from sqlite3 import SQLITE_BUSY, SQLITE_LOCKED
from time import sleep

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Table,
    func,
    select,
)
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError

from shepherd_workflow_service.adapters.real.tenant_sql import tenant_id_column_type
from shepherd_workflow_service.models.scheduling import CostEstimate, TenantPolicy


class AdmissionLedger:
    def __init__(self, metadata, prefix):
        self.pool = Table(
            prefix + "admission_pool",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("mode", String(16), nullable=False),
            Column("capacity", Integer, nullable=True),
            Column("cursor", tenant_id_column_type()),
            Column("visit_open", Boolean, nullable=False, default=False),
            Column("admissions", BigInteger, nullable=False, default=0),
        )
        self.policies = Table(
            prefix + "admission_policies",
            metadata,
            Column("org_id", tenant_id_column_type(), primary_key=True),
            Column("version", Integer, primary_key=True),
            Column("actor", String(256), nullable=False),
            Column("weight", Integer, nullable=False),
            Column("backlog_cap", Integer, nullable=False),
            Column("in_flight_cap", Integer, nullable=False),
        )
        self.tenants = Table(
            prefix + "admission_tenants",
            metadata,
            Column("org_id", tenant_id_column_type(), primary_key=True),
            Column("version", Integer, nullable=False),
            Column("deficit", Integer, nullable=False, default=0),
        )
        self.backlog = Table(
            prefix + "admission_backlog",
            metadata,
            Column("org_id", tenant_id_column_type(), primary_key=True),
            Column("job_id", String(64), primary_key=True),
        )
        self.ready = Table(
            prefix + "admission_ready",
            metadata,
            Column("org_id", tenant_id_column_type(), primary_key=True),
            Column("stage_run_id", String(64), primary_key=True),
            Column("job_id", String(64), nullable=False),
            Column("ready_at", DateTime(timezone=True), nullable=False),
            Index(prefix + "admission_ready_head", "org_id", "ready_at", "stage_run_id"),
        )
        self.attempts = Table(
            prefix + "admission_attempts",
            metadata,
            Column("org_id", tenant_id_column_type(), primary_key=True),
            Column("stage_run_id", String(64), primary_key=True),
            Column("fence", BigInteger, primary_key=True),
            Column("workflow_digest", String(64), nullable=False),
            Column("stage_key", tenant_id_column_type(128), nullable=False),
            Column("started_at", DateTime(timezone=True), nullable=False),
            Column("ready_at", DateTime(timezone=True)),
            Column("charged_seconds", Integer, nullable=False),
            Column("age_override", Boolean, nullable=False, default=False),
            Column("observed_seconds", Float(precision=53)),
            Column("timed_out", Boolean),
            Column("finished_at", DateTime(timezone=True)),
            Index(prefix + "admission_attempts_active", "finished_at", "org_id"),
        )
        self.fallbacks = Table(
            prefix + "admission_cost_policy",
            metadata,
            Column("workflow_digest", String(64), primary_key=True),
            Column("stage_key", tenant_id_column_type(128), primary_key=True),
            Column("seconds", Integer, nullable=False),
        )
        self.estimates = Table(
            prefix + "admission_estimates",
            metadata,
            Column("org_id", tenant_id_column_type(), primary_key=True),
            Column("workflow_digest", String(64), primary_key=True),
            Column("stage_key", tenant_id_column_type(128), primary_key=True),
            Column("samples", BigInteger, nullable=False),
            Column("ewma_seconds", Float(precision=53)),
            Column("timeouts", BigInteger, nullable=False),
        )

    @contextmanager
    def transaction(self, connection):
        """Lock the pool before touching any tenant, inbox, stage or accounting row."""
        if connection.dialect.name == "sqlite":
            # DBAPI's default busy timeout is five seconds per try. Keep the total
            # bounded while allowing concurrent ticks to wait for each other.
            connection.exec_driver_sql("PRAGMA busy_timeout=250")
            connection.commit()
            for attempt in range(5):
                try:
                    connection.exec_driver_sql("BEGIN IMMEDIATE")
                    break
                except OperationalError as exc:
                    connection.rollback()
                    code = getattr(exc.orig, "sqlite_errorcode", 0)
                    if code & 0xFF not in (SQLITE_BUSY, SQLITE_LOCKED) or attempt == 4:
                        raise
                    sleep(0.01 * (attempt + 1))
        else:
            connection.begin()
        try:
            # The singleton starts in legacy mode. A no-op upsert safely handles
            # simultaneous initialization on MySQL without taking a gap lock first.
            if connection.dialect.name == "mysql":
                insert = mysql_insert(self.pool).values(id=1, mode="legacy")
                connection.execute(insert.on_duplicate_key_update(id=insert.inserted.id))
            elif connection.dialect.name == "sqlite":
                connection.execute(
                    sqlite_insert(self.pool).values(id=1, mode="legacy").on_conflict_do_nothing()
                )
            pool = (
                connection.execute(select(self.pool).where(self.pool.c.id == 1).with_for_update())
                .mappings()
                .one()
            )
            yield pool
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    def set_pool(self, connection, **values):
        connection.execute(self.pool.update().where(self.pool.c.id == 1).values(**values))

    def policy_rows(self, connection):
        t, p = self.tenants, self.policies
        return connection.execute(
            select(p, t.c.deficit).join(
                t, (p.c.org_id == t.c.org_id) & (p.c.version == t.c.version)
            )
        ).mappings()

    @staticmethod
    def policy(row):
        return TenantPolicy(
            **{
                name: row[name]
                for name in ("weight", "backlog_cap", "in_flight_cap", "version", "actor")
            }
        )

    def set_policy(self, connection, org_id, policy, *, initial=False):
        t = self.tenants
        previous = connection.execute(
            select(t.c.version).where(t.c.org_id == org_id)
        ).scalar_one_or_none()
        if previous is None and not initial and policy.version != 1:
            raise ValueError("new policy must start at version 1")
        if previous is not None and policy.version <= previous:
            raise ValueError("tenant policy version must increase")
        connection.execute(
            self.policies.insert().values(
                org_id=org_id,
                version=policy.version,
                actor=policy.actor,
                weight=policy.weight,
                backlog_cap=policy.backlog_cap,
                in_flight_cap=policy.in_flight_cap,
            )
        )
        if previous is None:
            connection.execute(t.insert().values(org_id=org_id, version=policy.version))
        else:
            connection.execute(
                t.update()
                .where(t.c.org_id == org_id)
                .values(
                    version=policy.version,
                    deficit=0,
                )
            )

    def charge_backlog(self, connection, org_id, job_ids):
        connection.execute(
            self.backlog.insert(), [{"org_id": org_id, "job_id": job_id} for job_id in job_ids]
        )

    def check_backlog(self, connection, org_id, new_jobs):
        t, p, b = self.tenants, self.policies, self.backlog
        cap = connection.execute(
            select(p.c.backlog_cap)
            .join(t, (p.c.org_id == t.c.org_id) & (p.c.version == t.c.version))
            .where(t.c.org_id == org_id)
        ).scalar_one_or_none()
        count = connection.execute(
            select(func.count()).select_from(b).where(b.c.org_id == org_id)
        ).scalar_one()
        return cap is not None and count + new_jobs <= cap

    def release_backlog(self, connection, org_id, job_id):
        b = self.backlog
        connection.execute(b.delete().where(b.c.org_id == org_id, b.c.job_id == job_id))

    def mark_ready(self, connection, *, org_id, job_id, stage_run_id, ready_at):
        connection.execute(
            self.ready.insert().values(
                org_id=org_id,
                job_id=job_id,
                stage_run_id=stage_run_id,
                ready_at=ready_at,
            )
        )

    def clear_ready(self, connection, org_id, stage_run_id):
        r = self.ready
        connection.execute(
            r.delete().where(
                r.c.org_id == org_id,
                r.c.stage_run_id == stage_run_id,
            )
        )

    def active_counts(self, connection):
        a = self.attempts
        return dict(
            connection.execute(
                select(a.c.org_id, func.count())
                .where(a.c.finished_at.is_(None))
                .group_by(a.c.org_id)
            )
            .tuples()
            .all()
        )

    def record_lease(
        self,
        connection,
        *,
        org_id,
        stage_run_id,
        fence,
        workflow_digest,
        stage_key,
        started_at,
        cost,
        age_override=False,
        ready_at=None,
    ):
        connection.execute(
            self.attempts.insert().values(
                org_id=org_id,
                stage_run_id=stage_run_id,
                fence=fence,
                workflow_digest=workflow_digest,
                stage_key=stage_key,
                started_at=started_at,
                ready_at=ready_at,
                charged_seconds=cost,
                age_override=age_override,
            )
        )

    def estimate(self, connection, org_id, workflow_digest, stage_key):
        e = self.estimates
        row = (
            connection.execute(
                select(e).where(
                    e.c.org_id == org_id,
                    e.c.workflow_digest == workflow_digest,
                    e.c.stage_key == stage_key,
                )
            )
            .mappings()
            .one_or_none()
        )
        return (
            CostEstimate(
                samples=row["samples"],
                ewma_seconds=row["ewma_seconds"],
                timeouts=row["timeouts"],
            )
            if row is not None
            else CostEstimate()
        )

    def record_finish(self, connection, *, org_id, stage_run_id, fence, now, timed_out):
        a = self.attempts
        identity = (
            (a.c.org_id == org_id)
            & (a.c.stage_run_id == stage_run_id)
            & (a.c.fence == fence)
            & a.c.finished_at.is_(None)
        )
        row = connection.execute(select(a).where(identity)).mappings().one()
        started = row["started_at"].replace(tzinfo=now.tzinfo)
        seconds = max(0.0, (now - started).total_seconds())
        connection.execute(
            a.update()
            .where(identity)
            .values(
                observed_seconds=seconds,
                timed_out=timed_out,
                finished_at=now,
            )
        )
        e = self.estimates
        org, digest, stage = org_id, row["workflow_digest"], row["stage_key"]
        prior = self.estimate(connection, org, digest, stage)
        updated = prior.observe(seconds, timed_out=timed_out)
        values = dict(
            samples=updated.samples, ewma_seconds=updated.ewma_seconds, timeouts=updated.timeouts
        )
        if prior.samples:
            connection.execute(
                e.update()
                .where(
                    e.c.org_id == org,
                    e.c.workflow_digest == digest,
                    e.c.stage_key == stage,
                )
                .values(**values)
            )
        else:
            connection.execute(
                e.insert().values(
                    org_id=org,
                    workflow_digest=digest,
                    stage_key=stage,
                    **values,
                )
            )
