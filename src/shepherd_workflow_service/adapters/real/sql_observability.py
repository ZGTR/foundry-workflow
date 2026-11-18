"""SQL read projections. No identifiers or artifact contents become metric labels."""
from datetime import UTC

from sqlalchemy import case, func, or_, select

from shepherd_workflow_service.errors import StageRunNotFound


class SqlObservability:
    def __init__(self, engine, clock):
        self.engine = engine
        self.clock = clock

    def metrics(self):
        tables = self.engine.tables
        runs, outbox = tables["stage_runs"], tables["outbox"]
        with self.engine.open_connection() as connection:
            states = dict(connection.execute(select(
                runs.c.status, func.count()
            ).group_by(runs.c.status)).all())
            retries = connection.execute(select(func.sum(case(
                (runs.c.fence > 1, runs.c.fence - 1), else_=0
            )))).scalar_one() or 0
            pending, oldest = connection.execute(select(
                func.count(), func.min(outbox.c.created_at)
            ).where(outbox.c.sent_at.is_(None))).one()
            overdue = connection.execute(select(func.count()).select_from(runs).where(
                runs.c.status.in_(["pending", "leased", "retry_wait"]),
                or_(
                    runs.c.deadline_at <= self.clock.now(),
                    (runs.c.status == "leased") & (runs.c.lease_expires_at <= self.clock.now()),
                    (runs.c.status == "retry_wait") & (runs.c.next_attempt_at <= self.clock.now()),
                ),
            )).scalar_one()
        age = 0
        if oldest is not None:
            oldest = oldest.replace(tzinfo=UTC) if oldest.tzinfo is None else oldest
            age = max(0, (self.clock.now() - oldest).total_seconds())
        return {
            "PendingStages": states.get("pending", 0),
            "RunningStages": states.get("leased", 0),
            "RetryWaitingStages": states.get("retry_wait", 0),
            "FailedStages": states.get("failed", 0),
            "ExpiredStages": states.get("expired", 0),
            "RetryAttempts": retries,
            "PendingOutbox": pending,
            "OldestOutboxAgeSeconds": age,
            "OverdueStages": overdue,
        }

    def history(self, job_id, principal, limit):
        principal.require("jobs:read")
        jobs, outbox = self.engine.tables["jobs"], self.engine.tables["outbox"]
        with self.engine.open_connection() as connection:
            visible = connection.execute(select(jobs.c.job_id).where(
                jobs.c.job_id == job_id, jobs.c.org_id == principal.org_id
            )).scalar_one_or_none()
            if visible is None:
                raise StageRunNotFound(job_id)
            rows = connection.execute(select(outbox).where(
                outbox.c.org_id == principal.org_id,
                or_(
                    outbox.c.payload["job_id"].as_string() == job_id,
                    outbox.c.payload["envelope"]["correlation"]["job_id"].as_string() == job_id,
                ),
            ).order_by(outbox.c.created_at.desc(), outbox.c.key.desc()).limit(limit)).mappings()
            return [{
                "event_id": row["key"], "type": row["event_class"],
                "occurred_at": row["created_at"],
                "stage_run_id": row["payload"].get("stage_run_id") or row["payload"].get(
                    "envelope", {}
                ).get("correlation", {}).get("stage_run_id"),
                "state_version": row["payload"].get("state_version"),
                "fence": row["payload"].get("fence") or row["payload"].get(
                    "envelope", {}
                ).get("correlation", {}).get("fence"),
                "status": row["payload"].get("status"),
                "published": row["sent_at"] is not None,
            } for row in rows]
