"""Owner-checked, independently leased deliveries from the workflow ledger."""

import re
from datetime import UTC, timedelta

from pydantic import ValidationError
from pydantic_core import PydanticSerializationError
from shepherd_contracts import PossibleSend, derive_id
from shepherd_workflow.dispatcher import DispatchReport
from sqlalchemy import Column, DateTime, Index, Integer, String, Table, select
from sqlalchemy.exc import IntegrityError

from shepherd_workflow_service.adapters.real.signed_webhook import PermanentDeliveryFailure
from shepherd_workflow_service.models.execution import EventEnvelope
from shepherd_workflow_service.models.tenancy import validate_tenant_id

_PRIVATE = "private"
_PRIVATE_VERSION = "1"
_MAX_ATTEMPTS = 8
_LEASE = timedelta(seconds=60)
_HEADER_ID = re.compile(r"[a-zA-Z0-9_.:-]{1,64}", re.ASCII)


class _LostClaim(Exception):
    """The worker's delivery lease expired or was reclaimed."""


class TenantOutboxDelivery:
    """The delivery registry and broker are service configuration, never event data."""

    def __init__(
        self,
        *,
        engine,
        clock,
        producer,
        private_broker,
        destinations=(),
        bindings=None,
        authorize_ref=None,
    ):
        self.engine = engine
        self.clock = clock
        self.producer = producer
        self.broker = private_broker
        self.bindings = bindings
        self.authorize_ref = authorize_ref or (lambda _owner, _ref: False)
        self.destinations = {}
        endpoint_owners = {}
        for item in destinations:
            key = validate_tenant_id(item.tenant_id), item.destination_id
            prior = endpoint_owners.setdefault(item.endpoint, item.tenant_id)
            if (
                prior != item.tenant_id
                or not _HEADER_ID.fullmatch(item.destination_id)
                or item.destination_id == _PRIVATE
                or key in self.destinations
                or not item.event_types
            ):
                raise ValueError("invalid or duplicate delivery destination")
            self.destinations[key] = item
        outbox = engine.tables["outbox"]
        name = outbox.name + "_delivery"
        metadata = outbox.metadata
        self.deliveries = (
            metadata.tables[name]
            if name in metadata.tables
            else Table(
                name,
                metadata,
                Column("delivery_id", String(64), primary_key=True),
                Column("event_id", String(64), nullable=False, index=True),
                Column("org_id", outbox.c.org_id.type, nullable=False),
                Column("destination", String(64), nullable=False),
                Column("destination_version", String(64), nullable=False),
                Column("state", String(16), nullable=False),
                Column("fence", Integer, nullable=False, default=0),
                Column("attempts", Integer, nullable=False, default=0),
                Column("available_at", DateTime(timezone=True), nullable=False),
                Column("reason", String(64)),
                Index(f"ix_{name}_available", "state", "available_at"),
            )
        )
        self._schema_ready = False

    def check_ready(self):
        if self._schema_ready:
            return
        with self.engine.open_connection() as connection:
            self.deliveries.create(connection, checkfirst=True)
            connection.commit()
        self._schema_ready = True

    def _event(self, connection, source, owner):
        payload = source["payload"]
        event_id, event_type = source["key"], source["event_class"]
        try:
            validate_tenant_id(owner)
        except ValueError:
            raise PermanentDeliveryFailure("noncanonical_outbox_owner") from None
        if (
            not _HEADER_ID.fullmatch(event_id)
            or not isinstance(payload, dict)
            or payload.get("org_id") != owner
        ):
            raise PermanentDeliveryFailure("outbox_owner_mismatch")
        has_envelope = "envelope" in payload
        try:
            if has_envelope:
                event = EventEnvelope.model_validate(payload["envelope"])
            else:
                when = source["created_at"]
                event = EventEnvelope(
                    event_id=event_id,
                    type=event_type,
                    tenant_id=owner,
                    producer=self.producer,
                    occurred_at=when.replace(tzinfo=UTC) if when.tzinfo is None else when,
                    output_ref=payload.get("output_ref"),
                    data=payload,
                )
        except ValidationError:
            raise PermanentDeliveryFailure("malformed_event") from None
        if (
            event.tenant_id != owner
            or event.event_id != event_id
            or event.type != event_type
            or event.producer != self.producer
        ):
            raise PermanentDeliveryFailure("envelope_binding_mismatch")
        correlation = event.correlation
        ids = set()
        stage_ids = set()
        digests = set()
        for record in (payload, event.data):
            for field in ("org_id", "tenant_id"):
                if field in record and record[field] != owner:
                    raise PermanentDeliveryFailure("payload_owner_mismatch")
            for field, target in (
                ("job_id", ids),
                ("stage_run_id", stage_ids),
                ("workflow_digest", digests),
            ):
                value = record.get(field)
                if value is not None:
                    if not isinstance(value, str) or not _HEADER_ID.fullmatch(value):
                        raise PermanentDeliveryFailure("malformed_job_binding")
                    target.add(value)
        if correlation:
            ids.add(correlation.job_id)
            stage_ids.add(correlation.stage_run_id)
            digests.add(correlation.workflow_digest)
        if len(ids) != 1 or len(stage_ids) > 1 or len(digests) > 1:
            raise PermanentDeliveryFailure("malformed_job_binding")
        if not has_envelope and not stage_ids:
            raise PermanentDeliveryFailure("missing_stage_binding")
        job_id = next(iter(ids))
        jobs = self.engine.tables["jobs"]
        job = connection.execute(
            select(jobs.c.job_id, jobs.c.org_id, jobs.c.workflow_digest).where(
                jobs.c.job_id == job_id,
                jobs.c.org_id == owner,
            )
        ).first()
        if (
            job is None
            or job.job_id != job_id
            or job.org_id != owner
            or (digests and job.workflow_digest not in digests)
        ):
            raise PermanentDeliveryFailure("job_owner_mismatch")
        if stage_ids:
            runs = self.engine.tables["stage_runs"]
            stage_id = next(iter(stage_ids))
            run = connection.execute(
                select(runs.c.stage_run_id, runs.c.job_id, runs.c.org_id).where(
                    runs.c.stage_run_id == stage_id,
                    runs.c.job_id == job_id,
                    runs.c.org_id == owner,
                )
            ).first()
            if run is None or tuple(run) != (stage_id, job_id, owner):
                raise PermanentDeliveryFailure("stage_owner_mismatch")
        binding = None
        if self.bindings is not None:
            bindings = self.bindings
            binding = connection.execute(
                select(bindings.c.job_id, bindings.c.org_id).where(
                    bindings.c.job_id == job_id,
                )
            ).first()
            if binding is not None and tuple(binding) != (job_id, owner):
                raise PermanentDeliveryFailure("execution_owner_mismatch")
        if has_envelope and binding is None:
            raise PermanentDeliveryFailure("missing_execution_binding")
        for ref in (
            event.input_ref,
            event.output_ref,
            *(payload.get(key) for key in ("input_ref", "output_ref")),
            *(event.data.get(key) for key in ("input_ref", "output_ref")),
        ):
            if ref is not None and not self.authorize_ref(owner, ref):
                raise PermanentDeliveryFailure("artifact_owner_mismatch")
        try:
            serialized = event.model_dump_json().encode()
        except (PydanticSerializationError, UnicodeError):
            raise PermanentDeliveryFailure("malformed_event") from None
        if len(serialized) > 240_000:
            raise PermanentDeliveryFailure("event_too_large")
        return event, binding is not None

    def _plan(self, connection, source, *, replay=False):
        owner, event_id = source["org_id"], source["key"]
        routes = [(_PRIVATE, _PRIVATE_VERSION)]
        reason = None
        try:
            event, callback_eligible = self._event(connection, source, owner)
        except PermanentDeliveryFailure as exc:
            reason = str(exc)
        else:
            routes.extend(
                (dest.destination_id, dest.routing_version)
                for (tenant, _), dest in self.destinations.items()
                if callback_eligible and tenant == owner and event.type in dest.event_types
            )
        for destination, version in routes:
            delivery_id = derive_id("dlv", owner, event_id, destination, version)
            values = {
                "state": "quarantined" if reason else "ready",
                "available_at": self.clock.now(),
                "reason": reason,
                "attempts": 0,
            }
            table = self.deliveries
            if (
                replay
                and connection.execute(
                    table.update()
                    .where(
                        table.c.delivery_id == delivery_id,
                        table.c.org_id == owner,
                        table.c.event_id == event_id,
                        table.c.state != "leased",
                    )
                    .values(**values, fence=table.c.fence + 1)
                ).rowcount
            ):
                continue
            try:
                with connection.begin_nested():
                    connection.execute(
                        table.insert().values(
                            **values,
                            delivery_id=delivery_id,
                            event_id=event_id,
                            org_id=owner,
                            destination=destination,
                            destination_version=version,
                        )
                    )
            except IntegrityError:
                if (
                    connection.execute(
                        select(table.c.delivery_id).where(table.c.delivery_id == delivery_id)
                    ).first()
                    is None
                ):
                    raise

    def _plan_pending(self, batch_size):
        table = self.deliveries
        outbox = self.engine.tables["outbox"]
        with self.engine.open_connection() as connection, connection.begin():
            if connection.dialect.name == "sqlite":
                connection.exec_driver_sql("BEGIN IMMEDIATE")
            sources = (
                connection.execute(
                    select(outbox)
                    .where(
                        outbox.c.sent_at.is_(None),
                        ~select(table.c.delivery_id)
                        .where(
                            table.c.event_id == outbox.c.key,
                            table.c.org_id == outbox.c.org_id,
                            table.c.destination == _PRIVATE,
                        )
                        .exists(),
                    )
                    .order_by(outbox.c.created_at, outbox.c.key)
                    .limit(batch_size)
                    .with_for_update(skip_locked=True)
                )
                .mappings()
                .all()
            )
            for source in sources:
                self._plan(connection, source)

    def _claim(self):
        table = self.deliveries
        now = self.clock.now()
        with self.engine.open_connection() as connection, connection.begin():
            row = (
                connection.execute(
                    select(table)
                    .where(
                        table.c.state.in_(["ready", "leased"]),
                        table.c.available_at <= now,
                    )
                    .order_by(table.c.available_at, table.c.delivery_id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            claimed = connection.execute(
                table.update()
                .where(
                    table.c.delivery_id == row["delivery_id"],
                    table.c.org_id == row["org_id"],
                    table.c.fence == row["fence"],
                    table.c.state == row["state"],
                    table.c.available_at <= now,
                )
                .values(
                    fence=row["fence"] + 1,
                    attempts=row["attempts"] + 1,
                    state="leased",
                    available_at=now + _LEASE,
                )
            ).rowcount
            if claimed != 1:
                return None
            return {**row, "fence": row["fence"] + 1, "attempts": row["attempts"] + 1}

    def _send(self, row):
        if row["attempts"] > _MAX_ATTEMPTS:
            raise PermanentDeliveryFailure("attempts_exhausted")
        outbox = self.engine.tables["outbox"]
        owner, event_id = row["org_id"], row["event_id"]
        with self.engine.open_connection() as connection:
            if (
                connection.scalar(
                    select(self.deliveries.c.delivery_id).where(*self._active_claim(row))
                )
                is None
            ):
                raise _LostClaim
            source = (
                connection.execute(
                    select(outbox).where(outbox.c.key == event_id, outbox.c.org_id == owner)
                )
                .mappings()
                .first()
            )
            if source is None or source["org_id"] != owner or source["key"] != event_id:
                raise PermanentDeliveryFailure("source_owner_mismatch")
            event, callback_eligible = self._event(connection, source, owner)
        if event.expires_at is not None and (
            event.expires_at.tzinfo is None or event.expires_at <= self.clock.now()
        ):
            raise PermanentDeliveryFailure("event_expired")
        route = row["destination"]
        if route == _PRIVATE:
            if row["destination_version"] != _PRIVATE_VERSION:
                raise PermanentDeliveryFailure("invalid_private_route")
            self.broker.publish(
                key=event_id,
                event_class=event.type,
                payload={"org_id": owner, "envelope": event.model_dump(mode="json")},
            )
        else:
            destination = self.destinations.get((owner, route))
            if (
                not callback_eligible
                or destination is None
                or destination.routing_version != row["destination_version"]
                or event.type not in destination.event_types
            ):
                raise PermanentDeliveryFailure("destination_inactive")
            destination.send(event)

    def _active_claim(self, row):
        table = self.deliveries.c
        return (
            table.delivery_id == row["delivery_id"],
            table.org_id == row["org_id"],
            table.event_id == row["event_id"],
            table.destination == row["destination"],
            table.destination_version == row["destination_version"],
            table.fence == row["fence"],
            table.state == "leased",
            table.available_at > self.clock.now(),
        )

    def _finish(self, row, *, state, reason=None) -> bool:
        now = self.clock.now()
        with self.engine.open_connection() as connection, connection.begin():
            updated = connection.execute(
                self.deliveries.update()
                .where(*self._active_claim(row))
                .values(
                    state=state,
                    reason=reason,
                    available_at=now + timedelta(seconds=min(3600, 5 * 2 ** row["attempts"])),
                )
            ).rowcount
            if updated and state == "sent" and row["destination"] == _PRIVATE:
                outbox = self.engine.tables["outbox"]
                connection.execute(
                    outbox.update()
                    .where(
                        outbox.c.key == row["event_id"],
                        outbox.c.org_id == row["org_id"],
                    )
                    .values(sent_at=now)
                )
            return updated == 1

    def drain(self, *, batch_size=100) -> DispatchReport:
        if not 1 <= batch_size <= 1_000:
            raise ValueError("delivery batch size must be between 1 and 1000")
        self.check_ready()
        self._plan_pending(batch_size)
        published = ambiguous = failed = 0
        for _ in range(batch_size):
            row = self._claim()
            if row is None:
                break
            try:
                self._send(row)
            except _LostClaim:
                continue
            except PermanentDeliveryFailure as exc:
                self._finish(row, state="quarantined", reason=str(exc))
                failed += 1
            except Exception as exc:
                exhausted = row["attempts"] >= _MAX_ATTEMPTS
                self._finish(
                    row,
                    state="quarantined" if exhausted else "ready",
                    reason="attempts_exhausted" if exhausted else "transient_failure",
                )
                if isinstance(exc, PossibleSend):
                    ambiguous += 1
                else:
                    failed += 1
            else:
                if self._finish(row, state="sent"):
                    published += 1
                else:
                    ambiguous += 1
        return DispatchReport(published=published, ambiguous=ambiguous, failed=failed)

    def replay(self, *, owner, event_id) -> bool:
        """Operator-only entry point. Re-evaluate current routing for the original owner."""
        validate_tenant_id(owner)
        outbox = self.engine.tables["outbox"]
        self.check_ready()
        with self.engine.open_connection() as connection, connection.begin():
            if connection.dialect.name == "sqlite":
                connection.exec_driver_sql("BEGIN IMMEDIATE")
            source = (
                connection.execute(
                    select(outbox)
                    .where(outbox.c.key == event_id, outbox.c.org_id == owner)
                    .with_for_update()
                )
                .mappings()
                .first()
            )
            if source is None or source["org_id"] != owner or source["key"] != event_id:
                return False
            self._plan(connection, source, replay=True)
            return True
