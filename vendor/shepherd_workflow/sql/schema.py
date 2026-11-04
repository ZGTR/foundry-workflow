"""The tables shepherd-workflow owns, as a function of a caller-supplied prefix.

The prefix is not decoration. Two things need it, and they are different needs:

- A consumer that already has a `jobs` table must be able to host these without
  a collision, so the names cannot be fixed strings.
- The system this was extracted from is physically multi-tenant: it gives each
  app its own table prefix, so `app_a` and `app_b` are separate tables, not
  separate rows. A package with hardcoded names cannot be deployed there at all.

A logically multi-tenant host, by contrast, partitions on `org_id`. Both work here:
prefix chooses the physical partition, `org_id` on every row scopes the logical
one, and a deployment picks whichever it already uses.
"""

from __future__ import annotations

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
)

from shepherd_workflow.ports import DEFAULT_TABLE_PREFIX, LOGICAL_TABLES


def build_tables(metadata: MetaData, *, prefix: str = DEFAULT_TABLE_PREFIX) -> dict[str, Table]:
    """Define shepherd-workflow' five tables on ``metadata`` and return them by logical name."""
    if not prefix.replace("_", "").isalnum() and prefix != "":
        raise ValueError(f"invalid table prefix: {prefix!r}")

    jobs = Table(
        f"{prefix}jobs",
        metadata,
        Column("job_id", String(64), primary_key=True),
        Column("org_id", String(64), nullable=False, index=True),
        Column("workflow_key", String(128), nullable=False),
        Column("workflow_digest", String(64), nullable=False),
        Column("status", String(32), nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False),
    )

    stage_runs = Table(
        f"{prefix}stage_runs",
        metadata,
        Column("stage_run_id", String(64), primary_key=True),
        Column("job_id", String(64), nullable=False, index=True),
        Column("org_id", String(64), nullable=False, index=True),
        Column("workflow_digest", String(64), nullable=False),
        Column("stage_key", String(128), nullable=False),
        # state_version is the compare-and-set column. Every conditional UPDATE
        # in repository.py keys on it, and it is why two writers cannot both win.
        Column("state_version", Integer, nullable=False, default=0),
        Column("fence", BigInteger, nullable=True),
        Column("attempt_id", String(64), nullable=False),
        Column("dispatch_key", String(64), nullable=False),
        Column("input_digest", String(64), nullable=False),
        Column("status", String(32), nullable=False),
        Column("deadline_at", DateTime(timezone=True), nullable=False),
        Column("lease_expires_at", DateTime(timezone=True), nullable=True),
        Column("next_attempt_at", DateTime(timezone=True), nullable=True),
        Column("artifact_digest", String(64), nullable=True),
        Column("output_ref", String(2048), nullable=True),
        Column("next_stage_run_id", String(64), nullable=True),
        Column("feature_enabled", Boolean, nullable=False, default=True),
        Column("cancelled", Boolean, nullable=False, default=False),
        # One live stage run per unit of work. This is the database half of the
        # idempotency contract: the reducer refuses a duplicate it can see, and
        # this refuses one it cannot, such as two API calls racing on a cold job.
        UniqueConstraint("dispatch_key", name=f"uq_{prefix}stage_runs_dispatch_key"),
        Index(f"ix_{prefix}stage_runs_claimable", "org_id", "status", "deadline_at"),
    )

    attempts = Table(
        f"{prefix}attempts",
        metadata,
        Column("attempt_id", String(64), primary_key=True),
        Column("stage_run_id", String(64), nullable=False, index=True),
        Column("org_id", String(64), nullable=False),
        Column("fence", BigInteger, nullable=False),
        Column("started_at", DateTime(timezone=True), nullable=False),
        Column("outcome", String(32), nullable=True),
    )

    events = Table(
        f"{prefix}events",
        metadata,
        Column("event_id", String(64), primary_key=True),
        Column("stage_run_id", String(64), nullable=False, index=True),
        Column("org_id", String(64), nullable=False),
        Column("event_class", String(64), nullable=False),
        Column("occurred_at", DateTime(timezone=True), nullable=False),
        Column("payload", JSON, nullable=False),
    )

    outbox = Table(
        f"{prefix}outbox",
        metadata,
        # The key is the primary key, not a surrogate. That single choice is what
        # makes a retried enqueue a no-op instead of a duplicate publish -- the
        # database refuses it rather than the application remembering to.
        Column("key", String(64), primary_key=True),
        Column("org_id", String(64), nullable=False, index=True),
        Column("event_class", String(64), nullable=False),
        Column("payload", JSON, nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("sent_at", DateTime(timezone=True), nullable=True),
        Column("ambiguous", Boolean, nullable=False, default=False),
        Index(f"ix_{prefix}outbox_pending", "sent_at", "created_at"),
    )

    tables = {
        "jobs": jobs,
        "stage_runs": stage_runs,
        "attempts": attempts,
        "events": events,
        "outbox": outbox,
    }
    # Two DIFFERENT things have to agree here, and an earlier version of this
    # guard checked only the first.
    #
    # 1. The logical KEY set must be LOGICAL_TABLES, so adding a table here
    #    without declaring it produces a table nothing claims to own.
    if set(tables) != set(LOGICAL_TABLES):
        raise AssertionError(
            f"build_tables defines {sorted(tables)} but LOGICAL_TABLES declares "
            f"{sorted(LOGICAL_TABLES)}; a table nobody owns cannot be written."
        )
    # 2. Each PHYSICAL name must be exactly f"{prefix}{logical}". The physical
    #    names above are handwritten, one per Table(...) call, so the key set
    #    cannot see them: a typo lands in the physical name while the key stays
    #    correct. Measured by an adversarial review -- `{"jobs": "app_jobz",
    #    ...}` passed the key-set check at exit 0, while the comment here
    #    claimed the two "can never drift apart". They can; only this second
    #    check makes that claim true, and `owned_tables(prefix)` is what the
    #    write-guard compares against, so a typo here means a table the runtime
    #    creates but refuses to write.
    misnamed = {
        logical: table.name
        for logical, table in tables.items()
        if table.name != f"{prefix}{logical}"
    }
    if misnamed:
        raise AssertionError(
            f"build_tables produced physical name(s) that do not match "
            f"prefix {prefix!r} + the logical name: {misnamed}; "
            f"owned_tables({prefix!r}) would refuse them."
        )
    return tables
