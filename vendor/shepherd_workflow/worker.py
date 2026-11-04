"""The loop that turns a queue of dispatches into completed stages.

Everything else in this package is a decision or a store. This is the only part
that *executes*, and it is deliberately the thinnest piece: claim, run, report.

The interesting content is the order and what happens when each step fails,
because a worker loop is defined by its crashes rather than its happy path.
Three rules, each of which costs money or correctness if inverted:

1. **Lease before work.** The fence is minted and durably written before the
   executor is called. A worker that runs first and claims afterwards has
   already spent the provider's money before finding out it lost the race.

2. **Never let an executor's exception end the loop.** One poisoned dispatch
   must fail its own stage and leave the worker alive for the next one; a
   worker that dies on a bad payload turns one broken job into an outage.

   The guard originally covered the EXECUTOR only, and the lease write sits
   outside it -- so `ConcurrentUpdate`, the one exception `ports.py` documents
   as normal and retryable ("the row moved between the read and the write.
   Re-read and re-decide"), was the exception that took the worker down and
   skipped every dispatch key behind it in the drain. Measured 2026-09-04.
   Contention is now counted and the key is skipped.

   The line is drawn there deliberately. `ConcurrentUpdate` means another worker
   holds the row, which is the design working. An outbox enqueue failure or an
   unknown store error is NOT normal, and it still ends the drain: the writes
   live in the caller's transaction, so continuing past a broken store would
   accumulate work the caller is about to roll back, and swallowing it would
   turn an outbox outage into silent data loss. A loop that survives everything
   is not robust, it is unfalsifiable.

3. **`PossibleSend` is not a failure.** If the executor cannot say whether the
   external effect happened, the stage is left in its leased state and the
   ambiguity is reported. Failing it would re-run work that may already have
   been billed; completing it would record an artifact that may not exist.
   Leaving it leased means the lease expires, the fence advances, and a fresh
   attempt is made under a new fence -- which is safe precisely because the old
   fence can no longer write.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from shepherd_contracts import CallerPrincipalV1, PossibleSend
from shepherd_contracts.unit_of_work import UnitOfWork

from shepherd_workflow.commands import CommandKind, CommandV1, LedgerSnapshotV1
from shepherd_workflow.ports import ConcurrentUpdate
from shepherd_workflow.runner import StageRunner


@runtime_checkable
class StageExecutor(Protocol):
    """Does the actual work of one stage. Everything domain-specific lives here.

    Returns the digest of whatever it produced. Raising ``PossibleSend`` is a
    supported, meaningful answer -- see rule 3 above -- and is not an error.
    """

    def execute(
        self, snapshot: LedgerSnapshotV1, *, principal: CallerPrincipalV1
    ) -> tuple[str, dict]:
        """Return ``(payload_digest, provider_pin)`` for the work performed."""
        ...


@dataclass(frozen=True, slots=True)
class WorkerReport:
    """What one pass of the loop did. Ambiguity is counted apart from failure."""

    claimed: int = 0
    completed: int = 0
    failed: int = 0
    ambiguous: int = 0
    refused: int = 0
    #: Keys skipped because another worker moved the row between the read and
    #: the write. Counted apart from `refused`: a refusal is a decision this
    #: package made, contention is a race it lost.
    contended: int = 0
    refusals: tuple[str, ...] = field(default_factory=tuple)

    @property
    def processed(self) -> int:
        return self.completed + self.failed + self.ambiguous + self.refused + self.contended


class StageWorker:
    """Drains dispatch keys, executing each stage exactly once per fence."""

    def __init__(
        self,
        *,
        runner: StageRunner,
        executor: StageExecutor,
        principal: CallerPrincipalV1,
    ) -> None:
        self._runner = runner
        self._executor = executor
        self._principal = principal

    def drain(self, dispatch_keys: list[str], *, uow: UnitOfWork) -> WorkerReport:
        claimed = completed = failed = ambiguous = refused = contended = 0
        refusals: list[str] = []

        for key in dispatch_keys:
            snapshot = self._runner.repository.claim_next(dispatch_key=key)
            if snapshot is None:
                continue

            try:
                lease = self._runner.apply(
                    self._command(snapshot, CommandKind.LEASE), principal=self._principal, uow=uow
                )
            except ConcurrentUpdate:
                # Rule 2, lease half. Another worker moved the row; that is the
                # design working, not a failure. Skip the key and keep draining.
                contended += 1
                continue
            if not lease.accepted:
                refused += 1
                refusals.append(str(lease.refusal))
                continue
            claimed += 1
            leased = lease.snapshot
            assert leased is not None  # an accepted lease always returns a snapshot

            try:
                payload_digest, provider_pin = self._executor.execute(
                    leased, principal=self._principal
                )
            except PossibleSend:
                # Rule 3. Leave the stage leased; the lease will expire and a
                # later attempt runs under a higher fence, which is what makes
                # the retry safe rather than a double-charge.
                ambiguous += 1
                continue
            except Exception:
                # Rule 2. Fail this stage, keep the worker alive.
                try:
                    self._runner.apply(
                        self._command(leased, CommandKind.FAIL, reason="executor_error"),
                        principal=self._principal,
                        uow=uow,
                    )
                except ConcurrentUpdate:
                    # The lease expired while the executor ran and another worker
                    # took the row. Recording the failure would be writing under a
                    # fence that no longer exists.
                    contended += 1
                    continue
                failed += 1
                continue

            try:
                done = self._runner.apply(
                    self._command(
                        leased,
                        CommandKind.COMPLETE,
                        payload_digest=payload_digest,
                        provider_pin=provider_pin,
                    ),
                    principal=self._principal,
                    uow=uow,
                )
            except ConcurrentUpdate:
                # Same race, at the other end: the work was done but the fence
                # moved. The stage is left for the worker that now holds it.
                contended += 1
                continue
            if done.accepted:
                completed += 1
            else:
                refused += 1
                refusals.append(str(done.refusal))

        return WorkerReport(
            claimed=claimed,
            completed=completed,
            failed=failed,
            ambiguous=ambiguous,
            contended=contended,
            refused=refused,
            refusals=tuple(refusals),
        )

    def _command(self, snapshot: LedgerSnapshotV1, kind: CommandKind, **over) -> CommandV1:
        from datetime import datetime

        now: datetime = self._runner.clock.now()
        base = dict(
            kind=kind,
            job_id=snapshot.job_id,
            stage_run_id=snapshot.stage_run_id,
            stage_key=snapshot.stage_key,
            workflow_digest=self._runner.workflow.digest,
            issued_at=now,
            expected_state_version=snapshot.state_version,
            fence=snapshot.fence,
            dispatch_key=snapshot.dispatch_key,
        )
        base.update(over)
        return CommandV1(**base)
