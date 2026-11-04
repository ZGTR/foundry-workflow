"""Relaying the workflow ledger's outbox. A SECOND port, deliberately.

WHY THIS IS NOT A METHOD ON `WorkflowEnginePort`. That port's own docstring
says what it is for: "what the HTTP layer needs from a durable workflow engine
-- start a job, submit a command against a stage run, read a stage run back".
Draining is none of those. It is never called from the request path, it takes
no principal, and the controller must not be able to reach it at all. Widening
the request-path port to carry a scheduled concern would put a method the HTTP
layer must never call on the one contract the HTTP layer depends on.

WHY A PORT RATHER THAN A CONCRETE METHOD ON THE SQL ADAPTER. The composition
root has to bind this to something, and a scheduled entrypoint that reached for
`SqlWorkflowEngine` by name would be untestable without a database. The port
also buys `tests/shape/test_layering.py`'s check 3 for free: a fake is required
to exist, so the drain has an in-memory implementation whether or not anyone
remembers to write one.

WHAT "DRAINED" MEANS HERE, because the order is the whole content of the
mechanism and it is chosen against the failure that actually happens:
`shepherd_workflow.dispatcher.OutboxDispatcher` PUBLISHES a row and only then
marks it sent. A crash between the two re-publishes -- a duplicate, which is
recoverable because every row carries a stable key. The other order loses the
event, which is not recoverable at all, because nothing records that it should
have existed. An implementation of this port must preserve that order; the
package's dispatcher is the intended way to get it, not a re-implementation.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from shepherd_workflow.dispatcher import Broker, DispatchReport


@runtime_checkable
class LedgerOutboxDrain(Protocol):
    """Moves pending ledger outbox rows to `broker`, at-least-once."""

    def drain_pending(self, *, broker: Broker, batch_size: int = 100) -> DispatchReport:
        """Publish every pending row through `broker` and mark what landed.

        Returns the package's own `DispatchReport`: published, ambiguous and
        failed counted apart. A failed row stays pending on purpose -- the next
        drain retries it -- so a caller must not read `failed > 0` as data loss.
        """
        ...
