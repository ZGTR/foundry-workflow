"""The real `ReadinessCheck`: can the workflow ledger actually be reached.

`/readyz` has to ask the thing the service cannot serve a request without, and
for this service that is the ledger. A probe against any other store would go
green while every `POST /v1/jobs` returned a 500, which is worse than no probe
at all: a load balancer would keep sending traffic to an instance that cannot
do the one thing it exists to do.

Deliberately cheap -- one `get()` for an id that does not exist, which is a
single indexed SELECT, not a full query -- because `/readyz` is polled
frequently and must never become the bottleneck it is supposed to detect.

THE PROBE PRINCIPAL IS POWERLESS ON PURPOSE. It carries no scopes and an org id
no tenant can hold. `WorkflowEnginePort.get` answers `None` for both "no such
stage run" and "not yours", so even if the probe id somehow collided with a real
stage run, this check could not read it: the only two outcomes are `None` and an
exception. Readiness is an availability question, and an availability question
that can also read tenant data is a data-exposure surface with a health-check
name on it.
"""

from __future__ import annotations

from shepherd_contracts.principal import CallerPrincipalV1

from shepherd_workflow_service.ports.workflow_engine import WorkflowEnginePort

#: An id shaped like a real stage run id (`srun_` prefix) so the probe exercises
#: the same lookup path a caller does, but derived from nothing, so it can never
#: name an actual row.
_PROBE_STAGE_RUN_ID = "srun_readyz_probe"

_PROBE_PRINCIPAL = CallerPrincipalV1(
    org_id="__readyz_probe__",
    subject_id="__readyz_probe__",
    scopes=frozenset(),
    issued_by="readiness",
)


class EngineReadinessCheck:
    def __init__(self, engine: WorkflowEnginePort) -> None:
        self._engine = engine

    def check(self) -> bool:
        try:
            self._engine.get(_PROBE_STAGE_RUN_ID, principal=_PROBE_PRINCIPAL)
        except Exception:  # noqa: BLE001 -- any failure means "not ready"
            return False
        return True
