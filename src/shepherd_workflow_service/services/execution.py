"""Authentication boundary shared by HTTP callbacks and trusted queue consumption."""

from shepherd_contracts import CallerPrincipalV1
from shepherd_contracts.errors import Unauthorized

from shepherd_workflow_service.errors import StageRunNotFound
from shepherd_workflow_service.models.execution import EventEnvelope
from shepherd_workflow_service.models.tenancy import TenantRegistry
from shepherd_workflow_service.ports.execution import ExecutionRepository
from shepherd_workflow_service.ports.workflow_engine import StageRunView


class ExecutionService:
    def __init__(
        self, repository: ExecutionRepository, tenant_registry: TenantRegistry | None = None
    ):
        self.repository = repository
        self.tenant_registry = tenant_registry

    def _require_tenant(self, principal: CallerPrincipalV1, scope: str) -> None:
        workload = self.tenant_registry.active_workload(principal) if self.tenant_registry else None
        if workload is None or scope not in workload.scopes or scope not in principal.scopes:
            raise Unauthorized("workload_access_denied")

    def receive(self, event: EventEnvelope, principal: CallerPrincipalV1) -> list[str]:
        if self.tenant_registry is None:
            principal.require("jobs:write")
        else:
            self._require_tenant(
                principal,
                "workflow:complete" if event.correlation else "workflow:submit",
            )
        if event.tenant_id != principal.org_id or event.producer != principal.subject_id:
            raise Unauthorized("event tenant/producer does not match authenticated principal")
        return self.repository.receive(event, principal)

    def require_job(self, job_id: str, principal: CallerPrincipalV1) -> None:
        self._require_tenant(principal, "workflow:read")
        if not self.repository.owns_job(job_id, principal):
            raise StageRunNotFound("job or stage not found")

    def get(self, stage_run_id: str, principal: CallerPrincipalV1) -> StageRunView | None:
        self._require_tenant(principal, "workflow:read")
        return self.repository.get(stage_run_id, principal)

    def stages(self, job_id: str, principal: CallerPrincipalV1) -> list[StageRunView]:
        self._require_tenant(principal, "workflow:read")
        return self.repository.stages(job_id, principal)

    def tick(self, limit: int = 100) -> int:
        return self.repository.tick(limit)

    def admit(self, limit: int = 100) -> int:
        return self.repository.admit(limit)

    def scheduler_metrics(self) -> dict[str, float | int]:
        return self.repository.scheduler_metrics()
