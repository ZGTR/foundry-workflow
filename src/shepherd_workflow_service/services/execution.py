"""Authentication boundary shared by HTTP callbacks and trusted queue consumption."""
from shepherd_contracts.errors import Unauthorized

from shepherd_workflow_service.ports.execution import ExecutionRepository


class ExecutionService:
    def __init__(self, repository: ExecutionRepository):
        self.repository = repository

    def receive(self, event, principal):
        principal.require("jobs:write")
        if event.tenant_id != principal.org_id or event.producer != principal.subject_id:
            raise Unauthorized("event tenant/producer does not match authenticated principal")
        return self.repository.receive(event, principal)

    def tick(self, limit=100):
        return self.repository.tick(limit)
