"""Atomic operations needed by the execution application service."""
from typing import Protocol

from shepherd_contracts import CallerPrincipalV1

from shepherd_workflow_service.models.execution import EventEnvelope


class ExecutionRepository(Protocol):
    def check_ready(self) -> None: ...
    def receive(self, event: EventEnvelope, principal: CallerPrincipalV1) -> list[str]: ...
    def tick(self, limit: int = 100) -> int: ...


class ExecutionConsumer(Protocol):
    def poll(self) -> int:
        """Consume one batch and return the number of failed deliveries."""
        ...
