"""Read-only operational projections, independent of transport and database."""
from typing import Protocol

from shepherd_contracts import CallerPrincipalV1


class Observability(Protocol):
    def metrics(self) -> dict[str, float]: ...
    def history(self, job_id: str, principal: CallerPrincipalV1, limit: int) -> list[dict]: ...


class MetricsSink(Protocol):
    def emit(self, values: dict[str, float]) -> None: ...
