"""Allowlisted tenant intake and declarative job projections."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, Request
from shepherd_contracts.principal import CallerPrincipalV1

from shepherd_workflow_service.errors import StageRunNotFound
from shepherd_workflow_service.models.execution import EventEnvelope
from shepherd_workflow_service.ports.auth import AuthenticateTenant
from shepherd_workflow_service.ports.observability import Observability
from shepherd_workflow_service.schemas.workflow import StageRunResponse
from shepherd_workflow_service.services.execution import ExecutionService

ExecutionId = Annotated[str, Path(min_length=1, max_length=64)]
HistoryLimit = Annotated[int, Query(ge=1, le=500)]


def build_router(
    service: ExecutionService, observations: Observability, authenticate: AuthenticateTenant
) -> APIRouter:
    router = APIRouter(prefix="/v2", tags=["execution"])

    def tenant_principal(request: Request) -> CallerPrincipalV1:
        peer_ip = request.client.host if request.client else None
        return authenticate(request.headers.raw, peer_ip=peer_ip)

    principal_dependency = Depends(tenant_principal)

    @router.post("/events", status_code=202)
    def receive(event: EventEnvelope, principal: CallerPrincipalV1 = principal_dependency):
        return {"job_ids": service.receive(event, principal)}

    @router.get(
        "/stage-runs/{stage_run_id}", response_model=StageRunResponse,
        response_model_exclude={"output_ref"},
    )
    def stage(stage_run_id: ExecutionId, principal: CallerPrincipalV1 = principal_dependency):
        view = service.get(stage_run_id, principal)
        if view is None:
            raise StageRunNotFound("job or stage not found")
        return StageRunResponse.from_view(view)

    @router.get(
        "/jobs/{job_id}/stages", response_model=list[StageRunResponse],
        response_model_exclude={"__all__": {"output_ref"}},
    )
    def stages(job_id: ExecutionId, principal: CallerPrincipalV1 = principal_dependency):
        views = service.stages(job_id, principal)
        if not views:
            raise StageRunNotFound("job or stage not found")
        return [StageRunResponse.from_view(view) for view in views]

    @router.get("/jobs/{job_id}/history")
    def history(
        job_id: ExecutionId, limit: HistoryLimit = 100,
        principal: CallerPrincipalV1 = principal_dependency,
    ):
        service.require_job(job_id, principal)
        return observations.history(job_id, principal, limit)

    return router
