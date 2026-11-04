"""Routes for the workflow ledger. Thin: parse, authenticate, call, serialise.

No try/except and no status-code arithmetic: a refusal reaches here as an
exception the service raised, and `app_factory` maps its type to 404/409/422
once. `tests/shape/test_layering.py` holds every handler under 12 lines, which is
the mechanical reason none of them can grow a rule.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

from fastapi import APIRouter, Request, status
from shepherd_contracts.principal import CallerPrincipalV1

from shepherd_workflow_service.schemas.workflow import (
    CommandRequest,
    JobStartRequest,
    StageRunResponse,
)
from shepherd_workflow_service.services.workflow_service import WorkflowService

Authenticate = Callable[[Mapping[str, str]], CallerPrincipalV1]


def build_router(service: WorkflowService, authenticate: Authenticate) -> APIRouter:
    router = APIRouter(prefix="/v1", tags=["workflow"])

    @router.post("/jobs", status_code=status.HTTP_201_CREATED, response_model=StageRunResponse)
    def start_job(payload: JobStartRequest, request: Request) -> StageRunResponse:
        principal = authenticate(request.headers)
        view = service.start_job(
            principal, job_id=payload.job_id, input_digest=payload.input_digest
        )
        return StageRunResponse.from_view(view)

    @router.post("/stage-runs/{stage_run_id}/commands", response_model=StageRunResponse)
    def submit_command(
        stage_run_id: str, payload: CommandRequest, request: Request
    ) -> StageRunResponse:
        principal = authenticate(request.headers)
        view = service.submit(principal, stage_run_id=stage_run_id, **payload.model_dump())
        return StageRunResponse.from_view(view)

    @router.get("/stage-runs/{stage_run_id}", response_model=StageRunResponse)
    def get_stage_run(stage_run_id: str, request: Request) -> StageRunResponse:
        principal = authenticate(request.headers)
        return StageRunResponse.from_view(service.get(principal, stage_run_id=stage_run_id))

    @router.get("/jobs/{job_id}/stages", response_model=list[StageRunResponse])
    def get_job_stages(job_id: str, request: Request) -> list[StageRunResponse]:
        principal = authenticate(request.headers)
        return [
            StageRunResponse.from_view(view) for view in service.stages(principal, job_id=job_id)
        ]

    return router
