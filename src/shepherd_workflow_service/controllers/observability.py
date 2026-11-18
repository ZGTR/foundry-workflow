"""Tenant-scoped job history. Fleet metrics are emitted through the worker only."""
from fastapi import APIRouter, Query, Request


def build_router(observations, authenticate):
    router = APIRouter(prefix="/v2", tags=["operations"])

    @router.get("/jobs/{job_id}/history")
    def history(job_id: str, request: Request, limit: int = Query(100, ge=1, le=500)):
        return observations.history(job_id, authenticate(request.headers), limit)

    return router
