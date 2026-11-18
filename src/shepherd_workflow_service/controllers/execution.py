"""Authenticated event/webhook ingress; responses acknowledge durable acceptance."""
from fastapi import APIRouter, Request

from shepherd_workflow_service.models.execution import EventEnvelope


def build_router(service, authenticate):
    router = APIRouter(prefix="/v2", tags=["execution"])

    @router.post("/events", status_code=202)
    def receive(event: EventEnvelope, request: Request):
        return {"job_ids": service.receive(event, authenticate(request.headers))}

    return router
