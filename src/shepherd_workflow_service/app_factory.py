"""Builds the FastAPI app from a `Container`. No route registers a
try/except -- domain errors are translated to HTTP status codes here, once,
so every controller handler stays a straight-line parse/call/serialise.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from shepherd_contracts.errors import ConfigurationError, PolicyRefused, Unauthorized

from shepherd_workflow_service.container import Container
from shepherd_workflow_service.controllers import execution, health, observability, workflow
from shepherd_workflow_service.errors import (
    StageRunConflict,
    StageRunNotFound,
    StageRunRejected,
)
from shepherd_workflow_service.ports.workflow_engine import UnknownCommandKind

_STATUS_BY_ERROR = (
    (Unauthorized, 401),
    (StageRunNotFound, 404),
    # A refusal is an ANSWER, so it is a 4xx naming the refusal class, never a
    # 5xx. 409 says the ledger moved and re-reading then retrying is correct;
    # 422 says the command itself will not become applicable by retrying.
    (StageRunConflict, 409),
    (StageRunRejected, 422),
    (UnknownCommandKind, 422),
    (PolicyRefused, 403),
    (ConfigurationError, 500),
)


def create_app(container: Container) -> FastAPI:
    app = FastAPI(title="foundry-workflow")
    app.include_router(health.build_router(container.readiness_check))
    app.include_router(
        workflow.build_router(container.workflow_service, container.authenticate)
    )

    if container.execution_service is not None:
        app.include_router(execution.build_router(
            container.execution_service, container.authenticate
        ))

    if container.observations is not None:
        app.include_router(
            observability.build_router(container.observations, container.authenticate)
        )

    for error_type, status_code in _STATUS_BY_ERROR:

        def _handler(request: Request, exc: Exception, *, _code: int = status_code) -> JSONResponse:
            return JSONResponse(status_code=_code, content={"detail": str(exc)})

        app.add_exception_handler(error_type, _handler)

    return app
