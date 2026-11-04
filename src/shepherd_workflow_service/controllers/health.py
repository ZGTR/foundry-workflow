"""`/healthz` and `/readyz` -- deliberately different questions.

`/healthz` never touches the container: if this handler runs at all, the
process is up. `/readyz` calls the injected `ReadinessCheck` port and can
disagree with `/healthz` at any time -- `tests/integration/test_health_ready.py`
proves it with a `FakeReadinessCheck(ready=False)`.
"""

from __future__ import annotations

from fastapi import APIRouter, Response, status

from shepherd_workflow_service.ports.readiness import ReadinessCheck


def build_router(readiness: ReadinessCheck) -> APIRouter:
    router = APIRouter(tags=["health"])

    @router.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "up"}

    @router.get("/readyz")
    def readyz(response: Response) -> dict[str, bool]:
        ready = readiness.check()
        response.status_code = status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE
        return {"ready": ready}

    return router
