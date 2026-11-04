"""`/healthz` and `/readyz` answer different questions and must be able to
disagree. This is the direct proof: force readiness false and watch liveness
stay true.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from shepherd_workflow_service.adapters.fake.event_bus import RecordingPublisher
from shepherd_workflow_service.adapters.fake.readiness import FakeReadinessCheck
from shepherd_workflow_service.app_factory import create_app
from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import build_container
from tests.conftest import WORKFLOW_DEFINITION_PATH


def _config() -> ServiceConfig:
    return ServiceConfig(
        env="test",
        port=8080,
        runtime_mode="http",
        issuer="test-issuer",
        db_path=":memory:",
        event_bus_mode="in_process",
        sqs_queue_url=None,
        aws_region=None,
        workflow_definition_path=WORKFLOW_DEFINITION_PATH,
    )


def _client(*, ready: bool) -> TestClient:
    container = build_container(
        _config(),
        publisher=RecordingPublisher(),
        readiness_check=FakeReadinessCheck(ready=ready),
    )
    return TestClient(create_app(container))


def test_healthz_is_always_up() -> None:
    client = _client(ready=False)

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "up"}


def test_readyz_reflects_the_readiness_check() -> None:
    client = _client(ready=True)

    response = client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {"ready": True}


def test_healthz_and_readyz_can_disagree() -> None:
    client = _client(ready=False)

    healthz = client.get("/healthz")
    readyz = client.get("/readyz")

    assert healthz.status_code == 200
    assert readyz.status_code == 503
    assert readyz.json() == {"ready": False}
