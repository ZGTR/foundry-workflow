"""The workflow surface end to end through the real ASGI app.

The engine here is the SQL one against a temp SQLite file, not the in-memory
fake. The fake is faithful — it drives the same reducer — but the deployed
service runs the SQL engine, and the translations this file asserts (a refusal
becoming 409, a bad command becoming 422, another org's id becoming 404) sit
BETWEEN the engine and HTTP, where only a real request can exercise them.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from shepherd_contracts.clock import FrozenClock

from shepherd_workflow_service.app_factory import create_app
from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import build_container
from tests.conftest import WORKFLOW_DEFINITION_PATH

HEADERS = {
    "x-org-id": "org_1",
    "x-subject-id": "user_1",
    "x-scopes": "jobs:write,jobs:read",
}
OTHER_ORG = {**HEADERS, "x-org-id": "org_2"}


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    config = ServiceConfig(
        env="test",
        port=8080,
        runtime_mode="http",
        issuer="test-issuer",
        db_path=str(tmp_path / "workflow.db"),
        event_bus_mode="in_process",
        sqs_queue_url=None,
        aws_region=None,
        workflow_definition_path=WORKFLOW_DEFINITION_PATH,
    )
    container = build_container(config, clock=FrozenClock(datetime(2026, 9, 5, tzinfo=UTC)))
    return TestClient(create_app(container))


def _start(client: TestClient, headers: dict[str, str] = HEADERS) -> dict:
    response = client.post(
        "/v1/jobs", json={"job_id": "job_1", "input_digest": "dig_1"}, headers=headers
    )
    assert response.status_code == 201, response.text
    return response.json()


def test_start_lease_and_read_back(client: TestClient) -> None:
    started = _start(client)
    assert started["stage"] == "plan"
    assert started["status"] == "pending"

    leased = client.post(
        f"/v1/stage-runs/{started['stage_run_id']}/commands",
        json={"kind": "lease", "state_version": 0},
        headers=HEADERS,
    )
    assert leased.status_code == 200, leased.text
    assert leased.json()["status"] == "leased"
    assert leased.json()["state_version"] == 1
    assert leased.json()["fence"] == 1

    read = client.get(f"/v1/stage-runs/{started['stage_run_id']}", headers=HEADERS)
    assert read.status_code == 200
    assert read.json() == leased.json()


def test_a_stale_state_version_is_409_naming_the_refusal_class(client: TestClient) -> None:
    """409, not 500 and not a silent 200. The class is the body, verbatim.

    An operator reading a 409 has to know whether to re-read and retry or to stop,
    and only the refusal class distinguishes them. A paraphrase would not.
    """
    started = _start(client)
    url = f"/v1/stage-runs/{started['stage_run_id']}/commands"
    client.post(url, json={"kind": "lease", "state_version": 0}, headers=HEADERS)

    conflict = client.post(url, json={"kind": "lease", "state_version": 0}, headers=HEADERS)

    assert conflict.status_code == 409
    assert conflict.json()["detail"] == "state_version_conflict"


def test_a_command_that_is_not_a_command_is_422(client: TestClient) -> None:
    started = _start(client)

    response = client.post(
        f"/v1/stage-runs/{started['stage_run_id']}/commands",
        json={"kind": "teleport", "state_version": 0},
        headers=HEADERS,
    )

    assert response.status_code == 422
    assert "teleport" in response.json()["detail"]


def test_a_misspelled_field_is_refused_rather_than_defaulted(client: TestClient) -> None:
    """`extra="forbid"`: `state_versoin` must not silently become the default.

    Without it the command would be applied at version 0 whatever the caller
    read, which is the concurrency guarantee removed by a typo.
    """
    started = _start(client)

    response = client.post(
        f"/v1/stage-runs/{started['stage_run_id']}/commands",
        json={"kind": "lease", "state_versoin": 3, "state_version": 0},
        headers=HEADERS,
    )

    assert response.status_code == 422


def test_another_orgs_stage_run_is_404_not_403(client: TestClient) -> None:
    started = _start(client)

    response = client.get(f"/v1/stage-runs/{started['stage_run_id']}", headers=OTHER_ORG)

    assert response.status_code == 404


def test_a_missing_scope_is_401(client: TestClient) -> None:
    response = client.post(
        "/v1/jobs",
        json={"job_id": "job_1", "input_digest": "dig_1"},
        headers={**HEADERS, "x-scopes": "jobs:read"},
    )

    assert response.status_code == 401


def test_no_auth_headers_is_401(client: TestClient) -> None:
    assert client.get("/v1/stage-runs/srun_whatever").status_code == 401


def test_an_unknown_stage_run_is_404(client: TestClient) -> None:
    assert client.get("/v1/stage-runs/srun_missing", headers=HEADERS).status_code == 404


def test_the_full_stage_advances_through_the_workflow(client: TestClient) -> None:
    """plan -> leased -> succeeded, with the fence echoed back on completion.

    The completion carries the fence the lease granted. A worker that echoed a
    stale fence would be refused, which is the guarantee that a late write from a
    worker whose lease expired cannot land.
    """
    started = _start(client)
    url = f"/v1/stage-runs/{started['stage_run_id']}/commands"

    leased = client.post(url, json={"kind": "lease", "state_version": 0}, headers=HEADERS).json()
    completed = client.post(
        url,
        json={
            "kind": "complete",
            "state_version": leased["state_version"],
            "fence": leased["fence"],
            "payload": {"payload_digest": "artifact_1"},
        },
        headers=HEADERS,
    )

    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "succeeded"


def test_a_stale_fence_on_completion_is_409(client: TestClient) -> None:
    started = _start(client)
    url = f"/v1/stage-runs/{started['stage_run_id']}/commands"
    leased = client.post(url, json={"kind": "lease", "state_version": 0}, headers=HEADERS).json()

    response = client.post(
        url,
        json={
            "kind": "complete",
            "state_version": leased["state_version"],
            "fence": leased["fence"] + 7,
            "payload": {"payload_digest": "artifact_1"},
        },
        headers=HEADERS,
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "stale_fence"
