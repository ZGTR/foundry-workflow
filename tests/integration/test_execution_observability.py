import io
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from shepherd_workflow_service.adapters.fake.observability import (
    EmptyObservability,
    RecordingMetrics,
)
from shepherd_workflow_service.adapters.real.emf_metrics import EmfMetrics
from shepherd_workflow_service.app_factory import create_app
from shepherd_workflow_service.services.execution_cycle import ExecutionCycle
from tests.integration.test_declarative_execution import event, receive, setup


def test_sql_metrics_include_backlog_age_and_due_recovery(tmp_path):
    container, clock = setup(tmp_path)
    receive(container, event(clock))
    assert container.observations.metrics()["RunningStages"] == 1
    assert container.observations.metrics()["PendingOutbox"] == 2
    clock.advance(901)
    metrics = container.observations.metrics()
    assert metrics["OldestOutboxAgeSeconds"] == 901
    assert metrics["OverdueStages"] == 1
    container.execution_service.tick()
    assert container.observations.metrics()["RetryWaitingStages"] == 1
    clock.advance(30)
    container.execution_service.tick()
    assert container.observations.metrics()["RetryAttempts"] == 1


def test_history_requires_tenant_and_scope_and_omits_artifacts(tmp_path):
    container, clock = setup(tmp_path)
    job_id = receive(container, event(clock, input_ref="private-artifact"))[0]
    client = TestClient(create_app(container))
    url = f"/v2/jobs/{job_id}/history"
    headers = {"x-org-id": "org", "x-subject-id": "operator", "x-scopes": "jobs:read"}
    response = client.get(url, headers=headers)
    assert response.status_code == 200, response.text
    assert {e["type"] for e in response.json()} == {"stage.leased", "example.generate.requested"}
    assert "private-artifact" not in response.text
    assert len(client.get(url + "?limit=1", headers=headers).json()) == 1
    assert client.get(url + "?limit=501", headers=headers).status_code == 422
    assert client.get(url, headers={**headers, "x-org-id": "other"}).status_code == 404
    assert client.get(url, headers={**headers, "x-scopes": "jobs:write"}).status_code == 401


@pytest.mark.parametrize("failed_operation", ["consume", "recover", "publish"])
def test_cycle_isolates_failures_and_still_emits_heartbeat(failed_operation):
    calls = []

    def action(name):
        def run():
            calls.append(name)
            if name == failed_operation:
                raise RuntimeError("offline")
            return SimpleNamespace(failed=0, ambiguous=0)
        return run

    sink = RecordingMetrics()
    cycle = ExecutionCycle(
        consumer=SimpleNamespace(poll=action("consume")),
        service=SimpleNamespace(tick=action("recover")), drain=action("publish"),
        observations=EmptyObservability(), metrics=sink,
    )
    assert cycle.run() == 1
    assert calls == ["consume", "recover", "publish"]
    assert sink.samples == [{"WorkerHeartbeat": 1, "WorkerCycleFailures": 1}]


def test_emf_uses_bounded_dimensions_and_seconds_units(tmp_path):
    _, clock = setup(tmp_path)
    stream = io.StringIO()
    EmfMetrics(stream=stream, service="foundry-workflow", environment="test", clock=clock).emit(
        {"WorkerHeartbeat": 1, "OldestOutboxAgeSeconds": 25}
    )
    record = json.loads(stream.getvalue())
    metric = record["_aws"]["CloudWatchMetrics"][0]
    assert metric["Dimensions"] == [["Service", "Environment"]]
    assert metric["Metrics"][1] == {"Name": "OldestOutboxAgeSeconds", "Unit": "Seconds"}
    assert record["_aws"]["Timestamp"] == int(clock.now().timestamp() * 1000)
