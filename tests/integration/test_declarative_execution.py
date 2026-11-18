import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from shepherd_contracts import PrincipalMinter
from shepherd_contracts.clock import FrozenClock
from sqlalchemy import select

from shepherd_workflow_service.app_factory import create_app
from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import build_container
from shepherd_workflow_service.errors import StageRunRejected
from shepherd_workflow_service.models.execution import EventEnvelope

EXAMPLE = Path("contracts/examples/catalog.v2.json")


def setup(tmp_path, catalog=None):
    path = tmp_path / "catalog.json"
    if catalog is not None or not path.exists():
        path.write_text(json.dumps(catalog or json.loads(EXAMPLE.read_text())))
    clock = FrozenClock(datetime(2026, 11, 17, tzinfo=UTC))
    config = ServiceConfig(
        env="test",
        port=8080,
        runtime_mode="http",
        issuer="foundry-workflow",
        db_path=str(tmp_path / "workflow.db"),
        event_bus_mode="in_process",
        sqs_queue_url=None,
        aws_region=None,
        workflow_catalog_path=str(path),
    )
    container = build_container(config, clock=clock)
    return container, clock


def event(clock, *, id="trigger", type="example.requested", producer="example-api", **kw):
    return EventEnvelope(
        event_id=id, type=type, producer=producer, tenant_id="org", occurred_at=clock.now(), **kw
    )


def receive(container, event):
    principal = PrincipalMinter(issuer="test").mint(
        org_id=event.tenant_id, subject_id=event.producer, scopes=["jobs:write"]
    )
    return container.execution_service.receive(event, principal)


def rows(container, name):
    repository = container.execution_service.repository
    engine = repository._base()
    with engine.open_connection() as connection:
        return connection.execute(select(engine.tables[name])).mappings().all()


def dispatched(container):
    return [
        row["payload"]["envelope"]
        for row in rows(container, "outbox")
        if "envelope" in row["payload"]
    ]


def result(clock, dispatch, *, id="result", success=True, **kw):
    raw = dict(
        event(
            clock,
            id=id,
            producer="example-worker",
            type="example.generate.succeeded" if success else "example.generate.failed",
            **kw,
        ).model_dump(mode="json")
    )
    raw["correlation"] = dispatch["correlation"]
    return EventEnvelope.model_validate_json(json.dumps(raw))


def test_http_trigger_result_duplicate_and_tenant_isolation(tmp_path):
    container, clock = setup(tmp_path)
    client = TestClient(create_app(container))
    start = event(clock, input_ref="s3://example/input")
    headers = {"x-org-id": "org", "x-subject-id": "example-api", "x-scopes": "jobs:write"}
    response = client.post("/v2/events", json=start.model_dump(mode="json"), headers=headers)
    assert response.status_code == 202, response.text
    assert len(dispatched(container)) == 1
    assert (
        client.post("/v2/events", json=start.model_dump(mode="json"), headers=headers).status_code
        == 202
    )
    done = result(
        clock, dispatched(container)[0], payload_digest="digest", output_ref="s3://example/output"
    )
    headers["x-subject-id"] = "example-worker"
    headers["x-org-id"] = "other"
    assert (
        client.post("/v2/events", json=done.model_dump(mode="json"), headers=headers).status_code
        == 401
    )
    headers["x-org-id"] = "org"
    assert (
        client.post("/v2/events", json=done.model_dump(mode="json"), headers=headers).status_code
        == 202
    )
    receive(container, done)
    assert rows(container, "stage_runs")[0]["status"] == "succeeded"
    assert any(r["event_class"] == "job.succeeded" for r in rows(container, "outbox"))


def test_timeout_retry_stale_result_exhaustion_restart(tmp_path):
    container, clock = setup(tmp_path)
    receive(container, event(clock))
    first = dispatched(container)[0]
    clock.advance(901)
    restarted, _ = setup(tmp_path)
    # use the injected shared clock across process-equivalent reconstruction
    restarted = build_container(restarted.config, clock=clock)
    assert restarted.execution_service.tick() == 1
    assert rows(restarted, "stage_runs")[0]["status"] == "retry_wait"
    assert restarted.execution_service.tick() == 0
    clock.advance(30)
    assert restarted.execution_service.tick() == 1
    assert dispatched(restarted)[-1]["correlation"]["fence"] == 2
    receive(restarted, result(clock, first, payload_digest="late"))
    assert rows(restarted, "stage_runs")[0]["status"] == "leased"
    receive(
        restarted,
        result(clock, dispatched(restarted)[-1], id="failure2", success=False, retryable=True),
    )
    clock.advance(30)
    restarted.execution_service.tick()
    receive(
        restarted,
        result(clock, dispatched(restarted)[-1], id="failure3", success=False, retryable=True),
    )
    assert rows(restarted, "stage_runs")[0]["status"] == "failed"


def test_pinned_definition_multi_stage_output_after_catalog_change(tmp_path):
    raw = json.loads(EXAMPLE.read_text())
    raw["workflows"][0]["stages"].append({**raw["workflows"][0]["stages"][0], "key": "deliver"})
    container, clock = setup(tmp_path, raw)
    receive(container, event(clock))
    first = dispatched(container)[0]
    raw["workflows"][0]["version"] = 2
    raw["workflows"][0]["completion"] = "review"
    restarted, _ = setup(tmp_path, raw)
    receive(restarted, result(clock, first, payload_digest="d", output_ref="artifact"))
    assert len(dispatched(restarted)) == 2
    assert dispatched(restarted)[-1]["input_ref"] == "artifact"
    receive(restarted, result(clock, dispatched(restarted)[-1], id="done2", payload_digest="d2"))
    assert all(r["status"] == "succeeded" for r in rows(restarted, "stage_runs"))


def test_wrong_result_rolls_back_inbox_and_cross_job_correlation(tmp_path):
    container, clock = setup(tmp_path)
    receive(container, event(clock))
    done = result(clock, dispatched(container)[0], payload_digest="d")
    bad = done.model_copy(update={"producer": "intruder"})
    with pytest.raises(StageRunRejected):
        receive(container, bad)
    assert rows(container, "stage_runs")[0]["status"] == "leased"
    receive(container, done)
    assert rows(container, "stage_runs")[0]["status"] == "succeeded"


def test_multiple_products_and_versions(tmp_path):
    raw = json.loads(EXAMPLE.read_text())
    second = json.loads(json.dumps(raw["workflows"][0]))
    second["workflow_key"] = "another.flow"
    raw["workflows"].append(second)
    container, clock = setup(tmp_path, raw)
    assert len(receive(container, event(clock))) == 2
    assert len(dispatched(container)) == 2


def test_dispatch_failure_rolls_back_start_and_inbox(tmp_path, monkeypatch):
    container, clock = setup(tmp_path)
    repository = container.execution_service.repository
    original = repository._dispatch

    def fail(*args):
        original(*args)
        raise RuntimeError("crash before commit")

    monkeypatch.setattr(repository, "_dispatch", fail)
    trigger = event(clock)
    with pytest.raises(RuntimeError):
        receive(container, trigger)
    assert rows(container, "jobs") == []
    assert rows(container, "stage_runs") == []
    assert rows(container, "outbox") == []
    monkeypatch.setattr(repository, "_dispatch", original)
    receive(container, trigger)
    assert len(dispatched(container)) == 1


def test_long_async_job_and_deadline(tmp_path):
    container, clock = setup(tmp_path)
    receive(container, event(clock))
    clock.advance(600)  # longer than the legacy five-minute lease
    receive(container, result(clock, dispatched(container)[0], payload_digest="d"))
    assert rows(container, "stage_runs")[0]["status"] == "succeeded"
    receive(container, event(clock, id="another"))
    clock.advance(3600)
    container.execution_service.tick()
    assert {r["status"] for r in rows(container, "stage_runs")} == {"succeeded", "expired"}


def test_outbox_publish_failure_keeps_stable_event_for_replay(tmp_path):
    from shepherd_workflow_service.adapters.real.eventbridge_broker import EventBridgeBroker

    container, clock = setup(tmp_path)
    receive(container, event(clock))

    class Bus:
        fail = True
        messages = []

        def describe_event_bus(self, **kwargs):
            return {}

        def put_events(self, **kwargs):
            self.messages.extend(kwargs["Entries"])
            return {"FailedEntryCount": int(self.fail), "Entries": [{"ErrorCode": "Throttled"}]}

    bus = Bus()
    broker = EventBridgeBroker(
        client=bus, bus_name="test", producer="foundry-workflow", clock=clock
    )
    report = container.workflow_engine.drain_pending(broker=broker)
    assert report.failed > 0
    assert all(r["sent_at"] is None for r in rows(container, "outbox"))
    before = list(bus.messages)
    clock.advance(10)
    bus.fail = False
    report = container.workflow_engine.drain_pending(broker=broker)
    assert report.failed == 0
    assert before == bus.messages[len(before) :]
    assert all(r["sent_at"] is not None for r in rows(container, "outbox"))


def test_unknown_json_fields_fail_loudly(tmp_path):
    from shepherd_contracts.errors import ConfigurationError

    from shepherd_workflow_service.adapters.real.workflow_catalog import JsonWorkflowCatalog

    raw = json.loads(EXAMPLE.read_text())
    raw["workflows"][0]["stages"][0]["subscribee"] = "typo"
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(raw))
    with pytest.raises(ConfigurationError, match="subscribee"):
        JsonWorkflowCatalog(str(path)).load()


def test_review_is_opt_in(tmp_path):
    raw = json.loads(EXAMPLE.read_text())
    raw["workflows"][0]["completion"] = "review"
    container, clock = setup(tmp_path, raw)
    receive(container, event(clock))
    receive(container, result(clock, dispatched(container)[0], payload_digest="d"))
    assert rows(container, "stage_runs")[0]["status"] == "review_wait"


def test_concurrent_duplicate_delivery_has_one_effect(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    container, clock = setup(tmp_path)
    container.execution_service.repository.check_ready()
    trigger = event(clock)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: receive(container, trigger), range(2)))
    assert len(results[0]) == 1
    assert results[0] == results[1]
    assert len(rows(container, "jobs")) == 1
    assert len(dispatched(container)) == 1


def test_invalid_catalog_fails_readiness(tmp_path):
    container, _ = setup(tmp_path, {"workflows": []})
    response = TestClient(create_app(container)).get("/readyz")
    assert response.status_code == 503


def test_event_id_reuse_with_different_content_is_conflict(tmp_path):
    from shepherd_workflow_service.errors import StageRunConflict

    container, clock = setup(tmp_path)
    receive(container, event(clock, input_ref="one"))
    with pytest.raises(StageRunConflict, match="event_identity_conflict"):
        receive(container, event(clock, input_ref="two"))
    assert len(dispatched(container)) == 1


def test_composed_queue_to_ledger_to_bus_and_result(tmp_path, monkeypatch):
    """Exercise the production composition root with bounded in-memory AWS transports."""
    from dataclasses import replace

    import boto3

    container, clock = setup(tmp_path)
    trigger = event(clock, input_ref="artifact")
    messages = []
    published = []
    deleted = []

    def enqueue(envelope):
        messages.append({
            "MessageId": envelope.event_id, "ReceiptHandle": envelope.event_id,
            "Body": json.dumps({
                "source": envelope.producer, "detail-type": envelope.type,
                "detail": envelope.model_dump(mode="json"),
            }),
        })

    class Sqs:
        def receive_message(self, **kwargs):
            batch = list(messages)
            messages.clear()
            return {"Messages": batch}

        def delete_message(self, **kwargs):
            deleted.append(kwargs["ReceiptHandle"])

        def change_message_visibility(self, **kwargs):
            pytest.fail("a valid committed delivery must not be retried")

    class Bus:
        def describe_event_bus(self, **kwargs):
            assert kwargs["Name"] == "existing-bus"
            return {}

        def put_events(self, **kwargs):
            published.extend(json.loads(e["Detail"]) for e in kwargs["Entries"])
            return {"FailedEntryCount": 0}

    monkeypatch.setattr(boto3, "client", lambda name, **kw: Sqs() if name == "sqs" else Bus())
    config = replace(container.config, workflow_event_bus="existing-bus",
                     workflow_ingress_queue_url="queue", workflow_event_tenants=("org",))
    container = build_container(config, clock=clock)
    consumer = container.execution_consumer_factory()
    enqueue(trigger)
    consumer.poll()
    assert deleted == ["trigger"]
    assert container.drain_ledger_outbox().published == 2
    dispatch = next(e for e in published if e["type"] == "example.generate.requested")
    enqueue(result(clock, dispatch, payload_digest="result-digest"))
    consumer.poll()
    container.drain_ledger_outbox()
    assert deleted == ["trigger", "result"]
    assert rows(container, "jobs")[0]["status"] == "succeeded"
    assert any(e["type"] == "job.succeeded" for e in published)
