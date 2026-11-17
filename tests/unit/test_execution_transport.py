import json
from datetime import UTC, datetime

from shepherd_workflow_service.adapters.fake.execution import RecordingExecutionRepository
from shepherd_workflow_service.adapters.real.queue_execution import QueueExecutionConsumer
from shepherd_workflow_service.adapters.real.workflow_catalog import JsonWorkflowCatalog
from shepherd_workflow_service.models.execution import EventEnvelope
from shepherd_workflow_service.services.execution import ExecutionService
from shepherd_workflow_service.subscriptions_main import compile_subscriptions


def test_sqs_ack_only_after_commit_and_rejects_untrusted_identity():
    event = EventEnvelope(
        event_id="1",
        type="requested",
        producer="product",
        tenant_id="tenant",
        occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    class Queue:
        deleted = []
        deferred = []

        def receive_message(self, **kw):
            return {
                "Messages": [
                    {
                        "MessageId": source,
                        "ReceiptHandle": source,
                        "Body": json.dumps(
                            {
                                "source": source,
                                "detail-type": event.type,
                                "detail": event.model_dump(mode="json"),
                            }
                        ),
                    }
                    for source in ["forged", "product"]
                ]
            }

        def delete_message(self, **kw):
            self.deleted.append(kw["ReceiptHandle"])

        def change_message_visibility(self, **kw):
            self.deferred.append(kw["ReceiptHandle"])

    repo = RecordingExecutionRepository()
    queue = Queue()
    QueueExecutionConsumer(
        client=queue,
        queue_url="queue",
        service=ExecutionService(repo),
        issuer="workflow",
        allowed_tenants=["tenant"],
    ).poll()
    assert queue.deleted == ["product"]
    assert queue.deferred == ["forged"]
    assert len(repo.events) == 1


def test_subscription_compiler_matches_runtime_selectors():
    catalog = JsonWorkflowCatalog("contracts/examples/catalog.v2.json").load()
    subscriptions = compile_subscriptions(catalog, "bus")["execution_subscriptions"]
    patterns = [json.loads(v["event_pattern"]) for v in subscriptions.values()]
    assert {p["source"][0] for p in patterns} == {"example-api", "example-worker"}
    assert next(p for p in patterns if p["source"] == ["example-worker"]) == {
        "source": ["example-worker"],
        "detail-type": ["example.generate.failed", "example.generate.succeeded"],
    }


def test_published_schemas_match_runtime_contracts():
    from pathlib import Path

    from shepherd_workflow_service.models.definition import Catalog

    for name, model in [("workflow-catalog.v2", Catalog), ("workflow-event.v1", EventEnvelope)]:
        published = json.loads(Path(f"contracts/{name}.schema.json").read_text())
        published.pop("$schema")
        assert published == model.model_json_schema()
