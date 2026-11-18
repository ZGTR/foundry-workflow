"""Opt-in real EventBridge/SQS transport probe; owns and removes one local container.

Run: PYTHONPATH=.:src:vendor .venv/bin/python -m pytest -v -s \
     deploy/standalone/test_event_execution.py
Requires an already-pulled localstack/localstack:3.8 image. No AWS credentials used.
"""
import json
import sqlite3
import subprocess
import time
from dataclasses import replace
from uuid import uuid4

import boto3
import pytest
from botocore.config import Config

from shepherd_workflow_service.adapters.real.workflow_catalog import JsonWorkflowCatalog
from shepherd_workflow_service.container import build_container
from shepherd_workflow_service.subscriptions_main import compile_subscriptions
from tests.integration.test_declarative_execution import EXAMPLE, event, result, rows, setup


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


@pytest.fixture
def transport():
    name = "workflow-event-probe-" + uuid4().hex[:10]
    try:
        docker("run", "-d", "--pull=never", "--name", name,
               "--memory=768m", "--cpus=2", "--pids-limit=512",
               "-e", "SERVICES=events,sqs", "-e", "SQS_ENDPOINT_STRATEGY=path",
               "-p", "127.0.0.1::4566", "localstack/localstack:3.8")
        port = int(docker("port", name, "4566/tcp").split(":")[-1])
        endpoint = f"http://127.0.0.1:{port}"
        clients = {name: boto3.client(
            name, endpoint_url=endpoint, aws_access_key_id="test", aws_secret_access_key="test",
            region_name="us-east-1", config=Config(retries={"max_attempts": 0},
                                                 connect_timeout=2, read_timeout=15,
                                                 proxies={}),
        ) for name in ("events", "sqs")}
        for attempt in range(60):
            try:
                clients["events"].list_event_buses()
                break
            except Exception:
                if attempt == 59:
                    raise
                time.sleep(1)
        yield clients
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name], check=True, capture_output=True)


def test_two_profiles_redelivery_crash_retry_timeout_and_dead_letter(
    tmp_path, monkeypatch, transport
):
    bus, sqs = transport["events"], transport["sqs"]
    bus_name = "workflow-probe"
    bus.create_event_bus(Name=bus_name)

    def queue(name, attributes=None):
        url = sqs.create_queue(QueueName=name, Attributes=attributes or {})["QueueUrl"]
        arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]
        return url, arn

    dead, dead_arn = queue("dead")
    ingress, ingress_arn = queue("ingress", {
        "VisibilityTimeout": "0",
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dead_arn, "maxReceiveCount": "2"}),
    })
    executor, executor_arn = queue("executor", {"VisibilityTimeout": "0"})
    catalog = json.loads(EXAMPLE.read_text())
    # Onboard a second producer entirely through JSON, including different worker selectors.
    profile = json.loads(json.dumps(catalog["workflows"][0]).replace("example", "second"))
    catalog["workflows"].append(profile)
    base, clock = setup(tmp_path, catalog)
    parsed = JsonWorkflowCatalog(base.config.workflow_catalog_path).load()
    subscriptions = compile_subscriptions(parsed, bus_name)["execution_subscriptions"]
    patterns = [json.loads(v["event_pattern"]) for v in subscriptions.values()]
    for index, pattern in enumerate(patterns + [{"source": ["foundry-workflow"],
            "detail-type": [s.dispatch.event_type for w in parsed.workflows for s in w.stages]}]):
        rule = f"rule-{index}"
        bus.put_rule(Name=rule, EventBusName=bus_name, EventPattern=json.dumps(pattern))
        bus.put_targets(Rule=rule, EventBusName=bus_name,
                        Targets=[{"Id": "queue", "Arn": executor_arn if index == len(patterns)
                                  else ingress_arn}])
    for url, arn in [(ingress, ingress_arn), (executor, executor_arn)]:
        sqs.set_queue_attributes(QueueUrl=url, Attributes={"Policy": json.dumps({
            "Version": "2012-10-17", "Statement": [{"Effect": "Allow",
                "Principal": {"Service": "events.amazonaws.com"}, "Action": "sqs:SendMessage",
                "Resource": arn, "Condition": {"ArnLike": {"aws:SourceArn":
                    f"arn:aws:events:us-east-1:000000000000:rule/{bus_name}/*"}}}],
        })})
    monkeypatch.setattr(boto3, "client", lambda name, **kw: transport[name])
    config = replace(base.config, workflow_event_bus=bus_name,
                     workflow_ingress_queue_url=ingress, workflow_event_tenants=("org",))
    container = build_container(config, clock=clock)

    def publish(envelope):
        response = bus.put_events(Entries=[{
            "EventBusName": bus_name, "Source": envelope.producer, "DetailType": envelope.type,
            "Detail": envelope.model_dump_json(),
        }])
        assert response["FailedEntryCount"] == 0

    def take(url):
        for _ in range(20):
            messages = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1,
                                           WaitTimeSeconds=1).get("Messages", [])
            if messages:
                return messages[0]
        pytest.fail(f"No message delivered to local queue {url}")

    def delete(url, message):
        sqs.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])

    def consume():
        assert container.execution_consumer_factory().poll() == 0

    def drain():
        report = container.drain_ledger_outbox()
        assert report.failed == report.ambiguous == 0

    effects = tmp_path / "executor.db"
    with sqlite3.connect(effects) as db:
        db.execute("CREATE TABLE effects (stage_run_id TEXT PRIMARY KEY)")

    def effect(dispatch):
        # An external executor must persist its idempotency key with its side effect.
        with sqlite3.connect(effects) as db:
            db.execute("INSERT OR IGNORE INTO effects VALUES (?)",
                       (dispatch["correlation"]["stage_run_id"],))

    for product in ("example", "second"):
        trigger = event(clock, id=product, type=f"{product}.requested", producer=f"{product}-api")
        publish(trigger)
        publish(trigger)
        consume()
        drain()
        message = take(executor)
        dispatch = json.loads(message["Body"])["detail"]
        effect(dispatch)
        # Crash after effect, before ACK; a fresh process/connection sees the same durable key.
        redelivery = take(executor)
        assert json.loads(redelivery["Body"])["detail"]["event_id"] == dispatch["event_id"]
        effect(dispatch)
        delete(executor, redelivery)
        done = result(clock, dispatch, id=f"{product}-done", payload_digest="digest").model_copy(
            update={"producer": f"{product}-worker", "type": f"{product}.generate.succeeded"}
        )
        container = build_container(config, clock=clock)  # simulated process restart
        publish(done)
        publish(done)
        consume()
        drain()
    assert len(rows(container, "jobs")) == 2
    assert all(row["status"] == "succeeded" for row in rows(container, "jobs"))
    with sqlite3.connect(effects) as db:
        assert db.execute("SELECT count(*) FROM effects").fetchone()[0] == 2

    publish(event(clock, id="recover"))
    consume()
    drain()
    message = take(executor)
    first = json.loads(message["Body"])["detail"]
    delete(executor, message)
    publish(result(clock, first, id="transient", success=False, retryable=True))
    consume()
    clock.advance(30)
    container.execution_service.tick()
    drain()
    message = take(executor)
    second = json.loads(message["Body"])["detail"]
    delete(executor, message)
    assert second["correlation"]["fence"] == 2
    clock.advance(901)
    container = build_container(config, clock=clock)
    container.execution_service.tick()
    clock.advance(30)
    container.execution_service.tick()
    drain()
    message = take(executor)
    third = json.loads(message["Body"])["detail"]
    delete(executor, message)
    assert third["correlation"]["fence"] == 3
    publish(result(clock, second, id="late", payload_digest="stale"))
    consume()
    assert next(r for r in rows(container, "stage_runs") if
                r["stage_run_id"] == third["correlation"]["stage_run_id"])["status"] == "leased"
    publish(result(clock, third, id="recovered", payload_digest="valid"))
    consume()
    drain()
    assert all(row["status"] == "succeeded" for row in rows(container, "jobs"))

    # Real SQS redrive after repeated delivery without acknowledgment, then replay unchanged.
    replay = event(clock, id="redrive")
    wrapped = json.dumps({"source": replay.producer, "detail-type": replay.type,
                          "detail": replay.model_dump(mode="json")})
    sqs.send_message(QueueUrl=ingress, MessageBody=wrapped)
    take(ingress)
    take(ingress)
    sqs.receive_message(QueueUrl=ingress, WaitTimeSeconds=1)
    message = take(dead)
    assert json.loads(message["Body"])["detail"]["event_id"] == "redrive"
    publish(replay)
    delete(dead, message)
    consume()
    assert len(rows(container, "jobs")) == 4
    assert container.observations.metrics()["RetryAttempts"] == 2
