"""Authenticated shared deployment through the app, scheduler, and outbox."""

import base64
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import quote_from_bytes

import jwt
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509.oid import NameOID
from fastapi.testclient import TestClient
from shepherd_contracts.clock import FrozenClock

from shepherd_workflow_service.adapters.real.in_process_event_bus import InProcessEventBus
from shepherd_workflow_service.adapters.real.jwt_principal_auth import JwksKeyResolver
from shepherd_workflow_service.app_factory import create_app
from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import build_container, build_execution_cycle
from shepherd_workflow_service.models.definition import Catalog
from shepherd_workflow_service.models.scheduling import TenantPolicy

_CATALOG = Path(__file__).resolve().parents[2] / "contracts/examples/catalog.v2.json"
_ISSUER = "https://auth.example.invalid"
_AUDIENCE = "https://workflow.example.invalid"


def _workload(key, now, tenant, client, producer, scopes):
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, client)])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    fingerprint = base64.urlsafe_b64encode(
        certificate.fingerprint(hashes.SHA256())
    ).decode().rstrip("=")
    token = jwt.encode(
        {"iss": _ISSUER, "aud": _AUDIENCE, "sub": client, "client_id": client,
         "scope": " ".join(scopes), "iat": int(now.timestamp()),
         "exp": int((now + timedelta(minutes=5)).timestamp()), "jti": client,
         "cnf": {"x5t#S256": fingerprint}},
        key, algorithm="RS256", headers={"kid": "test-key", "typ": "at+jwt"},
    )
    return (
        {"issuer": _ISSUER, "client_id": client, "subject": client, "tenant_id": tenant,
         "producer": producer, "scopes": scopes, "active": True,
         "certificate_thumbprints": [fingerprint]},
        {"Authorization": f"Bearer {token}",
         "X-Amzn-Mtls-Clientcert-Leaf": quote_from_bytes(certificate.public_bytes(Encoding.PEM))},
    )


def test_profile_isolates_jobs_and_completes_owned_work(tmp_path, monkeypatch):
    clock = FrozenClock(datetime.now(UTC))
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    monkeypatch.setattr(JwksKeyResolver, "signing_key_for", lambda _self, _token: key.public_key())
    submit_scopes = ["workflow:submit", "workflow:read"]
    submit_a, a_headers = _workload(key, clock.now(), "tenant-a", "a", "example-api", submit_scopes)
    submit_b, b_headers = _workload(key, clock.now(), "tenant-b", "b", "example-api", submit_scopes)
    execute_a, executor_headers = _workload(
        key, clock.now(), "tenant-a", "executor", "example-worker", ["workflow:complete"],
    )
    digest = Catalog.model_validate_json(_CATALOG.read_text()).workflows[0].digest
    registry = {
        "workloads": [submit_a, submit_b, execute_a],
        "triggers": [
            {"tenant_id": tenant, "producer": "example-api", "event_type": "example.requested",
             "workflow_digest": digest}
            for tenant in ("tenant-a", "tenant-b")
        ],
        "executors": [{"tenant_id": "tenant-a", "issuer": _ISSUER, "client_id": "executor",
                       "workflow_digest": digest, "stage": "generate"}],
    }
    config = ServiceConfig(
        env="test", port=8080, runtime_mode="http", issuer="foundry-workflow",
        db_path=str(tmp_path / "shares.sqlite3"), event_bus_mode="in_process",
        sqs_queue_url=None, aws_region=None, auth_issuer=_ISSUER, auth_audience=_AUDIENCE,
        auth_jwks_uri=f"{_ISSUER}/jwks", workflow_catalog_path=str(_CATALOG),
        tenant_http_profile=True, tenant_proxy_cidrs=("10.4.5.0/24",),
        tenant_registry_json=json.dumps(registry), tenant_pool_capacity=2,
        tenant_queue_age_seconds=45,
        tenant_artifact_prefixes_json=json.dumps({
            "tenant-a": ["s3://test-artifacts/tenant-a/"],
            "tenant-b": ["s3://test-artifacts/tenant-b/"],
        }),
    )
    messages = []
    broker = InProcessEventBus()
    broker.subscribe("example.generate.requested", messages.append)
    container = build_container(config, clock=clock, publisher=broker)
    service = container.execution_service
    assert service.repository.scheduler.age_threshold == timedelta(seconds=45)
    service.repository.reconcile_and_enable(tenant_policies={
        "tenant-a": TenantPolicy(weight=2),
        "tenant-b": TenantPolicy(weight=1),
    })
    assert container.execution_consumer_factory is None
    client = TestClient(create_app(container), client=("10.4.5.10", 443))
    assert client.get("/readyz").status_code == 200
    assert client.get("/openapi.json").status_code == 404
    assert client.post("/v1/jobs", headers=a_headers).status_code == 404

    event = {"event_id": "same-external-id", "occurred_at": clock.now().isoformat(),
             "type": "example.requested", "producer": "example-api", "tenant_id": "tenant-a"}
    foreign_artifact = {**event, "input_ref": "s3://test-artifacts/tenant-b/input"}
    assert client.post("/v2/events", headers=a_headers, json=foreign_artifact).status_code == 401
    a = client.post("/v2/events", headers=a_headers, json={
        **event, "input_ref": "s3://test-artifacts/tenant-a/input",
    })
    b = client.post("/v2/events", headers=b_headers, json={**event, "tenant_id": "tenant-b"})
    assert (a.status_code, b.status_code) == (202, 202)
    a_job, b_job = a.json()["job_ids"][0], b.json()["job_ids"][0]
    assert a_job != b_job
    assert client.get(f"/v2/jobs/{a_job}/history", headers=b_headers).json() == client.get(
        "/v2/jobs/unallocated/history", headers=b_headers
    ).json()
    assert client.get(f"/v2/jobs/{b_job}/stages", headers=a_headers).status_code == 404
    assert service.admit(10) == 2
    report = container.drain_ledger_outbox()
    assert report.failed == 0
    assert {message.org_id for message in messages} == {"tenant-a", "tenant-b"}
    a_dispatch, b_dispatch = (
        next(message.payload["envelope"] for message in messages if message.org_id == tenant)
        for tenant in ("tenant-a", "tenant-b")
    )
    result = {**event, "event_id": "completion", "type": "example.generate.succeeded",
              "producer": "example-worker", "payload_digest": "test-result",
              "correlation": b_dispatch["correlation"]}
    foreign_result = client.post("/v2/events", headers=executor_headers, json=result)
    unknown_result = client.post("/v2/events", headers=executor_headers, json={
        **result, "correlation": {**result["correlation"], "job_id": "unallocated"},
    })
    assert foreign_result.status_code == unknown_result.status_code == 422
    assert foreign_result.json() == unknown_result.json()
    complete = client.post("/v2/events", headers=executor_headers, json={
        **result, "correlation": a_dispatch["correlation"],
        "output_ref": "s3://test-artifacts/tenant-a/result",
    })
    assert complete.status_code == 202
    stage = client.get(f"/v2/jobs/{a_job}/stages", headers=a_headers).json()[0]
    assert stage["status"] == "succeeded"
    assert "output_ref" not in stage
    assert client.get(f"/v2/jobs/{b_job}/stages", headers=b_headers).json()[0]["status"] == "leased"

    metrics = []
    cycle = build_execution_cycle(container)
    monkeypatch.setattr(cycle.metrics, "emit", metrics.append)
    cycle._observe()
    assert metrics[0]["WorkerCycleFailures"] == 0
    assert metrics[0]["InFlight"] == 1
    assert metrics[0]["ChargedSlotSeconds"] == 2
