"""Signed webhook coverage and artifact reference canonicalization."""

import base64
import hashlib
import hmac
import json
import socket
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from shepherd_contracts import FrozenClock
from shepherd_contracts.errors import ConfigurationError

from shepherd_workflow_service.adapters.real import signed_webhook
from shepherd_workflow_service.adapters.real.signed_webhook import (
    PermanentDeliveryFailure,
    SignedWebhookDestination,
)
from shepherd_workflow_service.models.artifact_access import ArtifactAccess
from shepherd_workflow_service.models.execution import EventEnvelope
from shepherd_workflow_service.ports.secrets import SecretNotFound
from shepherd_workflow_service.tenant_runtime import _callback_reader, _destinations

NOW = datetime(2026, 11, 20, tzinfo=UTC)
URL = "https://tenant.example.com/foundry"
KEY = b"k" * 32


def _destination(url=URL):
    return SignedWebhookDestination(
        tenant_id="tenant-a",
        destination_id="test",
        version="1",
        event_types=["stage.leased"],
        url=url,
        key_id="tenant-a-key",
        secret_provider=lambda: KEY,
        clock=FrozenClock(NOW),
    )


def test_signature_covers_target_method_digest_and_event_id_on_each_attempt(monkeypatch):
    requests = []

    class Connection:
        def __init__(self, hostname):
            assert hostname == "tenant.example.com"

        def request(self, method, path, *, body, headers):
            requests.append((method, path, body, headers))

        def getresponse(self):
            return SimpleNamespace(status=204)

        def close(self):
            pass

    monkeypatch.setattr(signed_webhook, "_PublicHTTPSConnection", Connection)
    destination = _destination()
    event = EventEnvelope(
        event_id="evt_example",
        type="stage.leased",
        producer="foundry",
        tenant_id="tenant-a",
        occurred_at=NOW,
    )
    destination.send(event)
    destination.send(event)
    for method, path, body, headers in requests:
        assert (method, path) == ("POST", "/foundry")
        assert headers["X-Event-ID"] == "evt_example"
        digest = base64.b64encode(hashlib.sha256(body).digest()).decode()
        assert headers["Content-Digest"] == f"sha-256=:{digest}:"
        params = headers["Signature-Input"].removeprefix("foundry=")
        assert f";created={int(NOW.timestamp())};expires={int(NOW.timestamp()) + 60}" in params
        expected = "\n".join(
            (
                '"@method": POST',
                f'"@target-uri": {URL}',
                f'"content-digest": sha-256=:{digest}:',
                '"x-event-id": evt_example',
                f'"@signature-params": {params}',
            )
        ).encode()
        signature = base64.b64encode(hmac.digest(KEY, expected, "sha256")).decode()
        assert headers["Signature"] == f"foundry=:{signature}:"
    assert requests[0][3]["Signature"] != requests[1][3]["Signature"]
    with pytest.raises(PermanentDeliveryFailure):
        destination.send(event.model_copy(update={"tenant_id": "tenant-b"}))


@pytest.mark.parametrize(
    "host", ["127.0.0.1", "169.254.169.254", "10.0.0.1", "::1", "64:ff9b::a00:1"]
)
def test_connection_validates_resolved_addresses_before_opening_socket(monkeypatch, host):
    monkeypatch.setattr(
        socket, "getaddrinfo", lambda *_args, **_kwargs: [(0, 0, 0, "", (host, 443))]
    )
    connection = signed_webhook._PublicHTTPSConnection("tenant.example.com")
    with pytest.raises(PermanentDeliveryFailure, match="non-public"):
        connection.connect()


def test_slow_dns_cannot_send_with_stale_callback_authorization(monkeypatch):
    connection = signed_webhook._PublicHTTPSConnection("tenant.example.com")
    connection.authorization_deadline = 100
    monkeypatch.setattr(signed_webhook, "monotonic", lambda: 101)
    monkeypatch.setattr(
        socket, "getaddrinfo", lambda *_args, **_kwargs: [(0, 0, 0, "", ("8.8.8.8", 443))]
    )
    monkeypatch.setattr(
        socket, "socket", lambda *_args: pytest.fail("expired authorization opened a connection")
    )
    with pytest.raises(TimeoutError, match="authorization expired"):
        connection.connect()


@pytest.mark.parametrize(
    "url",
    [
        "http://tenant.example.com/foundry",
        "https://tenant.example.com:444/foundry",
        "https://user@tenant.example.com/foundry",
        "https://127.0.0.1/foundry",
        "https://tenant.example.com/a/../foundry",
        "https://tenant.example.com/foundry?q=x",
    ],
)
def test_registration_rejects_noncanonical_targets(url):
    with pytest.raises(ValueError):
        _destination(url)


@pytest.mark.parametrize(
    "ref",
    [
        "s3://artifacts/tenant-a-other/file",
        "s3://artifacts/tenant-b/file",
        "s3://artifacts/tenant-a/../tenant-b/file",
        "s3://artifacts/tenant-a/%2e%2e/file",
        "s3://ARTIFACTS/tenant-a/file",
        "s3://artifacts/tenant-a/file?signature=x",
        "s3://artifacts/tenant-a//file",
        "https://artifacts/tenant-a/file",
    ],
)
def test_artifact_prefix_requires_canonical_same_tenant_reference(ref):
    policy = ArtifactAccess({"tenant-a": ["s3://artifacts/tenant-a/"]})
    assert policy.allows("tenant-a", "s3://artifacts/tenant-a/folder/item.json")
    assert not policy.allows("tenant-a", ref)
    assert not policy.allows("tenant-b", "s3://artifacts/tenant-a/folder/item.json")


@pytest.mark.parametrize(
    "prefixes",
    [
        ["s3://artifacts/team/", "s3://artifacts/team/other/"],
        ["s3://artifacts/team/other/", "s3://artifacts/team/"],
        ["s3://artifacts/team/", "s3://artifacts/team/"],
    ],
)
def test_artifact_prefixes_do_not_overlap_between_tenants(prefixes):
    with pytest.raises(ValueError, match="overlap"):
        ArtifactAccess({"tenant-a": [prefixes[0]], "tenant-b": [prefixes[1]]})


def _registration(**changes):
    return {
        "tenant_id": "tenant-a",
        "destination_id": "destination",
        "event_types": ["stage.leased"],
        "url": URL,
        "key_id": "key-identity",
        "version": "one",
        "secret_id": "signing-key",
        **changes,
    }


def test_production_rejects_immutable_callback_configuration():
    config = SimpleNamespace(
        env="staging",
        tenant_callback_destinations_secret_id=None,
        tenant_callback_destinations_json=json.dumps([_registration()]),
    )
    with pytest.raises(ConfigurationError, match="live callback registration"):
        _callback_reader(config, object(), SimpleNamespace(tenant_ids={"tenant-a"}))


@pytest.mark.parametrize(
    "updated",
    [
        [],
        [_registration(version="two")],
        [_registration(url="https://other.example.com/")],
        [_registration(tenant_id="tenant-b")],
        [_registration(secret_id="new-key")],
    ],
)
def test_live_callback_revocation_and_changes_block_queued_delivery(monkeypatch, updated):
    requests = []

    class Connection:
        def __init__(self, hostname):
            requests.append(hostname)

        def request(self, *args, **kwargs):
            pass

        def getresponse(self):
            return SimpleNamespace(status=204)

        def close(self):
            pass

    monkeypatch.setattr(signed_webhook, "_PublicHTTPSConnection", Connection)
    reads = []
    initial = [_registration()]
    policy = initial
    signing_key = KEY

    class Secrets:
        def get(self, name):
            reads.append(name)
            if name == "policy":
                if policy is None:
                    raise SecretNotFound()
                return json.dumps(policy)
            return base64.b64encode(signing_key).decode()

    config = SimpleNamespace(
        env="staging",
        tenant_callback_destinations_secret_id="policy",
        tenant_callback_destinations_json="[]",
    )
    registry = SimpleNamespace(tenant_ids={"tenant-a", "tenant-b"})
    secrets = Secrets()
    reader = _callback_reader(config, secrets, registry)
    assert reads == []
    (destination,) = _destinations(reader, FrozenClock(NOW), secrets)
    event = EventEnvelope(
        event_id="evt_1",
        type="stage.leased",
        producer="foundry",
        tenant_id="tenant-a",
        occurred_at=NOW,
    )
    destination.send(event)
    assert reads == ["policy", "policy", "signing-key"]
    assert requests == ["tenant.example.com"]

    policy = None
    with pytest.raises(RuntimeError, match="callback policy unavailable"):
        destination.send(event)
    assert requests == ["tenant.example.com"]
    assert reads[-1] == "policy"

    policy = updated
    with pytest.raises(PermanentDeliveryFailure, match="destination_inactive"):
        destination.send(event)
    assert requests == ["tenant.example.com"]
    policy = initial
    with pytest.raises(PermanentDeliveryFailure, match="destination_inactive"):
        destination.send(event)
    assert requests == ["tenant.example.com"]
