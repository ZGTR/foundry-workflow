from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from ipaddress import ip_network
from types import SimpleNamespace
from urllib.parse import quote_from_bytes

import jwt
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509.oid import NameOID
from fastapi.testclient import TestClient
from shepherd_contracts.clock import FrozenClock
from shepherd_contracts.errors import ConfigurationError, Unauthorized

from shepherd_workflow_service.adapters.real.jwt_principal_auth import JwksKeyResolver
from shepherd_workflow_service.adapters.real.tenant_registry_snapshot import TenantRegistrySnapshot
from shepherd_workflow_service.adapters.real.tenant_workload_auth import TenantWorkloadAuth
from shepherd_workflow_service.app_factory import create_app
from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.errors import ExecutionBackpressure
from shepherd_workflow_service.models.tenancy import TenantRegistry, validate_tenant_id
from shepherd_workflow_service.ports.workflow_engine import StageRunView
from shepherd_workflow_service.services.execution import ExecutionService

ISSUER = "https://issuer.example/foundry"
AUDIENCE = "https://tenant.foundry.example"
NOW = datetime(2026, 11, 20, 12, tzinfo=UTC)
PEER = "10.4.5.10"
CERT_HEADER = b"x-amzn-mtls-clientcert-leaf"


@pytest.fixture(scope="module")
def identities():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    identities = []
    for name in ("a", "b"):
        principal = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
        cert = (
            x509.CertificateBuilder()
            .subject_name(principal)
            .issuer_name(principal)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(NOW - timedelta(days=1))
            .not_valid_after(NOW + timedelta(days=1))
            .sign(key, hashes.SHA256())
        )
        thumbprint = (
            base64.urlsafe_b64encode(cert.fingerprint(hashes.SHA256())).decode().rstrip("=")
        )
        header = quote_from_bytes(cert.public_bytes(Encoding.PEM), safe="").encode()
        identities.append((header, thumbprint))
    return key, identities


@pytest.fixture
def auth_data(identities):
    key, ((pem_a, cert_a), (pem_b, cert_b)) = identities
    settings = {
        "workloads": [
            {
                "issuer": ISSUER,
                "client_id": f"client-{name}",
                "subject": f"subject-{name}",
                "tenant_id": f"tenant-{name}",
                "producer": f"producer-{name}",
                "scopes": ["workflow:submit", "workflow:read"],
                "active": True,
                "certificate_thumbprints": [certificate],
            }
            for name, certificate in (("a", cert_a), ("b", cert_b))
        ],
        "triggers": [
            {
                "tenant_id": "tenant-a",
                "producer": "producer-a",
                "event_type": "requested",
                "workflow_digest": "a" * 64,
            }
        ],
    }

    def adapter(profile="rfc9068"):
        return TenantWorkloadAuth(
            key_resolver=JwksKeyResolver(f"{ISSUER}/jwks", fetch=lambda token: key.public_key()),
            registry=lambda: TenantRegistry.from_json(json.dumps(settings)),
            issuer=ISSUER,
            audience=AUDIENCE,
            proxy_networks=[ip_network("10.4.5.0/24")],
            profile=profile,
            clock=FrozenClock(NOW),
        )

    def headers(*, cert=pem_a, typ="at+jwt", claim_changes=None, omit=()):
        issued = int(NOW.timestamp())
        claims = {
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": "subject-a",
            "client_id": "client-a",
            "scope": "workflow:read workflow:submit workflow:complete extra",
            "jti": "token-a",
            "iat": issued - 60,
            "nbf": issued - 60,
            "exp": issued + 60,
            "cnf": {"x5t#S256": cert_a},
        }
        claims.update(claim_changes or {})
        for name in omit:
            del claims[name]
        token = jwt.encode(claims, key, algorithm="RS256", headers={"kid": "key-1", "typ": typ})
        return [(b"authorization", f"Bearer {token}".encode()), (CERT_HEADER, cert)]

    return SimpleNamespace(
        adapter=adapter,
        headers=headers,
        registry=settings,
        pem_b=pem_b,
        cert_b=cert_b,
    )


def test_workload_binding_mints_registered_identity_and_intersects_scopes(auth_data):
    principal = auth_data.adapter().authenticate(auth_data.headers(), peer_ip=PEER)
    assert principal.org_id == "tenant-a"
    assert principal.subject_id == "producer-a"
    assert principal.scopes == {"workflow:read", "workflow:submit"}
    assert principal.attributes == {
        "principal_class": "tenant",
        "client_id": "client-a",
        "token_issuer": ISSUER,
        "subject": "subject-a",
    }
    registry = TenantRegistry.from_json(json.dumps(auth_data.registry))
    assert registry.allows_trigger(principal, event_type="requested", workflow_digest="a" * 64)
    assert not registry.allows_trigger(principal, event_type="requested", workflow_digest="b" * 64)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("iss", "https://other.example"),
        ("aud", [AUDIENCE, "other"]),
        ("aud", 4),
        ("sub", True),
        ("sub", "subject-b"),
        ("client_id", "client-b"),
        ("client_id", []),
        ("azp", "client-b"),
        ("scope", ["workflow:read"]),
        ("scope", "workflow:read\tworkflow:submit"),
        ("iat", str(int(NOW.timestamp()))),
        ("exp", False),
        ("nbf", 1.0),
        ("exp", int(NOW.timestamp())),
        ("nbf", int(NOW.timestamp()) + 40),
        ("exp", int(NOW.timestamp()) + 3800),
        ("tenant_id", "Tenant-A"),
        ("producer", "producer-b"),
        ("custom:org_id", "tenant-b"),
        ("token_use", "id"),
        ("cnf", {"x5t#S256": "wrong"}),
    ],
)
def test_strict_signed_claims(name, value, auth_data):
    with pytest.raises(Unauthorized, match="tenant workload authentication failed"):
        auth_data.adapter().authenticate(
            auth_data.headers(claim_changes={name: value}), peer_ip=PEER
        )


def test_required_token_profile_and_active_registry(auth_data):
    adapter = auth_data.adapter()
    for headers in (
        auth_data.headers(typ="JWT"),
        auth_data.headers(omit=("cnf",)),
        auth_data.headers(omit=("jti",)),
        auth_data.headers(omit=("client_id",)),
    ):
        with pytest.raises(Unauthorized):
            adapter.authenticate(headers, peer_ip=PEER)
    auth_data.registry["workloads"][0]["active"] = False
    with pytest.raises(Unauthorized):
        adapter.authenticate(auth_data.headers(), peer_ip=PEER)


def test_cognito_profile_requires_access_discriminator_and_same_workload_certificate(auth_data):
    adapter = auth_data.adapter("cognito")
    valid = {"typ": "JWT", "claim_changes": {"token_use": "access"}, "omit": ("cnf", "jti")}
    assert adapter.authenticate(auth_data.headers(**valid), peer_ip=PEER).org_id == "tenant-a"
    with pytest.raises(Unauthorized):
        adapter.authenticate(auth_data.headers(cert=auth_data.pem_b, **valid), peer_ip=PEER)
    with pytest.raises(Unauthorized):
        adapter.authenticate(auth_data.headers(typ="JWT"), peer_ip=PEER)


def test_proxy_requires_one_socket_authenticated_identity(auth_data):
    adapter = auth_data.adapter()
    headers = auth_data.headers()
    untrusted = headers + [(b"x-forwarded-for", PEER.encode())]
    for given, peer in (
        (untrusted, "198.51.100.4"),
        (headers, None),
        (headers + [headers[0]], PEER),
        (headers + [(CERT_HEADER, auth_data.pem_b)], PEER),
        (headers[:1], PEER),
        (
            auth_data.headers(
                cert=auth_data.pem_b, claim_changes={"cnf": {"x5t#S256": auth_data.cert_b}}
            ),
            PEER,
        ),
    ):
        with pytest.raises(Unauthorized):
            adapter.authenticate(given, peer_ip=peer)


def test_forged_jwt_signature_is_rejected(auth_data):
    headers = auth_data.headers()
    name, authorization = headers[0]
    prefix, signature = authorization.rsplit(b".", 1)
    forged = (b"A" if signature[:1] != b"A" else b"B") + signature[1:]
    with pytest.raises(Unauthorized):
        auth_data.adapter().authenticate([(name, prefix + b"." + forged), headers[1]], peer_ip=PEER)


@pytest.mark.parametrize(
    "tenant", ["Tenant", "teñant", "-tenant", "tenant-", "tenant\n", "tenant ", "a" * 65]
)
def test_tenant_ids_are_exact_and_canonical(tenant):
    with pytest.raises(ValueError):
        validate_tenant_id(tenant)
    assert validate_tenant_id("tenant-7") == "tenant-7"


def test_registry_refuses_duplicate_json_and_shared_certificates(auth_data):
    with pytest.raises(ValueError, match="duplicate JSON member"):
        TenantRegistry.from_json('{"workloads": [], "workloads": []}')
    a, b = auth_data.registry["workloads"]
    b["certificate_thumbprints"] = a["certificate_thumbprints"]
    with pytest.raises(ValueError, match="shared workload certificate"):
        TenantRegistry.from_json(json.dumps(auth_data.registry))


def test_registry_change_invalidates_cached_authorizations_within_refresh(auth_data):
    registry = TenantRegistry.from_json(json.dumps(auth_data.registry))
    now = 0
    available = True

    def load():
        if not available:
            raise ConfigurationError("registry unavailable")
        return TenantRegistry.from_json(json.dumps(auth_data.registry))

    cached = TenantRegistrySnapshot(
        registry, load=load, refresh_seconds=30, time_source=lambda: now
    )
    now = 29.9
    available = False
    assert cached.current() is registry
    now = 30
    with pytest.raises(ConfigurationError, match="unavailable"):
        cached.current()
    now = 30.1
    with pytest.raises(ConfigurationError, match="could not be refreshed"):
        cached.current()
    available = True
    now = 31
    assert cached.current() is registry
    auth_data.registry["workloads"][0]["active"] = False
    now = 61
    with pytest.raises(ConfigurationError, match="changed"):
        cached.current()
    auth_data.registry["workloads"][0]["active"] = True
    now = 91
    with pytest.raises(ConfigurationError, match="changed"):
        cached.current()


def test_secret_registry_config_is_validated_and_reads_the_named_value(auth_data):
    args = {
        "env": "test",
        "port": 8080,
        "runtime_mode": "http",
        "issuer": "event-producer",
        "db_path": "unused",
        "event_bus_mode": "in_process",
        "sqs_queue_url": None,
        "aws_region": None,
        "auth_issuer": ISSUER,
        "auth_audience": AUDIENCE,
        "auth_jwks_uri": f"{ISSUER}/jwks",
        "workflow_catalog_path": "unused",
        "tenant_http_profile": True,
        "tenant_proxy_cidrs": ("10.4.5.0/24",),
        "tenant_registry_secret_id": "registry",
    }
    config = ServiceConfig(**args)
    assert config.load_tenant_registry(
        read_secret={"registry": json.dumps(auth_data.registry)}.__getitem__,
    ).tenant_ids == {"tenant-a", "tenant-b"}
    with pytest.raises(ConfigurationError, match="secret provider"):
        config.load_tenant_registry()
    with pytest.raises(ValueError, match="requires HTTP runtime"):
        ServiceConfig(**{**args, "runtime_mode": "lambda"})
    with pytest.raises(ValueError, match="private proxy networks"):
        ServiceConfig(**{**args, "tenant_proxy_cidrs": ("0.0.0.0/0",)})
    with pytest.raises(ValueError, match="AUTH_ISSUER must differ"):
        ServiceConfig(**{**args, "issuer": ISSUER})


def test_http_allowlist_and_revocations_before_execution(auth_data):
    seen = []

    def receive(event, principal):
        seen.append((event.event_id, principal.org_id))
        raise ExecutionBackpressure()

    owned_stage = StageRunView(
        stage_run_id="stage-a",
        job_id="job-a",
        workflow="test",
        stage="first",
        status="succeeded",
        state_version=3,
        fence=1,
        attempts=1,
        output_ref="s3://bucket-b/pre-cutover-foreign-capability",
    )
    repository = SimpleNamespace(
        receive=receive,
        get=lambda stage_id, _: owned_stage if stage_id == "stage-a" else None,
        stages=lambda *_: [owned_stage],
        owns_job=lambda *_: False,
    )
    app = create_app(
        SimpleNamespace(
            config=SimpleNamespace(tenant_http_profile=True),
            readiness_check=SimpleNamespace(check=lambda: True),
            tenant_authenticate=auth_data.adapter().authenticate,
            execution_service=ExecutionService(
                repository,
                TenantRegistry.from_json(json.dumps(auth_data.registry)),
            ),
            observations=SimpleNamespace(),
        )
    )
    client = TestClient(app, client=(PEER, 1000))
    headers = auth_data.headers()
    for path in ("/v1/jobs", "/openapi.json", "/docs", "/redoc", "/debug"):
        assert client.get(path, headers=headers).status_code == 404
    assert client.post("/v1/jobs", headers=headers).status_code == 404
    missing = client.get("/v2/stage-runs/missing", headers=headers)
    foreign = client.get("/v2/stage-runs/foreign", headers=headers)
    assert missing.status_code == foreign.status_code == 404
    assert missing.json() == foreign.json() == {"detail": "job or stage not found"}
    state = client.get("/v2/stage-runs/stage-a", headers=headers)
    assert state.status_code == 200 and "output_ref" not in state.json()
    stages = client.get("/v2/jobs/job-a/stages", headers=headers)
    assert stages.status_code == 200 and "output_ref" not in stages.json()[0]
    history = client.get("/v2/jobs/foreign/history", headers=headers)
    assert history.status_code == 404 and history.json() == foreign.json()
    body = {
        "event_id": "event-1",
        "type": "requested",
        "occurred_at": NOW.isoformat(),
        "tenant_id": "tenant-a",
        "producer": "producer-a",
    }
    read_only = auth_data.headers(claim_changes={"scope": "workflow:read"})
    assert client.post("/v2/events", json=body, headers=read_only).status_code == 401
    mismatched = {**body, "tenant_id": "tenant-b"}
    assert client.post("/v2/events", json=mismatched, headers=headers).status_code == 401
    assert seen == []
    limited = client.post("/v2/events", json=body, headers=headers)
    assert limited.status_code == 429 and limited.headers["Retry-After"] == "5"
    auth_data.registry["workloads"][0]["active"] = False
    revoked = client.post("/v2/events", json=body, headers=headers)
    assert revoked.status_code == 401 and revoked.json() == {"detail": "unauthorized"}
    assert seen == [("event-1", "tenant-a")]
