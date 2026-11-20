"""Two-tenant isolation through real TLS sockets and the Foundry API."""

import base64
import hashlib
import json
import os
import re
import socket
import ssl
from argparse import Namespace
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from functools import partial
from http.client import HTTPSConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from ipaddress import ip_network
from pathlib import Path
from threading import Thread
from time import monotonic, sleep
from urllib.parse import quote_from_bytes

import jwt
import pytest
import uvicorn
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from shepherd_contracts.clock import FrozenClock

from deploy.standalone.verify_mtls import verify
from shepherd_workflow_service import config as service_config
from shepherd_workflow_service.adapters.real import tenant_registry_snapshot
from shepherd_workflow_service.adapters.real.in_process_event_bus import InProcessEventBus
from shepherd_workflow_service.adapters.real.jwt_principal_auth import JwksKeyResolver
from shepherd_workflow_service.app_factory import create_app
from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import build_container
from shepherd_workflow_service.models.definition import Catalog
from shepherd_workflow_service.models.scheduling import TenantPolicy

ISSUER = "https://issuer.example.invalid"
AUDIENCE = "workflow"
NOW = datetime.now(UTC).replace(microsecond=0)
LEAF_HEADER = "X-Amzn-Mtls-Clientcert-Leaf"
CATALOG = Path(__file__).resolve().parents[2] / "contracts/examples/catalog.v2.json"
TENANTS = ("tenant-a", "tenant-b")
STAGE_ROUTES = re.compile(r"/v2/(stage-runs/[^/]+|jobs/[^/]+/(stages|history))")
SUBMIT_SCOPES = ("workflow:read", "workflow:submit")
RESULT_SCOPES = ("workflow:complete",)


def _certificate(name, *, authority=None, purpose=None, expired=False):
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    authority_cert, signing_key = authority or (None, key)
    until = NOW - timedelta(days=1) if expired else NOW + timedelta(days=1)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(authority_cert.subject if authority_cert else subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(NOW - timedelta(days=2))
        .not_valid_after(until)
        .add_extension(x509.BasicConstraints(ca=authority is None, path_length=None), critical=True)
    )
    if purpose:
        builder = builder.add_extension(x509.ExtendedKeyUsage([purpose]), critical=False)
    if purpose == ExtendedKeyUsageOID.SERVER_AUTH:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.DNSName(name)]), critical=False
        )
    return builder.sign(signing_key, hashes.SHA256()), key


def _files(directory, name, identity):
    certificate, key = identity
    chain_path = directory / f"{name}.pem"
    key_path = directory / f"{name}.key"
    chain_path.write_bytes(certificate.public_bytes(Encoding.PEM))
    with open(key_path, "xb", opener=_private_opener) as file:
        file.write(key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()))
    return chain_path, key_path


def _private_opener(path, flags):
    return os.open(path, flags, 0o600)


def _server_trust(directory, authority, revoked):
    ca, key = authority
    crl = (
        x509.CertificateRevocationListBuilder()
        .issuer_name(ca.subject)
        .last_update(NOW - timedelta(minutes=1))
        .next_update(NOW + timedelta(hours=1))
    )
    for cert in revoked:
        crl = crl.add_revoked_certificate(
            x509.RevokedCertificateBuilder()
            .serial_number(cert.serial_number)
            .revocation_date(NOW - timedelta(minutes=1))
            .build()
        )
    crl = crl.sign(key, hashes.SHA256())
    bundle = directory / "client-trust.pem"
    bundle.write_bytes(ca.public_bytes(Encoding.PEM) + crl.public_bytes(Encoding.PEM))
    return bundle


def _fingerprint(cert):
    digest = hashlib.sha256(cert.public_bytes(Encoding.DER)).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


def _workloads(directory, authority, signing_key):
    registry = {"workloads": [], "triggers": [], "executors": []}
    clients = {}
    digest = Catalog.model_validate_json(CATALOG.read_text()).workflows[0].digest
    for tenant in TENANTS:
        registry["triggers"].append({
            "tenant_id": tenant, "producer": "example-api",
            "event_type": "example.requested", "workflow_digest": digest,
        })
        for role, scopes in (("submit", SUBMIT_SCOPES), ("execute", RESULT_SCOPES)):
            client_id = f"{tenant}-{role}"
            producer = "example-api" if role == "submit" else "example-worker"
            cert, key = _certificate(
                client_id, authority=authority, purpose=ExtendedKeyUsageOID.CLIENT_AUTH
            )
            fingerprint = _fingerprint(cert)
            registry["workloads"].append({
                "tenant_id": tenant, "client_id": client_id, "subject": client_id,
                "producer": producer, "issuer": ISSUER, "scopes": scopes, "active": True,
                "certificate_thumbprints": [fingerprint],
            })
            if role == "execute":
                registry["executors"].append({
                    "tenant_id": tenant, "issuer": ISSUER, "client_id": client_id,
                    "workflow_digest": digest, "stage": "generate",
                })
            now = int(NOW.timestamp())
            token = jwt.encode(
                {"iss": ISSUER, "aud": AUDIENCE, "sub": client_id, "client_id": client_id,
                 "iat": now, "exp": now + 300, "jti": client_id, "scope": " ".join(scopes),
                 "cnf": {"x5t#S256": fingerprint}},
                signing_key, algorithm="ES256", headers={"kid": "probe", "typ": "at+jwt"},
            )
            clients[tenant, role] = (cert, _files(directory, client_id, (cert, key)), token)
    return registry, clients


@contextmanager
def _origin(application, server_credentials):
    certificate, key = server_credentials
    settings = uvicorn.Config(
        application, access_log=False, log_level="critical", proxy_headers=False, lifespan="off",
        ssl_certfile=str(certificate), ssl_keyfile=str(key),
    )
    server = uvicorn.Server(settings)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        worker = Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        worker.start()
        try:
            deadline = monotonic() + 5
            while not server.started and worker.is_alive() and monotonic() < deadline:
                sleep(0.01)
            assert server.started, "the private HTTPS origin failed to start"
            yield listener.getsockname()[1]
        finally:
            server.should_exit = True
            worker.join(timeout=5)
            assert not worker.is_alive(), "the origin failed to stop"


def _tls_policy(server_credentials, client_trust):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED
    context.verify_flags |= ssl.VERIFY_CRL_CHECK_LEAF
    context.load_cert_chain(*server_credentials)
    context.load_verify_locations(client_trust)
    return context


@contextmanager
def _edge(origin_port, server_ca, context):
    origin_tls = ssl.create_default_context(cafile=str(server_ca))

    class EdgeHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def do_GET(self):
            if STAGE_ROUTES.fullmatch(self.path):
                self._forward()
            else:
                self.send_error(404)

        def do_POST(self):
            if self.path == "/v2/events":
                self._forward()
            else:
                self.send_error(404)

        def _forward(self):
            if any(name.lower().startswith("x-amzn-mtls-") for name in self.headers):
                self.send_error(403)
                return
            certificate = self.connection.getpeercert(binary_form=True)
            payload = self.rfile.read(int(self.headers.get("Content-Length", "0"))) or None
            headers = {
                LEAF_HEADER: quote_from_bytes(ssl.DER_cert_to_PEM_cert(certificate).encode()),
                "Authorization": self.headers.get("Authorization", ""),
                "Content-Type": "application/json",
            }
            upstream = HTTPSConnection(
                "localhost", origin_port, context=origin_tls, timeout=3,
                source_address=("127.0.0.2", 0),
            )
            try:
                upstream.request(self.command, self.path, body=payload, headers=headers)
                response = upstream.getresponse()
                body = response.read()
                self.send_response(response.status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            finally:
                upstream.close()

    class EdgeServer(ThreadingHTTPServer):
        def get_request(self):
            client, address = super().get_request()
            client.settimeout(3)
            try:
                return self.tls.wrap_socket(client, server_side=True), address
            except OSError:
                client.close()
                raise

    with EdgeServer(("127.0.0.1", 0), EdgeHandler) as server:
        server.tls = context
        worker = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        worker.start()
        try:
            yield server
        finally:
            server.shutdown()
            worker.join(timeout=5)


@contextmanager
def _client(port, server_ca, credentials=None, *, maximum_version=None):
    context = ssl.create_default_context(cafile=str(server_ca))
    if maximum_version is not None:
        context.maximum_version = maximum_version
    if credentials is not None:
        context.load_cert_chain(*credentials)
    connection = HTTPSConnection("localhost", port, context=context, timeout=3)
    try:
        yield connection
    finally:
        connection.close()


def _call(connection, token, path, *, payload=None, headers=None):
    fields = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    fields.update(headers or {})
    connection.request(
        "GET" if payload is None else "POST", path,
        body=None if payload is None else json.dumps(payload).encode(), headers=fields,
    )
    response = connection.getresponse()
    body = response.read()
    return response.status, json.loads(body) if body.startswith((b"{", b"[")) else body


def _accept_and_complete(edge_port, server_ca, clients, container, dispatches):
    jobs, events = {}, {}
    for tenant in TENANTS:
        _, credentials, token = clients[tenant, "submit"]
        other = next(name for name in TENANTS if name != tenant)
        events[tenant] = {
            "event_id": "shared-submission-id", "type": "example.requested",
            "producer": "example-api", "tenant_id": tenant, "occurred_at": NOW.isoformat(),
            "input_ref": f"s3://test-artifacts/{tenant}/input",
        }
        with _client(edge_port, server_ca, credentials) as connection:
            status, _ = _call(connection, token, "/v2/events", payload={
                **events[tenant], "tenant_id": other,
            })
            assert status == 401
            status, _ = _call(connection, token, "/v2/events", payload={
                **events[tenant], "input_ref": f"s3://test-artifacts/{other}/input",
            })
            assert status == 401
            status, accepted = _call(connection, token, "/v2/events", payload=events[tenant])
            assert status == 202
            jobs[tenant], = accepted["job_ids"]
    assert len(set(jobs.values())) == 2
    assert container.execution_service.admit(10) == 2
    assert container.drain_ledger_outbox().failed == 0
    envelopes = {event.org_id: event.payload["envelope"] for event in dispatches}
    assert envelopes.keys() == jobs.keys()
    for tenant in TENANTS:
        _, credentials, token = clients[tenant, "execute"]
        other = next(name for name in TENANTS if name != tenant)
        adapted = {
            **events[tenant], "event_id": "shared-result-id", "producer": "example-worker",
            "type": "example.generate.succeeded", "payload_digest": "a" * 64,
            "output_ref": f"s3://test-artifacts/{tenant}/result",
            "correlation": envelopes[tenant]["correlation"],
        }
        with _client(edge_port, server_ca, credentials) as connection:
            foreign = _call(connection, token, "/v2/events", payload={
                **adapted, "correlation": envelopes[other]["correlation"],
            })
            missing = _call(connection, token, "/v2/events", payload={
                **adapted, "correlation": {**adapted["correlation"], "job_id": "unknown"},
            })
            assert foreign == missing and foreign[0] == 422
            assert _call(connection, token, "/v2/events", payload=adapted)[0] == 202
    for tenant in TENANTS:
        _, credentials, token = clients[tenant, "submit"]
        other = next(name for name in TENANTS if name != tenant)
        with _client(edge_port, server_ca, credentials) as connection:
            status, stages = _call(connection, token, f"/v2/jobs/{jobs[tenant]}/stages")
            assert status == 200 and stages[0]["status"] == "succeeded"
            for suffix in ("history", "stages"):
                foreign = _call(connection, token, f"/v2/jobs/{jobs[other]}/{suffix}")
                missing = _call(connection, token, f"/v2/jobs/unknown/{suffix}")
                assert foreign == missing and foreign[0] == 404
            foreign_stage = envelopes[other]["correlation"]["stage_run_id"]
            assert _call(connection, token, f"/v2/stage-runs/{foreign_stage}")[0] == 404
    return jobs


def _verify_transport(directory, origin, edge, ca, server_ca, clients, jobs):
    certificate, credentials, token = clients["tenant-a", "submit"]
    token_file = directory / "token"
    with open(token_file, "x", opener=_private_opener) as file:
        file.write(token)
    untrusted_ca = _certificate("untrusted-ca")
    invalid = {
        name: _files(directory, name, _certificate(
            name, authority=issuer, purpose=ExtendedKeyUsageOID.CLIENT_AUTH, expired=expired,
        ))
        for name, issuer, expired in (("untrusted", untrusted_ca, False), ("expired", ca, True))
    }
    a_history = f"/v2/jobs/{jobs['tenant-a']}/history"
    args = Namespace(
        endpoint=f"https://localhost:{edge.server_port}{a_history}",
        client_cert=credentials[0], client_key=credentials[1],
        server_ca=server_ca, token_file=token_file, expected_status=200,
        untrusted_client=invalid["untrusted"], expired_client=invalid["expired"],
        revoked_client=None, mismatched_client=clients["tenant-b", "submit"][1],
        foreign_path=f"/v2/jobs/{jobs['tenant-b']}/history",
        missing_path="/v2/jobs/unknown/history",
    )
    assert verify(args) == [
        "verified-server-and-registered-client", "missing-client-refused-at-tls",
        "untrusted-client-refused-at-tls", "expired-client-refused-at-tls",
        "certificate-token-mismatch-refused", "foreign-object-hidden",
    ]
    with pytest.raises(ssl.SSLCertVerificationError):
        verify(Namespace(**{**vars(args), "endpoint": args.endpoint.replace(
            "localhost", "127.0.0.1"
        )}))
    synthetic_leaf = quote_from_bytes(certificate.public_bytes(Encoding.PEM))
    with _client(origin, server_ca) as direct:
        assert _call(direct, token, a_history)[0] == 401
        assert _call(direct, token, a_history, headers={
            LEAF_HEADER: synthetic_leaf, "X-Forwarded-For": "127.0.0.2",
        })[0] == 401
    with _client(edge.server_port, server_ca, credentials) as connection:
        assert _call(connection, token, a_history, headers={LEAF_HEADER: synthetic_leaf})[0] == 403
        assert _call(connection, token, "/openapi.json")[0] == 404
        assert _call(connection, token, "/readyz")[0] == 404
        assert _call(connection, token, "/v1/jobs", payload={})[0] == 404


def _verify_revocation(
    directory, edge, server_credentials, server_ca, ca, clients, jobs, revoke_workload,
    advance_refresh,
):
    cert, credentials, token = clients["tenant-a", "submit"]
    history = f"/v2/jobs/{jobs['tenant-a']}/history"
    b = clients["tenant-b", "submit"]
    b_history = f"/v2/jobs/{jobs['tenant-b']}/history"

    with _client(edge.server_port, server_ca, credentials) as existing:
        assert _call(existing, token, history)[0] == 200
        authenticated_socket = existing.sock
        updated_tls = _tls_policy(server_credentials, _server_trust(directory, ca, (cert,)))
        published = monotonic()
        edge.tls = updated_tls
        # TLS 1.2 surfaces the explicit revocation alert before an HTTP write.
        with _client(
            edge.server_port, server_ca, credentials, maximum_version=ssl.TLSVersion.TLSv1_2,
        ) as fresh:
            with pytest.raises(ssl.SSLError, match="CERTIFICATE_REVOKED"):
                fresh.connect()
        crl_delay = monotonic() - published
        with _client(edge.server_port, server_ca, b[1]) as still_valid:
            assert _call(still_valid, b[2], b_history)[0] == 200
        assert _call(existing, token, history)[0] == 200
        assert existing.sock is authenticated_socket

        published = monotonic()
        revoke_workload()
        if advance_refresh is not None:
            advance_refresh(29.999)
            assert _call(existing, token, history)[0] == 200
            advance_refresh(30)
        else:
            deadline = published + 33
            while monotonic() < deadline and _call(existing, token, history)[0] == 200:
                assert existing.sock is authenticated_socket
                sleep(0.1)
        assert _call(existing, token, history)[0] == 401
        assert existing.sock is authenticated_socket
        return {
            "fresh_tls_crl_rejection_ms": round(crl_delay * 1_000, 2),
            "open_connection_registry_rejection_seconds": (
                round(monotonic() - published, 2) if advance_refresh is None else 30
            ),
            "registry_clock": "monotonic wall clock" if advance_refresh is None else "controlled",
        }


def _run_isolation(directory, monkeypatch, *, real_time=False):
    # Use two distinct loopback source addresses to make the production
    # immediate-peer check exercise an actual socket boundary in the harness.
    monkeypatch.setattr(
        service_config, "_PRIVATE_NETWORKS",
        (*service_config._PRIVATE_NETWORKS, ip_network("127.0.0.0/8")),
    )
    signing_key = ec.generate_private_key(ec.SECP256R1())
    monkeypatch.setattr(
        JwksKeyResolver, "signing_key_for", lambda _self, _token: signing_key.public_key()
    )
    controlled = [0]
    if not real_time:
        monkeypatch.setattr(
            tenant_registry_snapshot, "TenantRegistrySnapshot",
            partial(tenant_registry_snapshot.TenantRegistrySnapshot,
                    time_source=lambda: controlled[0]),
        )

    ca = _certificate("workload-ca")
    server_ca = _certificate("service-ca")
    server_credentials = _files(directory, "service", _certificate(
        "localhost", authority=server_ca, purpose=ExtendedKeyUsageOID.SERVER_AUTH,
    ))
    server_anchor = directory / "server-ca.pem"
    server_anchor.write_bytes(server_ca[0].public_bytes(Encoding.PEM))
    registry, clients = _workloads(directory, ca, signing_key)

    def revoke():
        account = next(workload for workload in registry["workloads"]
                       if workload["client_id"] == "tenant-a-submit")
        account["active"] = False

    class Secrets:
        def get(self, name):
            assert name == "workload-registry"
            return json.dumps(registry)

    configuration = ServiceConfig(
        env="test", port=8080, runtime_mode="http", issuer="foundry-workflow",
        db_path=str(directory / "ledger.sqlite"), event_bus_mode="in_process",
        sqs_queue_url=None, aws_region=None, auth_issuer=ISSUER, auth_audience=AUDIENCE,
        auth_jwks_uri=f"{ISSUER}/jwks", workflow_catalog_path=str(CATALOG),
        tenant_http_profile=True, tenant_proxy_cidrs=("127.0.0.2/32",),
        tenant_registry_secret_id="workload-registry", tenant_pool_capacity=2,
        tenant_artifact_prefixes_json=json.dumps({
            tenant: [f"s3://test-artifacts/{tenant}/"] for tenant in TENANTS
        }),
    )
    dispatches = []
    bus = InProcessEventBus()
    bus.subscribe("example.generate.requested", dispatches.append)
    build = partial(
        build_container, configuration, clock=FrozenClock(NOW), publisher=bus, secrets=Secrets(),
    )
    container = build()
    container.execution_service.repository.reconcile_and_enable(tenant_policies={
        tenant: TenantPolicy() for tenant in TENANTS
    })
    application = create_app(container)

    async def route(scope, receive, send):
        await application(scope, receive, send)

    initial_tls = _tls_policy(server_credentials, _server_trust(directory, ca, ()))
    with _origin(route, server_credentials) as origin, _edge(
        origin, server_anchor, initial_tls,
    ) as edge:
        jobs = _accept_and_complete(edge.server_port, server_anchor, clients, container, dispatches)
        _verify_transport(directory, origin, edge, ca, server_anchor, clients, jobs)
        result = _verify_revocation(
            directory, edge, server_credentials, server_anchor, ca, clients, jobs, revoke,
            None if real_time else lambda now: controlled.__setitem__(0, now),
        )
        _, b_credentials, token = clients["tenant-b", "submit"]
        with _client(edge.server_port, server_anchor, b_credentials) as connection:
            path = f"/v2/jobs/{jobs['tenant-b']}/history"
            assert _call(connection, token, path)[0] == 401
            application = create_app(build())
            assert _call(connection, token, path)[0] == 200
    return result


def test_real_mutual_tls_and_revocation(tmp_path, monkeypatch):
    _run_isolation(tmp_path, monkeypatch)
