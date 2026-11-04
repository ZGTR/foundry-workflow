"""Verify the standalone deployment's real HTTP-JWKS authentication boundary."""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from shepherd_contracts.errors import Unauthorized
from shepherd_contracts.principal import PrincipalMinter

from shepherd_workflow_service.adapters.real.jwt_principal_auth import (
    JwksKeyResolver,
    JwtPrincipalAuth,
)

ISSUER = "https://issuer.example.invalid"
AUDIENCE = "shepherd-workflow-printables"


@pytest.fixture
def workload():
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(private.public_key()))
    public.update(kid="workload-v1", alg="RS256", use="sig")
    payload = json.dumps({"keys": [public]}).encode()

    class PublicKeys(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/jwks.json":
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), PublicKeys)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    auth = JwtPrincipalAuth(
        PrincipalMinter(issuer=ISSUER),
        JwksKeyResolver(f"http://127.0.0.1:{server.server_port}/jwks.json"),
        issuer=ISSUER,
        audience=AUDIENCE,
    )

    def token(**overrides):
        claims = {
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": "printables-worker",
            "custom:org_id": "example-org",
            "scope": "jobs:write jobs:read",
            "iat": int(time.time()),
            "exp": int(time.time()) + 300,
        }
        claims.update(overrides)
        return jwt.encode(claims, private, algorithm="RS256", headers={"kid": "workload-v1"})

    try:
        yield auth, token
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_signed_workload_uses_actual_http_resolver(workload):
    auth, token = workload
    principal = auth.authenticate({"authorization": f"Bearer {token()}"})
    assert principal.org_id == "example-org"
    assert principal.subject_id == "printables-worker"
    assert principal.scopes == {"jobs:write", "jobs:read"}


@pytest.mark.parametrize(
    "overrides",
    [{"aud": "different-service"}, {"iss": "https://untrusted.invalid"}, {"exp": 1}],
)
def test_signed_but_invalid_workload_is_refused(workload, overrides):
    auth, token = workload
    with pytest.raises(Unauthorized):
        auth.authenticate({"authorization": f"Bearer {token(**overrides)}"})


def test_asserted_identity_headers_cannot_replace_signed_workload(workload):
    auth, _ = workload
    with pytest.raises(Unauthorized):
        auth.authenticate(
            {
                "x-org-id": "example-org",
                "x-subject-id": "printables-worker",
                "x-scopes": "jobs:write",
            }
        )
