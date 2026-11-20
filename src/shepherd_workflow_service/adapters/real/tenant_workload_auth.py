"""Authenticate OAuth access tokens and the verified ALB mutual-TLS peer."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from hmac import compare_digest
from ipaddress import IPv4Network, IPv6Network, ip_address
from typing import Literal
from urllib.parse import unquote_to_bytes

from shepherd_contracts.clock import Clock
from shepherd_contracts.errors import Unauthorized
from shepherd_contracts.principal import CallerPrincipalV1, PrincipalMinter

from shepherd_workflow_service.adapters.real.jwt_principal_auth import (
    DEFAULT_ALGORITHMS,
    JwksKeyResolver,
)
from shepherd_workflow_service.models.tenancy import (
    TenantRegistry,
    reject_duplicate_json_keys,
    validate_identifier,
    validate_thumbprint,
)

_CERTIFICATE_HEADER = b"x-amzn-mtls-clientcert-leaf"
_COMPACT_JWT = re.compile(r"[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+){2}", re.ASCII)
_PEM = re.compile(
    rb"-----BEGIN CERTIFICATE-----\r?\n(?:[A-Za-z0-9+/=]+\r?\n)+-----END CERTIFICATE-----(?:\r?\n)?"
)
_SCOPE = re.compile(r"[\x21\x23-\x5b\x5d-\x7e]+(?: [\x21\x23-\x5b\x5d-\x7e]+)*", re.ASCII)
_MAX_TOKEN_AGE_SECONDS = 3600
_CLOCK_TOLERANCE_SECONDS = 30


class TenantWorkloadAuth:
    def __init__(
        self,
        *,
        key_resolver: JwksKeyResolver,
        registry: Callable[[], TenantRegistry],
        issuer: str,
        audience: str,
        proxy_networks: Sequence[IPv4Network | IPv6Network],
        profile: Literal["rfc9068", "cognito"],
        clock: Clock,
    ) -> None:
        self._keys = key_resolver
        self._registry = registry
        self._issuer = issuer
        self._audience = audience
        self._proxy_networks = tuple(proxy_networks)
        self._profile = profile
        self._clock = clock
        self._minter = PrincipalMinter(issuer=issuer)

    def authenticate(
        self, headers: Sequence[tuple[bytes, bytes]], *, peer_ip: str | None
    ) -> CallerPrincipalV1:
        try:
            return self._authenticate(headers, peer_ip)
        except Exception:  # Deliberately hide token, certificate and resolver details.
            raise Unauthorized("tenant workload authentication failed") from None

    def _authenticate(
        self, headers: Sequence[tuple[bytes, bytes]], peer_ip: str | None
    ) -> CallerPrincipalV1:
        import jwt

        now = self._clock.now()
        presented = self._certificate_thumbprint(headers, peer_ip, now)
        token_parts = _single_header(headers, b"authorization").decode("ascii").split(" ")
        if (
            len(token_parts) != 2 or token_parts[0].lower() != "bearer"
            or len(token_parts[1]) > 8192 or not _COMPACT_JWT.fullmatch(token_parts[1])
        ):
            raise ValueError("invalid bearer header")
        token = token_parts[1]
        protected, payload, _ = token.split(".")
        header = _jwt_json(protected)
        _jwt_json(payload)
        algorithms = ("RS256",) if self._profile == "cognito" else DEFAULT_ALGORITHMS
        if header.get("alg") not in algorithms:
            raise ValueError("invalid signing algorithm")
        validate_identifier(header.get("kid"))
        if any(name in header for name in ("jku", "jwk", "x5u", "x5c", "crit", "b64")):
            raise ValueError("unsupported JOSE header")

        claims = jwt.decode(
            token, key=self._keys.signing_key_for(token), algorithms=algorithms,
            issuer=self._issuer, audience=self._audience,
            options={
                "require": ["iss", "aud", "sub", "client_id", "iat", "exp", "scope"],
                "verify_exp": False, "verify_iat": False, "verify_nbf": False,
            },
        )
        if claims["iss"] != self._issuer:
            raise ValueError("unexpected token issuer")
        self._verify_time_claims(claims, now)
        self._verify_token_profile(header, claims, presented)
        client_id = validate_identifier(claims["client_id"])
        subject = validate_identifier(claims["sub"])
        if "azp" in claims and claims["azp"] != client_id:
            raise ValueError("invalid authorized party")
        raw_scope = claims["scope"]
        if not isinstance(raw_scope, str) or not _SCOPE.fullmatch(raw_scope):
            raise ValueError("invalid scope claim")

        workload = self._registry().workload(self._issuer, client_id)
        if (
            workload is None or not workload.active or workload.subject != subject
            or not any(compare_digest(presented, cert) for cert in workload.certificate_thumbprints)
        ):
            raise ValueError("workload binding mismatch")
        for name, expected in (
            ("tenant_id", workload.tenant_id), ("custom:org_id", workload.tenant_id),
            ("org_id", workload.tenant_id), ("producer", workload.producer),
        ):
            if name in claims and claims[name] != expected:
                raise ValueError("inconsistent workload claim")
        return self._minter.mint(
            org_id=workload.tenant_id, subject_id=workload.producer,
            scopes=set(raw_scope.split(" ")).intersection(workload.scopes),
            attributes={
                "principal_class": "tenant", "client_id": client_id,
                "token_issuer": self._issuer, "subject": subject,
            },
        )

    def _certificate_thumbprint(
        self, headers: Sequence[tuple[bytes, bytes]], peer_ip: str | None, now: datetime
    ) -> str:
        from cryptography import x509
        from cryptography.hazmat.primitives.serialization import Encoding

        address = ip_address(peer_ip or "")
        if not any(address in network for network in self._proxy_networks):
            raise ValueError("untrusted proxy")
        wire_value = _single_header(headers, _CERTIFICATE_HEADER)
        if len(wire_value) > 16384 or b"\r" in wire_value or b"\n" in wire_value:
            raise ValueError("invalid certificate header")
        cert_pem = unquote_to_bytes(wire_value)
        if not _PEM.fullmatch(cert_pem):
            raise ValueError("invalid certificate header")
        cert = x509.load_pem_x509_certificate(cert_pem)
        if not cert.not_valid_before_utc <= now.astimezone(UTC) < cert.not_valid_after_utc:
            raise ValueError("expired certificate")
        digest = hashlib.sha256(cert.public_bytes(Encoding.DER)).digest()
        return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")

    @staticmethod
    def _verify_time_claims(claims: dict, now: datetime) -> None:
        issued, expiry = claims["iat"], claims["exp"]
        not_before = claims.get("nbf", issued)
        if any(type(item) is not int for item in (issued, expiry, not_before)):
            raise ValueError("invalid time claim")
        current = now.timestamp()
        if (
            issued <= 0 or not 0 < expiry - issued <= _MAX_TOKEN_AGE_SECONDS
            or expiry <= current or issued > current + _CLOCK_TOLERANCE_SECONDS
            or not_before > current + _CLOCK_TOLERANCE_SECONDS or not_before >= expiry
        ):
            raise ValueError("invalid token lifetime")

    def _verify_token_profile(self, header: dict, claims: dict, presented: str) -> None:
        token_type = header.get("typ")
        if self._profile == "rfc9068":
            if not isinstance(token_type, str) or token_type.lower() not in (
                "at+jwt", "application/at+jwt",
            ):
                raise ValueError("not a JWT access token")
            validate_identifier(claims.get("jti"))
        elif claims.get("token_use") != "access":
            raise ValueError("not a provider access token")
        if "token_use" in claims and claims["token_use"] != "access":
            raise ValueError("conflicting token type")

        if claims["aud"] not in (self._audience, [self._audience]):
            raise ValueError("unexpected token audience")
        if self._profile == "cognito" and "cnf" not in claims:
            return
        confirmation = claims.get("cnf")
        if not isinstance(confirmation, dict) or set(confirmation) != {"x5t#S256"}:
            raise ValueError("invalid certificate confirmation")
        if not compare_digest(validate_thumbprint(confirmation["x5t#S256"]), presented):
            raise ValueError("certificate confirmation mismatch")


def _single_header(headers: Sequence[tuple[bytes, bytes]], name: bytes) -> bytes:
    matching = [value for key, value in headers if key.lower() == name]
    if len(matching) != 1:
        raise ValueError("missing or ambiguous credential header")
    return matching[0]


def _jwt_json(encoded: str) -> dict:
    decoded = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
    document = json.loads(decoded, object_pairs_hook=reject_duplicate_json_keys)
    if not isinstance(document, dict):
        raise ValueError("invalid JWT document")
    return document
