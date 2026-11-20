"""Signed HTTPS delivery to a verified, operator-provisioned receiver."""

import base64
import hashlib
import hmac
import http.client
import ipaddress
import json
import re
import secrets
import socket
import ssl
from collections.abc import Callable, Iterable
from contextlib import suppress
from threading import Timer
from time import monotonic
from urllib.parse import urlsplit

from shepherd_contracts import Clock
from shepherd_contracts.errors import ConfigurationError

from shepherd_workflow_service.models.execution import EventEnvelope


class PermanentDeliveryFailure(Exception):
    """A delivery that requires operator intervention."""


def _b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


_PUBLIC_IPV6 = ipaddress.ip_network("2000::/3")
_MAX_POLICY_AGE = 30


class _PublicHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, hostname: str) -> None:
        super().__init__(hostname, port=443, timeout=4, context=ssl.create_default_context())
        self.authorization_deadline = float("inf")

    def _require_fresh_authorization(self) -> None:
        if monotonic() >= self.authorization_deadline:
            raise TimeoutError("Receiver authorization expired before sending")

    def connect(self) -> None:
        addresses = socket.getaddrinfo(self.host + ".", self.port, type=socket.SOCK_STREAM)
        if not addresses:
            raise OSError("receiver has no address")
        for _, _, _, _, address in addresses:
            ip = ipaddress.ip_address(address[0])
            if (
                not ip.is_global
                or ip.is_multicast
                or ip.is_reserved
                or (
                    isinstance(ip, ipaddress.IPv6Address)
                    and (ip not in _PUBLIC_IPV6 or ip.sixtofour or ip.teredo)
                )
            ):
                raise PermanentDeliveryFailure("receiver has a non-public address")
        family, socktype, protocol, _, address = addresses[0]
        self._require_fresh_authorization()
        with socket.socket(family, socktype, protocol) as raw:
            raw.settimeout(self.timeout)
            raw.connect(address)
            self.sock = self._context.wrap_socket(raw, server_hostname=self.host)
        self._require_fresh_authorization()

    def getresponse(self) -> http.client.HTTPResponse:
        # A per-read timeout permits a peer to indefinitely trickle headers.
        sock = self.sock
        if sock is None:
            return super().getresponse()

        def interrupt_response():
            with suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)

        watchdog = Timer(self.timeout, interrupt_response)
        watchdog.daemon = True
        watchdog.start()
        try:
            return super().getresponse()
        finally:
            watchdog.cancel()


class SignedWebhookDestination:
    """RFC 9421 HMAC-SHA256 messages with an event id and bounded signature lifespan.

    Provision the key through a tenant-specific secret and verify URL ownership
    before registration. The receiver must check freshness and deduplicate event ids.
    """

    def __init__(
        self,
        *,
        tenant_id: str,
        destination_id: str,
        version: str,
        event_types: Iterable[str],
        url: str,
        key_id: str,
        secret_provider: Callable[[], bytes],
        clock: Clock,
        credential_id: str = "",
    ) -> None:
        parsed = urlsplit(url)
        hostname = parsed.hostname or ""
        if (
            parsed.scheme != "https"
            or parsed.netloc != hostname
            or not re.fullmatch(
                r"(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
                r"[a-z][a-z0-9-]{0,61}[a-z0-9]",
                hostname,
            )
            or not re.fullmatch(r"/[A-Za-z0-9._~/\-]*", parsed.path)
            or any(part in {".", ".."} for part in parsed.path.split("/"))
            or url != f"https://{hostname}{parsed.path}"
            or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", key_id)
        ):
            raise ValueError("invalid webhook registration")
        self.tenant_id = tenant_id
        self.destination_id = destination_id
        self.version = version
        self.event_types = frozenset(event_types)
        self._url = url
        self._host = hostname
        self._path = parsed.path
        self._key_id = key_id
        self._credential_id = credential_id
        self._secret_provider = secret_provider
        self._clock = clock

    @property
    def endpoint(self) -> str:
        return self._url

    @property
    def routing_version(self) -> str:
        registration = json.dumps(
            [self.version, self._url, self._key_id, self._credential_id, sorted(self.event_types)],
            separators=(",", ":"),
        )
        return hashlib.sha256(registration.encode()).hexdigest()

    def send(self, event: EventEnvelope) -> None:
        if event.tenant_id != self.tenant_id or event.type not in self.event_types:
            raise PermanentDeliveryFailure("receiver is not authorized for event")
        authorization_deadline = monotonic() + _MAX_POLICY_AGE
        try:
            secret = self._secret_provider()
        except ConfigurationError:
            raise PermanentDeliveryFailure("invalid receiver signing key") from None
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise PermanentDeliveryFailure("invalid receiver signing key")
        body = event.model_dump_json().encode()
        digest = f"sha-256=:{_b64(hashlib.sha256(body).digest())}:"
        now = int(self._clock.now().timestamp())
        nonce = secrets.token_hex(16)
        params = (
            '("@method" "@target-uri" "content-digest" "x-event-id")'
            f';created={now};expires={now + 60};keyid="{self._key_id}";alg="hmac-sha256"'
            f';nonce="{nonce}"'
        )
        signed = "\n".join(
            (
                '"@method": POST',
                f'"@target-uri": {self._url}',
                f'"content-digest": {digest}',
                f'"x-event-id": {event.event_id}',
                f'"@signature-params": {params}',
            )
        )
        signature = _b64(hmac.digest(secret, signed.encode(), "sha256"))
        connection = _PublicHTTPSConnection(self._host)
        connection.authorization_deadline = authorization_deadline
        try:
            connection.request(
                "POST",
                self._path,
                body=body,
                headers={
                    "Content-Type": "application/json",
                    "Content-Digest": digest,
                    "X-Event-ID": event.event_id,
                    "Signature-Input": f"foundry={params}",
                    "Signature": f"foundry=:{signature}:",
                },
            )
            response = connection.getresponse()
            if 200 <= response.status < 300:
                return
            if response.status in {408, 425, 429} or response.status >= 500:
                raise RuntimeError("transient receiver response")
            raise PermanentDeliveryFailure("receiver rejected delivery")
        finally:
            connection.close()
