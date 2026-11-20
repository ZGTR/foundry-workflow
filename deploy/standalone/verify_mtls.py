"""Check an HTTPS endpoint with real client certificates and server verification."""

from __future__ import annotations

import argparse
import http.client
import json
import ssl
from pathlib import Path
from urllib.parse import urlsplit

_CLIENT_ALERTS = {
    "missing-client": {"TLSV13_ALERT_CERTIFICATE_REQUIRED", "SSLV3_ALERT_HANDSHAKE_FAILURE"},
    "untrusted-client": {"TLSV1_ALERT_UNKNOWN_CA"},
    "expired-client": {"SSLV3_ALERT_CERTIFICATE_EXPIRED"},
    "revoked-client": {"SSLV3_ALERT_CERTIFICATE_REVOKED"},
}


def _context(ca: Path | None, identity: tuple[Path, Path] | None) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=str(ca) if ca else None)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    if identity:
        certificate, key = identity
        context.load_cert_chain(certificate, key)
    return context


def _target(value: str) -> tuple[str, int, str]:
    if not value.isascii() or any(ord(char) <= 32 for char in value):
        raise ValueError("endpoint must be an ASCII HTTPS URL")
    url = urlsplit(value)
    if (
        url.scheme != "https" or not url.hostname or url.username is not None
        or url.password is not None or url.fragment or url.path.startswith("//")
    ):
        raise ValueError("endpoint must be an HTTPS URL without credentials or a fragment")
    port = url.port if url.port is not None else 443
    if not port:
        raise ValueError("HTTPS endpoint port must be positive")
    path = url.path or "/"
    if url.query:
        path = f"{path}?{url.query}"
    return url.hostname, port, path


def _get(host, port, path, context, token) -> tuple[int, bytes]:
    connection = http.client.HTTPSConnection(host, port, timeout=10, context=context)
    try:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        connection.request("GET", path, headers=headers)
        response = connection.getresponse()
        body = response.read(64 * 1024 + 1)
        if len(body) > 64 * 1024:
            raise ValueError("probe response exceeds 64 KiB")
        return response.status, body
    finally:
        connection.close()


def _must_reject_client(host, port, path, context, token, label):
    # With TLS 1.3 the client can send HTTP before the server's client-auth
    # decision is visible. TLS 1.2 returns the refusal during connect itself.
    context.maximum_version = ssl.TLSVersion.TLSv1_2
    try:
        _get(host, port, path, context, token)
    except ssl.SSLCertVerificationError:
        raise RuntimeError(f"{label} failed server certificate verification") from None
    except ssl.SSLError as exc:
        if exc.reason in _CLIENT_ALERTS[label]:
            return
        raise RuntimeError(f"{label} failed with an unexpected TLS alert") from None
    raise RuntimeError(f"{label} reached HTTP without a valid client certificate")


def verify(args: argparse.Namespace) -> list[str]:
    host, port, path = _target(args.endpoint)
    token = args.token_file.read_text(encoding="ascii").strip() if args.token_file else None
    if args.token_file and (not token or any(char.isspace() for char in token)):
        raise ValueError("token file must contain one bearer token")
    context = _context(args.server_ca, (args.client_cert, args.client_key))
    status, _ = _get(host, port, path, context, token)
    if status != args.expected_status:
        raise RuntimeError(f"registered client returned HTTP {status}")
    checks = ["verified-server-and-registered-client"]
    cases = (
        ("missing-client", None),
        ("untrusted-client", args.untrusted_client),
        ("expired-client", args.expired_client),
        ("revoked-client", args.revoked_client),
    )
    for label, identity in cases:
        if label == "missing-client" or identity:
            _must_reject_client(host, port, path, _context(args.server_ca, identity), token, label)
            checks.append(f"{label}-refused-at-tls")
    if args.mismatched_client:
        if token is None:
            raise ValueError("the token file is required for the certificate binding check")
        other_context = _context(args.server_ca, args.mismatched_client)
        status, _ = _get(host, port, path, other_context, token)
        if status not in {401, 403}:
            raise RuntimeError(f"certificate and token mismatch returned HTTP {status}")
        checks.append("certificate-token-mismatch-refused")
    if args.foreign_path or args.missing_path:
        paths = (args.foreign_path, args.missing_path)
        if any(
            not value or not value.startswith("/") or value.startswith("//")
            or not value.isascii() or any(ord(char) <= 32 for char in value)
            for value in paths
        ):
            raise ValueError("provide foreign and missing paths on the verified origin")
        foreign, missing = (_get(host, port, value, context, token) for value in paths)
        if foreign[0] != 404 or foreign != missing:
            raise RuntimeError("foreign and missing objects must produce the same HTTP 404")
        checks.append("foreign-object-hidden")
    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("endpoint", help="HTTPS URL of one readable object or verifier route")
    parser.add_argument("--client-cert", type=Path, required=True)
    parser.add_argument("--client-key", type=Path, required=True)
    parser.add_argument("--server-ca", type=Path, help="server CA bundle, defaults to system CAs")
    parser.add_argument("--token-file", type=Path, help="file containing one bearer token")
    parser.add_argument("--expected-status", type=int, default=200)
    for label in ("untrusted", "expired", "revoked", "mismatched"):
        parser.add_argument(f"--{label}-client", type=Path, nargs=2, metavar=("CERT", "KEY"))
    parser.add_argument("--foreign-path")
    parser.add_argument("--missing-path")
    args = parser.parse_args()
    try:
        checks = verify(args)
    except http.client.HTTPException:
        parser.exit(1, "mTLS verification failed: invalid HTTP response\n")
    except (OSError, ValueError, RuntimeError) as exc:
        parser.exit(1, f"mTLS verification failed: {exc}\n")
    print(json.dumps({"passed": checks}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
