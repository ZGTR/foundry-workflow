"""Read-only staging checks: SQL readiness, auth denial and authenticated tenant lookup."""
import argparse
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


def check(url, expected, token=None):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        with urlopen(Request(url, headers=headers), timeout=15) as response:
            status = response.status
    except HTTPError as error:
        status = error.code
    if status != expected:
        raise RuntimeError(f"Smoke returned {status}; expected {expected}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    args = parser.parse_args()
    base, token = args.base_url.rstrip("/"), sys.stdin.read().strip()
    if not base.startswith("https://") or not token:
        raise ValueError("Smoke requires HTTPS and a signed jobs:read token")
    check(base + "/readyz", 200)
    path = base + "/v2/jobs/smoke-missing-" + uuid4().hex + "/history"
    check(path, 401)
    check(path, 404, token)
    print("PASS: SQL readiness, unauthenticated denial, authenticated tenant lookup")


if __name__ == "__main__":
    main()
