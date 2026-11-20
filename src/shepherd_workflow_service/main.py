"""The process entrypoint. The ONLY module that runs at import time in the
container -- everything it calls (`ServiceConfig.from_environment()`,
`build_container()`, `create_app()`) takes its inputs explicitly.

The server listens on `PORT` (default 8080). The ECS tenant profile serves TLS
to the ALB and uses the immediate socket peer for certificate provenance.
The legacy listener serves HTTP, including loopback traffic from the Lambda
Web Adapter.
"""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import uvicorn

from shepherd_workflow_service.adapters.real.aws_secrets_manager import AwsSecretsManagerSecrets
from shepherd_workflow_service.app_factory import create_app
from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import build_container

config = ServiceConfig.from_environment()
app = create_app(build_container(config))


def run() -> None:
    if not config.tenant_http_profile:
        uvicorn.run(app, host="0.0.0.0", port=config.port)  # noqa: S104 -- container-internal bind
        return

    if not config.tenant_tls_secret_id or not config.aws_region:
        raise ValueError("Tenant HTTP requires a backend TLS secret and AWS region")

    secrets = AwsSecretsManagerSecrets(region=config.aws_region)
    try:
        tls = json.loads(secrets.get(config.tenant_tls_secret_id))
        certificate, private_key = tls["certificate"], tls["private_key"]
        if not isinstance(certificate, str) or not isinstance(private_key, str):
            raise TypeError
    except (ValueError, KeyError, TypeError):
        raise ValueError(
            "Backend TLS secret requires certificate and private_key PEM strings"
        ) from None

    # The directory is mode 0700. Uvicorn loads the files into its SSLContext.
    with TemporaryDirectory(prefix="workflow-tls-") as directory:
        cert_path = Path(directory) / "certificate.pem"
        key_path = Path(directory) / "private_key.pem"
        cert_path.write_text(certificate, encoding="ascii")
        key_path.touch(mode=0o600)
        key_path.write_text(private_key, encoding="ascii")
        uvicorn.run(
            app,
            host="0.0.0.0",  # noqa: S104 -- a private task reached only by its ALB
            port=config.port,
            ssl_certfile=str(cert_path),
            ssl_keyfile=str(key_path),
            # Certificate provenance must use the immediate socket peer.
            proxy_headers=False,
            access_log=False,
        )


if __name__ == "__main__":
    run()
