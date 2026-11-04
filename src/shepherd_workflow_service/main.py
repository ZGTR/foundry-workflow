"""The process entrypoint. The ONLY module that runs at import time in the
container -- everything it calls (`ServiceConfig.from_environment()`,
`build_container()`, `create_app()`) takes its inputs explicitly.

This same entrypoint runs unmodified under both deploy targets:

* **ECS Fargate** -- this file is the container's `CMD`. `uvicorn` binds
  `PORT` (default 8080) and serves traffic directly behind the ALB.
* **Lambda (image package type)** -- the AWS Lambda Web Adapter, installed as
  a layer in `Dockerfile`, is itself the process the Lambda runtime invokes.
  It starts this exact command as a child process, forwards each Lambda
  invocation to `http://127.0.0.1:$PORT` as an ordinary HTTP request, and
  returns the HTTP response as the Lambda response. This file never learns
  which target it is running under; `AWS_LAMBDA_FUNCTION_NAME` only ever
  affects `ServiceConfig.runtime_mode`, which nothing here branches on -- it
  exists for adapters/handlers that care (e.g. choosing a Lambda-friendly log
  format), not for this module.
"""

from __future__ import annotations

import uvicorn

from shepherd_workflow_service.app_factory import create_app
from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import build_container

config = ServiceConfig.from_environment()
app = create_app(build_container(config))


def run() -> None:
    uvicorn.run(app, host="0.0.0.0", port=config.port)  # noqa: S104 -- container-internal bind


if __name__ == "__main__":
    run()
