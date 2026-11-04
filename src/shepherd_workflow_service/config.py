"""The composition root's configuration.

This module is the ONE place in this package allowed to read the process
environment (`ruff`'s `flake8-tidy-imports.banned-api` bans `os.environ`
everywhere else in the workspace, and `tests/shape/test_layering.py` asserts
that no other module under `src/` even names it).

Every other module receives a `ServiceConfig` value through a constructor
argument. Reading the environment happens exactly once, at process start, in
`from_environment()` -- never lazily, never at import time. An ambient value
picked up mid-request is exactly the class of bug this pattern exists to rule
out: a `stage`/`STAGE`/`APP_NAME` value read
at import time once and silently retargeting a seeding script at production.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal

from shepherd_workflow.ports import DEFAULT_TABLE_PREFIX

RuntimeMode = Literal["http", "lambda"]
EventBusMode = Literal["in_process", "sqs", "outbox"]
DbBackend = Literal["sqlite", "mysql"]


@dataclass(frozen=True, slots=True)
class ServiceConfig:
    """Everything the container needs to wire the service, resolved once."""

    env: str
    port: int
    runtime_mode: RuntimeMode
    issuer: str
    db_path: str
    event_bus_mode: EventBusMode
    sqs_queue_url: str | None
    aws_region: str | None
    #: JWKS endpoint and audience for the signature-verifying auth adapter. Both
    #: are None on a local run, where the header adapter is the intended default.
    #: Non-locally, `build_container()` refuses to start without them rather than
    #: falling back to unverified headers.
    auth_jwks_uri: str | None = None
    auth_audience: str | None = None
    #: Which persistence adapter `build_container()` wires. `sqlite` is the local
    #: default and needs no server; `mysql` is what every deployed service in this
    #: estate actually runs (MySQL 8.0.46, see `docker-compose.yml`).
    db_backend: DbBackend = "sqlite"
    db_host: str | None = None
    db_port: int | None = None
    db_name: str | None = None
    db_user: str | None = None
    #: The NAME of the secret holding the database password -- never the password.
    #: `build_container()` resolves it through a `SecretsPort`, so the credential
    #: never sits on a config object that a log line or a traceback can print.
    db_password_secret_id: str | None = None
    #: The PHYSICAL table prefix this deployment's workflow tables carry. Every
    #: check over those names must derive from this ONE value: `build_tables`
    #: names the tables with it and `owned_tables()` names the write-guard's set
    #: with it. A second literal written at the default is the defect this rule
    #: exists to prevent -- a host on `app_` created `app_jobs` and was then
    #: refused it by a guard comparing against `wf_`.
    workflow_table_prefix: str = DEFAULT_TABLE_PREFIX
    #: Path to the JSON workflow definition this deployment runs. No default and
    #: no built-in workflow: a service that invented one would accept jobs it can
    #: never advance, and every command would be refused as a digest mismatch
    #: pointing at the caller instead of at the missing file.
    workflow_definition_path: str | None = None
    #: How long a stage run has before `EXPIRE` may be accepted against it.
    workflow_job_deadline_seconds: int = 3600

    def __post_init__(self) -> None:
        if not self.env:
            raise ValueError("ServiceConfig.env must be non-empty")
        if self.port <= 0:
            raise ValueError("ServiceConfig.port must be positive")
        if self.event_bus_mode == "sqs" and not self.sqs_queue_url:
            raise ValueError("event_bus_mode='sqs' requires sqs_queue_url")
        if self.workflow_job_deadline_seconds <= 0:
            raise ValueError("workflow_job_deadline_seconds must be positive")
        if self.db_backend == "mysql":
            # Enumerated and checked HERE rather than at first query: a service
            # missing its database name should die at startup, not on the first
            # request that reaches persistence.
            missing = [
                field
                for field, value in (
                    ("db_host", self.db_host),
                    ("db_port", self.db_port),
                    ("db_name", self.db_name),
                    ("db_user", self.db_user),
                    ("db_password_secret_id", self.db_password_secret_id),
                )
                if not value
            ]
            if missing:
                raise ValueError(f"db_backend='mysql' requires {missing}")

    @classmethod
    def from_environment(cls) -> ServiceConfig:
        """Read `ServiceConfig` from `os.environ`.

        AWS Lambda sets `AWS_LAMBDA_FUNCTION_NAME` on every real invocation
        environment; the presence of that variable -- not a guess about the
        caller's intent -- is what selects `runtime_mode="lambda"`. Everything
        else defaults to values safe for local development.
        """
        runtime_mode: RuntimeMode
        if os.environ.get("AWS_LAMBDA_FUNCTION_NAME") or os.environ.get("RUNTIME_MODE") == "lambda":
            runtime_mode = "lambda"
        else:
            runtime_mode = "http"

        return cls(
            env=os.environ.get("SERVICE_ENV", "local"),
            port=int(os.environ.get("PORT", "8080")),
            runtime_mode=runtime_mode,
            issuer=os.environ.get("PRINCIPAL_ISSUER", "foundry-workflow"),
            db_path=os.environ.get("DB_PATH", "workflow.db"),
            event_bus_mode=os.environ.get("EVENT_BUS_MODE", "in_process"),  # type: ignore[arg-type]
            sqs_queue_url=os.environ.get("SQS_QUEUE_URL"),
            aws_region=os.environ.get("AWS_REGION"),
            auth_jwks_uri=os.environ.get("AUTH_JWKS_URI"),
            auth_audience=os.environ.get("AUTH_AUDIENCE"),
            db_backend=os.environ.get("DB_BACKEND", "sqlite"),  # type: ignore[arg-type]
            db_host=os.environ.get("DB_HOST"),
            db_port=int(os.environ["DB_PORT"]) if os.environ.get("DB_PORT") else None,
            db_name=os.environ.get("DB_NAME"),
            db_user=os.environ.get("DB_USER"),
            # The secret's NAME, deliberately. There is no DB_PASSWORD read here
            # and there must never be one: a password in the process environment
            # is readable by every child process and printed by every crash dump
            # that dumps the environment.
            db_password_secret_id=os.environ.get("DB_PASSWORD_SECRET_ID"),
            workflow_table_prefix=os.environ.get("WORKFLOW_TABLE_PREFIX", DEFAULT_TABLE_PREFIX),
            workflow_definition_path=os.environ.get("WORKFLOW_DEFINITION_PATH"),
            workflow_job_deadline_seconds=int(
                os.environ.get("WORKFLOW_JOB_DEADLINE_SECONDS", "3600")
            ),
        )
