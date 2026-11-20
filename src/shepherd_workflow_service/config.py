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
from collections.abc import Callable
from dataclasses import dataclass
from ipaddress import IPv4Network, IPv6Network, ip_network
from pathlib import Path
from typing import Literal

from shepherd_contracts.errors import ConfigurationError
from shepherd_workflow.ports import DEFAULT_TABLE_PREFIX

from shepherd_workflow_service.models.tenancy import (
    TenantRegistry,
    validate_https_url,
    validate_identifier,
)

RuntimeMode = Literal["http", "lambda"]
EventBusMode = Literal["in_process", "sqs", "outbox"]
DbBackend = Literal["sqlite", "mysql"]
ProxyNetwork = IPv4Network | IPv6Network
_PRIVATE_NETWORKS = tuple(map(ip_network, ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
                                           "fc00::/7")))


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

    workflow_catalog_path: str | None = None
    workflow_event_bus: str | None = None
    workflow_ingress_queue_url: str | None = None
    workflow_event_tenants: tuple[str, ...] = ()
    auth_issuer: str | None = None
    tenant_http_profile: bool = False
    tenant_registry_json: str | None = None
    tenant_registry_path: str | None = None
    tenant_registry_secret_id: str | None = None
    tenant_proxy_cidrs: tuple[str, ...] = ()
    tenant_token_profile: Literal["rfc9068", "cognito"] = "rfc9068"
    tenant_tls_secret_id: str | None = None
    tenant_pool_capacity: int = 16
    tenant_queue_age_seconds: int = 60
    tenant_artifact_prefixes_json: str = "{}"
    tenant_callback_destinations_json: str = "[]"
    tenant_callback_destinations_secret_id: str | None = None

    def __post_init__(self) -> None:
        if not self.env:
            raise ValueError("ServiceConfig.env must be non-empty")
        if self.port <= 0:
            raise ValueError("ServiceConfig.port must be positive")
        if self.event_bus_mode == "sqs" and not self.sqs_queue_url:
            raise ValueError("event_bus_mode='sqs' requires sqs_queue_url")
        if self.workflow_job_deadline_seconds <= 0:
            raise ValueError("workflow_job_deadline_seconds must be positive")
        if type(self.tenant_pool_capacity) is not int or self.tenant_pool_capacity <= 0:
            raise ValueError("tenant_pool_capacity must be a positive integer")
        if (
            type(self.tenant_queue_age_seconds) is not int
            or not 1 <= self.tenant_queue_age_seconds <= 604800
        ):
            raise ValueError("tenant_queue_age_seconds must be between 1 and 604800")
        if self.tenant_token_profile not in ("rfc9068", "cognito"):
            raise ValueError("invalid TENANT_TOKEN_PROFILE")
        if sum(source is not None for source in (
            self.tenant_registry_json, self.tenant_registry_path, self.tenant_registry_secret_id,
        )) > 1:
            raise ValueError("set only one tenant registry source")
        if type(self.tenant_http_profile) is not bool:
            raise ValueError("tenant_http_profile must be boolean")
        if self.tenant_http_profile:
            self._validate_tenant_profile()
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

    def _validate_tenant_profile(self) -> None:
        if self.runtime_mode != "http" or not self.workflow_catalog_path:
            raise ValueError("tenant HTTP profile requires HTTP runtime and WORKFLOW_CATALOG_PATH")
        if not (
            self.tenant_registry_json or self.tenant_registry_path or self.tenant_registry_secret_id
        ):
            raise ValueError("tenant HTTP profile requires workload registry configuration")
        if not self.auth_issuer or not self.auth_jwks_uri or not self.auth_audience:
            raise ValueError(
                "tenant HTTP profile requires AUTH_ISSUER, AUTH_JWKS_URI and AUTH_AUDIENCE"
            )
        validate_https_url(self.auth_issuer)
        validate_https_url(self.auth_jwks_uri)
        validate_identifier(self.auth_audience)
        if self.auth_issuer == self.issuer:
            raise ValueError("AUTH_ISSUER must differ from PRINCIPAL_ISSUER in tenant HTTP profile")
        self.tenant_proxy_networks()

    def tenant_proxy_networks(self) -> tuple[ProxyNetwork, ...]:
        networks = tuple(ip_network(value, strict=True) for value in self.tenant_proxy_cidrs)
        if not networks or any(
            not any(n.version == allowed.version and n.subnet_of(allowed)
                    for allowed in _PRIVATE_NETWORKS)
            for n in networks
        ):
            raise ValueError("TENANT_PROXY_CIDRS requires explicit private proxy networks")
        return networks

    def load_tenant_registry(
        self, *, read_secret: Callable[[str], str] | None = None
    ) -> TenantRegistry | None:
        if all(source is None for source in (
            self.tenant_registry_json, self.tenant_registry_path, self.tenant_registry_secret_id,
        )):
            return None
        try:
            content = self.tenant_registry_json
            if self.tenant_registry_path is not None:
                content = Path(self.tenant_registry_path).read_text(encoding="utf-8")
            if self.tenant_registry_secret_id is not None:
                if read_secret is None:
                    raise ConfigurationError("tenant workload registry has no secret provider")
                content = read_secret(self.tenant_registry_secret_id)
            registry = TenantRegistry.from_json(content or "")
        except (ValueError, OSError, RuntimeError):
            raise ConfigurationError("tenant workload registry could not be loaded") from None
        if self.tenant_http_profile and any(
            item.issuer != self.auth_issuer for item in registry.workloads
        ):
            raise ConfigurationError("tenant workload registry contains an unconfigured issuer")
        if registry.tenant_ids.intersection(self.workflow_event_tenants):
            raise ConfigurationError("HTTPS tenant cannot be enabled for legacy event consumption")
        return registry

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
        tenant_http_profile = os.environ.get("TENANT_HTTP_PROFILE", "false")
        if tenant_http_profile not in ("true", "false"):
            raise ValueError("TENANT_HTTP_PROFILE must be 'true' or 'false'")

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
            auth_issuer=os.environ.get("AUTH_ISSUER"),
            tenant_http_profile=tenant_http_profile == "true",
            tenant_registry_json=os.environ.get("TENANT_REGISTRY_JSON"),
            tenant_registry_path=os.environ.get("TENANT_REGISTRY_PATH"),
            tenant_registry_secret_id=os.environ.get("TENANT_REGISTRY_SECRET_ID"),
            tenant_proxy_cidrs=tuple(os.environ["TENANT_PROXY_CIDRS"].split(","))
            if "TENANT_PROXY_CIDRS" in os.environ else (),
            tenant_token_profile=os.environ.get("TENANT_TOKEN_PROFILE", "rfc9068"),
            tenant_tls_secret_id=os.environ.get("TENANT_TLS_SECRET_ID"),
            tenant_pool_capacity=int(os.environ.get("TENANT_POOL_CAPACITY", "16")),
            tenant_queue_age_seconds=int(os.environ.get("TENANT_QUEUE_AGE_SECONDS", "60")),
            tenant_artifact_prefixes_json=os.environ.get("TENANT_ARTIFACT_PREFIXES_JSON", "{}"),
            tenant_callback_destinations_json=os.environ.get(
                "TENANT_CALLBACK_DESTINATIONS_JSON", "[]"
            ),
            tenant_callback_destinations_secret_id=os.environ.get(
                "TENANT_CALLBACK_DESTINATIONS_SECRET_ID"
            ),
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
            workflow_catalog_path=os.environ.get("WORKFLOW_CATALOG_PATH"),
            workflow_event_bus=os.environ.get("WORKFLOW_EVENT_BUS"),
            workflow_ingress_queue_url=os.environ.get("WORKFLOW_INGRESS_QUEUE_URL"),
            workflow_event_tenants=tuple(filter(None,
                os.environ.get("WORKFLOW_EVENT_TENANTS", "").split(","))),
            workflow_job_deadline_seconds=int(
                os.environ.get("WORKFLOW_JOB_DEADLINE_SECONDS", "3600")
            ),
        )
