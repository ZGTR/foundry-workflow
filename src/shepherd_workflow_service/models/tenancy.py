"""Operator-owned workload identity and immutable workflow grants."""

from __future__ import annotations

import base64
import json
import re
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit

from pydantic import Field, field_validator, model_validator
from shepherd_contracts.principal import CallerPrincipalV1

from shepherd_workflow_service.models.definition import Contract, Name

TenantScope = Literal["workflow:submit", "workflow:read", "workflow:complete"]
_TENANT_ID = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?", re.ASCII)
_IDENTIFIER = re.compile(r"[\x21-\x7e]{1,255}", re.ASCII)
_THUMBPRINT = re.compile(r"[A-Za-z0-9_-]{43}", re.ASCII)
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


def validate_tenant_id(value: str) -> str:
    if not isinstance(value, str) or not _TENANT_ID.fullmatch(value):
        raise ValueError("tenant ID must be 1–64 lowercase ASCII letters, digits or interior '-'")
    return value


def validate_identifier(value: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError("invalid workload identifier")
    return value


def validate_https_url(value: str) -> str:
    validate_identifier(value)
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
        or parsed.password is not None or parsed.query or parsed.fragment
        or not value.startswith("https://") or "\\" in value
    ):
        raise ValueError("expected a pinned HTTPS URL")
    _ = parsed.port
    return value


def validate_thumbprint(value: str) -> str:
    if not isinstance(value, str) or not _THUMBPRINT.fullmatch(value):
        raise ValueError("invalid SHA-256 certificate thumbprint")
    decoded = base64.urlsafe_b64decode(value + "=")
    if base64.urlsafe_b64encode(decoded).decode().rstrip("=") != value:
        raise ValueError("noncanonical SHA-256 certificate thumbprint")
    return value


def is_tenant_principal(principal: CallerPrincipalV1) -> bool:
    return principal.attributes.get("principal_class") == "tenant"


class TenantWorkload(Contract):
    issuer: str
    client_id: str
    subject: str
    tenant_id: str
    producer: Name
    scopes: tuple[TenantScope, ...] = Field(min_length=1)
    certificate_thumbprints: tuple[str, ...] = Field(min_length=1)
    active: bool

    _issuer = field_validator("issuer")(validate_https_url)
    _identity = field_validator("client_id", "subject", "producer")(validate_identifier)
    _tenant = field_validator("tenant_id")(validate_tenant_id)

    @field_validator("certificate_thumbprints")
    @classmethod
    def validate_certificates(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        for thumbprint in values:
            validate_thumbprint(thumbprint)
        return values

    @model_validator(mode="after")
    def unique_values(self) -> TenantWorkload:
        if len(set(self.scopes)) != len(self.scopes):
            raise ValueError("duplicate workload scopes")
        if len(set(self.certificate_thumbprints)) != len(self.certificate_thumbprints):
            raise ValueError("duplicate workload certificates")
        return self


class TriggerGrant(Contract):
    tenant_id: str
    producer: Name
    event_type: Name
    workflow_digest: Digest

    _tenant = field_validator("tenant_id")(validate_tenant_id)
    _names = field_validator("producer", "event_type")(validate_identifier)


class ExecutorGrant(Contract):
    tenant_id: str
    issuer: str
    client_id: str
    workflow_digest: Digest
    stage: Name

    _tenant = field_validator("tenant_id")(validate_tenant_id)
    _issuer = field_validator("issuer")(validate_https_url)
    _names = field_validator("client_id", "stage")(validate_identifier)


def reject_duplicate_json_keys(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result = dict(items)
    if len(result) != len(items):
        raise ValueError("duplicate JSON member")
    return result


class TenantRegistry(Contract):
    workloads: tuple[TenantWorkload, ...] = Field(min_length=1)
    triggers: tuple[TriggerGrant, ...] = ()
    executors: tuple[ExecutorGrant, ...] = ()

    @classmethod
    def from_json(cls, content: str) -> TenantRegistry:
        json.loads(content, object_pairs_hook=reject_duplicate_json_keys)
        return cls.model_validate_json(content)

    @model_validator(mode="after")
    def validate_grants(self) -> TenantRegistry:
        clients = {(item.issuer, item.client_id): item for item in self.workloads}
        submitters = {
            (item.tenant_id, item.producer)
            for item in self.workloads if "workflow:submit" in item.scopes
        }
        certificates = [cert for item in self.workloads for cert in item.certificate_thumbprints]
        if len(clients) != len(self.workloads) or len(set(certificates)) != len(certificates):
            raise ValueError("duplicate workload or shared workload certificate")
        triggers = {
            (grant.tenant_id, grant.producer, grant.event_type, grant.workflow_digest)
            for grant in self.triggers
        }
        executors = {
            (grant.issuer, grant.client_id, grant.workflow_digest, grant.stage)
            for grant in self.executors
        }
        if len(triggers) != len(self.triggers) or len(executors) != len(self.executors):
            raise ValueError("duplicate workflow grant")
        for grant in self.triggers:
            if (grant.tenant_id, grant.producer) not in submitters:
                raise ValueError("trigger grant refers to a workload without submit scope")
        for grant in self.executors:
            workload = clients.get((grant.issuer, grant.client_id))
            if (
                not workload or workload.tenant_id != grant.tenant_id
                or "workflow:complete" not in workload.scopes
            ):
                raise ValueError("executor grant refers to a workload without complete scope")
        return self

    @property
    def tenant_ids(self) -> frozenset[str]:
        return frozenset(item.tenant_id for item in self.workloads)

    def workload(self, issuer: str, client_id: str) -> TenantWorkload | None:
        return next(
            (
                item for item in self.workloads
                if (item.issuer, item.client_id) == (issuer, client_id)
            ),
            None,
        )

    def active_workload(self, principal: CallerPrincipalV1) -> TenantWorkload | None:
        if not is_tenant_principal(principal):
            return None
        workload = self.workload(
            principal.attributes.get("token_issuer", ""), principal.attributes.get("client_id", "")
        )
        if (
            workload and workload.active and workload.tenant_id == principal.org_id
            and workload.producer == principal.subject_id
            and workload.subject == principal.attributes.get("subject")
        ):
            return workload
        return None

    def allows_trigger(
        self, principal: CallerPrincipalV1, *, event_type: str, workflow_digest: str
    ) -> bool:
        workload = self.active_workload(principal)
        return bool(
            workload and "workflow:submit" in workload.scopes and any(
                (grant.tenant_id, grant.producer, grant.event_type, grant.workflow_digest)
                == (workload.tenant_id, workload.producer, event_type, workflow_digest)
                for grant in self.triggers
            )
        )

    def allows_executor(
        self, principal: CallerPrincipalV1, *, workflow_digest: str, stage: str
    ) -> bool:
        workload = self.active_workload(principal)
        return bool(
            workload and "workflow:complete" in workload.scopes and any(
                (grant.issuer, grant.client_id, grant.workflow_digest, grant.stage)
                == (workload.issuer, workload.client_id, workflow_digest, stage)
                for grant in self.executors
            )
        )
