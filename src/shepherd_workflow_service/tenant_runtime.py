"""Composition helpers for the tenant-only ECS execution profile."""

import json
from base64 import b64decode
from binascii import Error
from threading import Lock

from pydantic import Field, TypeAdapter
from shepherd_contracts.errors import ConfigurationError

from shepherd_workflow_service.adapters.real.signed_webhook import (
    PermanentDeliveryFailure,
    SignedWebhookDestination,
)
from shepherd_workflow_service.adapters.real.tenant_outbox import TenantOutboxDelivery
from shepherd_workflow_service.models.artifact_access import ArtifactAccess
from shepherd_workflow_service.models.definition import Contract, Name
from shepherd_workflow_service.models.tenancy import reject_duplicate_json_keys
from shepherd_workflow_service.ports.secrets import SecretNotFound


class CallbackRegistration(Contract):
    tenant_id: Name
    destination_id: Name
    version: Name
    event_types: tuple[Name, ...] = Field(min_length=1)
    url: str
    key_id: Name
    secret_id: str = Field(min_length=1, max_length=512)


def build_artifact_access(config, registry):
    raw = config.tenant_artifact_prefixes_json
    try:
        json.loads(raw, object_pairs_hook=reject_duplicate_json_keys)
        prefixes = TypeAdapter(dict[str, list[str]]).validate_json(raw, strict=True)
        if prefixes.keys() - registry.tenant_ids:
            raise ValueError("artifact prefixes refer to an unknown tenant")
        return ArtifactAccess(prefixes)
    except ValueError as exc:
        raise ConfigurationError("Invalid tenant artifact registrations") from exc


def _key(secrets, secret_id):
    try:
        value = b64decode(secrets.get(secret_id), validate=True)
    except (Error, ValueError):
        raise ConfigurationError("Invalid tenant webhook signing secret") from None
    if len(value) < 32:
        raise ConfigurationError("Invalid tenant webhook signing secret")
    return value


def _parse_callbacks(raw, registry):
    try:
        json.loads(raw, object_pairs_hook=reject_duplicate_json_keys)
        registrations = TypeAdapter(list[CallbackRegistration]).validate_json(raw)
        owners_by_secret: dict[str, str] = {}
        destinations = {}
        for item in registrations:
            key = item.tenant_id, item.destination_id
            if item.tenant_id not in registry.tenant_ids or key in destinations:
                raise ValueError("unknown tenant or duplicate callback destination")
            if owners_by_secret.setdefault(item.secret_id, item.tenant_id) != item.tenant_id:
                raise ValueError("webhook signing secrets cannot be shared across tenants")
            destinations[key] = item
    except ValueError as exc:
        raise ConfigurationError("Invalid tenant callback registrations") from exc
    return destinations


def _callback_reader(config, secrets, registry):
    inline = _parse_callbacks(config.tenant_callback_destinations_json, registry)
    name = config.tenant_callback_destinations_secret_id
    if inline and (name or config.env not in {"local", "dev", "test"}):
        raise ConfigurationError("Tenant callbacks require a live callback registration secret")
    if (inline or name) and secrets is None:
        raise ConfigurationError("Tenant callbacks require a fresh secrets provider")
    if name:
        return lambda: _parse_callbacks(secrets.get(name), registry)
    return lambda: inline


class _CallbackSnapshot:
    """Pin routing at startup and recheck every send through a fresh secrets reader."""

    def __init__(self, read, secrets):
        self._read = read
        self._secrets = secrets
        self.registrations = read()
        self._revoked = set()
        self._lock = Lock()

    def key_for(self, identity):
        with self._lock:
            if identity in self._revoked:
                raise PermanentDeliveryFailure("destination_inactive")
            try:
                current = self._read()
            except (ConfigurationError, SecretNotFound):
                # Leave the delivery pending for a bounded retry. The startup
                # snapshot alone cannot authorize a send.
                raise RuntimeError("callback policy unavailable") from None
            self._revoked.update(
                key for key, spec in self.registrations.items() if current.get(key) != spec
            )
            if identity in self._revoked:
                raise PermanentDeliveryFailure("destination_inactive")
            return _key(self._secrets, self.registrations[identity].secret_id)


def _destinations(read, clock, secrets):
    snapshot = _CallbackSnapshot(read, secrets)
    return tuple(
        SignedWebhookDestination(
            tenant_id=spec.tenant_id,
            destination_id=spec.destination_id,
            version=spec.version,
            event_types=spec.event_types,
            url=spec.url,
            key_id=spec.key_id,
            secret_provider=lambda key=key: snapshot.key_for(key),
            credential_id=spec.secret_id,
            clock=clock,
        )
        for key, spec in snapshot.registrations.items()
    )


def build_tenant_delivery(config, *, engine, repository, broker_factory, secrets, clock, registry):
    read_callbacks = _callback_reader(config, secrets, registry)
    lock = Lock()
    delivery = None

    def get_delivery():
        nonlocal delivery
        with lock:
            if delivery is None:
                repository.check_ready()
                destinations = _destinations(read_callbacks, clock, secrets)
                delivery = TenantOutboxDelivery(
                    engine=engine,
                    clock=clock,
                    producer=config.issuer,
                    private_broker=broker_factory(),
                    destinations=destinations,
                    bindings=repository.bindings,
                    authorize_ref=repository.authorize_artifact,
                )
        return delivery

    return get_delivery
