"""Bound the age of a tenant workload authorization snapshot."""

from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from time import monotonic

from shepherd_contracts.errors import ConfigurationError

from shepherd_workflow_service.models.tenancy import TenantRegistry

TENANT_REGISTRY_REFRESH_SECONDS = 30


class TenantRegistrySnapshot:
    """A changed registry invalidates this process until its writers restart together."""

    def __init__(
        self,
        snapshot: TenantRegistry,
        *,
        load: Callable[[], TenantRegistry | None],
        refresh_seconds: int,
        time_source: Callable[[], float] = monotonic,
    ) -> None:
        self._snapshot = snapshot
        self._load = load
        self._time = time_source
        self._refresh_seconds = refresh_seconds
        self._lock = Lock()
        self._check_at = self._time() + refresh_seconds
        self._validated = True
        self._changed = False

    def current(self) -> TenantRegistry:
        with self._lock:
            if self._changed:
                raise ConfigurationError("tenant registry changed after startup")
            now = self._time()
            if now >= self._check_at:
                self._validated = False
                self._check_at = now + 1
                if self._load() != self._snapshot:
                    self._changed = True
                    raise ConfigurationError("tenant registry changed after startup")
                self._validated = True
                self._check_at = now + self._refresh_seconds
            if not self._validated:
                raise ConfigurationError("tenant registry could not be refreshed")
            return self._snapshot
