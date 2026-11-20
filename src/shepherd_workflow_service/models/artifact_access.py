"""Tenant-specific authorization for canonical S3 artifact references."""

import re
from collections.abc import Iterable, Mapping

from shepherd_workflow_service.models.tenancy import validate_tenant_id

_BUCKET = r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]"
_SEGMENT = r"[a-zA-Z0-9._~-]+"
_URI = re.compile(rf"s3://({_BUCKET})/({_SEGMENT}(?:/{_SEGMENT})*/?)", re.ASCII)


def _parse(ref: str) -> tuple[str, str] | None:
    match = _URI.fullmatch(ref) if isinstance(ref, str) else None
    if match is None:
        return None
    bucket, key = match.groups()
    if (
        ".." in bucket
        or ".-" in bucket
        or "-." in bucket
        or re.fullmatch(r"\d+(?:\.\d+){3}", bucket)
        or any(part in {".", ".."} for part in key.rstrip("/").split("/"))
    ):
        return None
    return bucket, key


class ArtifactAccess:
    def __init__(self, prefixes: Mapping[str, Iterable[str]]) -> None:
        self._prefixes: dict[str, tuple[tuple[str, str], ...]] = {}
        owners_by_bucket: dict[str, list[tuple[str, str]]] = {}
        for tenant, refs in prefixes.items():
            validate_tenant_id(tenant)
            if isinstance(refs, str):
                raise ValueError("artifact prefixes must be a collection")
            parsed = []
            for ref in refs:
                location = _parse(ref)
                if location is None or not location[1].endswith("/"):
                    raise ValueError("invalid artifact storage prefix")
                bucket, prefix = location
                owners = owners_by_bucket.setdefault(bucket, [])
                if any(
                    owner != tenant and (prefix.startswith(existing) or existing.startswith(prefix))
                    for existing, owner in owners
                ):
                    raise ValueError("artifact storage prefixes overlap across tenants")
                owners.append((prefix, tenant))
                parsed.append(location)
            self._prefixes[tenant] = tuple(parsed)

    def allows(self, tenant_id: str, ref: str) -> bool:
        location = _parse(ref)
        if location is None or location[1].endswith("/"):
            return False
        bucket, key = location
        return any(
            bucket == registered_bucket and key.startswith(prefix)
            for registered_bucket, prefix in self._prefixes.get(tenant_id, ())
        )
