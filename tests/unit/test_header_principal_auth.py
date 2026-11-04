from __future__ import annotations

import pytest
from shepherd_contracts.errors import Unauthorized
from shepherd_contracts.principal import PrincipalMinter

from shepherd_workflow_service.adapters.fake.header_principal_auth import HeaderPrincipalAuth


def _auth() -> HeaderPrincipalAuth:
    return HeaderPrincipalAuth(PrincipalMinter(issuer="test-issuer"))


def test_mints_principal_from_headers() -> None:
    principal = _auth().authenticate(
        {"x-org-id": "org_1", "x-subject-id": "user_1", "x-scopes": "jobs:read, jobs:write"}
    )

    assert principal.org_id == "org_1"
    assert principal.subject_id == "user_1"
    assert principal.scopes == frozenset({"jobs:read", "jobs:write"})
    assert principal.issued_by == "test-issuer"


def test_missing_org_header_is_unauthorized() -> None:
    with pytest.raises(Unauthorized):
        _auth().authenticate({"x-subject-id": "user_1"})


def test_missing_subject_header_is_unauthorized() -> None:
    with pytest.raises(Unauthorized):
        _auth().authenticate({"x-org-id": "org_1"})


def test_missing_scopes_header_yields_no_scopes() -> None:
    principal = _auth().authenticate({"x-org-id": "org_1", "x-subject-id": "user_1"})

    assert principal.scopes == frozenset()
