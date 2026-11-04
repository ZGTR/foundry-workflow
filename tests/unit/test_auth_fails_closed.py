"""Both directions of the fail-closed auth default.

The bug this pins was not "the adapter is weak" -- it was that a weak adapter
sat in `adapters/real/` and was wired as the default, so a service inherited
unverified header auth by doing nothing. A test that only asserted the refusal
would leave the other half unproven: a container that refused EVERY environment
would also pass it, and would break every local run.
"""

from __future__ import annotations

import pytest

from shepherd_workflow_service.adapters.fake.header_principal_auth import HeaderPrincipalAuth
from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import (
    LOCAL_ENVS,
    InsecureDefaultAuth,
    build_container,
)
from tests.conftest import WORKFLOW_DEFINITION_PATH


def _config(env: str) -> ServiceConfig:
    return ServiceConfig(
        env=env,
        port=8080,
        runtime_mode="http",
        issuer="test-issuer",
        db_path=":memory:",
        event_bus_mode="in_process",
        sqs_queue_url=None,
        aws_region=None,
        workflow_definition_path=WORKFLOW_DEFINITION_PATH,
    )


@pytest.mark.parametrize("env", sorted(LOCAL_ENVS))
def test_local_envs_still_get_the_header_adapter(env: str) -> None:
    """The QUIET direction. Without this, refusing everything would look correct."""
    container = build_container(_config(env))
    assert isinstance(container.principal_auth, HeaderPrincipalAuth)


@pytest.mark.parametrize("env", ["prod", "production", "staging", "eu-west-1", "brand-new-env"])
def test_non_local_envs_refuse_to_start_without_real_auth(env: str) -> None:
    """The LOUD direction, including an env nobody has enumerated.

    `brand-new-env` is the case that matters: a name added later must be secure
    by default rather than by someone remembering to add it to a deny list.
    """
    with pytest.raises(InsecureDefaultAuth) as excinfo:
        build_container(_config(env))
    message = str(excinfo.value)
    assert env in message
    assert "principal_auth" in message, "the refusal must name the fix, not just complain"


def test_an_explicit_adapter_is_accepted_in_production() -> None:
    """The refusal must be about the DEFAULT, not a blanket ban on starting."""
    supplied = HeaderPrincipalAuth.__new__(HeaderPrincipalAuth)  # any object satisfying the port
    supplied._minter = None  # type: ignore[attr-defined]
    container = build_container(_config("prod"), principal_auth=supplied)
    assert container.principal_auth is supplied


def test_jwks_configuration_selects_the_verifying_adapter_even_in_production() -> None:
    """A correctly-configured service gets real auth without writing an adapter.

    This is what makes the refusal above a configuration error rather than a
    permanent obstacle: the InsecureDefaultAuth path should be unreachable for
    any service that set the two variables.
    """
    pytest.importorskip("jwt", reason="PyJWT not installed")
    from shepherd_workflow_service.adapters.real.jwt_principal_auth import JwtPrincipalAuth

    config = ServiceConfig(
        env="prod",
        port=8080,
        runtime_mode="http",
        issuer="https://cognito-idp.eu-west-1.amazonaws.com/pool",
        db_path=":memory:",
        event_bus_mode="in_process",
        sqs_queue_url=None,
        aws_region="eu-west-1",
        workflow_definition_path=WORKFLOW_DEFINITION_PATH,
        auth_jwks_uri="https://cognito-idp.eu-west-1.amazonaws.com/pool/.well-known/jwks.json",
        auth_audience="example-audience",
    )
    container = build_container(config)
    assert isinstance(container.principal_auth, JwtPrincipalAuth)


def test_half_configured_auth_is_refused_rather_than_silently_downgraded() -> None:
    """One of the two variables set is a misconfiguration, not a local run.

    Falling back to header auth here would be the worst outcome: someone
    intended real auth, typed one variable, and got none of it.
    """
    for jwks, audience in (("https://example.invalid/jwks.json", None), (None, "aud")):
        config = ServiceConfig(
            env="prod",
            port=8080,
            runtime_mode="http",
            issuer="iss",
            db_path=":memory:",
            event_bus_mode="in_process",
            sqs_queue_url=None,
            aws_region=None,
            workflow_definition_path=WORKFLOW_DEFINITION_PATH,
            auth_jwks_uri=jwks,
            auth_audience=audience,
        )
        with pytest.raises(InsecureDefaultAuth):
            build_container(config)
