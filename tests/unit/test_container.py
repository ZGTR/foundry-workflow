from __future__ import annotations

import socket
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from shepherd_contracts.clock import FrozenClock
from shepherd_contracts.errors import ConfigurationError
from shepherd_contracts.principal import PrincipalMinter
from shepherd_workflow.sql.schema import build_tables
from sqlalchemy import MetaData, create_engine, select

from shepherd_workflow_service import container as container_module
from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import build_container
from tests.conftest import WORKFLOW_DEFINITION_PATH


def _config(**overrides: object) -> ServiceConfig:
    fields = dict(
        env="test",
        port=8080,
        runtime_mode="http",
        issuer="test-issuer",
        db_path=":memory:",
        event_bus_mode="in_process",
        sqs_queue_url=None,
        aws_region=None,
        workflow_definition_path=WORKFLOW_DEFINITION_PATH,
    )
    fields.update(overrides)
    return ServiceConfig(**fields)


def test_container_builds_from_config_alone() -> None:
    container = build_container(_config())

    assert container.config.env == "test"
    assert container.workflow_service is not None
    # `readiness_check` is the real `EngineReadinessCheck` here, so this line is
    # a round trip against a real (in-memory) ledger, not a fake answering True.
    assert container.readiness_check.check() is True


def test_container_builds_with_zero_network_access(monkeypatch: pytest.MonkeyPatch) -> None:
    def _no_sockets(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("build_container() must not open a network socket")

    monkeypatch.setattr(socket, "socket", _no_sockets)

    container = build_container(_config())

    assert container is not None


def _mysql_config(**overrides: object) -> ServiceConfig:
    fields: dict[str, object] = dict(
        db_backend="mysql",
        db_host="db.internal",
        db_port=3306,
        db_name="workflow",
        db_user="svc",
        db_password_secret_id="workflow/db/password",
        aws_region="eu-west-1",
    )
    fields.update(overrides)
    return _config(**fields)


def test_mysql_builds_without_an_injected_secrets_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    """The bug, from the entrypoints' side.

    `main.py` and `ledger_drain_main.py` both call `build_container(config)` with no
    `secrets=`, so before the fix every deployment on DB_BACKEND=mysql --
    which is every deployment in this estate -- raised ConfigurationError at startup.
    This drives the exact call both entrypoints make, not a call shape only a test uses.
    """

    def _no_sockets(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("build_container() must not open a network socket")

    monkeypatch.setattr(socket, "socket", _no_sockets)

    container = build_container(_mysql_config())

    assert container is not None


def test_the_default_secrets_adapter_reaches_the_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """That the adapter is CONSTRUCTED proves nothing about it being USED.

    A container that builds a SecretsPort and then resolves its URL from somewhere else
    passes the test above while leaving the defect in place, so this follows the value
    all the way into the URL string: the password below can only appear there if the
    default adapter was constructed AND consulted by `_database_url`.
    """
    import shepherd_workflow_service.adapters.real.aws_secrets_manager as aws_module

    constructed: list[str] = []

    class RecordingSecrets:
        def __init__(self, *, region: str, client: object = None) -> None:
            constructed.append(region)

        def get(self, name: str) -> str:
            assert name == "workflow/db/password"
            return "p@ss word/with+specials"

    monkeypatch.setattr(aws_module, "AwsSecretsManagerSecrets", RecordingSecrets)

    config = _mysql_config()
    url = container_module._database_url(config, container_module._default_secrets(config))

    assert constructed == ["eu-west-1"]
    # quote_plus, so the password never appears raw and the URL stays parseable.
    assert "p%40ss+word%2Fwith%2Bspecials" in url
    assert "mysql+pymysql://svc:" in url
    assert "@db.internal:3306/workflow" in url


def test_mysql_without_a_region_refuses_with_an_actionable_message() -> None:
    """The refusal must name the fix a DEPLOYMENT can apply.

    Its previous wording said only "pass secrets=<adapter> to build_container()", which
    an ECS task or a Lambda has no call site to do. Naming AWS_REGION is the difference
    between a message that can be acted on and one that can only be read.
    """
    with pytest.raises(ConfigurationError) as raised:
        build_container(_mysql_config(aws_region=None))

    message = str(raised.value)
    assert "AWS_REGION" in message
    assert "workflow/db/password" in message


def test_an_injected_secrets_adapter_still_wins_over_the_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default must not SHADOW an explicit argument.

    This assertion has to go through `build_container`, and the first version of it did
    not -- it called `_database_url(config, StubSecrets())` and asserted the stub's value
    came back, which only proves an argument is an argument. Swapping the precedence to
    `_default_secrets(config) or secrets` left the whole 187-test suite GREEN, so the
    test named a caller-level property and could not observe it: exactly the
    "a unit test cannot see its own caller" shape this estate has a wiki note about.

    Observing it means asking whether the REAL adapter was constructed at all. With one
    injected, `secrets or _default_secrets(config)` short-circuits and never builds it;
    under the swapped precedence it is built and wins.
    """
    import shepherd_workflow_service.adapters.real.aws_secrets_manager as aws_module

    constructed: list[str] = []

    class ShouldNotBeBuilt:
        def __init__(self, *, region: str, client: object = None) -> None:
            constructed.append(region)

        def get(self, name: str) -> str:  # pragma: no cover -- reaching this is the bug
            return "default-adapter-value"

    monkeypatch.setattr(aws_module, "AwsSecretsManagerSecrets", ShouldNotBeBuilt)

    class StubSecrets:
        def get(self, name: str) -> str:
            return "injected"

    build_container(_mysql_config(), secrets=StubSecrets())

    assert constructed == [], (
        "build_container() built the default SecretsPort even though one was injected; "
        "the default is shadowing its own parameter"
    )


def test_a_fake_clock_reaches_the_deployed_engine(tmp_path: Path) -> None:
    """An injected clock is only proved to have arrived if a STORED value depends
    on it. The ledger's clock-derived column is `deadline_at`, which no view
    exposes, so this reads the row the engine actually wrote rather than asking
    the container which clock it is holding -- comparing `container.clock` would
    pass even if nothing downstream ever called it.
    """
    db_path = tmp_path / "ledger.db"
    frozen = FrozenClock(datetime(2020, 1, 1, tzinfo=UTC))
    config = _config(db_path=str(db_path))
    container = build_container(config, clock=frozen)
    principal = PrincipalMinter(issuer="t").mint(
        org_id="org_x", subject_id="user_x", scopes={"jobs:write"}
    )

    view = container.workflow_service.start_job(principal, job_id="job_1", input_digest="d1")

    stage_runs = build_tables(MetaData(), prefix=config.workflow_table_prefix)["stage_runs"]
    with create_engine(f"sqlite+pysqlite:///{db_path}", future=True).connect() as connection:
        deadline = connection.execute(
            select(stage_runs.c.deadline_at).where(stage_runs.c.stage_run_id == view.stage_run_id)
        ).scalar_one()

    expected = frozen.now() + timedelta(seconds=config.workflow_job_deadline_seconds)
    assert deadline.replace(tzinfo=UTC) == expected


def test_every_port_can_be_overridden_independently() -> None:
    from shepherd_workflow_service.adapters.fake.event_bus import RecordingPublisher
    from shepherd_workflow_service.adapters.fake.readiness import FakeReadinessCheck
    from shepherd_workflow_service.adapters.fake.workflow_engine import InMemoryWorkflowEngine
    from shepherd_workflow_service.adapters.real.json_workflow_definition import (
        load_workflow_definition,
    )

    publisher = RecordingPublisher()
    readiness = FakeReadinessCheck(ready=False)
    engine = InMemoryWorkflowEngine(
        workflow=load_workflow_definition(WORKFLOW_DEFINITION_PATH),
        clock=FrozenClock(datetime(2026, 1, 1, tzinfo=UTC)),
        job_deadline=timedelta(hours=1),
    )

    container = build_container(
        _config(),
        publisher=publisher,
        readiness_check=readiness,
        workflow_engine=engine,
    )

    assert container.workflow_engine is engine
    assert container.publisher is publisher
    assert container.readiness_check is readiness
    assert container.readiness_check.check() is False
